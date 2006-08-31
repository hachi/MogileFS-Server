package MogileFS::Worker::Checker;
# checks files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw( every error );
use List::Util ();
use LWP::UserAgent;
use POSIX;
use Time::HiRes ();

use constant SUCCESS => 0;
use constant TEMPORARY => 1;
use constant PERMANENT => 2;
use constant REPLICATE => 3;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub is_valid_level {
    my $level = shift;
    return $level =~ /^(?:locations|quick|full|off)$/ ? 1 : 0;
}

sub work {
    my $self = shift;
    my $psock = $self->{psock};

    my %retvals;
    my ($run_count, $total_time, $total_done) = (0, 0, 0);

    every(15.0, sub {
        my $sleep_set = shift;

        $self->parent_ping;

        # see if we're even enabled
        my $setting = Mgd::get_server_setting('fsck_enable');
        return unless defined $setting && $setting ne 'off' && is_valid_level($setting);

        # checking doesn't go well if the monitor job hasn't actively started
        # marking things as being available
        unless ($self->monitor_has_run) {
            # only warn on runs after the first.  gives the monitor job some time to work
            # before we throw a message.
            if ($run_count++ > 0) {
                error("waiting for monitor job to complete a cycle before beginning checking");
            }
            return;
        }

        # get dbh and create table if not exist
        $self->validate_dbh;
        my $dbh = $self->get_dbh or return;
        $dbh->do(qq{
            CREATE TABLE IF NOT EXISTS fsck (
                fid         INT UNSIGNED NOT NULL PRIMARY KEY,
                nextcheck   INT UNSIGNED NOT NULL,
                INDEX (nextcheck)
            )
        });

        # main processing loop
        while (1) {
            # select a group of fids to work on from the fsck table
            my $fids = $dbh->selectcol_arrayref("SELECT fid FROM fsck WHERE nextcheck < UNIX_TIMESTAMP() " .
                                                "ORDER BY nextcheck LIMIT 1000");
            return error("database error: " . $dbh->errstr) if $dbh->err;
            $self->still_alive;

            # if nothing to do, we're done
            last unless $fids && @$fids;

            # iterate randomly
            foreach my $fid (List::Util::shuffle(@$fids)) {
                # try to check this fid
                my $t1 = Time::HiRes::time();
                my $rv = check_fid($dbh, $fid, $setting) || 0;
                my $elapsed = Time::HiRes::time() - $t1;
                $self->still_alive;

                # process the return value to do something
                if ($rv == SUCCESS) {
                    $dbh->do("DELETE FROM fsck WHERE fid = ?", undef, $fid);

                } elsif ($rv == TEMPORARY) {
                    # temporary means - try again in 5-10 minutes
                    $dbh->do("UPDATE fsck SET nextcheck = UNIX_TIMESTAMP() + ? WHERE fid = ?",
                             undef, int((rand()*300)+300), $fid);

                } elsif ($rv == PERMANENT) {
                    # FIXME: should probably do something more than this here?
                    $dbh->do("DELETE FROM fsck WHERE fid = ?", undef, $fid);

                } elsif ($rv == REPLICATE) {
                    # FIXME: use nexttry = 1?  fromdevid should be specified as a known good, too.  flags
                    # should probably be set for something?  not sure yet.
                    $dbh->do("INSERT INTO file_to_replicate (fid, nexttry, fromdevid, failcount, flags) " .
                             "VALUES (?, 1, NULL, 0, 0)", undef, $fid);
                    $dbh->do("DELETE FROM fsck WHERE fid = ?", undef, $fid);
                }

                # store the stats now
                $total_time += $elapsed;
                $total_done++;
                $retvals{$rv}++;

                # dump some stats every 20 fids
                if ($total_done % 20 == 0) {
                    my $avg_time = $total_time / $total_done;
                    my $fids_sec = 1 / $avg_time;
                    error(sprintf('status: done=%d, seconds/fid=%0.2f, fids/second=%0.2f, retvals: %s',
                                  $total_done, $avg_time, $fids_sec, join(', ', map { "$_=$retvals{$_}" } sort keys %retvals)));
                }
            }
        }

        # if we fell out, there are no more fids, so let's grab a chunk and throw them
        # into the database to work on next
        my $highest_fid = Mgd::get_server_setting('fsck_highest_fid_checked') || 0;
        my $total_already = $dbh->selectrow_array('SELECT COUNT(*) FROM fsck WHERE nextcheck < UNIX_TIMESTAMP()') || 0;
        my $limit = 10_000 - $total_already;  # but only up to $limit items

        # now extract some files and re-insert them, but only if we need more
        print "limit=$limit, highest=$highest_fid, total=$total_already\n";
        if ($limit > 0) {
            my $rv = $dbh->do(qq{
                INSERT IGNORE INTO fsck (fid, nextcheck)
                    SELECT fid, 0 FROM file WHERE fid > ? ORDER BY fid LIMIT $limit
            }, undef, $highest_fid);
            return error("database error fetching new rows: " . $dbh->errstr) if $dbh->err;

            # this value will usually be correct, but it could be zero/undef already
            # if another process races us to it and deletes them all.  which is why
            # in the next step, we take the max of this and our old window position
            # plus the number of rows we inserted (which makes the window always make
            # some progress in the case of a race, never resetting forever to zero)
            my $max_fsck_fid = $dbh->selectrow_array('SELECT MAX(fid) FROM fsck');
            my $min_progress = $highest_fid + $rv;

            # the race buster:  (keeps window moving in race described above)
            my $new_max      = $max_fsck_fid > $min_progress ? $max_fsck_fid : $min_progress;

            Mgd::set_server_setting('fsck_highest_fid_checked', $new_max || 0);
            $sleep_set->(0); # don't sleep in next round.
        }
    });
}

# this sub actually does the checking of a fid.  we put it in its own sub so we can
# return from it using the unlock coderef.  always returns a number:
#
#   0 - file is just fine, drop it from the list
#   1 - temporary failure, check again later
#   2 - permanent failure, this file shouldn't get tried again
#   3 - needs replication, we found something not quite right
#
sub check_fid {
    my ($dbh, $fid, $level) = @_;

    # unlocker sub to be used
    my $lockname = "mgfs:fid:$fid:check";
    my $retunlock = sub {
        my $rv = shift()+0;

        # 0 means success, else some sort of failure
        if ($rv) {
            my $msg = shift() || "no error text";
            my $rvtype = {
                1 => 'temporary failure',
                2 => 'permanent failure',
                3 => 'needs replication',
            }->{$rv} || 'unknown error';
            error("check_fid($fid, $level) = $rvtype: $msg");
        }

        $dbh->do("SELECT RELEASE_LOCK(?)", undef, $lockname);
        return $rv;
    };

    # try to get the lock
    my $lock = $dbh->selectrow_array("SELECT GET_LOCK(?, 1)", undef, $lockname);
    return $retunlock->(TEMPORARY, "failed getting lock $lockname") unless $lock;

    # all checks require us to get the file paths
    my $devids = $dbh->selectcol_arrayref('SELECT devid FROM file_on WHERE fid = ?', undef, $fid);
    return $retunlock->(PERMANENT, 'no sources found') unless $devids && @$devids;

    # if it's a simple location check, we're done
    return $retunlock->(SUCCESS) if $level eq 'locations';

    # get the file size from the database, we're going to need it.  note that this could be
    # a 0 size, so we have to watch for defined.
    my $db_size = $dbh->selectrow_array('SELECT length FROM file WHERE fid = ?', undef, $fid);
    return $retunlock->(TEMPORARY, "database does not contain file size") unless defined $db_size;

    # iterate and do HEAD requests to determine some basic information about the file
    my %devs;
    foreach my $devid (@$devids) {
        # setup and do the request.  these failures are total failures in that we expect
        # them to work again later, as it's probably transient and will persist no matter
        # how many paths we try.
        my $path = Mgd::make_http_path($devid, $fid)
            or return $retunlock->(TEMPORARY, 'failure to create HTTP path to file');
        my $ua = LWP::UserAgent->new(timeout => 3)
            or return $retunlock->(TEMPORARY, 'failed to create LWP::UserAgent object');
        my $resp = $ua->head($path);

        # at this point we're going to assume that any error is based on the device alone
        # so we want to store the status and not return
        if ($resp->is_success) {
            # great, check the size against what's in the database
            if ($resp->header('Content-Length') == $db_size) {
                $devs{$devid} = SUCCESS;
            } else {
                $devs{$devid} = PERMANENT;
            }

        } else {
            # easy one, the request failed for some reason, 500 would tend to imply that the
            # mogstored is having issues so we should try again later, whereas a 404 is a
            # total and permanent failure
            error("check_fid($fid, $level): " . $resp->code . " on device $devid");
            if ($resp->code == 404) {
                $devs{$devid} = PERMANENT;
            } else {
                $devs{$devid} = TEMPORARY;
            }
        }
    }

    # at this point, we need to take actions.  if we discovered some PERMANENT failures in
    # a device scan, then we need to take care of those now by removing them.  but DO NOT
    # remove them if that would leave us with no mappings!  ONLY if there is at least one
    # SUCCESS mapping.
    # FIXME: implement

    # if they wanted a quick scan, let's stop here and throw a result based on the contents
    # of the %devs hash.  basically, if any of the devices had issues, then at this point we
    # want to throw a flag saying "please replicate this".  if not, then we tell them that
    # we're successful on this fid.
    if ($level eq 'quick') {
        foreach my $code (values %devs) {
            return $retunlock->(REPLICATE, "permanent failure on one or more devices")
                if $code != SUCCESS;
        }
        return $retunlock->(SUCCESS);
    }

    # full mode not here yet
    return $retunlock->(TEMPORARY, "sorry, $level mode is not implemented yet");
}

sub watchdog_timeout { 30 }

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
