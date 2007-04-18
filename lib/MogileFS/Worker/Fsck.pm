package MogileFS::Worker::Fsck;

use strict;
use base 'MogileFS::Worker';
use fields (
            'last_stop_check',  # unixtime 'should_stop_running' last called
            );
use MogileFS::Util qw(every error);
use List::Util ();
use Time::HiRes ();

use constant SUCCESS => 0;
use constant TEMPORARY => 1;
use constant PERMANENT => 2;
use constant REPLICATE => 3;

sub watchdog_timeout { 30 }

sub work {
    my $self = shift;

    my $run_count = 0;

    every(5.0, sub {
        my $sleep_set = shift;
        $self->parent_ping;

        # see if we're even enabled for this host.
        return unless $self->should_be_running;

        warn "[fsck] Running...\n";

        # checking doesn't go well if the monitor job hasn't actively started
        # marking things as being available
        unless ($self->monitor_has_run) {
            # only warn on runs after the first.  gives the monitor job some time to work
            # before we throw a message.
            error("waiting for monitor job to complete a cycle before beginning checking")
                if $run_count++ > 0;
            return;
        }

        my $sto       = Mgd::get_store();
        my $max_check = MogileFS::Config->server_setting('fsck_highest_fid_checked') || 0;
        my @fids      = $sto->get_fids_above_id($max_check, 100);

        unless (@fids) {
            warn "[fsck] no fids to check...\n";
            return;
        }

        warn("[fsck] fids=" . $fids[0]->id . " ~ " . $fids[-1]->id . "\n");
        MogileFS::FID->mass_load_devids(@fids);

        my $new_max;
        foreach my $fid (@fids) {
            $self->still_alive;
            last if $self->should_stop_running;
            last unless $self->check_fid($fid);
            $new_max = $fid->id;
        }

        warn "[fsck] new_max = $new_max\n";
        MogileFS::Config->set_server_setting('fsck_highest_fid_checked', $new_max) if $new_max;

        $sleep_set->(0); # don't sleep in next round.
    });
}

# this version is accurate,
sub should_be_running {
    my $self = shift;
    my $fhost = MogileFS::Config->server_setting('fsck_host')
        or return;
    return $fhost eq MogileFS::Config->hostname;
}

# this version is sloppy, optimized for speed.  only checks db every 5 seconds.
sub should_stop_running {
    my $self = shift;
    my $now  = time();
    return 0 if $now < ($self->{last_stop_check} || 0) + 5;
    $self->{last_stop_check} = $now;
    return ! $self->should_be_running;
}

# given a $fid (MogileFS::FID, with pre-populated ->devids data)
# return 0 if reachability problems.
# return 1 if fid was checked (regardless of there being problems or not)
#   if no problems, no action.
#   if problems, log & enqueue fixes
sub check_fid {
    my ($self, $fid) = @_;
    printf("FID %d (len=%d; cldid=%d) is on: %s\n",
           $fid->id,
           $fid->length,
           $fid->classid,
           join(",", $fid->devids),
           );
    return 1;
}

1;

__END__
Old stuff...

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
        my $dfid = MogileFS::DevFID->new($devid, $fid);
        my $path = $dfid->get_url
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

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
