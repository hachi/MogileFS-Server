package MogileFS::Worker::Delete;
# deletes files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw(error);

# we select 1000 but only do a random 100 of them, to allow
# for stateless paralleism
use constant LIMIT => 1000;
use constant PER_BATCH => 100;

# TODO: use LWP and persistent connections to do deletes.  less local ports used.

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub watchdog_timeout { 60 }

sub work {
    my $self = shift;

    my $sleep_for = 0; # we sleep longer and longer until we hit max_sleep
    my $sleep_max = 5; # max sleep when there's nothing to do.

    # wait for one pass of the monitor
    $self->wait_for_monitor;

  PASS:
    while (1) {
        $self->parent_ping;
        $self->validate_dbh;

        # see if we have anything from the parent
        my $start_time = time();
        my $end_time   = $start_time + 5;

        while (1) {
            # report in to parent periodically
            next PASS if time() >= $end_time;

            # call our workers, and have them do things
            #    RETVAL = 0; I think I am done working for now
            #    RETVAL = 1; I have more work to do
            my $tempres = $self->process_tempfiles;
            $self->reenqueue_delayed_deletes;
            my $delres = $self->process_deletes;

            # unless someone did some work, let's sleep
            unless ($tempres || $delres) {
                $sleep_for++ if $sleep_for < $sleep_max;
                sleep $sleep_for;
                next PASS;
            }
            $sleep_for = 0;
        }
    }

}

sub process_tempfiles {
    my $self = shift;
    # also clean the tempfile table
    #mysql> select * from tempfile where createtime < unix_timestamp() - 86400 limit 50;
    #+--------+------------+---------+------+---------+--------+
    #| fid    | createtime | classid | dmid | dkey    | devids |
    #+--------+------------+---------+------+---------+--------+
    #|   3253 | 1149451058 |       1 |    1 | file574 | 1,2    |
    #|   4559 | 1149451156 |       1 |    1 | file83  | 1,2    |
    #|  11024 | 1149451697 |       1 |    1 | file836 | 2,1    |
    #|  19885 | 1149454542 |       1 |    1 | file531 | 1,2    |

    # BUT NOTE:
    #    the fids might exist on one of the devices in devids column if we assigned them those,
    #    they wrote some to one of them, then they died or for wahtever reason didn't create_close
    #    to use, so we shouldn't delete from tempfile before going on a hunt of the missing fid.
    #    perhaps we should just add to the file_on table for both devids, and let the regular delete
    #    process discover via 404 that they're not there.
    # so we should:
    #    select fid, devids from tempfile where createtime < unix_timestamp() - 86400
    #    add file_on rows for both of those,
    #    add fid to fids_to_delete table,
    #    delete from tempfile where fid=?


    # dig up some temporary files to purge
    my $sto = Mgd::get_store();
    my $too_old = int($ENV{T_TEMPFILE_TOO_OLD}) || 3600;
    my $tempfiles = $sto->old_tempfiles($too_old);
    return 0 unless $tempfiles && @$tempfiles;

    # insert the right rows into file_on and file_to_delete and remove the
    # now expunged (or soon to be) rows from tempfile
    my (@devfids, @fidids);
    foreach my $row (@$tempfiles) {
        push @fidids, $row->[0];
        foreach my $devid (split /,/, $row->[1]) {
            push @devfids, MogileFS::DevFID->new($devid, $row->[0]);
        }
    }

    $sto->mass_insert_file_on(@devfids);
    $sto->enqueue_fids_to_delete(@fidids);
    $sto->dbh->do("DELETE FROM tempfile WHERE fid IN (" . join(',', @fidids) . ")");
    return 1;
}

sub process_deletes {
    my $self = shift;

    my $sto = Mgd::get_store();
    my $dbh = $sto->dbh;

    my $delmap = $dbh->selectall_arrayref("SELECT fd.fid, fo.devid ".
                                          "FROM file_to_delete fd ".
                                          "LEFT JOIN file_on fo ON fd.fid=fo.fid ".
                                          "LIMIT " . LIMIT);
    my $count = $delmap ? scalar @$delmap : 0;
    return 0 unless $count;

    my $done = 0;
    foreach my $dm (List::Util::shuffle(@$delmap)) {
        last if ++$done > PER_BATCH;

        $self->still_alive;
        $self->read_from_parent;
        my ($fid, $devid) = @$dm;
        error("deleting fid $fid, on devid $devid...") if $Mgd::DEBUG >= 2;

        my $done_with_fid = sub {
            my $reason = shift;
            $dbh->do("DELETE FROM file_to_delete WHERE fid=?", undef, $fid);
            $sto->condthrow("Failure to delete from file_to_delete for fid=$fid");
        };

        my $done_with_devid = sub {
            my $reason = shift;
            $dbh->do("DELETE FROM file_on WHERE fid=? AND devid=?",
                     undef, $fid, $devid);
            $sto->condthrow("Failure to delete from file_on for $fid/$devid");
            die "Failed to delete from file_on: " . $dbh->errstr if $dbh->err;
        };

        my $reschedule_fid = sub {
            my ($secs, $reason) = (int(shift), shift);
            $dbh->do("INSERT IGNORE INTO file_to_delete_later (fid, delafter) ".
                     "VALUES (?,UNIX_TIMESTAMP()+$secs)", undef,
                     $fid);
            $sto->condthrow("Failure to insert into file_to_delete_later");
            error("delete of fid $fid rescheduled: $reason") if $Mgd::DEBUG >= 2;
            $done_with_fid->("rescheduled");
        };

        # Cases:
        #   devid is null:  doesn't exist anywhere anymore, we're done with this fid.
        #   devid is observed down/readonly: delay for 10 minutes
        #   devid is marked readonly: delay for 2 hours
        #   devid is marked dead or doesn't exist: consider it deleted on this devid.

        # CASE: devid is null, which means we're done deleting all instances.
        unless (defined $devid) {
            $done_with_fid->("no_more_locations");
            next;
        }

        # CASE: devid is marked dead or doesn't exist: consider it deleted on this devid.
        my $dev = MogileFS::Device->of_devid($devid);
        unless ($dev->exists) {
            $done_with_devid->("devid_doesnt_exist");
            next;
        }
        if ($dev->dstate->is_perm_dead) {
            $done_with_devid->("devid_marked_dead");
            next;
        }

        # CASE: devid is observed down/readonly: delay for 10 minutes
        unless ($dev->observed_writeable) {
            $reschedule_fid->(60 * 10, "not_observed_writeable");
            next;
        }

        # CASE: devid is marked readonly/down: delay for 2 hours
        if ($dev->status ne "alive") {
            $reschedule_fid->(60 * 60 * 2, "devid_marked_not_alive");
            next;
        }

        my $dfid = MogileFS::DevFID->new($dev, $fid);
        my $path = $dfid->url;

        # dormando: "There are cases where url can return undefined,
        # Mogile appears to try to replicate to bogus devices
        # sometimes?"
        unless ($path) {
            error("in deleter, url(devid=$devid, fid=$fid) returned nothing");
            next;
        }

        my $urlparts = MogileFS::Util::url_parts($path);

        # hit up the server and delete it
        # TODO: (optimization) use MogileFS->get_observed_state and don't try to delete things known to be down/etc
        my $sock = IO::Socket::INET->new(PeerAddr => $urlparts->[0],
                                         PeerPort => $urlparts->[1],
                                         Timeout => 2);
        unless ($sock) {
            # timeout or something, mark this device as down for now and move on
            $self->broadcast_host_unreachable($dev->hostid);
            $reschedule_fid->(60 * 60 * 2, "no_sock_to_hostid");
            next;
        }

        # send delete request
        error("Sending delete for $path") if $Mgd::DEBUG >= 2;

        $sock->write("DELETE $urlparts->[2] HTTP/1.0\r\n\r\n");
        my $response = <$sock>;
        if ($response =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            if (($1 >= 200 && $1 <= 299) || $1 == 404) {
                # effectively means all went well
                $done_with_devid->("deleted");
            } else {
                # remote file system error?  mark node as down
                my $httpcode = $1;
                error("Error: unlink failure: $path: HTTP code $httpcode");
                $reschedule_fid->(60 * 30, "http_code_$httpcode");
                next;
            }
        } else {
            error("Error: unknown response line deleting $path: $response");
        }
    }

    # as far as we know, we have more work to do
    return 1;
}

sub reenqueue_delayed_deletes {
    my $self = shift;

    my $sto = Mgd::get_store();
    my $dbh = $sto->dbh;

    my @fidids = $sto->fids_to_delete_again
        or return;

    $sto->enqueue_fids_to_delete(@fidids);

    $dbh->do("DELETE FROM file_to_delete_later WHERE fid IN (" .
             join(",", @fidids) . ")");
    $sto->condthrow("reenqueue file_to_delete_later delete");
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
