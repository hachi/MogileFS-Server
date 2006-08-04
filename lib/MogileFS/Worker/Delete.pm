package MogileFS::Worker::Delete;
# deletes files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw(error);

use POSIX ":sys_wait_h"; # argument for waitpid
use POSIX;

# we select 1000 but only do a random 100 of them, to allow
# for stateless paralleism
use constant LIMIT => 1000;
use constant PER_BATCH => 100;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub work {
    my $self = shift;

    my $sleep_for = 0; # we sleep longer and longer until we hit max_sleep
    my $sleep_max = 5; # max sleep when there's nothing to do.

  PASS:
    while (1) {
        $self->parent_ping;

        $self->validate_dbh;
        my $dbh = $self->get_dbh;

        # see if we have anything from the parent
        my $start_time = time();
        my $end_time   = $start_time + 5;

        while (1) {
            # report in to parent periodically
            next PASS if time() >= $end_time;

            # call our workers, and have them do things
            #    RETVAL = 0; I think I am done working for now
            #    RETVAL = 1; I have more work to do
            my $tempres = $self->process_tempfiles($dbh);
            my $delres = $self->process_deletes($dbh);

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

    my ($self, $dbh) = @_;

    # dig up some temporary files to purge
    my $too_old = int($ENV{T_TEMPFILE_TOO_OLD}) || 3600;
    my $tempfiles = $dbh->selectall_arrayref("SELECT fid, devids FROM tempfile " .
                                             "WHERE createtime < UNIX_TIMESTAMP() - $too_old LIMIT 50");
    return 0 unless $tempfiles && @$tempfiles;

    # insert the right rows into file_on and file_to_delete and remove the
    # now expunged (or soon to be) rows from tempfile
    my (@questions, @binds, @fids);
    foreach my $row (@$tempfiles) {
        push @fids, $row->[0];
        foreach my $devid (split /,/, $row->[1]) {
            push @questions, "(?, ?)";
            push @binds, $row->[0], $devid;
        }
    }

    # TODO: error checking
    $dbh->do("INSERT INTO file_on (fid, devid) VALUES " . join(',', @questions), undef, @binds);
    $dbh->do("INSERT INTO file_to_delete VALUES " . join(',', map { "(?)" } @fids), undef, @fids);
    $dbh->do("DELETE FROM tempfile WHERE fid IN (" . join(',', @fids) . ")");
    return 1;
}

sub process_deletes {
    my ($self, $dbh) = @_;

    my $delmap = $dbh->selectall_arrayref("SELECT fd.fid, fo.devid ".
                                          "FROM file_to_delete fd ".
                                          "LEFT JOIN file_on fo ON fd.fid=fo.fid ".
                                          "LIMIT " . LIMIT);
    my $count = $delmap ? scalar @$delmap : 0;
    return 0 unless $count;

    my %dev_down;  # devid -> 1 (when device times out due to EIO)
    my $done = 0;
    foreach my $dm (List::Util::shuffle(@$delmap)) {
        $self->read_from_parent;
        my ($fid, $devid) = @$dm;

        # if no device is returned from the query above, that
        # means there are no file_on rows for it, and we can consider
        # it now deleted.
        unless (defined $devid) {
            $dbh->do("DELETE FROM file_to_delete WHERE fid=?", undef, $fid);
            next;
        }

        # don't try to delete from this device if we earlier
        # found it to be timing out with EIO
        next if $dev_down{$devid};

        last if ++$done > PER_BATCH;

        my $path = Mgd::make_path($devid, $fid);
        my $rv = 0;
        if (my $urlref = Mgd::is_url($path)) {
            # hit up the server and delete it
            # TODO: (optimization) use MogileFS->get_observed_state and don't try to delete things known to be down/etc
            my $sock = IO::Socket::INET->new(PeerAddr => $urlref->[0],
                                             PeerPort => $urlref->[1],
                                             Timeout => 2);
            unless ($sock) {
                # timeout or something, mark this device as down for now and move on
                $dev_down{$devid} = 1;
                next;
            }

            # send delete request
            error("Sending delete for $path") if $Mgd::DEBUG >= 2;
            $sock->write("DELETE $urlref->[2] HTTP/1.0\r\n\r\n");
            my $response = <$sock>;
            if ($response =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
                if (($1 >= 200 && $1 <= 299) || $1 == 404) {
                    # effectively means all went well
                    $rv = 1;
                } else {
                    # remote file system error?  mark node as down
                    error("Error: unlink failure: $path: $1");
                    $dev_down{$devid} = 1;
                    next;
                }
            } else {
                error("Error: unknown response line: $response");
            }
        } else {
            # do normal unlink
            $rv = unlink "$Mgd::MOG_ROOT/$path";

            # device is timing out.  take note of it and
            # continue dealing with other deletes
            if (! $rv) {
                if ($! == EIO) {
                    $dev_down{$devid} = 1;
                    next;
                } elsif ($! == ENOENT) {
                    $rv = 1;  # count non-existent file as deleted
                }
            }
        }

        # if we deleted it, or it didn't exist, consider it
        # deleted.
        $dbh->do("DELETE FROM file_on WHERE fid=? AND devid=?",
                 undef, $fid, $devid) if $rv;
    }

    # as far as we know, we have more work to do
    return 1;
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
