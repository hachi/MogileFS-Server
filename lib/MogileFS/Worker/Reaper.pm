package MogileFS::Worker::Reaper;
# deletes files

use strict;
use base 'MogileFS::Worker';

use POSIX;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub work {
    my $self = shift;
    my $psock = $self->{psock};

    my $parse_parent_response = sub {
        # now see what was in our message queue
        while (defined (my $line = <$psock>)) {
            $line =~ s/\r?\n$//;
            last if $line eq '.';

            # now find out what command this is?
            if ($line eq 'shutdown') {
                exit 0;
            }
        }
    };

    while (1) {
        sleep 10;

        # get db and note we're starting a run
        error("Reaper running; looking for dead devices")
            if $Mgd::DEBUG >= 1;
        $self->validate_dbh;
        my $dbh = $self->get_dbh or return 0;

        # general report in to parent
        $self->send_to_parent('reaper_ping');
        $parse_parent_response->();

        # get a current list of devices
        my $devs = Mgd::get_device_summary();
        my @deaddevs = grep { $_->{status} =~ /^dead$/ } values %$devs;
        next unless @deaddevs;

        # now iterate over dead devices
        foreach my $dev (@deaddevs) {
            my $devid = $dev->{devid};

            # look for files on this device
            my $fids = $dbh->selectcol_arrayref('SELECT fid FROM file_on WHERE devid = ? LIMIT 1000',
                                                undef, $devid);
            if ($dbh->err) {
                error("Error selecting jobs to reap: " . $dbh->errstr);
                next;
            }
            next unless $fids && @$fids;

            # note we got some
            error("Found " . scalar(@$fids) . " files on dead device $devid");

            # now iterate
            foreach my $fid (@$fids) {
                $dbh->do('DELETE FROM file_on WHERE fid = ? AND devid = ?',
                         undef, $fid, $devid);
                if ($dbh->err) {
                    error("Error deleting from file_on (file $fid, device $devid): " . $dbh->errstr);
                    next;
                }

                # now update the fid count
                unless (Mgd::update_fid_devcount($fid)) {
                    error("Error updating fid $fid devcount");
                    next;
                }

                # if debugging on, note this is done
                error("Reaper noted fid $fid no longer on device $devid")
                    if $Mgd::DEBUG >= 2;
            }
        }
    }


}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
