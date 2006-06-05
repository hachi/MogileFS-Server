package MogileFS::Worker::Monitor;
# deletes files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw(every);

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

    every(2.5, sub {

        # get db and note we're starting a run
        error("Monitor running; scanning usage files")
            if $Mgd::DEBUG >= 1;
        $self->validate_dbh;
        my $dbh = $self->get_dbh or return 0;

        # general report in to parent
        $self->send_to_parent('monitor_ping');
        $parse_parent_response->();

        # get a current list of devices
        my $devs = Mgd::get_device_summary();
        next unless $devs && %$devs;

        # now iterate over devices
        foreach my $dev (values %$devs) {
            next if $dev->{status} =~ /^dead|down$/;

            my $host = $Mgd::cache_host{$dev->{hostid}};
            my $port = $host->{http_get_port} || $host->{http_port};
            my $url = "http://$host->{hostip}:$port/dev$dev->{devid}/usage";

            # now try to get the data with a short timeout
            my $timeout = 2;
            my $start_time = Time::HiRes::time();

            my $ua = LWP::UserAgent->new( timeout => 2 );
            my $response = $ua->get($url);
            my $res_time = Time::HiRes::time();

            unless ($response->is_success) {
                my $failed_after = $res_time - $start_time;
                if ($failed_after < 0.5) {
                    error("Port $port not listening on otherwise-alive machine $host->{hostip}?  Error was: " . $response->status_line);
                } else {
                    $failed_after = sprintf("%.02f", $failed_after);
                    error("Timeout contacting machine $host->{hostip} for dev $dev->{devid}:  took $failed_after seconds out of $timeout allowed");
                }
                return;
            }

            my %stats;
            my $data = $response->content;
            foreach (split(/\r?\n/, $data)) {
                next unless /^(\w+)\s*:\s*(.+)$/;
                $stats{$1} = $2;
            }

            my ($used, $total) = ($stats{used}, $stats{total});
            unless ($used && $total) {
                error("dev$dev->{devid} reports used = $used, total = $total, error?");
                next;
            }

            # bytes => megabytes
            $used /= 1024;
            $total /= 1024;

            $dbh->do("UPDATE device SET mb_total = ?, mb_used = ?, mb_asof = UNIX_TIMESTAMP() " .
                     "WHERE devid = ?", undef, int($total), int($used), $dev->{devid});
            if ($dbh->err) {
                error("Database error in update query: " . $dbh->errstr);
                next;
            }

            error("dev$dev->{devid}: used = $used, total = $total")
                if $Mgd::DEBUG >= 1;
        }
    });

}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
