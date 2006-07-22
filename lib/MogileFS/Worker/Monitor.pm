package MogileFS::Worker::Monitor;
# deletes files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw(every error);

use POSIX;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub watchdog_timeout {
    30;
}

sub work {
    my $self = shift;

    my $update_db_every = 15;
    my %last_db_update;  # devid -> time.  update db less often than poll interval.

    every(2.5, sub {
        $self->parent_ping;

        # get db and note we're starting a run
        error("Monitor running; scanning usage files")
            if $Mgd::DEBUG >= 1;
        $self->validate_dbh;
        my $dbh = $self->get_dbh or return 0;

        # get a current list of devices
        my $devs = Mgd::get_device_summary();
        next unless $devs && %$devs;

        # now iterate over devices
        my %skip_host;  # hostid -> 1 if already noted dead.
        foreach my $dev (values %$devs) {
            next if $dev->{status} =~ /^dead|down$/;
            next if $skip_host{$dev->{hostid}};

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
                    $self->broadcast_device_unreachable($dev->{devid});
                    error("Port $port not listening on otherwise-alive machine $host->{hostip}?  Error was: " . $response->status_line);
                } else {
                    $failed_after = sprintf("%.02f", $failed_after);
                    $self->broadcast_host_unreachable($dev->{hostid});
                    $skip_host{$dev->{hostid}} = 1;
                    error("Timeout contacting machine $host->{hostip} for dev $dev->{devid}:  took $failed_after seconds out of $timeout allowed");
                }
                next;
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

            # only update database every ~15 seconds per device
            my $last_update = $last_db_update{$dev->{devid}} || 0;
            my $next_update = $last_update + $update_db_every;
            my $now = time();
            if ($now >= $next_update) {
                $dbh->do("UPDATE device SET mb_total = ?, mb_used = ?, mb_asof = UNIX_TIMESTAMP() " .
                         "WHERE devid = ?", undef, int($total / 1024), int($used / 1024), $dev->{devid});
                if ($dbh->err) {
                    error("Database error in update query: " . $dbh->errstr);
                    next;
                }
                $last_db_update{$dev->{devid}} = $now;
            }

            # now we want to check if this device is writeable
            my $num = int(rand 10000);  # this was "$$-$now" before, but we don't yet have a cleaner in mogstored for these files
            my $puturl = "http://$host->{hostip}:$port/dev$dev->{devid}/test-write/test-write-$num";
            my $req = HTTP::Request->new(PUT => $puturl);
            $req->content(<<EOREQUEST);
## THIS IS AN AUTOMATICALLY GENERATED FILE USED TO TEST WRITEABILITY AND
## WILL BE CLEANED BY THE MOGSTORED USAGE PROCESS
EOREQUEST

            # TODO: hosts aren't writable.  they're "available"
            # TODO: re-check the file was written as put.
            # TODO: put something unique in the file
            # TODO: guard against race-conditions with double-check on failure

            # now, depending on what happens
            my $resp = $ua->request($req);
            if ($resp->is_success) {
                $self->broadcast_host_reachable($dev->{hostid});
                $self->broadcast_device_writeable($dev->{devid});
                error("dev$dev->{devid}: used = $used, total = $total, writeable = 1")
                    if $Mgd::DEBUG >= 1;
            } else {
                # merely readable
                $self->broadcast_host_reachable($dev->{hostid});
                $self->broadcast_device_readable($dev->{devid});
                error("dev$dev->{devid}: used = $used, total = $total, writeable = 0")
                    if $Mgd::DEBUG >= 1;
            }
        }
    });

}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
