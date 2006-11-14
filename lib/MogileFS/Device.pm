package MogileFS::Device;
use strict;
use warnings;

my %singleton;  # devid -> instance

sub of_devid {
    my ($class, $devid) = @_;
    return $singleton{$devid} ||= bless {
        devid    => $devid,
        no_mkcol => 0,
    }, $class;
}

sub status {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    my $disk = $dsum->{$self->{devid}} or return;
    return $disk->{status};
}

sub is_marked_dead {
    my $self = shift;
    return $self->status eq "dead";
}

sub is_marked_readonly {
    my $self = shift;
    return $self->status eq "readonly";
}

sub exists {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    return 1 if $dsum->{$self->{devid}};
    # be damn careful to never return 0 (doesn't exist) when it could just
    # be really new and not yet in cache
    my $dbh = Mgd::get_dbh();
    my $exists = $dbh->selectall_hashref("SELECT devid FROM device", "devid");
    MogileFS::Util::dbcheck($dbh, "failed to lookup devices");
    return 0 unless $exists->{$self->{devid}};
    Mgd::invalidate_device_cache();
    return 1;
}

sub hostid {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    my $disk = $dsum->{$self->{devid}} or return 0;
    return $disk->{hostid};
}

sub is_observed_writeable {
    my $self = shift;
    return
        MogileFS->observed_state("host", $self->hostid) eq "reachable" &&
        MogileFS->observed_state("device", $self->{devid}) eq "writeable";
}

sub doesnt_know_mkcol {
    my $self = shift;
    # TODO: forget this periodically?  maybe whenever host/device is observed down?
    # in case webserver changes.
    return $self->{no_mkcol};
}

my %dir_made;  # /dev<n>/path -> $time
my $dir_made_lastclean = 0;
sub create_directory {
    my ($self, $uri) = @_;
    return 1 if $self->doesnt_know_mkcol;
    next if $dir_made{$uri};

    my $hostid = $self->hostid;
    my $host = Mgd::hostid_ip($hostid)         or return 0;
    my $port = Mgd::hostid_http_port($hostid)  or return 0;
    my $peer = "$host:$port";

    my $sock = IO::Socket::INET->new(PeerAddr => $peer, Timeout => 1)
        or next;
    print $sock "MKCOL $uri HTTP/1.0\r\n".
        "Content-Length: 0\r\n\r\n";

    my $ans = <$sock>;

    # if they don't support this method, remember that
    if ($ans && $ans =~ m!HTTP/1\.[01] (400|405|501)!) {
        $self->{no_mkcol} = 1;
        return 1;
    }

    return 0 unless $ans =~ m!^HTTP/1.[01] 2\d\d!;

    my $now = time();
    $dir_made{$uri} = $now;

    # cleanup %dir_made occasionally.
    my $clean_interval = 300;  # every 5 minutes.
    if ($dir_made_lastclean < $now - $clean_interval) {
        $dir_made_lastclean = $now;
        foreach my $k (keys %dir_made) {
            delete $dir_made{$k} if $dir_made{$k} < $now - 3600;
        }
    }
}


1;
