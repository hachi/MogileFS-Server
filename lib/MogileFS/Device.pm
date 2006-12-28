package MogileFS::Device;
use strict;
use warnings;
use Carp qw(croak);

my %singleton;  # devid -> instance

sub of_devid {
    my ($class, $devid) = @_;
    croak("Invalid devid") unless $devid;
    return $singleton{$devid} ||= bless {
        devid    => $devid,
        no_mkcol => 0,
    }, $class;
}

sub vivify_directories {
    my ($class, $path) = @_;

    # $path is something like:
    #    http://10.0.0.26:7500/dev2/0/000/148/0000148056.fid

    # three directories we'll want to make:
    #    http://10.0.0.26:7500/dev2/0
    #    http://10.0.0.26:7500/dev2/0/000
    #    http://10.0.0.26:7500/dev2/0/000/148

    croak "non-HTTP mode no longer supported" unless $path =~ /^http/;
    return 0 unless $path =~ m!/dev(\d+)/(\d+)/(\d\d\d)/(\d\d\d)/\d+\.fid$!;
    my ($devid, $p1, $p2, $p3) = ($1, $2, $3, $4);

    my $dev = MogileFS::Device->of_devid($devid);
    return 0 unless $dev->exists;

    $dev->create_directory("/dev$devid/$p1");
    $dev->create_directory("/dev$devid/$p1/$p2");
    $dev->create_directory("/dev$devid/$p1/$p2/$p3");
}

# --------------------------------------------------------------------------

sub devid { return $_[0]{devid} }
sub id    { return $_[0]{devid} }

sub set_observed_state {
    my ($dev, $state) = @_;
    croak "set_observed_state() with invalid device state '$state', valid: writeable, readable, unreachable"
        if $state !~ /^(?:writeable|readable|unreachable)$/;
    $dev->{observed_state} = $state;
}

sub observed_writeable {
    my $dev = shift;
    return 0 unless $dev->{observed_state} && $dev->{observed_state} eq "writeable";
    my $host = $dev->host
        or return 0;
    return 0 unless $host->observed_reachable;
    return 1;
}

sub observed_readable {
    my $dev = shift;
    return $dev->{observed_state} && $dev->{observed_state} eq "readable";
}

sub observed_unreachable {
    my $dev = shift;
    return $dev->{observed_state} && $dev->{observed_state} eq "unreachable";
}

sub status {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    my $disk = $dsum->{$self->{devid}} or return;
    return $disk->{status};
}

sub is_marked_alive {
    my $self = shift;
    return $self->status eq "alive";
}

sub is_marked_dead {
    my $self = shift;
    return $self->status eq "dead";
}

sub is_marked_down {
    my $self = shift;
    return $self->status eq "down";
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

sub host {
    my $self = shift;
    my $hostid = $self->hostid
        or return undef;
    return MogileFS::Host->of_hostid($hostid);
}

sub hostid {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    my $disk = $dsum->{$self->{devid}} or return 0;
    return $disk->{hostid};
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
    my $host   = $self->host;
    my $hostip = $host->ip        or return 0;
    my $port   = $host->http_port or return 0;
    my $peer = "$hostip:$port";

    my $sock = IO::Socket::INET->new(PeerAddr => $peer, Timeout => 1)
        or next;
    print $sock "MKCOL $uri HTTP/1.0\r\n".
        "Content-Length: 0\r\n\r\n";

    my $ans = <$sock>;

    # if they don't support this method, remember that
    if ($ans && $ans =~ m!HTTP/1\.[01] (400|405|501)!) {
        $self->{no_mkcol} = 1;
        # TODO: move this into method on device, which propogates to parent
        # and also receive from parent.  so all query workers share this knowledge
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

# returns array of MogileFS::Device objects which are in state 'dead'.
sub dead_devices {
    my $class = shift;
    # get a current list of devices
    my $devs = Mgd::get_device_summary();
    return
        map { MogileFS::Device->of_devid($_->{devid}) }
        grep { $_->{status} eq "dead" }
        values %$devs;
}

# returns array of all MogileFS::Device objects
sub devices {
    my $class = shift;
    # get a current list of devices
    my $devs = Mgd::get_device_summary();
    return
        map { MogileFS::Device->of_devid($_->{devid}) }
        values %$devs;
}

sub fid_list {
    my ($self, %opts) = @_;
    my $limit = delete $opts{limit};
    croak("No limit specified") unless $limit && $limit =~ /^\d+$/;
    croak("Unknown options to fid_list") if %opts;

    my $dbh = Mgd::get_dbh();
    my $fidids = $dbh->selectcol_arrayref("SELECT fid FROM file_on WHERE devid = ? LIMIT $limit",
                                          undef, $self->devid);
    die "Error selecting jobs to reap: " . $dbh->errstr if $dbh->err;
    return map {
        MogileFS::FID->new($_)
    } @{$fidids || []};
}

sub forget_about {
    my ($dev, $fid) = @_;
    my $dbh = Mgd::get_dbh();
    $dbh->do('DELETE FROM file_on WHERE fid = ? AND devid = ?',
             undef, $fid->id, $dev->id);
    die $dbh->errstr if $dbh->err;
    return 1;
}

sub usage_url {
    my $dev = shift;
    my $host     = $dev->host;
    my $get_port = $host->http_get_port;
    my $hostip   = $host->ip;
    return "http://$hostip:$get_port/dev$dev->{devid}/usage";
}

1;
