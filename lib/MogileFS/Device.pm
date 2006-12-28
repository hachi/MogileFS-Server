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

# general purpose device locator.  example:
#
# my $devid = MogileFS::Device->find_deviceid(
#     random => 1,              # get random device (else find first suitable)
#     min_free_space => 100,    # with at least 100MB free
#     weight_by_free => 1,      # find result weighted by free space
#     max_disk_age => 5,        # minutes of age the last usage report can be before we ignore the disk
#     not_on_hosts => [ 1, 2 ], # no devices on hosts 1 and 2
#     must_be_alive => 1,       # if specified, device/host must be writeable (fully available)
# );
#
# returns undef if no suitable device was found.  else, if you wanted an
# array will return an array of the suitable devices--if you want just a
# single item, you get just the first one found.
sub find_deviceid {
    my $class = shift;
    my %opts = ( @_ );

    # validate we're getting called with known parameters
    my %valid_keys = map { $_ => 1 } qw( random min_free_space weight_by_free max_disk_age not_on_hosts must_be_writeable must_be_readable );
    warn "invalid key $_ in call to find_deviceid\n"
        foreach grep { ! $valid_keys{$_} } keys %opts;

    # copy down global minimum free space if not specified
    $opts{min_free_space} ||= MogileFS->config("min_free_space");
    $opts{max_disk_age}   ||= MogileFS->config("max_disk_age");
    if ($opts{max_disk_age}) {
        $opts{max_disk_age} = time() - ($opts{max_disk_age} * 60);
    }
    $opts{must_be_alive} = 1 unless defined $opts{must_be_alive};

    # setup for iterating over devices
    my $devs = Mgd::get_device_summary();
    my @devids = keys %{$devs || {}};
    my $devcount = scalar(@devids);
    my $start = $opts{random} ? int(rand($devcount)) : 0;
    my %not_on_host = ( map { $_ => 1 } @{$opts{not_on_hosts} || []} );
    my $total_free = 0;

    # now find a device that matches what they want
    my @list;
    for (my $i = 0; $i < $devcount; $i++) {
        my $idx = ($i + $start) % $devcount;
        my $dev = $devs->{$devids[$idx]};
        my $devo = MogileFS::Device->of_devid($dev->{devid});
        my $hosto = $devo->host or next;

        # series of suitability checks
        next unless $devo->is_marked_alive;
        next if $not_on_host{$dev->{hostid}};
        next if $opts{max_disk_age} && $dev->{mb_asof} &&
                $dev->{mb_asof} < $opts{max_disk_age};
        next if $opts{min_free_space} && $dev->{mb_total} &&
                $dev->{mb_free} < $opts{min_free_space};

        if ($opts{must_be_writeable}) {
            next unless $hosto->observed_reachable;
            next unless $devo->observed_writeable;
        } elsif ($opts{must_be_readable}) {
            next unless $hosto->observed_reachable;
            next unless $devo->observed_readable;
        }

        # we get here, this is a suitable device
        push @list, $dev->{devid};
        $total_free += $dev->{mb_free};
    }

    # now we have a list ordered randomly, do free space weighting
    if ($opts{weight_by_free}) {
        my $rand = int(rand($total_free));
        my $cur = 0;
        foreach my $devid (@list) {
            $cur += $devs->{$devid}->{mb_free};
            return $devid if $cur >= $rand;
        }
    }

    # return whole list if wanting array, else just first item
    return wantarray ? @list : shift(@list);
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

# returns hashref of devid -> $device_obj
sub map {
    my $class = shift;
    my $ret = {};
    foreach my $d (MogileFS::Device->devices) {
        $ret->{$d->id} = $d;
    }
    return $ret;
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
