package MogileFS::Device;
use strict;
use warnings;
use Carp qw(croak);
use MogileFS::Config qw(DEVICE_SUMMARY_CACHE_TIMEOUT);

BEGIN {
    my $testing = $ENV{TESTING} ? 1 : 0;
    eval "sub TESTING () { $testing }";
}

my %singleton;      # devid -> instance
my $last_load = 0;  # unixtime we last reloaded devices from database
my $all_loaded = 0; # bool: have we loaded all the devices?

# throws "dup" on duplicate devid.  returns new MogileFS::Device object on success.
# %args include devid, hostid, and status (in (alive, down, readonly))
sub create {
    my ($pkg, %args) = @_;
    my $devid = Mgd::get_store()->create_device(@args{qw(devid hostid status)});
    MogileFS::Device->invalidate_cache;
    return $pkg->of_devid($devid);
}

sub of_devid {
    my ($class, $devid) = @_;
    croak("Invalid devid") unless $devid;
    return $singleton{$devid} ||= bless {
        devid    => $devid,
        no_mkcol => 0,
        _loaded  => 0,
    }, $class;
}

sub from_devid_and_hostname {
    my ($class, $devid, $hostname) = @_;
    my $dev = MogileFS::Device->of_devid($devid)
        or return undef;
    return undef unless $dev->exists;
    my $host = $dev->host;
    return undef
        unless $host && $host->exists && $host->hostname eq $hostname;
    return $dev;
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
#     not_devs => { $devid => 1 }  # devids to not return.
# );
#
# returns undef if no suitable device was found.  else, if you wanted an
# array will return an array of the suitable devices--if you want just a
# single item, you get just the first one found.
sub find_deviceid {
    my $class = shift;
    my %opts = ( @_ );

    # validate we're getting called with known parameters
    my %valid_keys = map { $_ => 1 } qw( random min_free_space weight_by_free max_disk_age
                                         not_on_hosts must_be_writeable must_be_readable
                                         not_devs
                                         );
    warn "invalid key $_ in call to find_deviceid\n"
        foreach grep { ! $valid_keys{$_} } keys %opts;

    # copy down global minimum free space if not specified
    $opts{min_free_space} ||= MogileFS->config("min_free_space");
    $opts{max_disk_age}   ||= MogileFS->config("max_disk_age");
    if ($opts{max_disk_age}) {
        # FIXME: don't use local machine's time() for this.  time sync
        # issues!  instead, the monitor process should track this,
        # noting the difference in relative time between the server's
        # time (in Date: response header) and time in the usage.txt
        # file.
        $opts{max_disk_age} = time() - ($opts{max_disk_age} * 60);
    }
    $opts{must_be_alive} = 1 unless defined $opts{must_be_alive};
    $opts{not_devs}    ||= {};

    # setup for iterating over devices
    my $devs = MogileFS::Device->map;
    my @devids = grep { ! $opts{not_devs}{$_} } keys %$devs;
    my $devcount = scalar(@devids);
    my $start = $opts{random} ? int(rand($devcount)) : 0;
    my %not_on_host = ( map { $_ => 1 } @{$opts{not_on_hosts} || []} );
    my $total_free = 0;

    # now find a device that matches what they want
    my @list;
    for (my $i = 0; $i < $devcount; $i++) {
        my $idx = ($i + $start) % $devcount;
        my $dev = $devs->{$devids[$idx]};
        my $host = $dev->host or next;

        # series of suitability checks
        next unless $dev->is_marked_alive;
        next if $not_on_host{$dev->{hostid}};
        next if $opts{max_disk_age} && $dev->{mb_asof} &&
            $dev->{mb_asof} < $opts{max_disk_age};
        next if $opts{min_free_space} && $dev->{mb_total} &&
                $dev->{mb_free} < $opts{min_free_space};

        if ($opts{must_be_writeable}) {
            next unless $host->observed_reachable;
            next unless $dev->observed_writeable;
        } elsif ($opts{must_be_readable}) {
            next unless $host->observed_reachable;
            next unless $dev->observed_readable;
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
    MogileFS::Device->check_cache;
    return values %singleton;
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

sub reload_devices {
    my $class = shift;

    # mark them all invalid for now, until they're reloaded
    foreach my $dev (values %singleton) {
        $dev->{_loaded} = 0;
    }

    MogileFS::Host->check_cache;

    my $sto = Mgd::get_store();
    foreach my $row ($sto->get_all_devices) {
        my $dev =
            MogileFS::Device->of_devid($row->{devid});
        $dev->absorb_dbrow($row);
    }

    # get rid of ones that could've gone away:
    foreach my $devid (keys %singleton) {
        my $dev = $singleton{$devid};
        delete $singleton{$devid} unless $dev->{_loaded}
    }

    $all_loaded = 1;
    $last_load  = time();
}

sub invalidate_cache {
    my $class = shift;

    # so next time it's invalid and won't be used old
    $last_load    = 0;
    $all_loaded   = 0;
    $_->{_loaded} = 0 foreach values %singleton;

    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("device");
    }
}

sub check_cache {
    my $class = shift;
    my $now = time();
    return if $last_load > $now - DEVICE_SUMMARY_CACHE_TIMEOUT;
    MogileFS::Device->reload_devices;
}

# --------------------------------------------------------------------------

sub devid { return $_[0]{devid} }
sub id    { return $_[0]{devid} }

sub absorb_dbrow {
    my ($dev, $hashref) = @_;
    foreach my $k (qw(hostid mb_total mb_used mb_asof status weight)) {
        $dev->{$k} = $hashref->{$k};
    }

    $dev->{$_} ||= 0 foreach qw(mb_total mb_used mb_asof);

    # makes others have an easier time of finding devices by free space
    # FIXME: this should just be an accessor nowadays, not pre-calced
    $dev->{mb_free} = $dev->{mb_total} - $dev->{mb_used};

    my $host = MogileFS::Host->of_hostid($dev->{hostid});
    if ($host && $host->exists) {
        my $host_status = $host->status;
        die "No status" unless $host_status =~ /^\w+$/;
        # FIXME: not sure I like this, changing the in-memory version
        # of the configured status is.  I'd rather this be calculated
        # in an accessor.
        if ($dev->{status} eq 'alive' && $host_status ne 'alive') {
            $dev->{status} = "down"
        }
    } else {
        if ($dev->{status} eq "dead") {
            # ignore dead devices without hosts.  not a big deal.
        } else {
            die "No host for dev $dev->{devid} (host $dev->{hostid})";
        }
    }

    $dev->{_loaded} = 1;
}

our $util_no_broadcast = 0;

sub set_observed_utilization {
    my ($dev, $util) = @_;
    $dev->{utilization} = $util;
    my $devid = $dev->id;

    return if $util_no_broadcast;

    my $worker = MogileFS::ProcManager->is_child or return;
    $worker->send_to_parent(":set_dev_utilization $devid $util");
}

sub observed_utilization {
    my ($dev) = @_;

    if (TESTING) {
        my $weight_varname = 'T_FAKE_IO_DEV' . $dev->id;
        return $ENV{$weight_varname} if defined $ENV{$weight_varname};
    }

    return $dev->{utilization};
}

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
    my $dev = shift;
    $dev->_load;
    return $dev->{status};
}

sub weight {
    my $dev = shift;

    $dev->_load;

    return $dev->{weight};
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
    my $dev = shift;
    $dev->_try_load;
    return $dev->{_loaded};
}

sub host {
    my $dev = shift;
    return MogileFS::Host->of_hostid($dev->hostid);
}

sub hostid {
    my $dev = shift;
    $dev->_load;
    return $dev->{hostid};
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

    return 0 unless $ans && $ans =~ m!^HTTP/1.[01] 2\d\d!;

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
    return
        grep { $_->status eq "dead" }
        MogileFS::Device->devices;
}

sub fid_list {
    my ($self, %opts) = @_;
    my $limit = delete $opts{limit};
    croak("No limit specified") unless $limit && $limit =~ /^\d+$/;
    croak("Unknown options to fid_list") if %opts;

    my $sto = Mgd::get_store();
    my $fidids = $sto->get_fidids_by_device($self->devid, $limit);
    return map {
        MogileFS::FID->new($_)
    } @{$fidids || []};
}

sub forget_about {
    my ($dev, $fid) = @_;
    Mgd::get_store()->remove_fidid_from_devid($fid->id, $dev->id);
    return 1;
}

sub usage_url {
    my $dev = shift;
    my $host     = $dev->host;
    my $get_port = $host->http_get_port;
    my $hostip   = $host->ip;
    return "http://$hostip:$get_port/dev$dev->{devid}/usage";
}

sub overview_hashref {
    my $dev = shift;
    $dev->_load;

    my $ret = {};
    foreach my $k (qw(devid hostid status weight observed_state
                      mb_total mb_used mb_asof mb_free utilization)) {
        $ret->{$k} = $dev->{$k};
    }
    return $ret;
}

sub set_weight {
    my ($dev, $weight) = @_;
    my $sto = Mgd::get_store();
    $sto->set_device_weight($dev->id, $weight);
    MogileFS::Device->invalidate_cache;
}

sub set_state {
    my ($dev, $state) = @_;
    die "Bogus state" unless $state =~ /^(?:alive|down|dead|readonly)$/;
    my $sto = Mgd::get_store();
    $sto->set_device_state($dev->id, $state);
    MogileFS::Device->invalidate_cache;

    # wake a reaper process up from sleep to get started as soon as possible
    # on re-replication
    MogileFS::ProcManager->wake_a("reaper") if $state eq "dead";
}

# --------------------------------------------------------------------------

sub _load {
    return if $_[0]{_loaded};
    MogileFS::Device->reload_devices;
    return if $_[0]{_loaded};
    my $dev = shift;
    croak "Device $dev->{devid} doesn't exist.\n";
}

sub _try_load {
    return if $_[0]{_loaded};
    MogileFS::Device->reload_devices;
}

1;
