package MogileFS::Host;
use strict;
use warnings;
use Net::Netmask;
use Carp qw(croak);

my %singleton;        # hostid -> instance
my $last_load = 0;    # unixtime of last 'reload_hosts'
my $all_loaded = 0;   # bool: have we loaded all the hosts?

# returns MogileFS::Host object, or throws 'dup' error
sub create {
    my ($pkg, $hostname, $ip) = @_;
    my $hid = Mgd::get_store()->create_host($hostname, $ip);
    return MogileFS::Host->of_hostid($hid);
}

sub of_hostid {
    my ($class, $hostid) = @_;
    return undef unless $hostid;
    return $singleton{$hostid} ||= bless {
        hostid    => $hostid,
        _loaded   => 0,
    }, $class;
}

sub of_hostname {
    my ($class, $hostname) = @_;

    # reload if it's been awhile
    MogileFS::Host->check_cache;
    foreach my $host ($class->hosts) {
        return $host if $host->{hostname} eq $hostname;
    }

    # force a reload
    MogileFS::Host->reload_hosts;
    foreach my $host ($class->hosts) {
        return $host if $host->{hostname} eq $hostname;
    }

    return undef;
}

sub invalidate_cache {
    my $class = shift;

    # so next time it's invalid and won't be used old
    $last_load    = 0;
    $all_loaded   = 0;
    $_->{_loaded} = 0 foreach values %singleton;

    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("host");
    }
}

# force a reload of all host objects.
sub reload_hosts {
    my $class = shift;

    # mark them all invalid for now, until they're reloaded
    foreach my $host (values %singleton) {
        $host->{_loaded} = 0;
    }

    my $sto = Mgd::get_store();
    foreach my $row ($sto->get_all_hosts) {
        die unless $row->{status} =~ /^\w+$/;
        my $ho =
            MogileFS::Host->of_hostid($row->{hostid});
        $ho->absorb_dbrow($row);
    }

    # get rid of ones that could've gone away:
    foreach my $hostid (keys %singleton) {
        my $host = $singleton{$hostid};
        delete $singleton{$hostid} unless $host->{_loaded}
    }

    $all_loaded = 1;
    $last_load = time();
}

# reload host objects if it hasn't been done in last 5 seconds
sub check_cache {
    my $class = shift;
    my $now = time();
    return if $last_load > $now - 5;
    MogileFS::Host->reload_hosts;
}

sub hosts {
    my $class = shift;
    $class->reload_hosts unless $all_loaded;
    return values %singleton;
}

# --------------------------------------------------------------------------

sub id { $_[0]{hostid} }

sub absorb_dbrow {
    my ($host, $hashref) = @_;
    foreach my $k (qw(status hostname hostip http_port http_get_port altip altmask)) {
        $host->{$k} = $hashref->{$k};
    }
    $host->{mask} =
        ($host->{altip} && $host->{altmask}) ?
        Net::Netmask->new2($host->{altmask}) :
        undef;

    $host->{_loaded} = 1;
}

sub set_observed_state {
    my ($host, $state) = @_;
    croak "set_observed_state() with invalid host state '$state', valid: reachable, unreachable"
        if $state !~ /^(?:reachable|unreachable)$/;
    $host->{observed_state} = $state;
}

sub observed_reachable {
    my $host = shift;
    return $host->{observed_state} && $host->{observed_state} eq "reachable";
}

sub observed_unreachable {
    my $host = shift;
    return $host->{observed_state} && $host->{observed_state} eq "unreachable";
}

sub set_status        { shift->_set_field("status",        @_); }
sub set_ip            { shift->_set_field("hostip",        @_); } # throws 'dup'
sub set_http_port     { shift->_set_field("http_port",     @_); }
sub set_http_get_port { shift->_set_field("http_get_port", @_); }
sub set_alt_ip        { shift->_set_field("altip",         @_); }
sub set_alt_mask      { shift->_set_field("altmask",       @_); }

sub _set_field {
    my ($self, $field, $val) = @_;
    # $field is both the database column field and our member keys
    $self->_load;
    return 1 if $self->{$field} eq $val;
    return 0 unless Mgd::get_store()->update_host_property($self->id, $field, $val);
    $self->{$field} = $val;
    MogileFS::Host->invalidate_cache;
    return 1;
}

sub http_port {
    my $host = shift;
    $host->_load;
    return $host->{http_port};
}

sub http_get_port {
    my $host = shift;
    $host->_load;
    return $host->{http_get_port} || $host->{http_port};
}

sub ip {
    my $host = shift;
    $host->_load;
    if ($host->{mask} && $host->{altip} &&
        ($MogileFS::REQ_altzone || ($MogileFS::REQ_client_ip &&
                                    $host->{mask}->match($MogileFS::REQ_client_ip)))) {
        return $host->{altip};
    } else {
        return $host->{hostip};
    }
}

sub field {
    my ($host, $k) = @_;
    $host->_load;
    # TODO: validate $k to be in certain set of allowed keys?
    return $host->{$k};
}

sub status {
    my $host = shift;
    $host->_load;
    return $host->{status};
}

sub hostname {
    my $host = shift;
    $host->_load;
    return $host->{hostname};
}

sub is_marked_down {
    my $host = shift;
    die "FIXME";
    # ...
}

sub exists {
    my $host = shift;
    $host->_try_load;
    return $host->{_loaded};
}

sub overview_hashref {
    my $host = shift;
    $host->_load;
    my $ret = {};
    foreach my $k (qw(hostid status http_port http_get_port hostname hostip altip altmask)) {
        $ret->{$k} = $host->{$k};
    }
    return $ret;
}

sub delete {
    my $host = shift;
    my $rv = Mgd::get_store()->delete_host($host->id);
    MogileFS::Host->invalidate_cache;
    return $rv;
}

# --------------------------------------------------------------------------

sub _load {
    return if $_[0]{_loaded};
    MogileFS::Host->reload_hosts;
    return if $_[0]{_loaded};
    my $host = shift;
    croak "Host $host->{hostid} doesn't exist.\n";
}

sub _try_load {
    return if $_[0]{_loaded};
    MogileFS::Host->reload_hosts;
}


1;
