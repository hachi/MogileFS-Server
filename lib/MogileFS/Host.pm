package MogileFS::Host;
use strict;
use warnings;
use Net::Netmask;

my %singleton;  # hostid -> instance

sub of_hostid {
    my ($class, $hostid) = @_;
    return $singleton{$hostid} ||= bless {
        hostid    => $hostid,
    }, $class;
}

sub clear_cache {
    my ($class) = @_;
    # .... currently all done in mogilefsd package "Mgd".  should be
    # moved here.
}

# --------------------------------------------------------------------------

sub absorb_dbrow {
    my ($host, $hashref) = @_;
    foreach my $k (qw(status hostname hostip http_port http_get_port altip altmask)) {
        $host->{$k} = $hashref->{$k};
    }
    $host->{mask} = Net::Netmask->new2($host->{altmask})
        if $host->{altip} && $host->{altmask};
}

sub http_port {
    my $host = shift;
    Mgd::reload_host_cache() unless $host->{http_port};
    return $host->{http_port};

}

sub http_get_port {
    my $host = shift;
    Mgd::reload_host_cache() unless $host->{http_get_port} || $host->{http_port};
    return $host->{http_get_port} || $host->{http_port};
}

sub ip {
    my $host = shift;
    Mgd::reload_host_cache() unless $host->{hostip};
    return $host->{hostip};
}

sub status {
    my $self = shift;
    die "FIXME";
}

sub is_marked_down {
    my $self = shift;
    die "FIXME";
    # ...
}

sub exists {
    my $self = shift;
    die "FIXME";
}

1;
