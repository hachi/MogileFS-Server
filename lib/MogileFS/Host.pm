package MogileFS::Host;
use strict;
use warnings;

my %singleton;  # hostid -> instance

sub of_hostid {
    my ($class, $hostid) = @_;
    return $singleton{$hostid} ||= bless {
        hostid    => $hostid,
    }, $class;
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
