package MogileFS::Checksum;
use strict;
use warnings;
use overload '""' => \&as_string;

our %NAME2TYPE = (
	md5 => 1,
);

our %TYPE2NAME = map { $NAME2TYPE{$_} => $_} keys(%NAME2TYPE);

sub new {
    my ($class, $row) = @_;
    my $self = bless {
        fidid => $row->{fid},
        checksum => $row->{checksum},
        checksumtype => $row->{checksumtype}
    }, $class;

    return $self;
}

sub checksumname {
    my $self = shift;
    my $type = $self->{checksumtype};
    my $name = $TYPE2NAME{$type} or die "checksumtype=$type unknown";

    return $name;
}

sub hexdigest {
    my $self = shift;

    unpack("H*", $self->{checksum});
}

sub as_string {
    my $self = shift;
    my $name = $self->checksumname;
    my $hexdigest = $self->hexdigest;

    "Checksum[f=$self->{fidid};$name=$hexdigest]"
}

1;
