package MogileFS::Exception;
use strict;
use warnings;

sub new {
    my ($class, $errcode) = @_;
    return bless {
        code => $errcode,
    }, $class;
}

sub throw {
    my $self = shift;
    die $self;
}

sub code { $_[0]{code} }

1;
