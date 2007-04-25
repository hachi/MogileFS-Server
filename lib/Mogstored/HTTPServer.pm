package Mogstored::HTTPServer;
use strict;
sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;
    $self->{docroot}  = delete $opts{docroot};
    $self->{listen}   = delete $opts{listen};
    $self->{maxconns} = delete $opts{maxconns};
    die "unknown opts" if %opts;
    return $self;
}

sub start {
    my $self = shift;
    die "start not implemented for $self";
}

sub pre_daemonize {
    my $self = shift;
}

sub post_daemonize {
    my $self = shift;
}


1;
