package Mogstored::HTTPServer;
use strict;
sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;
    $self->{docroot}  = delete $opts{docroot};
    $self->{listen}   = delete $opts{listen};
    $self->{maxconns} = delete $opts{maxconns};
    $self->{bin}      = delete $opts{bin};
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

sub listen_port {
    my $self = shift;
    my $port = $self->{listen};
    $port =~ s/^.+://;
    die "not numeric port?" unless $port =~ /^\d+$/;
    return $port;
}

1;
