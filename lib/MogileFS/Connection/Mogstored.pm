package MogileFS::Connection::Mogstored;
use strict;
use IO::Socket::INET;

sub new {
    my ($class, $ip, $port) = @_;
    return bless {
        sock => undef,  # undef if not yet connected, else socket to host
        ip   => $ip,
        port => $port,
    }, $class;
}

# returns (or connects to & returns) raw socket to mogstored.
sub sock {
    my ($self, $timeout) = @_;
    return $self->{sock} if $self->{sock};
    return $self->{sock} = IO::Socket::INET->new(PeerAddr => $self->{ip},
                                                 PeerPort => $self->{port},
                                                 Timeout  => $timeout);
}

sub sock_if_connected {
    my $self = shift;
    return $self->{sock};
}

sub mark_dead {
    my $self = shift;
    $self->{sock} = undef;
}

1;
