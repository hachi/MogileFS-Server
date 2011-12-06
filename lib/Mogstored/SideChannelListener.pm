package Mogstored::SideChannelListener;
use strict;
use base 'Perlbal::TCPListener';
use Mogstored::SideChannelClient;
use Socket qw(SO_KEEPALIVE);

sub new {
    my ($class, $hostport) = @_;
    # we don't _really_ need this, but TCPListener kinda does, to keep it from
    # exploding/warning.  so we created this stub service above in our static
    # config, just for this.
    my $svc    = Perlbal->service("mgmt") or die "Where is mgmt service?";
    my $self = $class->SUPER::new($hostport, $svc);
    $self->{sock}->sockopt(SO_KEEPALIVE, 1);
    return $self;
}

sub event_read {
    my $self = shift;
    # accept as many connections as we can
    while (my ($csock, $peeraddr) = $self->{sock}->accept) {
        IO::Handle::blocking($csock, 0);
        my $client = Mogstored::SideChannelClient->new($csock);
        $client->watch_read(1);
    }
}

1;
