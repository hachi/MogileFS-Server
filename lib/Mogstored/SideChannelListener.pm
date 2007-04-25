package Mogstored::SideChannelListener;
use strict;
use base 'Perlbal::TCPListener';
use Mogstored::SideChannelClient;

sub new {
    my ($class, $hostport) = @_;
    # we don't _really_ need this, but TCPListener kinda does, to keep it from
    # exploding/warning.  so we created this stub service above in our static
    # config, just for this.
    my $svc    = Perlbal->service("mgmt") or die "Where is mgmt service?";
    return $class->SUPER::new($hostport, $svc);
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
