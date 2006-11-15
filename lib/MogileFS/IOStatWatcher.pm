package MogileFS::IOStatWatcher;
use strict;
use Danga::Socket;

sub new {
    my ($class) = @_;
    return bless {}, $class;
}

sub set_hosts {
    my ($self, @ips) = @_;
    # TODO: start/remove new Danga::Socket-based client sockets to
    # new/old @ips.

}

sub on_line {
    my ($self, $cb) = @_;
    $self->{on_line} = $cb;
}

sub run_event_loop_a_bit {
    my $self = shift;

}

1;

