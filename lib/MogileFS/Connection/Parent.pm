package MogileFS::Connection::Parent;
# maintains a connection in a worker process to the parent ProcManager process
# Only used by workers that use the Danga::Socket->EventLoop internally
# currently only Monitor
use warnings;
use strict;
use Danga::Socket ();
use base qw{Danga::Socket};
use fields qw(worker);

sub new {
    my ($self, $worker) = @_;
    $self = fields::new($self) unless ref $self;
    $self->SUPER::new($worker->psock);
    $self->{worker} = $worker;

    return $self;
}

sub ping {
    my ($self) = @_;

    $self->write(":ping\r\n");
}

sub event_read {
    my ($self) = @_;

    $self->{worker}->read_from_parent;
}

sub event_hup { $_[0]->close }
sub event_err { $_[0]->close }

1;
