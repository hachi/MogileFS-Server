package MogileFS::Connection::Worker;
# This class maintains a connection to one of the various classes of
# workers.

use strict;
use Danga::Socket ();
use base qw{Danga::Socket};

use fields qw{read_buf job pid cmd_buf reqid};

sub new {
    my MogileFS::Connection::Worker $self = shift;
    $self = fields::new($self) unless ref $self;
    $self->SUPER::new( @_ );

    # mark as not a worker by default
    $self->{pid} = 0;
    $self->{reqid} = 0;
    $self->{job} = undef;
    $self->{cmd_buf} = [];

    return $self;
}

sub event_read {
    my MogileFS::Connection::Worker $self = shift;

    my $bref = $self->read(1024);
    return $self->close() unless defined $bref;
    $self->{read_buf} .= $$bref;

    while ($self->{read_buf} =~ s/^(.+?)\r?\n//) {
        my $line = $1;
        if ($self->job eq 'queryworker' && (substr($line, 0, 5) ne 'error')) {
            MogileFS::ProcManager->HandleQueryWorkerResponse($self, $line);
        } else {
            MogileFS::ProcManager->HandleChildRequest($self, $line);
        }
    }
}

sub job {
    my MogileFS::Connection::Worker $self = shift;
    return $self->{job} unless @_;
    return $self->{job} = shift;
}

sub pid {
    my MogileFS::Connection::Worker $self = shift;
    return $self->{pid} unless @_;
    return $self->{pid} = shift;
}

sub event_hup { my $self = shift; $self->close; }

sub close {
    # mark us as being dead
    my MogileFS::Connection::Worker $self = shift;
    MogileFS::ProcManager->NoteDeadWorkerConn($self);
    $self->SUPER::close(@_);
}

sub enqueue_line {
    my MogileFS::Connection::Worker $self = $_[0];
    return if $self->job eq 'queryworker'; # they don't use this queueing
    my $msg = "$_[1]\r\n";
    push @{$self->{cmd_buf}}, $msg;
}

sub drain_queue {
    my MogileFS::Connection::Worker $worker = $_[0];
    foreach my $cmd (@{$worker->{cmd_buf}}) {
        $worker->write($cmd);
    }
    $worker->write(".\r\n");
    $worker->{cmd_buf} = [];
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
