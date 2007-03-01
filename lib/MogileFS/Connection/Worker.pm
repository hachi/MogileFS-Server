package MogileFS::Connection::Worker;
# This class maintains a connection to one of the various classes of
# workers.

use strict;
use Danga::Socket ();
use base qw{Danga::Socket};

use fields (
            'read_buf',
            'job',
            'pid',
            'reqid',
            'last_alive',  # unixtime
            );

sub new {
    my MogileFS::Connection::Worker $self = shift;
    $self = fields::new($self) unless ref $self;
    $self->SUPER::new( @_ );

    $self->{pid}        = 0;
    $self->{reqid}      = 0;
    $self->{job}        = undef;
    $self->{last_alive} = time();

    return $self;
}

sub note_alive {
    my $self = shift;
    $self->{last_alive} = time();
}

sub watchdog_check {
    my MogileFS::Connection::Worker $self = shift;

    my $timeout               = $self->worker_class->watchdog_timeout;
    my $time_since_last_alive = time() - $self->{last_alive};
    return $time_since_last_alive < $timeout;
}

sub event_read {
    my MogileFS::Connection::Worker $self = shift;

    # if we read data from it, it's not blocked on something else.
    $self->note_alive;

    my $bref = $self->read(1024);
    return $self->close() unless defined $bref;
    $self->{read_buf} .= $$bref;

    while ($self->{read_buf} =~ s/^(.+?)\r?\n//) {
        my $line = $1;
        if ($self->job eq 'queryworker' && $line !~ /^(?:\:|error|debug)/) {
            MogileFS::ProcManager->HandleQueryWorkerResponse($self, $line);
        } else {
            MogileFS::ProcManager->HandleChildRequest($self, $line);
        }
    }
}

sub event_write {
    my $self = shift;
    my $done = $self->write(undef);
    $self->watch_write(0) if $done;
}

sub job {
    my MogileFS::Connection::Worker $self = shift;
    return $self->{job} unless @_;
    return $self->{job} = shift;
}

sub worker_class {
    my MogileFS::Connection::Worker $self = shift;
    return MogileFS::ProcManager->job_to_class($self->{job});
}

sub pid {
    my MogileFS::Connection::Worker $self = shift;
    return $self->{pid} unless @_;
    return $self->{pid} = shift;
}

sub event_hup { my $self = shift; $self->close; }
sub event_err { my $self = shift; $self->close; }

sub close {
    # mark us as being dead
    my MogileFS::Connection::Worker $self = shift;
    MogileFS::ProcManager->NoteDeadWorkerConn($self);
    $self->SUPER::close(@_);
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
