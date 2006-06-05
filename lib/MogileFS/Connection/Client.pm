# A client is a user connection for sending requests to us.  Requests
# can either be normal user requests to be sent to a QueryWorker
# or management requests that start with a !.

package MogileFS::Connection::Client;

use Danga::Socket ();
use base qw{Danga::Socket};

use fields qw{read_buf};

sub new {
    my $self = shift;
    $self = fields::new($self) unless ref $self;
    $self->SUPER::new( @_ );
    $self->watch_read(1);
    return $self;
}

# Client
sub event_read {
    my MogileFS::Connection::Client $self = shift;

    my $bref = $self->read(1024);
    return $self->close() unless defined $bref;
    $self->{read_buf} .= $$bref;

    while ($self->{read_buf} =~ s/^(.*?)\r?\n//) {
        next unless length $1;
        MogileFS::ProcManager->HandleClientRequest($self, $1);
    }
}

# Client
sub event_err { my $self = shift; $self->close; }
sub event_hup { my $self = shift; $self->close; }

# just note that we've died
sub close {
    # mark us as being dead
    my $self = shift;
    MogileFS::ProcManager->NoteDeadClient($self);
    $self->SUPER::close(@_);
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
