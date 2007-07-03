package Gearman::Server::Job;
use strict;
use Sys::Hostname;

use fields (
            'func',
            'uniq',
            'argref',
            'listeners',  # arrayref of interested Clients
            'worker',
            'handle',
            'status',  # [1, 100]
            'require_listener',
            'server',  # Gearman::Server that owns us
            );

sub new {
    my Gearman::Server::Job $self = shift;
    my ($server, $func, $uniq, $argref, $highpri) = @_;

    $self = fields::new($self) unless ref $self;

    # if they specified a uniq, see if we have a dup job running already
    # to merge with
    if (length($uniq)) {
        # a unique value of "-" means "use my args as my unique key"
        $uniq = $$argref if $uniq eq "-";
        if (my $job = $server->job_of_unique($func, $uniq)) {
            # found a match
            return $job;
        }
        # create a new key
        $server->set_unique_job($func, $uniq => $self);
    }

    $self->{'server'} = $server;
    $self->{'func'}   = $func;
    $self->{'uniq'}   = $uniq;
    $self->{'argref'} = $argref;
    $self->{'require_listener'} = 1;
    $self->{'listeners'} = [];
    $self->{'handle'}  = $server->new_job_handle;

    $server->enqueue_job($self, $highpri);
    return $self;
}

sub add_listener {
    my Gearman::Server::Job $self = shift;
    my Gearman::Server::Client $li = shift;

    push @{$self->{listeners}}, $li;
    Scalar::Util::weaken($self->{listeners}->[-1]);
}

sub relay_to_listeners {
    my Gearman::Server::Job $self = shift;
    foreach my Gearman::Server::Client $c (@{$self->{listeners}}) {
        next if !$c || $c->{closed};
        $c->write($_[0]);
    }
}

sub clear_listeners {
    my Gearman::Server::Job $self = shift;
    $self->{listeners} = [];
}

sub note_finished {
    my Gearman::Server::Job $self = shift;
    my $success = shift;

    $self->{server}->note_job_finished($self);

    if ($Gearmand::graceful_shutdown) {
        Gearmand::shutdown_if_calm();
    }
}

# accessors:
sub worker {
    my Gearman::Server::Job $self = shift;
    return $self->{'worker'} unless @_;
    return $self->{'worker'} = shift;
}
sub require_listener {
    my Gearman::Server::Job $self = shift;
    return $self->{'require_listener'} unless @_;
    return $self->{'require_listener'} = shift;
}

# takes arrayref of [numerator,denominator]
sub status {
    my Gearman::Server::Job $self = shift;
    return $self->{'status'} unless @_;
    return $self->{'status'} = shift;
}

sub handle {
    my Gearman::Server::Job $self = shift;
    return $self->{'handle'};
}

sub func {
    my Gearman::Server::Job $self = shift;
    return $self->{'func'};
}

sub argref {
    my Gearman::Server::Job $self = shift;
    return $self->{'argref'};
}

1;
