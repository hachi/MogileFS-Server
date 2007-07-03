package Gearman::Server;
use strict;
use Gearman::Server::Client;
use Gearman::Server::Job;
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET SOCK_STREAM AF_UNIX SOCK_STREAM PF_UNSPEC);
use Carp qw(croak);
use Sys::Hostname ();

use fields (
            'client_map',    # fd -> Client
            'sleepers',      # func -> { "Client=HASH(0xdeadbeef)" => Client }
            'job_queue',     # job_name -> [Job, Job*]  (key only exists if non-empty)
            'job_of_handle', # handle -> Job
            'max_queue',     # func -> configured max jobqueue size
            'job_of_uniq',   # func -> uniq -> Job
            'handle_ct',     # atomic counter
            'handle_base',   # atomic counter
            );

our $VERSION = "1.09";

sub new {
    my ($class, %opts) = @_;
    my $self = ref $class ? $class : fields::new($class);

    $self->{client_map}    = {};
    $self->{sleepers}      = {};
    $self->{job_queue}     = {};
    $self->{job_of_handle} = {};
    $self->{max_queue}     = {};
    $self->{job_of_uniq}   = {};

    $self->{handle_ct} = 0;
    $self->{handle_base} = "H:" . Sys::Hostname::hostname() . ":";

    my $port = delete $opts{port};
    croak("Unknown options") if %opts;
    $self->create_listening_sock($port);

    return $self;
}

sub debug {
    my ($self, $msg) = @_;
    #warn "$msg\n";
}

sub create_listening_sock {
    my ($self, $portnum) = @_;
    my $ssock = IO::Socket::INET->new(LocalPort => $portnum,
                                      Type      => SOCK_STREAM,
                                      Proto     => IPPROTO_TCP,
                                      Blocking  => 0,
                                      Reuse     => 1,
                                      Listen    => 10 )
        or die "Error creating socket: $@\n";

    $self->setup_listening_sock($ssock);
    return $ssock;
}

sub setup_listening_sock {
    my ($self, $ssock) = @_;

    # make sure provided listening socket is non-blocking
    IO::Handle::blocking($ssock, 0);
    Danga::Socket->AddOtherFds(fileno($ssock) => sub {
        my $csock = $ssock->accept
            or return;

        $self->debug(sprintf("Listen child making a Client for %d.", fileno($csock)));

        IO::Handle::blocking($csock, 0);
        setsockopt($csock, IPPROTO_TCP, TCP_NODELAY, pack("l", 1)) or die;

        $self->new_client($csock);
    });
}

sub new_client {
    my ($self, $sock) = @_;
    my $client = Gearman::Server::Client->new($sock, $self);
    $client->watch_read(1);
    $self->{client_map}{$client->{fd}} = $client;
}

sub note_disconnected_client {
    my ($self, $client) = @_;
    delete $self->{client_map}{$client->{fd}};
}

sub clients {
    my $self = shift;
    return values %{ $self->{client_map} };
}

# Returns a socket that is connected to the server, we can then use this
# socket with a Gearman::Client::Async object to run clients and servers in the
# same thread.
sub to_inprocess_server {
    my $self = shift;

    my ($psock, $csock);
    socketpair($csock, $psock, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or  die "socketpair: $!";

    $csock->autoflush(1);
    $psock->autoflush(1);

    my $client = Gearman::Server::Client->new($csock, $self);

    my ($package, $file, $line) = caller;
    $client->{peer_ip}  = "[$package|$file|$line]";
    $client->watch_read(1);
    $self->{client_map}{$client->{fd}} = $client;

    return $psock;
}

sub start_worker {
    my ($self, $prog) = @_;

    my ($psock, $csock);
    socketpair($csock, $psock, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or  die "socketpair: $!";

    $csock->autoflush(1);
    $psock->autoflush(1);

    my $pid = fork;
    unless (defined $pid) {
        warn "fork failed: $!\n";
        return undef;
    }

    # child process
    unless ($pid) {
        local $ENV{'GEARMAN_WORKER_USE_STDIO'} = 1;
        close(STDIN);
        close(STDOUT);
        open(STDIN, '<&', $psock) or die "Unable to dup socketpair to STDIN: $!";
        open(STDOUT, '>&', $psock) or die "Unable to dup socketpair to STDOUT: $!";
        if (UNIVERSAL::isa($prog, "CODE")) {
            $prog->();
            exit 0; # shouldn't get here.  subref should exec.
        }
        exec $prog;
        die "Exec failed: $!";
    }

    close($psock);
    my $sock = $csock;

    my $client = Gearman::Server::Client->new($sock, $self);

    $client->{peer_ip}  = "[gearman_child]";
    $client->watch_read(1);
    $self->{client_map}{$client->{fd}} = $client;
    return wantarray ? ($pid, $client) : $pid;
}

sub enqueue_job {
    my ($self, $job, $highpri) = @_;
    my $jq = ($self->{job_queue}{$job->{func}} ||= []);

    if (defined (my $max_queue_size = $self->{max_queue}{$job->{func}})) {
        $max_queue_size--; # Subtract one, because we're about to add one more below.
        while (@$jq > $max_queue_size) {
            my $delete_job = pop @$jq;
            my $msg = Gearman::Util::pack_res_command("work_fail", $delete_job->handle);
            $delete_job->relay_to_listeners($msg);
            $delete_job->note_finished;
        }
    }

    if ($highpri) {
        unshift @$jq, $job;
    } else {
        push @$jq, $job;
    }

    $self->{job_of_handle}{$job->{'handle'}} = $job;
}

sub wake_up_sleepers {
    my ($self, $func) = @_;
    my $sleepmap = $self->{sleepers}{$func} or return;

    foreach my $addr (keys %$sleepmap) {
        my Gearman::Server::Client $c = $sleepmap->{$addr};
        next if $c->{closed} || ! $c->{sleeping};
        $c->res_packet("noop");
        $c->{sleeping} = 0;
    }

    delete $self->{sleepers}{$func};
    return;
}

sub on_client_sleep {
    my ($self, $cl) = @_;

    foreach my $cd (@{$cl->{can_do_list}}) {
        # immediately wake the sleeper up if there are things to be done
        if ($self->{job_queue}{$cd}) {
            $cl->res_packet("noop");
            $cl->{sleeping} = 0;
            return;
        }

        my $sleepmap = ($self->{sleepers}{$cd} ||= {});
        $sleepmap->{"$cl"} ||= $cl;
    }
}

sub jobs_outstanding {
    my Gearman::Server $self = shift;
    return scalar keys %{ $self->{job_queue} };
}

sub jobs {
    my Gearman::Server $self = shift;
    return values %{ $self->{job_of_handle} };
}

sub job_by_handle {
    my ($self, $handle) = @_;
    return $self->{job_of_handle}{$handle};
}

sub note_job_finished {
    my Gearman::Server $self = shift;
    my Gearman::Server::Job $job = shift;

    if (length($job->{uniq})) {
        delete $self->{job_of_uniq}{$job->{func}}{$job->{uniq}};
    }
    delete $self->{job_of_handle}{$job->{handle}};
}

# <0/undef/"" to reset.  else integer max depth.
sub set_max_queue {
    my ($self, $func, $max) = @_;
    if (defined $max && length $max && $max >= 0) {
        $self->{max_queue}{$func} = int($max);
    } else {
        delete $self->{max_queue}{$func};
    }
}

sub new_job_handle {
    my $self = shift;
    return $self->{handle_base} . (++$self->{handle_ct});
}

sub job_of_unique {
    my ($self, $func, $uniq) = @_;
    return undef unless $self->{job_of_uniq}{$func};
    return $self->{job_of_uniq}{$func}{$uniq};
}

sub set_unique_job {
    my ($self, $func, $uniq, $job) = @_;
    $self->{job_of_uniq}{$func} ||= {};
    $self->{job_of_uniq}{$func}{$uniq} = $job;
}

sub grab_job {
    my ($self, $func) = @_;
    return undef unless $self->{job_queue}{$func};

    my $empty = sub {
        delete $self->{job_queue}{$func};
        return undef;
    };

    my Gearman::Server::Job $job;
    while (1) {
        $job = shift @{$self->{job_queue}{$func}};
        return $empty->() unless $job;
        return $job unless $job->{require_listener};

        foreach my Gearman::Server::Client $c (@{$job->{listeners}}) {
            return $job if $c && ! $c->{closed};
        }
        $job->note_finished(0);
    }
}


1;
__END__

=head1 NAME

Gearman::Server - function call "router" and load balancer

=head1 DESCRIPTION

You run a Gearman server (or more likely, many of them for both
high-availability and load balancing), then have workers (using
L<Gearman::Worker> from the Gearman module, or libraries for other
languages) register their ability to do certain functions to all of
them, and then clients (using L<Gearman::Client>,
L<Gearman::Client::Async>, etc) request work to be done from one of
the Gearman servers.

The servers connect them, routing function call requests to the
appropriate workers, multiplexing responses to duplicate requests as
requested, etc.

More than likely, you want to use the provided L<gearmand> wrapper
script, and not use Gearman::Server directly.

=head1 SEE ALSO

L<gearmand>

=cut
