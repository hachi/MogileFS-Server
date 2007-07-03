package Gearman::Client::Async::Connection;
use strict;
use warnings;

use Danga::Socket;
use base 'Danga::Socket';
use fields (
            'state',       # one of 3 state constants below
            'waiting',     # hashref of $handle -> [ Task+ ]
            'need_handle', # arrayref of Gearman::Task objects which
                           # have been submitted but need handles.
            'parser',      # parser object
            'hostspec',    # scalar: "host:ip"
            'deadtime',    # unixtime we're marked dead until.
            'task2handle', # hashref of stringified Task -> scalar handle
            'on_ready',    # arrayref of on_ready callbacks to run on connect success
            'on_error',    # arrayref of on_error callbacks to run on connect failure
            't_offline',   # bool: fake being off the net for purposes of connecting, to force timeout
            );

our $T_ON_TIMEOUT;

use constant S_DISCONNECTED => \ "disconnected";
use constant S_CONNECTING   => \ "connecting";
use constant S_READY        => \ "ready";

use Carp qw(croak);
use Gearman::Task;
use Gearman::Util;
use Scalar::Util qw(weaken);

use IO::Handle;
use Socket qw(PF_INET IPPROTO_TCP TCP_NODELAY SOL_SOCKET SOCK_STREAM);

sub DEBUGGING () { 0 }

sub new {
    my Gearman::Client::Async::Connection $self = shift;

    my %opts = @_;

    $self = fields::new( $self ) unless ref $self;

    my $hostspec         = delete( $opts{hostspec} ) or
        croak("hostspec required");

    if (ref $hostspec eq 'GLOB') {
        # In this case we have been passed a globref, hopefully a socket that has already
        # been connected to the Gearman server in some way.
        $self->SUPER::new($hostspec);
        $self->{state}       = S_CONNECTING;
        $self->{parser} = Gearman::ResponseParser::Async->new( $self );
        $self->watch_write(1);
    } elsif (ref $hostspec && $hostspec->can("to_inprocess_server")) {
        # In this case we have been passed an object that looks like a Gearman::Server,
        # which we can just call "to_inprocess_server" on to get a socketpair connecting
        # to it.
        my $sock = $hostspec->to_inprocess_server;
        $self->SUPER::new($sock);
        $self->{state}       = S_CONNECTING;
        $self->{parser} = Gearman::ResponseParser::Async->new( $self );
        $self->watch_write(1);
    }else {
        $self->{state}       = S_DISCONNECTED;
    }

    $self->{hostspec}    = $hostspec;
    $self->{waiting}     = {};
    $self->{need_handle} = [];
    $self->{deadtime}    = 0;
    $self->{on_ready}    = [];
    $self->{on_error}    = [];
    $self->{task2handle} = {};

    croak "Unknown parameters: " . join(", ", keys %opts) if %opts;
    return $self;
}

sub close_when_finished {
    my Gearman::Client::Async::Connection $self = shift;
    # FIXME: implement
}

sub hostspec {
    my Gearman::Client::Async::Connection $self = shift;

    return $self->{hostspec};
}

sub connect {
    my Gearman::Client::Async::Connection $self = shift;

    $self->{state} = S_CONNECTING;

    my ($host, $port) = split /:/, $self->{hostspec};
    $port ||= 7003;

    warn "Connecting to $self->{hostspec}\n" if DEBUGGING;

    socket my $sock, PF_INET, SOCK_STREAM, IPPROTO_TCP;
    IO::Handle::blocking($sock, 0);
    setsockopt($sock, IPPROTO_TCP, TCP_NODELAY, pack("l", 1)) or die;

    unless ($sock && defined fileno($sock)) {
        warn( "Error creating socket: $!\n" );
        return undef;
    }

    $self->SUPER::new( $sock );
    $self->{parser} = Gearman::ResponseParser::Async->new( $self );

    eval {
        connect $sock, Socket::sockaddr_in($port, Socket::inet_aton($host));
    };
    if ($@) {
        $self->on_connect_error;
        return;
    }

    Danga::Socket->AddTimer(0.25, sub {
        return unless $self->{state} == S_CONNECTING;
        $T_ON_TIMEOUT->() if $T_ON_TIMEOUT;
        $self->on_connect_error;
    });

    # unless we're faking being offline for the test suite, connect and watch
    # for writabilty so we know the connect worked...
    unless ($self->{t_offline}) {
        $self->watch_write(1);
    }
}

sub event_write {
    my Gearman::Client::Async::Connection $self = shift;

    if ($self->{state} == S_CONNECTING) {
        $self->{state} = S_READY;
        $self->watch_read(1);
        warn "$self->{hostspec} connected and ready.\n" if DEBUGGING;
        $_->() foreach @{$self->{on_ready}};
        $self->destroy_callbacks;
    }

    $self->watch_write(0) if $self->write(undef);
}

sub destroy_callbacks {
    my Gearman::Client::Async::Connection $self = shift;
    $self->{on_ready} = [];
    $self->{on_error} = [];
}

sub event_read {
    my Gearman::Client::Async::Connection $self = shift;

    my $input = $self->read( 128 * 1024 );
    unless (defined $input) {
        $self->mark_dead if $self->stuff_outstanding;
        $self->close( "EOF" );
        return;
    }

    $self->{parser}->parse_data( $input );
}

sub event_err {
    my Gearman::Client::Async::Connection $self = shift;

    my $was_connecting = ($self->{state} == S_CONNECTING);

    if ($was_connecting && $self->{t_offline}) {
        $self->SUPER::close( "error" );
        return;
    }

    $self->mark_dead;
    $self->close( "error" );
    $self->on_connect_error if $was_connecting;
}

sub on_connect_error {
    my Gearman::Client::Async::Connection $self = shift;
    warn "Jobserver, $self->{hostspec} ($self) has failed to connect properly\n" if DEBUGGING;

    $self->mark_dead;
    $self->close( "error" );
    $_->() foreach @{$self->{on_error}};
    $self->destroy_callbacks;
}

sub close {
    my Gearman::Client::Async::Connection $self = shift;
    my $reason = shift;

    if ($self->{state} != S_DISCONNECTED) {
        $self->{state} = S_DISCONNECTED;
        $self->SUPER::close( $reason );
    }

    $self->_requeue_all;
}

sub mark_dead {
    my Gearman::Client::Async::Connection $self = shift;
    $self->{deadtime} = time + 10;
    warn "$self->{hostspec} marked dead for a bit." if DEBUGGING;
}

sub alive {
    my Gearman::Client::Async::Connection $self = shift;
    return $self->{deadtime} <= time;
}

sub add_task {
    my Gearman::Client::Async::Connection $self = shift;
    my Gearman::Task $task = shift;

    Carp::confess("add_task called when in wrong state")
        unless $self->{state} == S_READY;

    warn "writing task $task to $self->{hostspec}\n" if DEBUGGING;

    $self->write( $task->pack_submit_packet );
    push @{$self->{need_handle}}, $task;
    Scalar::Util::weaken($self->{need_handle}->[-1]);
}

sub stuff_outstanding {
    my Gearman::Client::Async::Connection $self = shift;
    return
        @{$self->{need_handle}} ||
        %{$self->{waiting}};
}

sub _requeue_all {
    my Gearman::Client::Async::Connection $self = shift;

    my $need_handle = $self->{need_handle};
    my $waiting     = $self->{waiting};

    $self->{need_handle} = [];
    $self->{waiting}     = {};

    while (@$need_handle) {
        my $task = shift @$need_handle;
        warn "Task $task in need_handle queue during socket error, queueing for redispatch\n" if DEBUGGING;
        $task->fail if $task;
    }

    while (my ($shandle, $tasklist) = each( %$waiting )) {
        foreach my $task (@$tasklist) {
            warn "Task $task ($shandle) in waiting queue during socket error, queueing for redispatch\n" if DEBUGGING;
            $task->fail;
        }
    }
}

sub process_packet {
    my Gearman::Client::Async::Connection $self = shift;
    my $res = shift;

    warn "Got packet '$res->{type}' from $self->{hostspec}\n" if DEBUGGING;

    if ($res->{type} eq "job_created") {

        die "Um, got an unexpected job_created notification" unless @{ $self->{need_handle} };
        my Gearman::Task $task = shift @{ $self->{need_handle} } or
            return 1;


        my $shandle = ${ $res->{'blobref'} };
        if ($task) {
            $self->{task2handle}{"$task"} = $shandle;
            push @{ $self->{waiting}->{$shandle} ||= [] }, $task;
        }
        return 1;
    }

    if ($res->{type} eq "work_fail") {
        my $shandle = ${ $res->{'blobref'} };
        $self->_fail_jshandle($shandle);
        return 1;
    }

    if ($res->{type} eq "work_complete") {
        ${ $res->{'blobref'} } =~ s/^(.+?)\0//
            or die "Bogus work_complete from server";
        my $shandle = $1;

        my $task_list = $self->{waiting}{$shandle} or
            return;

        my Gearman::Task $task = shift @$task_list or
            return;

        $task->complete($res->{'blobref'});

        unless (@$task_list) {
            delete $self->{waiting}{$shandle};
            delete $self->{task2handle}{"$task"};
        }

        warn "Jobs: " . scalar( keys( %{$self->{waiting}} ) ) . "\n" if DEBUGGING;

        return 1;
    }

    if ($res->{type} eq "work_status") {
        my ($shandle, $nu, $de) = split(/\0/, ${ $res->{'blobref'} });

        my $task_list = $self->{waiting}{$shandle} or
            return;

        foreach my Gearman::Task $task (@$task_list) {
            $task->status($nu, $de);
        }

        return 1;
    }

    die "Unknown/unimplemented packet type: $res->{type}";

}

sub give_up_on {
    my Gearman::Client::Async::Connection $self = shift;
    my $task = shift;

    my $shandle = $self->{task2handle}{"$task"} or return;
    my $task_list = $self->{waiting}{$shandle} or return;
    @$task_list = grep { $_ != $task } @$task_list;
    unless (@$task_list) {
        delete $self->{waiting}{$shandle};
    }

}

# note the failure of a task given by its jobserver-specific handle
sub _fail_jshandle {
    my Gearman::Client::Async::Connection $self = shift;
    my $shandle = shift;

    my $task_list = $self->{waiting}->{$shandle} or
        return;

    my Gearman::Task $task = shift @$task_list or
        return;

    # cleanup
    unless (@$task_list) {
        delete $self->{task2handle}{"$task"};
        delete $self->{waiting}{$shandle};
    }

    $task->fail;
}

sub get_in_ready_state {
    my ($self, $on_ready, $on_error) = @_;

    if ($self->{state} == S_READY) {
        $on_ready->();
        return;
    }

    push @{$self->{on_ready}}, $on_ready if $on_ready;
    push @{$self->{on_error}}, $on_error if $on_error;

    $self->connect if $self->{state} == S_DISCONNECTED;
}

sub t_set_offline {
    my ($self, $val) = @_;
    $val = 1 unless defined $val;
    $self->{t_offline} = $val;
}

package Gearman::ResponseParser::Async;

use strict;
use warnings;
use Scalar::Util qw(weaken);

use Gearman::ResponseParser;
use base 'Gearman::ResponseParser';

sub new {
    my $class = shift;

    my $self = $class->SUPER::new;

    $self->{_conn} = shift;
    weaken($self->{_conn});

    return $self;
}

sub on_packet {
    my $self = shift;
    my $packet = shift;

    return unless $self->{_conn};
    $self->{_conn}->process_packet( $packet );
}

sub on_error {
    my $self = shift;

    return unless $self->{_conn};
    $self->{_conn}->mark_unsafe;
    $self->{_conn}->close;
}

1;
