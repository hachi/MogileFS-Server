package Gearman::Client::Async;

=head1 NAME

Gearman::Client::Async - Asynchronous client module for Gearman for Danga::Socket applications

=head1 SYNOPSIS

    use Gearman::Client::Async;

    # Instantiate a new Gearman::Client::Async object.
    $client = Gearman::Client::Async->new(
        job_servers => [ '127.0.0.1', '192.168.0.1:123' ],
    );

    # Overwrite job server list with a new one.
    $client->set_job_servers( '10.0.0.1' );

    # Read list of job servers out of the client.
    $arrayref = $client->job_servers;
    @array = $client->job_servers;

    # Start a task
    $task = Gearman::Task->new(...); # with callbacks, etc
    $client->add_task( $task );

=head1 COPYRIGHT

Copyright 2006 Six Apart, Ltd.

License granted to use/distribute under the same terms as Perl itself.

=head1 WARRANTY

This is free software.  This comes with no warranty whatsoever.

=head1 AUTHORS

 Brad Fitzpatrick (brad@danga.com)
 Jonathan Steinert (hachi@cpan.org)

=cut

use strict;
use warnings;
use Carp qw(croak);

use fields (
            'job_servers',   # arrayref of Gearman::Client::Async::Connection objects
            't_no_random',   # don't randomize job server to use:  use first alive one.
            't_offline_host', # hashref: hostname -> $bool, if host should act as offline, for testing
            );

use Danga::Socket 1.52;
use Gearman::Objects;
use Gearman::Task;
use Gearman::JobStatus;
use Gearman::Client::Async::Connection;

use List::Util qw(first);
use vars qw($VERSION);

$VERSION = "0.94";

sub DEBUGGING () { 0 }

sub new {
    my ($class, %opts) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{job_servers}    = [];
    $self->{t_offline_host} = {};

    my $js = delete $opts{job_servers};
    $self->set_job_servers(@$js) if $js;

    croak "Unknown parameters: " . join(", ", keys %opts) if %opts;
    return $self;
}

# for testing.
sub t_set_disable_random {
    my $self = shift;
    $self->{t_no_random} = shift;
}

sub t_set_offline_host {
    my ($self, $host, $val) = @_;
    $val = 1 unless defined $val;
    $self->{t_offline_host}{$host} = $val;

    my $conn = first { $_->hostspec eq $host } @{ $self->{job_servers} }
        or die "No host found with that spec to mark offline";

    $conn->t_set_offline($val);
}

# set job servers, without shutting down dups, and shutting down old ones gracefully
sub set_job_servers {
    my Gearman::Client::Async $self = shift;

    my %being_set; # hostspec -> 1
    %being_set = map { $_, 1 } @_;

    my %exist;   # hostspec -> existing conn
    foreach my $econn (@{ $self->{job_servers} }) {
        my $spec = $econn->hostspec;
        if ($being_set{$spec}) {
            $exist{$spec} = $econn;
        } else {
            $econn->close_when_finished;
        }
    }

    my @newlist;
    foreach (@_) {
        push @newlist, $exist{$_} || Gearman::Client::Async::Connection->new( hostspec => $_ );
    }
    $self->{job_servers} = \@newlist;
}

# getter
sub job_servers {
    my Gearman::Client::Async $self = shift;
    croak "Not a setter" if @_;
    my @list = map { $_->hostspec } @{ $self->{job_servers} };
    return wantarray ? @list : \@list;
}

sub add_task {
    my Gearman::Client::Async $self = shift;
    my Gearman::Task $task = shift;

    my $try_again;
    $try_again = sub {

        my @job_servers = grep { $_->alive } @{$self->{job_servers}};
        warn "Alive servers: " . @job_servers . " out of " . @{$self->{job_servers}} . "\n" if DEBUGGING;
        unless (@job_servers) {
            $task->final_fail;
            $try_again = undef;
            return;
        }

        my $js;
        if (defined( my $hash = $task->hash )) {
            # Task is hashed, use key to fetch job server
            $js = @job_servers[$hash % @job_servers];
        }
        else {
            # Task is not hashed, random job server
            $js = @job_servers[$self->{t_no_random} ? 0 :
                               int( rand( @job_servers ))];
        }

        # TODO Fix this violation of object privacy.
        $task->{taskset} = $self;

        $js->get_in_ready_state(
                                # on_ready:
                                sub {
                                    my $timer;
                                    if (my $timeout = $task->{timeout}) {
                                        $timer = Danga::Socket->AddTimer($timeout, sub {
                                            $task->final_fail('timeout');
                                        });
                                    }
                                    $task->set_on_post_hooks(sub {
                                        $timer->cancel if $timer;

                                        # ALSO clean up our $js (connection's) waiting stuff:
                                        $js->give_up_on($task);
                                    });
                                    $js->add_task( $task );
                                    $try_again = undef;
                                },
                                # on_error:
                                $try_again,
                                );
    };
    $try_again->();
}

# Gearman::Client::Async sometimes fakes itself duck-typing style as a
# Gearman::Taskset, since a task"set" makes no sense in an async
# world, where there's no need to wait on a set of things... since
# everything happens at its own pace.  so for duck-typing reasons (or,
# er, "implementing an interface", say), we need to implement a the
# "taskset client method" but in our case, that's just us.
sub client { $_[0] }

# as a Gearman::Client-like thing, we'll be asked for our prefix, which this module
# currently doesn't support, but the base Gearman libraries expect.
sub prefix { "" }


1;
