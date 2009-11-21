# A client is a user connection for sending requests to us.  Requests
# can either be normal user requests to be sent to a QueryWorker
# or management requests that start with a !.

package MogileFS::Connection::Client;

use strict;
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
    return $self->close unless defined $bref;
    $self->{read_buf} .= $$bref;

    while ($self->{read_buf} =~ s/^(.*?)\r?\n//) {
        next unless length $1;
        $self->handle_request($1);
    }
}

sub handle_request {
    my ($self, $line) = @_;

    # if it's just 'help', 'h', '?', or something, do that
    #if ((substr($line, 0, 1) eq '?') || ($line eq 'help')) {
    #    MogileFS::ProcManager->SendHelp($_[1]);
    #    return;
    #}

    if ($line =~ /^!(\S+)(?:\s+(.+))?$/) {
        my ($cmd, $args) = ($1, $2);
        return $self->handle_admin_command($cmd, $args);
    }

    MogileFS::ProcManager->EnqueueCommandRequest($line, $self);
}

sub handle_admin_command {
    my ($self, $cmd, $args) = @_;

    my @out;
    if ($cmd =~ /^stats$/) {
        # print out some stats on the queues
        my $uptime = time() - MogileFS::ProcManager->server_starttime;
        my $ccount = MogileFS::ProcManager->PendingQueryCount;
        my $wcount = MogileFS::ProcManager->BoredQueryWorkerCount;
        my $ipcount = MogileFS::ProcManager->QueriesInProgressCount;
        my $stats = MogileFS::ProcManager->StatsHash;
        push @out, "uptime $uptime",
        "pending_queries $ccount",
        "processing_queries $ipcount",
        "bored_queryworkers $wcount",
        map { "$_ $stats->{$_}" } sort keys %$stats;

    } elsif ($cmd =~ /^shutdown/) {
        print "User requested shutdown: $args\n";
        kill 15, $$; # kill us, that kills our kids

    } elsif ($cmd =~ /^jobs/) {
        # dump out a list of running jobs and pids
        MogileFS::ProcManager->foreach_job(sub {
            my ($job, $ct, $desired, $pidlist) = @_;
            push @out, "$job count $ct";
            push @out, "$job desired $desired";
            push @out, "$job pids " . join(' ', @$pidlist);
        });

    } elsif ($cmd =~ /^want/) {
        # !want <count> <jobclass>
        # set the new desired staffing level for a class
        if ($args =~ /^(\d+)\s+(\S+)/) {
            my ($count, $job) = ($1, $2);

            $count = 500 if $count > 500;

            # now make sure it's a real job
            if (MogileFS::ProcManager->is_valid_job($job)) {
                MogileFS::ProcManager->request_job_process($job, $count);
                push @out, "Now desiring $count children doing '$job'.";
            } else {
                my $classes = join(", ", MogileFS::ProcManager->valid_jobs);
                push @out, "ERROR: Invalid class '$job'.  Valid classes: $classes";
            }
        } else {
            push @out, "ERROR: usage: !want <count> <jobclass>";
        }

    } elsif ($cmd =~ /^to/) {
        # !to <jobclass> <message>
        # sends <message> to all children of <jobclass>
        if ($args =~ /^(\S+)\s+(.+)/) {
            my $ct = MogileFS::ProcManager->ImmediateSendToChildrenByJob($1, $2);
            push @out, "Message sent to $ct children.";

        } else {
            push @out, "ERROR: usage: !to <jobclass> <message>";
        }

    } elsif ($cmd =~ /^queue/ || $cmd =~ /^pend/) {
        MogileFS::ProcManager->foreach_pending_query(sub {
            my ($client, $query) = @_;
            push @out, $query;
        });

    } elsif ($cmd =~ /^watch/) {
        if (MogileFS::ProcManager->RemoveErrorWatcher($self)) {
            push @out, "Removed you from watcher list.";
        } else {
            MogileFS::ProcManager->AddErrorWatcher($self);
            push @out, "Added you to watcher list.";
        }

    } elsif ($cmd =~ /^recent/) {
        # show the most recent N queries
        push @out, MogileFS::ProcManager->RecentQueries;

    } elsif ($cmd =~ /^version/) {
        # show the most recent N queries
        push @out, $MogileFS::Server::VERSION;

    } else {
        MogileFS::ProcManager->SendHelp($self, $args);
    }

    $self->write(join("\r\n", @out) . "\r\n") if @out;
    $self->write(".\r\n");
    return;
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
