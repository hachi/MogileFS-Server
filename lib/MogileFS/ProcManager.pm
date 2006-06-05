package MogileFS::ProcManager;
use strict;
use POSIX;
use POSIX ":sys_wait_h"; # argument for waitpid
use Symbol;
use Socket;

# This class handles keeping lists of workers and clients and
# assigning them to eachother when things happen.  You don't actually
# instantiate a procmanager.  the class itself holds all state.

# Mappings: fd => [ clientref, jobstring, starttime ]
# queues are just lists of Client class objects
# ChildrenByJob: job => { pid => $client }
# ErrorsTo: fid => Client
# RecentQueries: [ string, string, string, ... ]
# Stats: element => number
our ($IsChild, @QueryWorkerQueue, @ClientQueue, @RecentQueries,
     %Mappings, %ChildrenByJob, %ErrorsTo, %Stats);

$IsChild = 0;  # either false if we're the parent, or a MogileFS::Worker object

# keep track of what all child pids are doing, and what jobs are being
# satisifed.
my %child  = ();    # pid -> job
my %todie  = ();    # pid -> 1 (lists pids that we've asked to die)
my %jobs   = ();    # jobname -> [ min, current ]

our $allkidsup = 0;  # if true, all our kids are running. set to 0 when a kid dies.

*error = \&Mgd::error;

sub set_min_workers {
    my ($class, $job, $min) = @_;
    $jobs{$job} ||= [undef, 0];   # [min, current]
    $jobs{$job}->[0] = $min;

    # TODO: set allkipsup false, so spawner re-checks?
}

sub child_pids {
    return keys %child;
}

# returns a sub that Danga::Socket calls after each event loop round.
# the sub must return 1 for the program to continue running.
sub PostEventLoopChecker {
    my $lastspawntime = 0; # time we last ran spawn_children sub

    return sub {
        # run only once per second
        my $now = time();
        return 1 unless $now > $lastspawntime;
        $lastspawntime = $now;

        # see if anybody has died, but don't hang up on doing so
        my $pid = waitpid -1, WNOHANG;
        return 1 if $pid <= 0 && $allkidsup;
        $allkidsup = 0; # know something died

        # when a child dies, figure out what it was doing
        # and note that job has one less worker
        my $job;
        if ($pid > -1 && ($job = delete $child{$pid})) {
            my $extra = $todie{$pid} ? "expected" : "UNEXPECTED";
            error("Child $pid ($job) died: $? ($extra)");
            MogileFS::ProcManager->NoteDeadChild($pid);

            if (my $jobstat = $jobs{$job}) {
                # if the pid is in %todie, then we have asked it to shut down
                # and have already decremented the jobstat counter and don't
                # want to do it again
                unless (my $true = delete $todie{$pid}) {
                    # decrement the count of currently running jobs
                    $jobstat->[1]--;
                }
            }
        }

        # foreach job, fork enough children
        while (my ($job, $jobstat) = each %jobs) {
            my $need = $jobstat->[0] - $jobstat->[1];
            if ($need > 0) {
                error("Job $job has only $jobstat->[1], wants $jobstat->[0], making $need.");
                for (1..$need) {
                    my $cpid = make_new_child($job);
                    return 1 unless $cpid;
                    $child{$cpid} = $job;

                    # now increase the count of processes currently doing this job
                    $jobstat->[1]++;
                }
            }
        }

        # if we got this far, all jobs have been re-created.  note that
        # so we avoid more CPU usage in this post-event-loop callback later
        $allkidsup = 1;

        # true value keeps us running:
        return 1;
    };
}

sub make_new_child {
    my $job = shift;

    my $pid;
    my $sigset;

    # block signal for fork
    $sigset = POSIX::SigSet->new(SIGINT);
    sigprocmask(SIG_BLOCK, $sigset)
        or return error("Can't block SIGINT for fork: $!");

    return error("fork failed creating $job: $!")
        unless defined ($pid = fork);

    if ($pid) {
        sigprocmask(SIG_UNBLOCK, $sigset)
            or return error("Can't unblock SIGINT for fork: $!");
        return $pid;
    }

    # as a child, we want to close these and ignore them
    Mgd::close_listeners();

    $SIG{INT} = 'DEFAULT';
    $SIG{TERM} = 'DEFAULT';
    $0 .= " [$job]";

    # unblock signals
    sigprocmask(SIG_UNBLOCK, $sigset)
        or return error("Can't unblock SIGINT for fork: $!");

    # try to create a connection to the parent.  we die here because
    # we're the child and if we can't talk to the master we really need
    # to die so that a child isn't just sitting around without communication
    # to the parent.
    my $psock = IO::Socket::INET->new(PeerAddr => "127.0.0.1",
                                      PeerPort => MogileFS->config("worker_port"),
                                      Type     => SOCK_STREAM,
                                      Proto    => 'tcp',)
        or die "Error creating socket to master: $@\n";

    # advertise our pid/job to process manager now in parent
    $psock->write("$$ $job\n");

    my $class_suffix = {
        queryworker => "Query",
        delete      => "Delete",
        replicate   => "Replicate",
        reaper      => "Reaper",
        monitor     => "Monitor",
    }->{$job} or
        die "No worker class defined for job '$job'\n";

    # now call our job function
    my $class = "MogileFS::Worker::" . $class_suffix;
    my $worker = $class->new($psock);

    # set our frontend into child mode
    MogileFS::ProcManager->SetAsChild($worker);

    $worker->work;
    exit 0;
}

# when a child is spawned, they'll have copies of all the data from the
# parent, but they don't need it.  this method is called when you want
# to indicate that this procmanager is running on a child and should clean.
sub SetAsChild {
    my ($class, $worker) = @_;

    @QueryWorkerQueue = ();
    @ClientQueue = ();
    %Mappings = ();
    $IsChild = $worker;
    %ErrorsTo = ();

    # and now kill off our event loop so that we don't waste time
    Danga::Socket->SetPostLoopCallback(sub { return 0; });
}

# called when a child has died.  a child is someone doing a job for us,
# but it might be a queryworker or any other type of job.  we just want
# to remove them from our list of children.  they're actually respawned
# by the make_new_child function elsewhere in Mgd.
sub NoteDeadChild {
    my $pid = $_[1];
    foreach my $job (keys %ChildrenByJob) {
        return if # bail out if we actually delete one
            delete $ChildrenByJob{$job}->{$pid};
    }
}

# called when a client dies.  clients are users, management or non.
# we just want to remove them from the error reporting interface, if
# they happen to be part of it.
sub NoteDeadClient {
    my $client = $_[1];
    delete $ErrorsTo{$client->{fd}};
}

# called when the error function in Mgd is called and we're in the parent,
# so it's pretty simple that basically we just spit it out to folks listening
# to errors
sub NoteError {
    return unless %ErrorsTo;

    my $msg = ":: ${$_[1]}\r\n";
    foreach my $client (values %ErrorsTo) {
        $client->write(\$msg);
    }
}

# take a new connection that we know is from one of our children, but
# we're not sure what type of child, so just set it in read mode until
# they tell us what they are
sub RegisterWorkerConn {
    my MogileFS::Connection::Worker $worker = $_[1];
    $worker->watch_read(1);
}

# take a new worker and note that it's a worker and ready to be used
# for commands.  this is called when workers connect to the parent
sub RegisterQueryWorker {
    # basically take the worker, mark it as a worker, enqueue it,
    # and then try to process the outstanding queues
    my MogileFS::Connection::Worker $worker = $_[1];
    MogileFS::ProcManager->EnqueueQueryWorker($worker);
}

# puts a worker back in the queue, deleting any outstanding jobs in
# the mapping list for this fd.
sub EnqueueQueryWorker {
    # first arg is class, second is worker
    my MogileFS::Connection::Worker $worker = $_[1];
    delete $Mappings{$worker->{fd}};

    # see if we need to kill off some workers
    if (job_needs_reduction('queryworker')) {
        Mgd::error("Reducing queryworker headcount by 1.");
        MogileFS::ProcManager->AskWorkerToDie($worker);
        return;
    }

    # must be okay, so put it in the queue
    push @QueryWorkerQueue, $worker;
    MogileFS::ProcManager->ProcessQueues;
}

# if we need to kill off a worker, this function takes in the WorkerConn
# object, tells it to die, marks us as having requested its death, and decrements
# the count of running jobs.
sub AskWorkerToDie {
    my MogileFS::Connection::Worker $worker = $_[1];
    $worker->write("shutdown\r\n");
    note_pending_death($worker->job, $worker->pid);
}

# kill bored query workers so we can get down to the level requested.  this
# continues killing until we run out of folks to kill.
sub CullQueryWorkers {
    while (@QueryWorkerQueue && job_needs_reduction('queryworker')) {
        my MogileFS::Connection::Worker $worker = shift @QueryWorkerQueue;
        MogileFS::ProcManager->AskWorkerToDie($worker);
    }
}

# called when we get a response from a worker.  this reenqueues the
# worker so it can handle another response as well as passes the answer
# back on to the client.
sub HandleQueryWorkerResponse {
    return Mgd::error("ProcManager (Child) got worker response: $_[2]") if $IsChild;

    # got a response from a worker
    my MogileFS::Connection::Worker $worker = $_[1];
    return unless $worker && $Mappings{$worker->{fd}};

    # get the client we're working with (if any)
    my $client = $Mappings{$worker->{fd}}->[0];

    # if we have no client, then we just got a standard message from
    # the queryworker and need to pass it up the line
    return MogileFS::ProcManager->HandleChildRequest($worker, $_[2]) if !$client;

    # at this point it was a command response, but if the client has gone
    # away, just reenqueue this query worker
    return MogileFS::ProcManager->EnqueueQueryWorker($worker) if $client->{closed};

    # <numeric id> [client-side time to complete] <response>
    my ($time, $id, $res);
    if ($_[2] =~ /^(\d+-\d+)\s+(\d+\.\d+)\s+(.+)$/) {
        # save time and response for use later
        ($id, $time, $res) = ($1, $2, $3);
    } elsif ($_[2] =~ /^(\d+-\d+)\s(.+)$/) {
        # didn't match, must be in a different format?
        ($id, $time, $res) = ($1, 'undef', $2);
    }

    # now, if it doesn't match
    unless ($id eq "$worker->{pid}-$worker->{reqid}") {
        Mgd::error("Worker responded with id $id, expected $worker->{pid}-$worker->{reqid}, killing");
        $client->close('worker_mismatch');
        return MogileFS::ProcManager->AskWorkerToDie($worker);
    }

    # now time this interval and add to @RecentQueries
    my $tinterval = Time::HiRes::tv_interval([$Mappings{$worker->{fd}}->[2]]);
    push @RecentQueries, sprintf("%s %.4f %s", $Mappings{$worker->{fd}}->[1], $tinterval, $time);
    shift @RecentQueries if scalar(@RecentQueries) > 50;

    # send text to client, put worker back in queue
    $client->write("$res\r\n");
    MogileFS::ProcManager->EnqueueQueryWorker($worker);
}

# called from various spots to empty the queues of available pairs.
sub ProcessQueues {
    return if $IsChild;

    # try to match up a client with a worker
    while (@QueryWorkerQueue && @ClientQueue) {
        # get client that isn't closed
        my $clref;
        while (@ClientQueue) {
            $clref = shift @ClientQueue;
            if (!defined $clref || $clref->[0]->{closed}) {
                $clref = undef;
                next;
            }

            # if we get here the client is valid
            last;
        }
        next unless $clref;

        # get worker and make sure it's not closed already
        my MogileFS::Connection::Worker $worker = shift @QueryWorkerQueue;
        if (!defined $worker || $worker->{closed}) {
            unshift @ClientQueue, $clref;
            next;
        }

        # put in mapping and send data to worker
        push @$clref, Time::HiRes::gettimeofday();
        $Mappings{$worker->{fd}} = $clref;

        # increment our counter so we know what request counter this is going out
        $worker->{reqid}++;

        $worker->write("$worker->{pid}-$worker->{reqid} $clref->[1]\r\n");
        $worker->watch_read(1);
    }
}

# send short descriptions of commands we support to the user
sub SendHelp {
    my $client = $_[1];

    # not supported yet
    #my $whaton = $_[2];

    # send general purpose help
    $client->write(<<HELP);
Welcome to mogilefsd's built-in help system.  Available commands:

    !recent     Recently executed queries and how long they took.
    !queue      Queries that are pending execution.
    !stats      General stats on what we're up to.
    !watch      Observe errors/messages from children.
    !jobs       Outstanding job counts, desired level, and pids.
    !shutdown   IMMEDIATELY kill all of mogilefsd.  IMMEDIATELY.

    !replication
                See the replication status.  Output format:
                <domain> <class> <devcount> <files>

    !to <job class> <message>
                Send <message> to all workers of <job class>.
                Mostly used for debugging.

    !want <count> <job class>
                Alter the level of workers of this class desired.
                Example: !want 20 queryworker, !want 3 replicate.
                See !jobs for what jobs are available.

More to come...
.
HELP

}

# called when a client sends us text.  we just create a job for
# it and then call ProcessQueues.
sub HandleClientRequest {
    return Mgd::error("ProcManager (Child) got request from client: $_[2]") if $IsChild;

    # if it's just 'help', 'h', '?', or something, do that
    if ((substr($_[2], 0, 1) eq '?') || ($_[2] eq 'help') || ($_[2] eq '')) {
        MogileFS::ProcManager->SendHelp($_[1]);
        return;
    }

    # quick check to see if we the parent should handle this
    if (substr($_[2], 0, 1) eq '!') {
        my MogileFS::Connection::Client $client = $_[1];
        my ($cmd, $args) = ($_[2] =~ m/^!(.+?)(?:\s+(.+))?$/);

        my @out;
        if ($cmd =~ /^stats$/) {
            # print out some stats on the queues
            my $uptime = time - $Mgd::starttime;
            my $ccount = scalar(@ClientQueue);
            my $wcount = scalar(@QueryWorkerQueue);
            my $ipcount = scalar(keys %Mappings);
            push @out, "uptime $uptime",
                       "pending_queries $ccount",
                       "processing_queries $ipcount",
                       "bored_queryworkers $wcount",
                       map { "$_ $Stats{$_}" } sort keys %Stats;

        } elsif ($cmd =~ /^repl/) {
            Mgd::validate_dbh();
            my $dbh = Mgd::get_dbh();
            my $mdcs = Mgd::get_mindevcounts();
            foreach my $dmid (sort keys %$mdcs) {
                my $dmname = Mgd::domain_name($dmid);
                foreach my $classid (sort keys %{$mdcs->{$dmid}}) {
                    my $min = $mdcs->{$dmid}->{$classid};
                    next unless $min > 1;

                    my $classname = Mgd::class_name($dmid, $classid) || '_default';
                    foreach my $ct (1..$min-1) {
                        my $count = $dbh->selectrow_array('SELECT COUNT(*) FROM file WHERE dmid = ? AND classid = ? AND devcount = ?',
                                                          undef, $dmid, $classid, $ct);
                        push @out, "$dmname $classname $ct $count";
                    }
                }
            }

        } elsif ($cmd =~ /^shutdown/) {
            print "User requested shutdown: $args\n";
            kill 15, $$; # kill us, that kills our kids

        } elsif ($cmd =~ /^jobs/) {
            # dump out a list of running jobs and pids
            foreach my $job (sort keys %ChildrenByJob) {
                my $ct = scalar(keys %{$ChildrenByJob{$job}});
                push @out, "$job count $ct";
                push @out, "$job desired $jobs{$job}->[0]";
                push @out, "$job pids " . join(' ', sort { $a <=> $b } keys %{$ChildrenByJob{$job}});
            }

        } elsif ($cmd =~ /^want/) {
            # !want <count> <jobclass>
            # set the new desired staffing level for a class
            if ($args =~ /^(\d+)\s+(\S+)/) {
                my ($count, $job) = ($1, $2);

                # validate count
                $count = 0 if $count < 0;
                # FIXME ...add an upper limit?

                # now make sure it's a real job
                if (defined $jobs{$job}) {
                    $jobs{$job}->[0] = $count;
                    $Mgd::allkidsup = 0;
                    push @out, "Now desiring $count children doing '$job'.";

                    # try to clean out the queryworkers (if that's what we're doing?)
                    MogileFS::ProcManager->CullQueryWorkers
                        if $job eq 'queryworker';
                } else {
                    my $classes = join(", ", sort keys %jobs);
                    push @out, "ERROR: Invalid class '$job'.  Valid classes: $classes";
                }
            } else {
                push @out, "ERROR: usage: !want <count> <jobclass>";
            }

        } elsif ($cmd =~ /^to/) {
            # !to <jobclass> <message>
            # sends <message> to all children of <jobclass>
            if ($args =~ /^(\S+)\s+(.+)/) {
                my $ct = MogileFS::ProcManager->SendToChildrenByJob($1, $2);
                push @out, "Message sent to $ct children.";

            } else {
                push @out, "ERROR: usage: !to <jobclass> <message>";
            }

        } elsif ($cmd =~ /^queue/ || $cmd =~ /^pend/) {
            foreach my $clq (@ClientQueue) {
                push @out, $clq->[1];
            }

        } elsif ($cmd =~ /^watch/) {
            if (delete $ErrorsTo{$client->{fd}}) {
                push @out, "Removed you from watcher list.";
            } else {
                $ErrorsTo{$client->{fd}} = $client;
                push @out, "Added you to watcher list.";
            }

        } elsif ($cmd =~ /^recent/) {
            # show the most recent N queries
            push @out, @RecentQueries;

        } else {
            MogileFS::ProcManager->SendHelp($client, $args);
        }
        $client->write(join("\r\n", @out) . "\r\n") if @out;
        $client->write(".\r\n");
        return;
    }

    # just push the input onto the client queue
    $Stats{queries}++;
    push @ClientQueue, [ $_[1], "cmd " . ($_[1]->peer_ip_string || '0.0.0.0') . " $_[2]" ];
    MogileFS::ProcManager->ProcessQueues;
}

# a child has contacted us with some command/status/something.
sub HandleChildRequest {
    return Mgd::error("ProcManager (Child) got request from child: $_[2]") if $IsChild;

    # if they have no job set, then their first line is what job they are
    # and not a command.  they also specify their pid, just so we know what
    # connection goes with what pid, in case it's ever useful information.
    my MogileFS::Connection::Worker $child = $_[1];
    unless (defined $child->job) {
        my ($pid, $job) = ($_[2] =~ /^(\d+)\s+(.+)/);
        $child->job($job);
        $child->pid($pid);

        # now do any special case startup
        if ($job eq 'queryworker') {
            MogileFS::ProcManager->RegisterQueryWorker($child);
        }

        # add to normal list
        $ChildrenByJob{$job}->{$child->pid} = $child;
        return;
    }

    # see if we should downsize this child
    my $check_job = sub {
        if (job_needs_reduction($child->job)) {
            Mgd::error("Reducing headcount of " . $child->job . " job by 1.");
            MogileFS::ProcManager->AskWorkerToDie($child);
        } else {
            $child->drain_queue;
        }
    };

    # at this point we've got a command of some sort
    my $cmd = $_[2];
    if ($cmd =~ /^error (.+)$/i) {
        # pass it on to our error handler, prefaced with the child's job
        Mgd::error("[" . $child->job . "(" . $child->pid . ")] $1");

    } elsif ($cmd =~ /^queue/) {
        # send out what we have queued up for it
        $child->drain_queue;

    } elsif ($cmd =~ /^state_change (\w+) (\d+) (\w+)/) {
        my ($what, $whatid, $state) = ($1, $2, $3);
        state_change($what, $whatid, $state, $child);

    } elsif ($cmd =~ /^request_orders/) {
        $check_job->();

    } elsif ($cmd =~ /^monitor_ping/) {
        $check_job->();

    } elsif ($cmd =~ /^reaper_ping/) {
        $check_job->();

    } elsif ($cmd =~ /^repl_ping/) {
        $check_job->();

    } elsif ($cmd =~ /^repl_unreachable (\d+)/) {
        # announce to the other replicators that this fid can't be reached, but note
        # that we don't actually drain the queue to the requestor, as the replicator
        # isn't in a place where it can accept a queue drain right now.
        MogileFS::ProcManager->SendToChildrenByJob('replicate', "repl_unreachable $1", $child);

    } elsif ($cmd =~ /^repl_i_did (\d+)/) {
        my $fid = $1;

        # announce to the other replicators that this fid was done and then drain the
        # queue to this person.
        MogileFS::ProcManager->SendToChildrenByJob('replicate', "repl_was_done $fid", $child);
        $check_job->();

    } else {
        # unknown command
        Mgd::error("Unknown command [$_[2]] from child; job=" . $child->job);
    }
}

# given a job class, and a message, send it to all children of that job.  returns
# the number of children the message was sent to.
# arguments: ( jobclass, message, [ child ] )
# if child is specified, the message will be sent to members of the job class that
# aren't that child.  so you can exclude the one that originated the message.
sub SendToChildrenByJob {
    my $childref = $ChildrenByJob{$_[1]};
    return 0 unless defined $childref && %$childref;
    my $msg = $_[2];

    foreach my $child (values %$childref) {
        # ignore the child specified as the third arg if one is sent
        next if defined $_[3] && $_[3] == $child;

        # send the message to this child
        $child->enqueue_line($msg);
    }
    return scalar(keys %$childref);
}

# called when we notice that a worker has bit it.  we might have to restart a
# job that they had been working on.
sub NoteDeadWorkerConn {
    return if $IsChild;

    # get parms and error check
    my MogileFS::Connection::Worker $worker = $_[1];
    return unless $worker;

    # if there's a mapping for this worker's fd, they had a job that didn't get done
    if ($Mappings{$worker->{fd}}) {
        # unshift, since this one already went through the queue once
        unshift @ClientQueue, $Mappings{$worker->{fd}};
        delete $Mappings{$worker->{fd}};

        # now try to get it processing again
        MogileFS::ProcManager->ProcessQueues;
    }
}

# given (job, pid), record that this worker is about to die
sub note_pending_death {
    my ($job, $pid) = @_;

    die "$job not defined in call to note_pending_death.\n"
        unless defined $jobs{$job};

    $todie{$pid} = 1;
    $jobs{$job}->[1]--;
}

# see if we should reduce the number of active children
sub job_needs_reduction {
    my $job = shift;
    return $jobs{$job}->[0] < $jobs{$job}->[1];
}

sub is_child {
    return $IsChild;
}

sub state_change {
    my ($what, $whatid, $state, $child) = @_;
    warn "STATE CHANGE: $what<$whatid> = $state\n";
    MogileFS::ProcManager->SendToChildrenByJob('queryworker', ":state_change $what $whatid $state", $child);
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
