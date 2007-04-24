package MogileFS::Util;
use strict;
use Carp qw(croak);
use Time::HiRes;
use MogileFS::Exception;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
                    error undeferr debug fatal daemonize weighted_list every
                    wait_for_readability wait_for_writeability throw error_code
                    max min
                    );

sub every {
    my ($delay, $code) = @_;
    my ($worker, $psock_fd);
    if ($worker = MogileFS::ProcManager->is_child) {
        $psock_fd = $worker->psock_fd;
    }
  CODERUN:
    while (1) {
        my $start = Time::HiRes::time();
        my $explicit_sleep = undef;

        # run the code in a loop, so "next" will get out of it.
        foreach (1) {
            $code->(sub {
                $explicit_sleep = shift;
            });
        }

        my $now = Time::HiRes::time();
        my $took = $now - $start;
        my $sleep_for = defined $explicit_sleep ? $explicit_sleep : ($delay - $took);
        next unless $sleep_for > 0;

        # simple case, not in a child process (this never happens currently)
        unless ($psock_fd) {
            Time::HiRes::sleep($sleep_for);
            next;
        }

        while ($sleep_for > 0) {
            my $last_time_pre_sleep = $now;
            $worker->forget_woken_up;
            if (wait_for_readability($psock_fd, $sleep_for)) {
                # TODO: uncomment this and watch an idle server and how many wakeups.  could optimize.
                #local $Mgd::POST_SLEEP_DEBUG = 1;
                #warn "WOKEN UP FROM SLEEP in $worker [$$]\n";
                $worker->read_from_parent;
                next CODERUN if $worker->was_woken_up;
            }
            $now = Time::HiRes::time();
            $sleep_for -= ($now - $last_time_pre_sleep);
        }
    }
}

sub debug {
    my ($msg, $level) = @_;
    return unless $Mgd::DEBUG >= 1;
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->send_to_parent("debug $msg");
    } else {
        my $dbg = "[debug] $msg";
        MogileFS::ProcManager->NoteError(\$dbg);
        Mgd::log('debug', $msg);
    }
}

our $last_error;
sub error {
    my ($errmsg) = @_;
    $last_error = $errmsg;
    if (my $worker = MogileFS::ProcManager->is_child) {
        my $msg = "error $errmsg";
        $msg =~ s/\s+$//;
        $worker->send_to_parent($msg);
    } else {
        MogileFS::ProcManager->NoteError(\$errmsg);
        Mgd::log('debug', $errmsg);
    }
    return 0;
}

# like error(), but returns undef.
sub undeferr {
    error(@_);
    return undef;
}

sub last_error {
    return $last_error;
}

sub fatal {
    my ($errmsg) = @_;
    error($errmsg);
    die $errmsg;
}

sub throw {
    my ($errcode) = @_;
    MogileFS::Exception->new($errcode)->throw;
}

sub error_code {
    my ($ex) = @_;
    return "" unless UNIVERSAL::isa($ex, "MogileFS::Exception");
    return $ex->code;
}

sub daemonize {
    my($pid, $sess_id, $i);

    ## Fork and exit parent
    if ($pid = fork) { exit 0; }

    ## Detach ourselves from the terminal
    croak "Cannot detach from controlling terminal"
        unless $sess_id = POSIX::setsid();

    ## Prevent possibility of acquiring a controling terminal
    $SIG{'HUP'} = 'IGNORE';
    if ($pid = fork) { exit 0; }

    ## Change working directory
    chdir "/";

    ## Clear file creation mask
    umask 0;

    print STDERR "Daemon running as pid $$.\n" if $MogileFS::DEBUG;

    ## Close open file descriptors
    close(STDIN);
    close(STDOUT);
    close(STDERR);

    ## Reopen stderr, stdout, stdin to /dev/null
    if ( $MogileFS::DEBUG ) {
        open(STDIN,  "+>/tmp/mogilefsd.log");
    } else {
        open(STDIN,  "+>/dev/null");
    }
    open(STDOUT, "+>&STDIN");
    open(STDERR, "+>&STDIN");
}

# input:
#   given an array of arrayrefs of [ item, weight ], returns weighted randomized
#   list of items (without the weights, not arrayref; just list)
#
#   a weight of 0 means to exclude that item from the results list; i.e. it's not
#   ever used
#
# example:
#   my @items = weighted_list( [ 1, 10 ], [ 2, 20 ], [ 3, 0 ] );
#
#   returns (1, 2) or (2, 1) with the latter far more likely
sub weighted_list {
    my @list = grep { $_->[1] > 0 } @_;
    my @ret;

    my $sum = 0;
    $sum += $_->[1] foreach @list;

    my $getone = sub {
        return shift(@list)->[0]
            if scalar(@list) == 1;

        my $val = rand() * $sum;
        my $curval = 0;
        for (my $idx = 0; $idx < scalar(@list); $idx++) {
            my $item = $list[$idx];
            $curval += $item->[1];
            if ($curval >= $val) {
                my ($ret) = splice(@list, $idx, 1);
                $sum -= $item->[1];
                return $ret->[0];
            }
        }
    };

    push @ret, $getone->() while @list;
    return @ret;
}

# given a file descriptor number and a timeout, wait for that descriptor to
# become readable; returns 0 or 1 on if it did or not
sub wait_for_readability {
    my ($fileno, $timeout) = @_;
    return 0 unless $fileno && $timeout >= 0;

    my $rin = '';
    vec($rin, $fileno, 1) = 1;
    my $nfound = select($rin, undef, undef, $timeout);

    # nfound can be undef or 0, both failures, or 1, a success
    return $nfound ? 1 : 0;
}

sub wait_for_writeability {
    my ($fileno, $timeout) = @_;
    return 0 unless $fileno && $timeout;

    my $rout = '';
    vec($rout, $fileno, 1) = 1;
    my $nfound = select(undef, $rout, undef, $timeout);

    # nfound can be undef or 0, both failures, or 1, a success
    return $nfound ? 1 : 0;
}

# if given an HTTP URL, break it down into [ host, port, URI ], else
# returns die, because we don't support non-http-mode anymore
sub url_parts {
    my $path = shift;
    if ($path =~ m!^http://(.+?)(?::(\d+))?(/.+)$!) {
        return [ $1, $2 || 80, $3 ];
    }
    Carp::croak("Bogus URL: $path");
}

sub max {
    my ($n1, $n2) = @_;
    return $n1 if $n1 > $n2;
    return $n2;
}

sub min {
    my ($n1, $n2) = @_;
    return $n1 if $n1 < $n2;
    return $n2;
}

1;
