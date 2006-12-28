package MogileFS::Util;
use strict;
use Carp qw(croak);
use Time::HiRes;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
                    error debug fatal daemonize weighted_list every dbcheck
                    wait_for_readability wait_for_writeability
                    );

sub every {
    my ($delay, $code) = @_;
    while (1) {
        my $start = Time::HiRes::time();
        my $explicit_sleep = undef;

        # run the code in a loop, so "next" will get out of it.
        foreach (1) {
            $code->(sub {
                $explicit_sleep = shift;
            });
        }

        my $took = Time::HiRes::time() - $start;
        my $sleep_for = $delay - $took;
        if (defined $explicit_sleep) {
            Time::HiRes::sleep($explicit_sleep);
        } else {
            Time::HiRes::sleep($sleep_for) if $sleep_for > 0;
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
        $worker->send_to_parent("error $errmsg");
    } else {
        MogileFS::ProcManager->NoteError(\$errmsg);
        Mgd::log('debug', $errmsg);
    }
    return 0;
}
sub last_error {
    return $last_error;
}

sub fatal {
    my ($errmsg) = @_;
    error($errmsg);
    die $errmsg;
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

sub dbcheck {
    my ($dbh, $errmsg) = @_;
    return unless $dbh->err;
    $errmsg .= ": " . $dbh->err . ": " . $dbh->errstr;
    error($errmsg);  # propogate to parent for listeners
    die $errmsg;
}

# given a file descriptor number and a timeout, wait for that descriptor to
# become readable; returns 0 or 1 on if it did or not
sub wait_for_readability {
    my ($fileno, $timeout) = @_;
    return 0 unless $fileno && $timeout >= 0;

    my $rin;
    vec($rin, $fileno, 1) = 1;
    my $nfound = select($rin, undef, undef, $timeout);

    # nfound can be undef or 0, both failures, or 1, a success
    return $nfound ? 1 : 0;
}

sub wait_for_writeability {
    my ($fileno, $timeout) = @_;
    return 0 unless $fileno && $timeout;

    my $rout;
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


1;
