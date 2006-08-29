package MogileFS::Util;
use strict;
use Carp qw(croak);
use Time::HiRes;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(error daemonize weighted_list every);

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

sub error {
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->send_to_parent("error $_[0]");
    } else {
        MogileFS::ProcManager->NoteError(\$_[0]);
        Mgd::log('debug', $_[0]);
    }
    return 0;
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

1;
