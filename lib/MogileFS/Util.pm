package MogileFS::Util;
use strict;
use Carp qw(croak);
require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(error daemonize);

sub error {
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->send_to_parent("error $_[0]");
    } else {
        MogileFS::ProcManager->NoteError(\$_[0]);
        Mgd::log('debug', $_[0]);
    }
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

1;
