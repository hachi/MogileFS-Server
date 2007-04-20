package MogileFS::Server;
use strict;
use warnings;
use vars qw($VERSION);
$VERSION = "2.09";

=head1 NAME

MogileFS::Server - MogileFS (distributed filesystem) server

=head1 SYNOPSIS

 $s = MogileFS::Server->server;
 $s->run;

=cut

use IO::Socket;
use Symbol;
use POSIX;
use File::Copy ();
use Carp;
use File::Basename ();
use File::Path ();
use Sys::Syslog ();
use Time::HiRes ();
use Net::Netmask;
use LWP::UserAgent;
use List::Util;
use Socket ();

use MogileFS::Util qw(daemonize);
use MogileFS::Sys;
use MogileFS::Config;

use MogileFS::ProcManager;
use MogileFS::Connection::Client;
use MogileFS::Connection::Worker;

use MogileFS::Worker::Query;
use MogileFS::Worker::Delete;
use MogileFS::Worker::Replicate;
use MogileFS::Worker::Reaper;
use MogileFS::Worker::Monitor;
use MogileFS::Worker::Fsck;

use MogileFS::HTTPFile;
use MogileFS::Class;
use MogileFS::Device;
use MogileFS::Host;
use MogileFS::FID;
use MogileFS::Domain;
use MogileFS::DevFID;

use MogileFS::Store;
use MogileFS::Store::MySQL;  # FIXME: don't load this until after reading their config, but before fork.

use MogileFS::ReplicationPolicy::MultipleHosts;

my $server; # server singleton
sub server {
    my ($pkg) = @_;
    return $server ||= bless {}, $pkg;
}

# --------------------------------------------------------------------------
# instance methods:
# --------------------------------------------------------------------------

sub run {
    my $self = shift;

    MogileFS::Config->load_config;

    # don't run as root
    die "mogilefsd cannot be run as root\n"
        if $< == 0 && MogileFS->config('user') ne "root";

    MogileFS::Config->check_database;
    daemonize() if MogileFS->config("daemonize");

    MogileFS::ProcManager->set_min_workers('queryworker' => MogileFS->config('query_jobs'));
    MogileFS::ProcManager->set_min_workers('delete'      => MogileFS->config('delete_jobs'));
    MogileFS::ProcManager->set_min_workers('replicate'   => MogileFS->config('replicate_jobs'));
    MogileFS::ProcManager->set_min_workers('reaper'      => MogileFS->config('reaper_jobs'));
    MogileFS::ProcManager->set_min_workers('monitor'     => MogileFS->config('monitor_jobs'));
    MogileFS::ProcManager->set_min_workers('fsck'        => 1);

    # open up our log
    Sys::Syslog::openlog('mogilefsd', 'pid', 'daemon');
    Mgd::log('info', 'beginning run');

    unless (MogileFS::ProcManager->write_pidfile) {
        Mgd::log('info', "Couldn't write pidfile, ending run");
        Sys::Syslog::closelog();
        exit 1;
    }

    # Install signal handlers.
    $SIG{TERM}  = sub {
        my @children = MogileFS::ProcManager->child_pids;
        print STDERR scalar @children, " children to kill.\n" if $DEBUG;
        my $count = kill( 'TERM' => @children );
        print STDERR "Sent SIGTERM to $count children.\n" if $DEBUG;
        MogileFS::ProcManager->remove_pidfile;
        Mgd::log('info', 'ending run due to SIGTERM');
        Sys::Syslog::closelog();

        exit 0;
    };

    $SIG{INT}  = sub {
        my @children = MogileFS::ProcManager->child_pids;
        print STDERR scalar @children, " children to kill.\n" if $DEBUG;
        my $count = kill( 'INT' => @children );
        print STDERR "Sent SIGINT to $count children.\n" if $DEBUG;
        MogileFS::ProcManager->remove_pidfile;
        Mgd::log('info', 'ending run due to SIGINT');
        exit 0;
    };
    $SIG{PIPE} = 'IGNORE';  # catch them by hand

    # setup server sockets to listen for client connections
    my @servers;
    foreach my $listen (@{ MogileFS->config('listen') }) {
        my $server = IO::Socket::INET->new(LocalAddr => $listen,
                                           Type      => SOCK_STREAM,
                                           Proto     => 'tcp',
                                           Blocking  => 0,
                                           Reuse     => 1,
                                           Listen    => 10 )
            or die "Error creating socket: $@\n";

        # save sub to accept a client
        push @servers, $server;
        Danga::Socket->AddOtherFds( fileno($server) => sub {
                my $csock = $server->accept
                    or return;
                MogileFS::Connection::Client->new($csock);
            } );
    }

    MogileFS::ProcManager->push_pre_fork_cleanup(sub {
        # so children don't hold server connection open
        close($_) foreach @servers;
    });

    # setup the post event loop callback to spawn jobs, and the timeout
    Danga::Socket->DebugLevel(3);
    Danga::Socket->SetLoopTimeout( 250 ); # 250 milliseconds
    Danga::Socket->SetPostLoopCallback(MogileFS::ProcManager->PostEventLoopChecker);

    # and now, actually start listening for events
    eval {
        print( "Starting event loop for frontend job on pid $$.\n" ) if $DEBUG;
        Danga::Socket->EventLoop();
    };

    if ($@) {
        Mgd::log('err', "crash log: $@");
        exit 1;
    }
    Mgd::log('info', 'ending run');
    Sys::Syslog::closelog();
    exit(0);
}

# --------------------------------------------------------------------------

package MogileFS;
# just so MogileFS->config($key) will work:
use MogileFS::Config qw(config);

my %hooks;

sub register_worker_command {
    # just pass this through to the Worker class
    return MogileFS::Worker::Query::register_command(@_);
}

sub register_global_hook {
    $hooks{$_[0]} = $_[1];
    return 1;
}

sub unregister_global_hook {
    delete $hooks{$_[0]};
    return 1;
}

sub run_global_hook {
    my $hookname = shift;
    my $ref = $hooks{$hookname};
    return $ref->(@_) if defined $ref;
    return undef;
}

# --------------------------------------------------------------------------

package Mgd;  # conveniently short name
use strict;
use warnings;
use MogileFS::Util qw(error fatal debug); # for others calling Mgd::foo()

sub server {
    return MogileFS::Server->server;
}

# database checking/connecting
sub validate_dbh { Mgd::get_store()->recheck_dbh }
sub get_dbh      { return Mgd::get_store()->dbh  }

# the eventual replacement for callers asking for a dbh directly:
# they'll ask for the current store, which is a database abstraction
# layer.
my ($store, $store_pid);
sub get_store {
    return $store if $store && $store_pid == $$;
    $store_pid = $$;
    return $store = MogileFS::Store->new;
}

# only for t/ scripts to explicitly set a store, without loading in a config
sub set_store {
    my ($s) = @_;
    $store = $s;
    $store_pid = $$;
}

# log stuff to syslog or the screen
sub log {
    # simple logging functionality
    if (! $MogileFS::Config::daemonize) {
        # syslog acts like printf so we have to use printf and append a \n
        shift; # ignore the first parameter (info, warn, critical, etc)
        printf(shift(@_) . "\n", @_);
    } else {
        # just pass the parameters to syslog
        Sys::Syslog::syslog(@_);
    }
}

1;
__END__
#Just for MakeMaker's kinda lame regexp for ABSTRACT_FROM
=dummypod
mogilefs::server - MogileFS (distributed filesystem) server.

