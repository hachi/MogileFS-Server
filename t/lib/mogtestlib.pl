use strict;
use warnings;
use DBI;

use FindBin qw($Bin);
use IO::Socket::INET;
use MogileFS::Server;

sub temp_store {
    my $type = $ENV{MOGTEST_DBTYPE};

    # default to mysql, but make sure DBD::MySQL is installed
    unless ($type) {
        $type = "MySQL";
        eval "use DBD::mysql; 1" or
            die "DBD::mysql isn't installed.  Please install it or define MOGTEST_DBTYPE env. variable";
    }

    die "Bogus type" unless $type =~ /^\w+$/;
    my $store = "MogileFS::Store::$type";
    eval "use $store; 1;";
    if ($@) {
        die "Failed to load $store: $@\n";
    }
    my $sto = $store->new_temp;
    Mgd::set_store($sto);
    return $sto;
}


sub create_temp_tracker {
    my $sto = shift;

    my $pid = fork();
    my $whoami = `whoami`;
    chomp $whoami;

    my $connect = sub {
        return IO::Socket::INET->new(PeerAddr => "127.0.0.1:7001",
                                     Timeout  => 2);
    };

    my $conn = $connect->();
    die "Failed:  tracker already running on port 7001?\n" if $conn;

    unless ($pid) {
        exec("$Bin/../mogilefsd",
             ($whoami eq "root" ? "--user=root" : ()),
             "--skipconfig",
             "--workers=2",
             "--dsn=" . $sto->dsn,
             "--dbuser=" . $sto->user,
             "--dbpass=" . $sto->pass);
    }

    for (1..3) {
        if ($connect->()) {
            return TrackerHandle->new(pid => $pid);
        }
        sleep 1;
    }
    return undef;
}

sub create_mogstored {
    my ($ip, $root) = @_;

    my $pid = fork();

    my $connect = sub {
        return IO::Socket::INET->new(PeerAddr => "$ip:7500",
                                     Timeout  => 2);
    };

    my $conn = $connect->();
    die "Failed:  tracker already running on port 7500?\n" if $conn;

    unless ($pid) {
        exec("$Bin/../mogstored",
             "--httplisten=$ip:7500",
             "--mgmtlisten=$ip:7501",
             "--maxconns=1000",  # because we're not root, put it below 1024
             "--docroot=$root");
    }

    for (1..12) {
        if ($connect->()) {
            return MogstoredHandle->new(pid => $pid, ip => $ip, root => $root);
        }
        select undef, undef, undef, 0.25;
    }
    return undef;
}

############################################################################
package ProcessHandle;
sub new {
    my ($class, %args) = @_;
    bless \%args, $class;
}

sub pid { return $_[0]{pid} }

sub DESTROY {
    my $self = shift;
    return unless $self->{pid};
    kill 15, $self->{pid};
}


############################################################################

package TrackerHandle;
use base 'ProcessHandle';

sub ipport {
    my $self = shift;
    return "127.0.0.1:7001";
}

sub mogadm {
    my $self = shift;
    my @args = @_;
    unshift @args, "$FindBin::Bin/../../utils/mogadm", "--trackers=" . $self->ipport;
#    use Data::Dumper;
#    print Dumper(\@args);
    my $rv = system(@args);
    return !$rv;
}

############################################################################
package MogstoredHandle;
use base 'ProcessHandle';

# this space intentionally left blank.  all in super class for now.

############################################################################
package MogPath;
sub new {
    my ($class, $url) = @_;
    return bless {
        url => $url,
    }, $class;
}

sub host {
    my $self = shift;
    my ($host1) = $self->{url} =~ m!^http://(.+:\d+)!;
    return $host1
}

sub device {
    my $self = shift;
    my ($dev) = $self->{url} =~ m!dev(\d+)!;
    return $dev
}

sub path {
    my $self = shift;
    my $path = $self->{url};
    $path =~ s!^http://(.+:\d+)!!;
    return $path;
}

1;
