use strict;
use warnings;
use DBI;

use FindBin qw($Bin);
use IO::Socket::INET;

sub create_temp_db {
    my $dbname = "tmp_mogiletest";
    create_mysql_db($dbname);
    return DBHandle->new($dbname);
}

my $rootdbh;
sub _root_dbh {
    return $rootdbh ||= DBI->connect("DBI:mysql:mysql", "root", "", { RaiseError => 1 })
        or die "Couldn't connect to database";
}

sub create_mysql_db {
    my $dbname = shift;
    drop_mysql_db($dbname);
    _root_dbh()->do("CREATE DATABASE $dbname");
}

sub drop_mysql_db {
    my $dbname = shift;
    _root_dbh()->do("DROP DATABASE IF EXISTS $dbname");
}

sub create_temp_tracker {
    my $db = shift;

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
             "--dsn=" . $db->dsn,
             "--dbuser=" . $db->user,
             "--dbpass=" . $db->pass);
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
package DBHandle;
sub new {
    my ($class, $name) = @_;
    return bless { name => $name }, $class;
}

sub dbh {
    my $self = shift;
    return $self->{dbh} ||= DBI->connect($self->dsn, $self->user, $self->pass, { RaiseError => 1 })
        or die "Couldn't connect to '$self->{name}' database";
}

sub name {
    my $self = shift;
    return $self->{name};
}

sub dsn {
    my $self = shift;
    return "DBI:mysql:$self->{name}";
}

*username = \&user;
sub user { "root" }
*password = \&pass;
sub pass { "" }

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
