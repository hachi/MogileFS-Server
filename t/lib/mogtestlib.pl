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
}

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

package TrackerHandle;
sub new {
    my ($class, %args) = @_;
    bless \%args, $class;
}

sub pid { return $_[0]{pid} }

sub DESTROY {
    my $self = shift;
    return unless $self->{pid};
    warn "Killing $self->{pid}...\n";
    kill 15, $self->{pid};
}

1;
