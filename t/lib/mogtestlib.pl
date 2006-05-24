use strict;
use warnings;
use DBI;

use FindBin qw($Bin);

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

    unless ($pid) {
        exec("$Bin/../mogilefsd",
             "--skipconfig",
             "--dsn=" . $db->dsn,
             "--dbuser=" . $db->user,
             "--dbpass=" . $db->pass);
    }

    for (1..10) {
        my $pinged = kill 0, $pid;
        print "ping $pid = $pinged\n";
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

1;
