package MogileFS::Store::SQLite;
use strict;
use warnings;
use DBI;
use DBD::SQLite 1.13;
use MogileFS::Util qw(throw);
use base 'MogileFS::Store';
use File::Temp ();

# --------------------------------------------------------------------------
# Package methods we override
# --------------------------------------------------------------------------

sub post_dbi_connect {
    my $self = shift;
    $self->{dbh}->func(1000, 'busy_timeout');
}

sub want_raise_errors { 1 }

sub dsn_of_dbhost {
    my ($class, $dbname, $host) = @_;
    return "DBI:SQLite:$dbname";
}

sub dsn_of_root {
    my ($class, $dbname, $host) = @_;
    return "DBI:SQLite:$dbname";
}

sub can_replace { 1 }
sub can_insertignore { 0 }
sub unix_timestamp { "strftime('%s','now')" }

# --------------------------------------------------------------------------
# Store-related things we override
# --------------------------------------------------------------------------

sub was_duplicate_error {
    my $self = shift;
    my $dbh = $self->dbh;
    return 0 unless $dbh->err;
    my $errstr = $dbh->errstr;
    return 1 if $errstr =~ /(?:is|are) not unique/i;
    return 1 if $errstr =~ /must be unique/i;
    return 0;
}

# --------------------------------------------------------------------------
# Test suite things we override
# --------------------------------------------------------------------------

sub new_temp {
    my ($fh, $filename) = File::Temp::tempfile();

    system("$FindBin::Bin/../mogdbsetup", "--type=SQLite", "--yes", "--dbname=$filename")
        and die "Failed to run mogdbsetup ($FindBin::Bin/../mogdbsetup).";

    return MogileFS::Store->new_from_dsn_user_pass("DBI:SQLite:$filename",
                                                   "", "");
}

sub table_exists {
    my ($self, $table) = @_;
    return eval {
        my $sth = $self->dbh->prepare("EXPLAIN SELECT * FROM $table");
        $sth->execute;
        my $rec = $sth->fetchrow_hashref;
        return $rec ? 1 : 0;
    };
}

# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------

sub TABLE_class {
    "CREATE TABLE class (
      dmid          SMALLINT UNSIGNED NOT NULL,
      classid       TINYINT UNSIGNED NOT NULL,
      classname     VARCHAR(50),
      mindevcount   TINYINT UNSIGNED NOT NULL,
      UNIQUE (dmid,classid),
      UNIQUE      (dmid,classname)
)"
}

sub TABLE_file {
    "CREATE TABLE file (
   fid          INT UNSIGNED NOT NULL PRIMARY KEY,
   dmid          SMALLINT UNSIGNED NOT NULL,
   dkey           VARCHAR(255),
   length        INT UNSIGNED,
   classid       TINYINT UNSIGNED NOT NULL,
   devcount      TINYINT UNSIGNED NOT NULL,
   UNIQUE (dmid, dkey)
)"
}

sub TABLE_tempfile {
    "CREATE TABLE tempfile (
   fid          INTEGER PRIMARY KEY AUTOINCREMENT,
   createtime   INT UNSIGNED NOT NULL,
   classid      TINYINT UNSIGNED NOT NULL,
   dmid          SMALLINT UNSIGNED NOT NULL,
   dkey           VARCHAR(255),
   devids       VARCHAR(60)
)"
}

sub TABLE_unreachable_fids {
    "CREATE TABLE unreachable_fids (
   fid        INT UNSIGNED NOT NULL,
   lastupdate INT UNSIGNED NOT NULL,
   PRIMARY KEY (fid)
)"
}

sub INDEXES_unreachable_fids {
    ("CREATE INDEX lastupdate ON unreachable_fids (lastupdate)");
}

sub TABLE_file_on {
    "CREATE TABLE file_on (
   fid          INT UNSIGNED NOT NULL,
   devid        MEDIUMINT UNSIGNED NOT NULL,
   PRIMARY KEY (fid, devid)
)"
}

sub INDEXES_file_on {
    ("CREATE INDEX devid ON file_on (devid)");
}

sub INDEXES_device {
    ("CREATE INDEX status ON device (status)");
}

sub INDEXES_file_to_replicate {
    ("CREATE INDEX nexttry ON file_to_replicate (nexttry)");
}

sub INDEXES_file_to_delete_later {
    ("CREATE INDEX delafter ON file_to_delete_later (delafter)");
}

sub filter_create_sql {
    my ($self, $sql) = @_;
    $sql =~ s/\bENUM\(.+?\)/TEXT/g;

    my ($table) = $sql =~ /create\s+table\s+(\S+)/i;
    die "didn't find table" unless $table;
    if ($self->can("INDEXES_$table")) {
        $sql =~ s!,\s+INDEX\s+\(.+?\)!!mg;
    }

    return $sql;
}

# eh.  this is really atomic at all, but a) this is a demo db module,
# nobody should use SQLite in production, b) this method is going
# away, c) everything in SQLite is pretty atomic anyway with the
# db-level locks, d) the devcount field is no longer used.  so i'm not
# caring much about doing this correctly.
sub update_devcount_atomic {
    my ($self, $fidid) = @_;
    $self->update_devcount($fidid);
}

1;

__END__

=head1 NAME

MogileFS::Store::SQLite - For-testing-only not-for-production SQLite storage for MogileFS

=head1 SEE ALSO

L<MogileFS::Store>


