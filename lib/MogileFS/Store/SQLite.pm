package MogileFS::Store::SQLite;
use strict;
use warnings;
use DBI;
use DBD::SQLite;
use MogileFS::Util qw(throw);
use base 'MogileFS::Store';
use File::Temp ();

# --------------------------------------------------------------------------
# Package methods we override
# --------------------------------------------------------------------------

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

# --------------------------------------------------------------------------
# Data-access things we override
# --------------------------------------------------------------------------

sub mass_insert_file_on {
    my ($self, @devfids) = @_;
    foreach my $df (@devfids) {
        $self->SUPER::mass_insert_file_on($df);
    }
    return 1;
}


################# LEFTOVERS from MYSQL:

# throw 'dup' on duplicate name
sub create_class {
    my ($self, $dmid, $classname) = @_;
    my $dbh = $self->dbh;

    # get the max class id in this domain
    my $maxid = $dbh->selectrow_array
        ('SELECT MAX(classid) FROM class WHERE dmid = ?', undef, $dmid) || 0;

    # now insert the new class
    my $rv = eval {
        $dbh->do("INSERT INTO class (dmid, classid, classname, mindevcount) VALUES (?, ?, ?, ?)",
                 undef, $dmid, $maxid + 1, $classname, 2);
    };
    if ($@ || $dbh->err) {
        # first is mysql's error code for duplicates
        if ($self->was_duplicate_error) {
            throw("dup");
        }
    }
    return $maxid + 1 if $rv;
    $self->condthrow;
    die;
}

sub register_tempfile {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(fid dmid key classid devids)], @_);

    my $dbh = $self->dbh;
    my $fid = $arg{fid};

    my $explicit_fid_used = $fid ? 1 : 0;

    # setup the new mapping.  we store the devices that we picked for
    # this file in here, knowing that they might not be used.  create_close
    # is responsible for actually mapping in file_on.  NOTE: fid is being
    # passed in, it's either some number they gave us, or it's going to be
    # undef which translates into NULL which means to automatically create
    # one.  that should be fine.
    my $ins_tempfile = sub {
        $dbh->do("INSERT INTO tempfile SET ".
                 " fid=?, dmid=?, dkey=?, classid=?, createtime=UNIX_TIMESTAMP(), devids=?",
                 undef, $fid, $arg{dmid}, $arg{key}, $arg{classid}, $arg{devids});
        return undef if $dbh->err;

        unless (defined $fid) {
            # if they did not give us a fid, then we want to grab the one that was
            # theoretically automatically generated
            $fid = $dbh->{mysql_insertid};  # mysql-ism
        }
        return undef unless defined $fid && $fid > 0;
        return 1;
    };

    unless ($ins_tempfile->()) {
        return -1 if $explicit_fid_used;
        return undef;
    }

    my $fid_in_use = sub {
        my $exists = $dbh->selectrow_array("SELECT COUNT(*) FROM file WHERE fid=?", undef, $fid);
        $self->condthrow;
        return $exists ? 1 : 0;
    };

    # if the fid is in use, do something
    while ($fid_in_use->($fid)) {
        return -1 if $explicit_fid_used;

        # mysql could have been restarted with an empty tempfile table, causing
        # innodb to reuse a fid number.  so we need to seed the tempfile table...

        # get the highest fid from the filetable and insert a dummy row
        $fid = $dbh->selectrow_array("SELECT MAX(fid) FROM file");
        $ins_tempfile->();

        # then do a normal auto-increment
        $fid = undef;
        return undef unless $ins_tempfile->();
    }

    return $fid;
}

# returns 1 on success, 0 on duplicate key error, dies on exception
# TODO: need a test to hit the duplicate name error condition
sub rename_file {
    my ($self, $fidid, $to_key) = @_;
    my $dbh = $self->dbh;
    eval {
        $dbh->do('UPDATE file SET dkey = ? WHERE fid=?',
                 undef, $to_key, $fidid);
    };
    if ($@ || $dbh->err) {
        # first is mysql's error code for duplicates
        if ($self->was_duplicate_error) {
            return 0;
        } else {
            die $@;
        }
    }
    $self->condthrow;
    return 1;
}

# update the device count for a given fidid
sub update_devcount_atomic {
    my ($self, $fidid) = @_;
    my $lockname = "mgfs:fid:$fidid";

    my $lock = eval { $self->get_lock($lockname, 10) };

    # Check to make sure the lock didn't timeout, then we want to bail.
    return 0 if defined $lock && $lock == 0;

    # Checking $@ is pointless for the time because we just want to plow ahead
    # even if the get_lock trapped a recursion and threw a fatal error.

    $self->update_devcount($fidid);

    # Don't release the lock if we never got it.
    $self->release_lock($lockname) if $lock;
    return 1;
}

# enqueue a fidid for replication, from a specific deviceid (can be undef), in a given number of seconds.
sub enqueue_for_replication {
    my ($self, $fidid, $from_devid, $in) = @_;

    my $nexttry = 0;
    if ($in) {
        $nexttry = "UNIX_TIMESTAMP() + " . int($in);
    }

    $self->dbh->do("INSERT IGNORE INTO file_to_replicate ".
                   "SET fid=?, fromdevid=?, nexttry=$nexttry", undef, $fidid, $from_devid);
}

# reschedule all deferred replication, return number rescheduled
sub replicate_now {
    my ($self) = @_;
    return $self->dbh->do("UPDATE file_to_replicate SET nexttry = UNIX_TIMESTAMP() WHERE nexttry > UNIX_TIMESTAMP()");
}

sub reschedule_file_to_replicate_relative {
    my ($self, $fid, $in_n_secs) = @_;
    $self->dbh->do("UPDATE file_to_replicate SET nexttry = UNIX_TIMESTAMP() + ?, failcount = failcount + 1 WHERE fid = ?",
                   undef, $in_n_secs, $fid);
}

# creates a new domain, given a domain namespace string.  return the dmid on success,
# throw 'dup' on duplicate name.
sub create_domain {
    my ($self, $name) = @_;
    my $dbh = $self->dbh;

    # get the max domain id
    my $maxid = $dbh->selectrow_array('SELECT MAX(dmid) FROM domain') || 0;
    my $rv = eval {
        $dbh->do('INSERT INTO domain (dmid, namespace) VALUES (?, ?)',
                 undef, $maxid + 1, $name);
    };
    if ($self->was_duplicate_error) {
        throw("dup");
    }
    return $maxid+1 if $rv;
    die "failed to make domain";  # FIXME: the above is racy.
}

sub should_begin_replicating_fidid {
    my ($self, $fidid) = @_;
    my $lockname = "mgfs:fid:$fidid:replicate";
    return 1 if $self->get_lock($lockname, 1);
    return 0;
}

sub note_done_replicating {
    my ($self, $fidid) = @_;
    my $lockname = "mgfs:fid:$fidid:replicate";
    $self->release_lock($lockname);
}

1;

__END__

=head1 NAME

MogileFS::Store::SQLite - For-testing-only not-for-production SQLite storage for MogileFS

=head1 SEE ALSO

L<MogileFS::Store>


