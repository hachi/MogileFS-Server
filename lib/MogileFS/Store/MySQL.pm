package MogileFS::Store::MySQL;
use strict;
use warnings;
use DBI;
use DBD::mysql;
use MogileFS::Util qw(throw);
use base 'MogileFS::Store';

# --------------------------------------------------------------------------
# Package methods we override
# --------------------------------------------------------------------------

sub dsn_of_dbhost {
    my ($class, $dbname, $host) = @_;
    return "DBI:mysql:$dbname;host=$host";
}

sub dsn_of_root {
    my ($class, $dbname, $host) = @_;
    return "DBI:mysql:mysql";
}

# --------------------------------------------------------------------------
# Store-related things we override
# --------------------------------------------------------------------------

sub init {
    my $self = shift;
    $self->SUPER::init;
    $self->{lock_depth} = 0;
}

sub post_dbi_connect {
    my $self = shift;
    $self->SUPER::post_dbi_connect;
    $self->{lock_depth} = 0;
}

sub was_duplicate_error {
    my $self = shift;
    my $dbh = $self->dbh;
    return 0 unless $dbh->err;
    return 1 if $dbh->err == 1062 || $dbh->errstr =~ /duplicate/i;
}

sub table_exists {
    my ($self, $table) = @_;
    return eval {
        my $sth = $self->dbh->prepare("DESCRIBE $table");
        $sth->execute;
        my $rec = $sth->fetchrow_hashref;
        return $rec ? 1 : 0;
    };
}

sub can_replace      { 1 }
sub can_insertignore { 1 }
sub unix_timestamp { "UNIX_TIMESTAMP()" }

# --------------------------------------------------------------------------
# Functions specific to Store::MySQL subclass.  Not in parent.
# --------------------------------------------------------------------------

# attempt to grab a lock of lockname, and timeout after timeout seconds.
# returns 1 on success and 0 on timeout
sub get_lock {
    my ($self, $lockname, $timeout) = @_;
    die "Lock recursion detected (grabbing $lockname, had $self->{last_lock}).  Bailing out." if $self->{lock_depth};

    my $lock = $self->dbh->selectrow_array("SELECT GET_LOCK(?, ?)", undef, $lockname, $timeout);
    if ($lock) {
        $self->{lock_depth} = 1;
        $self->{last_lock}  = $lockname;
    }
    return $lock;
}

# attempt to release a lock of lockname.
# returns 1 on success and 0 if no lock we have has that name.
sub release_lock {
    my ($self, $lockname) = @_;
    my $rv = $self->dbh->selectrow_array("SELECT RELEASE_LOCK(?)", undef, $lockname);
    $self->{lock_depth} = 0;
    return $rv;
}

sub column_type {
    my ($self, $table, $col) = @_;
    my $sth = $self->dbh->prepare("DESCRIBE $table");
    $sth->execute;
    while (my $rec = $sth->fetchrow_hashref) {
        if ($rec->{Field} eq $col) {
            $sth->finish;
            return $rec->{Type};
        }
    }
    return undef;
}

# --------------------------------------------------------------------------
# Test suite things we override
# --------------------------------------------------------------------------

sub new_temp {
    my $dbname = "tmp_mogiletest";
    _create_mysql_db($dbname);

    system("$FindBin::Bin/../mogdbsetup", "--yes", "--dbname=$dbname")
        and die "Failed to run mogdbsetup ($FindBin::Bin/../mogdbsetup).";

    return MogileFS::Store->new_from_dsn_user_pass("DBI:mysql:$dbname",
                                                   "root",
                                                   "");
}

my $rootdbh;
sub _root_dbh {
    return $rootdbh ||= DBI->connect("DBI:mysql:mysql", "root", "", { RaiseError => 1 })
        or die "Couldn't connect to local MySQL database a root";
}

sub _create_mysql_db {
    my $dbname = shift;
    _drop_mysql_db($dbname);
    _root_dbh()->do("CREATE DATABASE $dbname");
}

sub _drop_mysql_db {
    my $dbname = shift;
    _root_dbh()->do("DROP DATABASE IF EXISTS $dbname");
}


# --------------------------------------------------------------------------
# Data-access things we override
# --------------------------------------------------------------------------

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

sub upgrade_add_host_getport {
    my $self = shift;
    # see if they have the get port, else update it
    unless ($self->column_type("host", "http_get_port")) {
        $self->dowell("ALTER TABLE host ADD COLUMN http_get_port MEDIUMINT UNSIGNED AFTER http_port");
    }

}
sub upgrade_add_host_altip {
    my $self = shift;
    unless ($self->column_type("host", "altip")) {
        $self->dowell("ALTER TABLE host ADD COLUMN altip VARCHAR(15) AFTER hostip");
        $self->dowell("ALTER TABLE host ADD COLUMN altmask VARCHAR(18) AFTER altip");
        $self->dowell("ALTER TABLE host ADD UNIQUE altip (altip)");
    }
}

sub upgrade_add_device_asof {
    my $self = shift;
    unless ($self->column_type("device", "mb_asof")) {
        $self->dowell("ALTER TABLE device ADD COLUMN mb_asof INT(10) UNSIGNED AFTER mb_used");
    }
}

sub upgrade_add_device_weight {
    my $self = shift;
    unless ($self->column_type("device", "weight")) {
        $self->dowell("ALTER TABLE device ADD COLUMN weight MEDIUMINT DEFAULT 100 AFTER status");
    }

}

sub upgrade_add_device_readonly {
    my $self = shift;
    unless ($self->column_type("device", "status") =~ /readonly/) {
        $self->dowell("ALTER TABLE device MODIFY COLUMN status ENUM('alive', 'dead', 'down', 'readonly')");
    }
}

1;

__END__

=head1 NAME

MogileFS::Store::MySQL - MySQL data storage for MogileFS

=head1 SEE ALSO

L<MogileFS::Store>


