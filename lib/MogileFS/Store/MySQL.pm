package MogileFS::Store::MySQL;
use strict;
use warnings;
use DBI 1.44;
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
sub can_insert_multi { 1 }
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

sub pre_daemonize_checks {
    my $self = shift;
    # Jay Buffington, from the mailing lists, writes:

    # > > Is your DBI version at least 1.43? The Makefile.PL of DBD::mysql shows
    # > > that code for last_insert_it is compiled in only if DBD::mysql is built
    # > > with DBI 1.43 or newer.
    #> Yes, I have 1.53.
    #> jay@webdev:~$ perl -MDBI -le 'print $DBI::VERSION'
    #> 1.53
    #>
    #> BUT I just re-installed 2.9006 while researching this and my test
    #> script started working.  I just reran the mogile server test suite and
    #> all test passed!
    #>
    #> Problem solved!
    #>
    #> The original DBD::mysql 2.9006 was installed from a RPM.  I bet that
    #> it was built against a DBI older than 1.43, so it didn't support
    #> LAST_INSERT_ID.

    # So...
    #   since we don't know what version of DBI their DBD::mysql was built against,
    #   let's just test that last_insert_id works.

    my $id = eval {
        $self->register_tempfile(dmid => 99,
                                 key  => "_server_startup_test");
    };
    unless ($id) {
        die "MySQL self-tests failed.  Your DBD::mysql might've been built against an old DBI version.\n";
    }

}

1;

__END__

=head1 NAME

MogileFS::Store::MySQL - MySQL data storage for MogileFS

=head1 SEE ALSO

L<MogileFS::Store>


