package MogileFS::Store;
use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class) = @_;
    my $dsn = MogileFS->config('db_dsn');
    my $subclass;
    if ($dsn =~ /^DBI:mysql:/i) {
        $subclass = "MogileFS::Store::MySQL";
    } else {
        die "Unknown database type: $dsn";
    }
    my $self = bless {
        dsn    => MogileFS->config('db_dsn'),
        user   => MogileFS->config('db_user'),
        pass   => MogileFS->config('db_pass'),
    }, $subclass;
    $self->init;
    return $self;
}

sub init { 1 }
sub post_dbi_connect { 1 }

sub recheck_dbh {
    my $self = shift;
    $self->{needs_ping} = 1;
}

sub dbh {
    my $self = shift;
    if ($self->{dbh}) {
        if ($self->{needs_ping}) {
            $self->{needs_ping} = 0;
            $self->{dbh} = undef unless $self->{dbh}->ping;
        }
        return $self->{dbh} if $self->{dbh};
    }

    $self->{dbh} = DBI->connect($self->{dsn}, $self->{user}, $self->{pass}, {
        PrintError => 0,
        AutoCommit => 1,
        RaiseError => 0,  # FIXME: FUTURE: turn this on.  have to validate all callers first
    }) or
        die "Failed to connect to database: " . DBI->errstr;
    $self->post_dbi_connect;
    return $self->{dbh};
}

sub ping {
    my $self = shift;
    return $self->dbh->ping;
}

sub condthrow {
    my $self = shift;
    my $dbh = $self->dbh;
    die "Database error: " . $dbh->errstr if $dbh->err;
}

sub _valid_params {
    my ($self, $vlist, %uarg) = @_;
    my %ret;
    $ret{$_} = delete $uarg{$_} foreach @$vlist;
    croak("Bogus options") if %uarg;
    return %ret;
}

sub was_duplicate_error {
    my $self = shift;
    my $dbh = $self->dbh;
    die "UNIMPLEMENTED";
}

# --------------------------------------------------------------------------

# return new classid on success (non-zero integer), die on failure
sub create_class {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(dmid classname mindevcount)], @_);
    die "UNIMPLEMENTED";
}

# return 1 on success (or miss), 0 on duplicate name failure, die (exception) on db failure
sub update_class {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(dmid classid classname mindevcount)], @_);
    my $rv = eval {
        $self->dbh->do("UPDATE class SET classname=?, mindevcount=? WHERE dmid=? AND classid=?",
                       undef, $arg{classname}, $arg{mindevcount}, $arg{dmid}, $arg{classid});
    };
    return 0 if $self->was_duplicate_error;
    $self->condthrow;
    return 1;
}

sub nfiles_with_dmid_classid_devcount {
    my ($self, $dmid, $classid, $devcount) = @_;
    return $self->dbh->selectrow_array('SELECT COUNT(*) FROM file WHERE dmid = ? AND classid = ? AND devcount = ?',
                                       undef, $dmid, $classid, $devcount);
}

sub set_server_setting {
    my ($self, $key, $val) = @_;
    my $dbh = $self->dbh;

    if (defined $val) {
        $dbh->do("REPLACE INTO server_settings (field, value) VALUES (?, ?)", undef, $key, $val);
    } else {
        $dbh->do("DELETE FROM server_settings WHERE field=?", undef, $key);
    }

    die "Error updating 'server_settings': " . $dbh->errstr if $dbh->err;
    return 1;
}

sub server_setting {
    my ($self, $key) = @_;
    return $self->dbh->selectrow_array("SELECT value FROM server_settings WHERE field=?",
                                       undef, $key);
}

# register a tempfile and return the fidid, which should be allocated
# using autoincrement/sequences if the passed in fid is undef.  however,
# if fid is passed in, that value should be used and returned.
#
# return -1 if the fid is already in use.
# return undef or 0 on any other error.
#
sub register_tempfile {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(fid dmid key classid devids)], @_);

    die "NOT IMPLEMENTED";
}

# return hashref of row containing columns "fid, dmid, dkey, length,
# classid, devcount" provided a $dmid and $key (dkey).  or undef if no
# row.
sub file_row_from_dmid_key {
    my ($self, $dmid, $key) = @_;
    return $self->dbh->selectrow_hashref("SELECT fid, dmid, dkey, length, classid, devcount ".
                                         "FROM file WHERE dmid=? AND dkey=?",
                                         undef, $dmid, $key);
}

# return hashref of columns classid, dmid, dkey, given a $fidid, or return undef
sub tempfile_row_from_fid {
    my ($self, $fidid) = @_;
    return $self->dbh->selectrow_hashref("SELECT classid, dmid, dkey ".
                                         "FROM tempfile WHERE fid=?",
                                         undef, $fidid);
}

sub update_device_usage {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(mb_total mb_used devid)], @_);
    $self->dbh->do("UPDATE device SET mb_total = ?, mb_used = ?, mb_asof = UNIX_TIMESTAMP() " .
                   "WHERE devid = ?", undef, $arg{mb_total}, $arg{mb_used}, $arg{devid});
    $self->condthrow;
}

sub mark_fidid_unreachable {
    my ($self, $fidid) = @_;
    $self->dbh->do("REPLACE INTO unreachable_fids VALUES (?, UNIX_TIMESTAMP())",
                   undef, $fidid);
}

sub set_device_weight {
    my ($self, $devid, $weight) = @_;
    $self->dbh->do('UPDATE device SET weight = ? WHERE devid = ?', undef, $weight, $devid);
    $self->condthrow;
}

sub set_device_state {
    my ($self, $devid, $state) = @_;
    $self->dbh->do('UPDATE device SET status = ? WHERE devid = ?', undef, $state, $devid);
    $self->condthrow;
}

sub delete_fidid {
    my ($self, $fidid) = @_;
    $self->dbh->do("DELETE FROM file WHERE fid=?", undef, $fidid);
    $self->condthrow;
    $self->dbh->do("DELETE FROM tempfile WHERE fid=?", undef, $fidid);
    $self->condthrow;
    $self->dbh->do("REPLACE INTO file_to_delete SET fid=?", undef, $fidid);
    $self->condthrow;
}

sub delete_tempfile_row {
    my ($self, $fidid) = @_;
    $self->dbh->do("DELETE FROM tempfile WHERE fid=?", undef, $fidid);
    $self->condthrow;
}

sub replace_into_file {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(fidid dmid key length classid)], @_);
    $self->dbh->do("REPLACE INTO file ".
                   "SET ".
                   "  fid=?, dmid=?, dkey=?, length=?, ".
                   "  classid=?, devcount=0", undef,
                   @arg{'fidid', 'dmid', 'key', 'length', 'classid'});
    $self->condthrow;
}

# returns 1 on success, 0 on duplicate key error, dies on exception
sub rename_file {
    my ($self, $fidid, $to_key) = @_;
    die "UNIMPLEMENTED";
}

# returns a hash of domains. Key is namespace, value is dmid.
sub get_all_domains {
    my ($self) = @_;
    my $domains = $self->dbh->selectall_arrayref('SELECT namespace, dmid FROM domain');
    return map { ($_->[0], $_->[1]) } @{$domains || []};
}

# add a record of fidid existing on devid
# returns 1 on success, 0 on duplicate
sub add_fidid_to_devid {
    my ($self, $fidid, $devid) = @_;
    die "UNIMPLEMENTED";
}

# get all hosts from database, returns them as list of hashrefs, hashrefs being the row contents.
sub get_all_hosts {
    my ($self) = @_;
    my $sth = $self->dbh->prepare("SELECT /*!40000 SQL_CACHE */ hostid, status, hostname, " .
                                  "hostip, http_port, http_get_port, altip, altmask FROM host");
    $sth->execute;
    my @ret;
    while (my $row = $sth->fetchrow_hashref) {
        push @ret, $row;
    }
    return @ret;
}

# get all devices from database, returns them as list of hashrefs, hashrefs being the row contents.
sub get_all_devices {
    my ($self) = @_;
    my $sth = $self->dbh->prepare("SELECT /*!40000 SQL_CACHE */ devid, hostid, mb_total, " .
                                "mb_used, mb_asof, status, weight FROM device");
    $sth->execute;
    my @return;
    while (my $row = $sth->fetchrow_hashref) {
        push @return, $row;
    }
    return @return;
}

sub update_devcount {
    my ($self, $fidid) = @_;
    my $dbh = $self->dbh;
    my $ct = $dbh->selectrow_array("SELECT COUNT(*) FROM file_on WHERE fid=?",
                                   undef, $fidid);

    $dbh->do("UPDATE file SET devcount=? WHERE fid=?", undef,
              $ct, $fidid);

}



1;

__END__

=head1 NAME

MogileFS::Store - data storage provider.  base class.

=head1 ABOUT

MogileFS aims to be database-independent (though currently as of late
2006 only works with MySQL).  In the future, the server will create a
singleton instance of type "MogileFS::Store", like
L<MogileFS::Store::MySQL>, and all database interaction will be
through it.

=head1 SEE ALSO

L<MogileFS::Store::MySQL>


