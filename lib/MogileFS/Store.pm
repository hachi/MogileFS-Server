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
    return bless {
        dsn    => MogileFS->config('db_dsn'),
        user   => MogileFS->config('db_user'),
        pass   => MogileFS->config('db_pass'),
    }, $subclass;
}

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

# --------------------------------------------------------------------------

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

sub update_device_usage {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(mb_total mb_used devid)], @_);
    $self->dbh->do("UPDATE device SET mb_total = ?, mb_used = ?, mb_asof = UNIX_TIMESTAMP() " .
                   "WHERE devid = ?", undef, $arg{mb_total}, $arg{mb_used}, $arg{mb_devid});
    $self->condthrow;
}

sub mark_fidid_unreachable {
    my ($self, $fidid) = @_;
    $self->dbh->do("REPLACE INTO unreachable_fids VALUES (?, UNIX_TIMESTAMP())",
                   undef, $fidid);
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


