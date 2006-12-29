package MogileFS::Store;
use strict;
use warnings;

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
    }) or
        die "Failed to connect to database: " . DBI->errstr;
    return $self->{dbh};
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


