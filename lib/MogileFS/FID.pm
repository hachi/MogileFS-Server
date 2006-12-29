package MogileFS::FID;
use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class, $fidid) = @_;
    croak("Invalid fidid") unless $fidid;
    return bless {
        fidid    => $fidid,
    }, $class;
}

# quick port of old API.  perhaps not ideal.
sub new_from_dmid_and_key {
    my ($class, $dmid, $key) = @_;
    my $row = Mgd::get_store()->file_row_from_dmid_key($dmid, $key)
        or return undef;
    $row->{fidid} = delete $row->{fid};
    return bless $row, $class;
}

# --------------------------------------------------------------------------

sub length {
    my $self = shift;
    die "FIXME: lazily load.  No length defined on this object" unless defined $self->{length};
    return $self->{length};
}

sub id { $_[0]{fidid} }

sub update_devcount {
    my ($self, %opts) = @_;

    my $no_lock = delete $opts{no_lock};
    croak "Bogus options" if %opts;

    my $fidid = $self->{fidid};
    my $dbh = Mgd::get_dbh()
        or return 0;

    my $lockname = "mgfs:fid:$fidid";
    unless ($no_lock) {
        my $lock = $dbh->selectrow_array("SELECT GET_LOCK(?, 10)", undef,
                                         $lockname);
        return 0 unless $lock;
    }
    my $ct = $dbh->selectrow_array("SELECT COUNT(*) FROM file_on WHERE fid=?",
                                   undef, $fidid);

    $dbh->do("UPDATE file SET devcount=? WHERE fid=?", undef,
             $ct, $fidid);

    unless ($no_lock) {
        $dbh->selectrow_array("SELECT RELEASE_LOCK(?)", undef, $lockname);
    }

    return 1;
}

sub enqueue_for_replication {
    my ($self, %opts) = @_;
    my $in = delete $opts{in};
    my $from_dev = delete $opts{from_device};
    croak("Unknown options to enqueue_for_replication") if %opts;
    my $dbh = Mgd::get_dbh();
    my $from_devid = $from_dev ? $from_dev->id : undef;
    my $nexttry = 0;
    if ($in) {
        $nexttry = "UNIX_TIMESTAMP() + " . int($in);
    }
    $dbh->do("INSERT IGNORE INTO file_to_replicate ".
             "SET fid=?, fromdevid=?, nexttry=$nexttry", undef, $self->id, $from_devid);
}

sub mark_unreachable {
    my $self = shift;
    # update database table
    Mgd::get_store()->mark_fidid_unreachable($self->id);
}

1;

__END__

=head1 NAME

MogileFS::FID - represents a unique, immutable version of a file

=head1 ABOUT

This class represents a "fid", or "file id", which is a unique
revision of a file.  If you upload a file with the same key
("filename") a dozen times, each one has a unique "fid".  Fids are
immutable, and are what are replicated around the MogileFS farm.
