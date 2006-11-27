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

sub id { $_[0]{fidid} }

sub update_devcount {
    my $self = shift;
    return Mgd::update_fid_devcount($self->{fidid});
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

1;
