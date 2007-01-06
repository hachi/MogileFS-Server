package MogileFS::DevFID;
use strict;
use warnings;

sub new {
    my ($class, $devarg, $fidarg) = @_;
    return bless {
        devid => ref $devarg ? $devarg->id : $devarg,
        dev   => ref $devarg ? $devarg     : undef,
        fidid => ref $fidarg ? $fidarg->id : $fidarg,
        fid   => ref $fidarg ? $fidarg     : undef,
    }, $class;
}

# --------------------------------------------------------------------------

sub devid { $_[0]{devid} }
sub fidid { $_[0]{fidid} }

sub device {
    my $self = shift;
    return $self->{dev} ||= MogileFS::Device->of_devid($self->{devid});
}

sub fid {
    my $self = shift;
    return $self->{fid} ||= MogileFS::FID->new($self->{fidid});
}

sub url {
    my $self = shift;
    return $self->_make_full_url(0);
}

sub get_url {
    my $self = shift;
    return $self->_make_full_url(1);
}

sub vivify_directories {
    my $self = shift;
    my $url = $self->url;
    MogileFS::Device->vivify_directories($url);
}

# returns true if size seen matches fid's length
sub size_matches {
    my $self = shift;
    my $url = $self->get_url;
    my $fid = $self->fid;
    return MogileFS::HTTPFile->at($url)->size == $fid->length;
}

# returns just the URI path component without scheme/host
sub uri_path {
    my $self = shift;
    my $devid = $self->{devid};
    my $fidid = $self->{fidid};

    my $nfid = sprintf '%010d', $fidid;
    my ( $b, $mmm, $ttt, $hto ) = ( $nfid =~ m{(\d)(\d{3})(\d{3})(\d{3})} );

    return "/dev$devid/$b/$mmm/$ttt/$nfid.fid";
}

sub _make_full_url {
    # set use_get_port to be true to specify to use the get port
    my ($self, $use_get_port) = @_;

    # get some information we'll need
    my $dev  = $self->device   or return undef;
    my $host = $dev->host      or return undef;
    return undef unless $host->exists;

    my $path   = $self->uri_path;
    my $hostip = $host->ip;
    my $port   = $use_get_port ? $host->http_get_port : $host->http_port;

    return "http://$hostip:$port$path";
}

sub add_to_db {
    my ($self, $no_lock) = @_;

    my $sto = Mgd::get_store();
    if ($sto->add_fidid_to_devid($self->{fidid}, $self->{devid})) {
        return $self->fid->update_devcount(no_lock => $no_lock);
    } else {
        # was already on that device
        return 1;
    }
}

1;

__END__

=head1 NAME

MogileFS::DevFID - represents a FID on a device

=head1 ABOUT

This class represents the (devid, fidid) tuple.  That is, a specific
version on a file on a specific device.  See L<MogileFS::Device> and
L<MogileFS::FID>.
