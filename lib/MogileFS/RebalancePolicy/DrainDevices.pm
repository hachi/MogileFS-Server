package MogileFS::RebalancePolicy::DrainDevices;
use strict;
use warnings;
use base 'MogileFS::RebalancePolicy';
use MogileFS::Util qw(weighted_list error);

# This rebalance policy is used by the replicator code itself,
# and not configured by the end-user.  (well, the end-user can set
# this class as their rebalance policy if they want, but that
# just means the Replicate worker will run it twice in a row...
# to no effect (we were already idle)

sub new {
    my $class = shift;
    my $self = $class->SUPER::new;
    $self->{paused_until}    = 0;
    $self->{device_is_empty} = {};
    return $self;
}

my $singleton;
sub instance {
    my $class = shift;
    return $singleton ||= $class->new;
}

sub devfids_to_rebalance {
    my ($self) = @_;

    my $now = time();
    if ($self->{paused_until} > $now) {
        return ();
    }

    # first, find a device.. only migrate away from disks which are in the 50th+ percentile,
    # in terms of fullness.  then picked one based on a weighted selection of their fullness.
    my @devs = List::Util::shuffle(
                                   grep { ! $self->{device_is_empty}{$_->devid} }
                                   grep { $_->dstate->should_drain }
                                   MogileFS::Device->devices);
    unless (@devs) {
        $self->{paused_until} = $now + 5;
        return ();
    }

    my @ret;
    my $sto = Mgd::get_store();
    while (my $dev = shift @devs) {
        my @fids = $sto->random_fids_on_device($dev->id, 50);
        unless (@fids) {
            error("Device is found to be empty, while draining: dev" . $dev->id);
            $self->{device_is_empty}{$dev->id} = 1;
        }
        foreach my $fid (@fids) {
            push @ret, MogileFS::DevFID->new($dev, $fid);
        }
        last if @ret >= 50;
    }

    return @ret;
}

1;
