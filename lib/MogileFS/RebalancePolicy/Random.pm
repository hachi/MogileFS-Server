package MogileFS::RebalancePolicy::Random;
use strict;
use warnings;
use base 'MogileFS::RebalancePolicy';
use List::Util ();

sub devfids_to_rebalance {
    my ($self) = @_;

    my @devs = List::Util::shuffle(grep
                                   { $_->can_read_from && $_->can_delete_from }
                                   MogileFS::Device->devices)
        or return ();

    my @ret;
    my $sto = Mgd::get_store();
    while (my $dev = shift @devs) {
        my @fids = $sto->random_fids_on_device($dev->id, 50);
        foreach my $fid (@fids) {
            push @ret, MogileFS::DevFID->new($dev, $fid);
        }
        last if @ret >= 50;
    }
    return @ret;
}

1;
