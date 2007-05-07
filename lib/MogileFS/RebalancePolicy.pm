package MogileFS::RebalancePolicy;
use strict;
use warnings;

sub new {
    my ($class) = @_;
    return bless {
        'devfid_magazine' => [], # devfids queued up for next
                                 # called to 'devfid_to_rebalance'
    }, $class;
}

# return DevFID (or undef) of a devid to migrate away
sub devfid_to_rebalance {
    my ($self) = @_;
    my $mag = $self->{devfid_magazine};
    return shift @$mag if @$mag;
    push @$mag, $self->devfids_to_rebalance;
    return shift @$mag;
}

sub devfids_to_rebalance {
    return ()
}

# return MogileFS::Device objects which shouldn't
# be replicated towards, since it wouldn't help
# out...
sub dest_devs_to_avoid {
    my $self = shift;
    ()
}

1;
