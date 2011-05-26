package MogileFS::Factory::Device;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::Device;

sub set {
    my ($self, $args) = @_;
    my $hostfactory = MogileFS::Factory::Host->get_factory;
    return $self->SUPER::set(MogileFS::Device->new_from_args($args, $hostfactory));
}

1;
