package MogileFS::Factory::Device;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::NewDevice;

sub set {
    my ($self, $args) = @_;
    my $hostfactory = MogileFS::Factory::Host->get_factory;
    return $self->SUPER::set(MogileFS::NewDevice->new_from_args($args, $hostfactory));
}

1;
