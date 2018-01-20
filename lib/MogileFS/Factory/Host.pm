package MogileFS::Factory::Host;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::Host;

sub set {
    my ($self, $args) = @_;
    my $devfactory = MogileFS::Factory::Device->get_factory;
    return $self->SUPER::set(MogileFS::Host->new_from_args($args, $devfactory));
}

1;
