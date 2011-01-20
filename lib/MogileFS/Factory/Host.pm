package MogileFS::Factory::Host;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::NewHost;

sub set {
    my ($self, $args) = @_;
    my $devfactory = MogileFS::Factory::Device->get_factory;
    return $self->SUPER::set(MogileFS::NewHost->new_from_args($args, $devfactory));
}

1;
