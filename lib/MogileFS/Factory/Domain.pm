package MogileFS::Factory::Domain;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::NewDomain;

sub set {
    my ($self, $args) = @_;
    my $classfactory = MogileFS::Factory::Class->get_factory;
    return $self->SUPER::set(MogileFS::NewDomain->new_from_args($args, $classfactory));
}

1;
