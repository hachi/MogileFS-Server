package MogileFS::Factory::Domain;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::Domain;

sub set {
    my ($self, $args) = @_;
    my $classfactory = MogileFS::Factory::Class->get_factory;
    my $dom = $self->SUPER::set(MogileFS::Domain->new_from_args($args, $classfactory));

    # Stupid awkward classes have a magic "default"
    # If it exists in the DB, it will be overridden.
    my $cls = $classfactory->get_by_id($dom->id, 0);
    unless ($cls) {
        $classfactory->set({ dmid => $dom->id, classid => 0,
            classname => 'default',
            mindevcount => MogileFS->config('default_mindevcount')});
    }
    return $dom;
}

1;
