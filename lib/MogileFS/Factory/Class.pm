package MogileFS::Factory::Class;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::NewClass;

# This class is a reimplementation since classids and classnames
# are not globally unique... uses the same interface.
# Stupid/wasteful.
sub set {
    my ($self, $domain, $args) = @_;
    my $domain_factory = MogileFS::Factory::Domain->get_factory;
    # FIXME: Inject the dmid into the class somehow.
    my $class = MogileFS::NewClass->new_from_args($args, $domain_factory);
    $self->{by_id}->{$domain->id}->{$class->id}     = $class;
    $self->{by_name}->{$domain->id}->{$class->name} = $class;
    return $class;
}

# Example of what we could use for testing.
# Test creates the object, injects its own factory, then hands it to us.
sub set_from_obj {
    my ($self, $obj) = @_;
}

sub remove {
    my $self  = shift;
    my $class = shift;
    my $domid = $class->domain->id;
    my $clsid = $class->id;
    if (exists $self->{by_id}->{$domid}->{$clsid}) {
        delete $self->{by_id}->{$domid}->{$clsid};
        delete $self->{by_name}->{$domid}->{$class->name};
    }
}

sub get_by_id {
    my ($self, $domain, $id) = @_;
    return $self->{by_id}->{$domain->id}->{$id};
}

sub get_by_name {
    my ($self, $domain, $name) = @_;
    return $self->{by_name}->{$domain->id}->{$name};
}

sub get_ids {
    my ($self, $domain) = @_;
    return keys %{$self->{by_id}->{$domain->id}};
}

sub get_names {
    my ($self, $domain) = @_;
    return keys %{$self->{by_name}->{$domain->id}};
}

sub get_all {
    my ($self, $domain) = @_;
    return values %{$self->{by_id}->{$domain->id}};
}

sub map_by_id {
    my ($self, $domain) = @_;
    my $set = $self->{by_id}->{$domain->id};
    return { map { $_ => $set->{$_} } keys %{$set} };
}

sub map_by_name {
    my ($self, $domain) = @_;
    my $set = $self->{by_name}->{$domain->id};
    return { map { $_ => $set->{$_} } keys %{$set} };
}

1;
