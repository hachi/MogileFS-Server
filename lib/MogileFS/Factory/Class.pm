package MogileFS::Factory::Class;
use strict;
use warnings;
use base 'MogileFS::Factory';

use MogileFS::Class;

# This class is a reimplementation since classids and classnames
# are not globally unique... uses the same interface.
# Stupid/wasteful.
sub set {
    my ($self, $args) = @_;
    my $domain_factory = MogileFS::Factory::Domain->get_factory;

    my $class = MogileFS::Class->new_from_args($args, $domain_factory);
    my $dmid = $class->dmid;
    $self->{by_id}->{$dmid}->{$class->id}     = $class;
    $self->{by_name}->{$dmid}->{$class->name} = $class;
    return $class;
}

# Class factory is very awkward. Lets be very flexible in what we take; a
# domain object + id, a dmid, or a string with dmid-classid.
sub _find_ids {
    my $self = shift;
    my $dom  = shift;
    my $dmid = ref $dom ? $dom->id : $dom;
    if ($dmid =~ m/^(\d+)-(\d+)$/) {
        return $1, $2;
    }
    return $dmid, @_;
}

# Example of what we could use for testing.
# Test creates the object, injects its own factory, then hands it to us.
sub set_from_obj {
    my ($self, $obj) = @_;
}

sub remove {
    my $self  = shift;
    my $class = shift;
    my $domid = $class->dmid;
    my $clsid = $class->id;
    if (exists $self->{by_id}->{$domid}->{$clsid}) {
        delete $self->{by_id}->{$domid}->{$clsid};
        delete $self->{by_name}->{$domid}->{$class->name};
    }
}

sub get_by_id {
    my $self = shift;
    my ($dmid, $id) = $self->_find_ids(@_);
    return $self->{by_id}->{$dmid}->{$id};
}

sub get_by_name {
    my $self = shift;
    my ($dmid, $name) = $self->_find_ids(@_);
    return $self->{by_name}->{$dmid}->{$name};
}

sub get_ids {
    my $self = shift;
    my ($dmid) = $self->_find_ids(@_);
    return keys %{$self->{by_id}->{$dmid}};
}

sub get_names {
    my $self = shift;
    my ($dmid) = $self->_find_ids(@_);
    return keys %{$self->{by_name}->{$dmid}};
}

sub get_all {
    my $self = shift;
    my ($dmid) = $self->_find_ids(@_);
    return values %{$self->{by_id}->{$dmid}};
}

sub map_by_id {
    my $self = shift;
    my ($dmid) = $self->_find_ids(@_);
    my $set = $self->{by_id}->{$dmid};
    return { map { $_ => $set->{$_} } keys %{$set} };
}

sub map_by_name {
    my $self = shift;
    my ($dmid) = $self->_find_ids(@_);
    my $set = $self->{by_name}->{$dmid};
    return { map { $_ => $set->{$_} } keys %{$set} };
}

1;
