package MogileFS::Factory;
use strict;
use warnings;

=head1

MogileFS::MogFactory - singleton class for holding some common objects.

=head1 ABOUT

This module holds a singleton for caching objects which are common but
relatively low in number. Such as devices, compared to fids.

This singleton is to be maintained by the parent process, and inherited to
children during fork. Post-fork, the cache is updated by natural commands, or
a monitor process pushing changes through the parent.

The purpose is to provide a fresh cache, without forcing new children to
wait for a monitoring run before becoming useful. It also should greatly
reduce the number of simple DB queries, as those should only happen
periodically directly from the monitor job.

=cut

my %singleton;

# Rename to new maybe?
sub get_factory {
    my $class = shift;
    if (!exists $singleton{$class}) {
        $singleton{$class} = bless {
           by_id   => {},
           by_name => {}, 
        }, $class;
    }
    return $singleton{$class};
}

# Allow unit tests to blow us up.
sub t_wipe {
    my $class = shift;
    delete $singleton{$class}; 
}

# because 'add' means bail if already exists.
sub set {
    my $self = shift;
    my $obj  = shift;
    $self->{by_id}->{$obj->id} = $obj;
    $self->{by_name}->{$obj->name} = $obj;
    return $obj;
}

sub remove {
    my $self = shift;
    my $obj  = shift;

    if (exists $self->{by_id}->{$obj->id}) {
        delete $self->{by_id}->{$obj->id};
        delete $self->{by_name}->{$obj->name};
    }
}

sub get_by_id {
    my ($self, $id) = @_;
    return $self->{by_id}->{$id};
}

sub get_by_name {
    my ($self, $name) = @_;
    return $self->{by_name}->{$name};
}

sub get_ids {
    my $self = shift;
    return keys %{$self->{by_id}};
}

sub get_names {
    my $self = shift;
    return keys %{$self->{by_name}};
}

sub get_all {
    my $self = shift;
    return values %{$self->{by_id}};
}

sub map_by_id {
    my $self = shift;
    my $set  = $self->{by_id};
    return { map { $_ => $set->{$_} } keys %{$set} };
}

sub map_by_name {
    my $self = shift;
    my $set  = $self->{by_name};
    return { map { $_ => $set->{$_} } keys %{$set} };
}

1;
