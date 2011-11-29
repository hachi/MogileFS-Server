package MogileFS::Class;
use strict;
use warnings;
use MogileFS::Util qw(throw);
use MogileFS::Checksum;

=head1

MogileFS::Class - Class class.

=cut

sub new_from_args {
    my ($class, $args, $domain_factory) = @_;
    return bless {
        domain_factory => $domain_factory,
        mindevcount => 2,
        %{$args},
    }, $class;
}

# Instance methods:

sub id   { $_[0]{classid} }
sub name { $_[0]{classname} }
sub mindevcount { $_[0]{mindevcount} }
sub dmid { $_[0]{dmid} }
sub hashtype { $_[0]{hashtype} }
sub hashname { $MogileFS::Checksum::TYPE2NAME{$_[0]{hashtype}} }

sub hashtype_string {
    my $self = shift;
    $self->hashtype ? $self->hashname : "NONE";
}

sub repl_policy_string {
    my $self = shift;
    return $self->{replpolicy} ? $self->{replpolicy}
        : 'MultipleHosts()';
}

sub repl_policy_obj {
    my $self = shift;
    if (! $self->{_repl_policy_obj}) {
        my $polstr = $self->repl_policy_string;
        # Parses the string.
        $self->{_repl_policy_obj} =
            MogileFS::ReplicationPolicy->new_from_policy_string($polstr);
    }
    return $self->{_repl_policy_obj};
}

sub domain {
    my $self = shift;
    return $self->{domain_factory}->get_by_id($self->{dmid});
}

sub has_files {
    my $self = shift;
    return Mgd::get_store()->class_has_files($self->{dmid}, $self->id);
}

sub observed_fields {
    return {};
}

1;
