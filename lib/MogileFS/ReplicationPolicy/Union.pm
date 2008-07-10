package MogileFS::ReplicationPolicy::Union;
use strict;
use base 'MogileFS::ReplicationPolicy';
use MogileFS::ReplicationRequest qw(ALL_GOOD TOO_GOOD TEMP_NO_ANSWER);

sub new_from_policy_args {
    my ($class, $argref) = @_;

    # first, eat off the open paren
    $$argref =~ s/^\s*\(\s*//;

    my @policies;
  POLICY:
    while (1) {
        my $pol = MogileFS::ReplicationPolicy->new_from_policy_string($argref);
        push @policies, $pol;
        # eat a comma if it's there.
        $$argref =~ s/^\s*\,\s*//;
        last if $$argref =~ s/^\s*\)\s*//;
    }

    return bless {
        policies => \@policies,
    }, $class;
}


sub replicate_to {
    my ($self, %args) = @_;

    # TODO: walk $self->{
    die "not implemented";
}

1;

__END__

=head1 NAME

MogileFS::ReplicationPolicy::Union -- satisfy 2 or more replication policies

=head1 RULES

Use this replication policy to satisfy multiple replication policies.
For instance:

    Union(MultipleHosts(3), OnDevice(7))

Would make sure a class' files replicate on 3 unique hosts, and are
also on device 7 (which is perhaps your backup device).

=head1 SEE ALSO

L<MogileFS::Worker::Replicate>

L<MogileFS::ReplicationPolicy>

L<MogileFS::ReplicationPolicy::MultipleHosts>

L<MogileFS::Class>



