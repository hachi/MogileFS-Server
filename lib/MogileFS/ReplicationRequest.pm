package MogileFS::ReplicationRequest;
use strict;
require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(rr_upgrade ALL_GOOD TEMP_NO_ANSWER);

my $no_answer = bless { temp_fail => 1 };
sub TEMP_NO_ANSWER () { $no_answer }
my $all_good = bless { all_good => 1 };
sub ALL_GOOD () { $all_good }

# upgrades the return values from old-style ReplicationPolicy classes
# to MogileFS::ReplicationRequest objects, unless they already are,
# in which case they're passed through unchanged.  provides peaceful
# upgrade path for old plugins.
sub rr_upgrade {
    my ($rv) = @_;
    return $rv            if ref $rv;
    return TEMP_NO_ANSWER if !defined $rv;
    return ALL_GOOD       if !$rv;
    return MogileFS::ReplicationRequest->replicate_to($rv);
}

# NOTE: this legacy interface provides no way to say that provided
# dev isn't ideal, so we just treat it as ideal here.
sub replicate_to {
    my ($class, $dev) = @_;
    $dev = MogileFS::Device->of_devid($dev) unless ref $dev;
    return bless {
        ideal_next => [ $dev ],
    }, $class;
}

############################################################################

sub is_happy {
    my $self = shift;
    return $self->{all_good};
}

# returns array of MogileFS::Device objs, in preferred order, one of
# which (but not multiple) would satisify the replication policy
# for its next step.  at which point the replication policy needs
# to be asked again what the next step is.
sub copy_to_one_of_ideally {
    my $self = shift;
    return @{ $self->{ideal_next} || [] };
}

# like above, but replication policy isn't happy about these choices,
# so a reevaluation of this replication decision should be made in the
# future, when new disks/hosts might be available.
sub copy_to_one_of_desperate {
    my $self = shift;
    return @{ $self->{desperate_next} || [] };
}


1;

