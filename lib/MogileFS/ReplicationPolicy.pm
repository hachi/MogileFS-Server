package MogileFS::ReplicationPolicy;
use strict;

=head1 NAME

MogileFS::ReplicationPolicy - base class for file replication policies

=head1 DESCRIPTION

A MogileFS replication policy class implements policy for how files
should be replicated around.

....

=cut

# parse a policy description string, instantiating object(s) along the way
# given $str can be either a scalar, or a scalarref that's eaten away as it's parsed.
sub new_from_policy_string {
    my ($class, $str_a) = @_;

    # simple case for normal callers:  they give us a scalar, but internally
    # we work with it as a scalarref that we eat away while parsing.
    my $strref = ref $str_a ? $str_a : \$str_a;

    $$strref =~ s/^\s*([\w:]+)// or die "Failed to parse policy string: $$strref";
    my ($polclass) = ($1);
    $polclass = "MogileFS::ReplicationPolicy::$polclass" unless $polclass =~ /:/;

    my $rv = eval "use $polclass; 1";
    if ($@ || !$rv) {
        die "Failed to load replication policy class $polclass: $@\n";
    }

    return $polclass->new_from_policy_args($strref);
}

# returns:
#   0:      replication sufficient
#   undef:  no suitable recommendations currently.
#   >0:     devid to replicate to.
sub replicate_to {
    my ($self, %args) = @_;
    my $fid      = delete $args{fid};      # fid scalar to copy
    my $on_devs  = delete $args{on_devs};  # arrayref of device objects
    my $all_devs = delete $args{all_devs}; # hashref of { devid => MogileFS::Device }
    my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round
    my $min      = delete $args{min};      # configured min devcount for this class

    warn "Unknown parameters: " . join(", ", sort keys %args) if %args;
    die "Missing parameters" unless $on_devs && $all_devs && $failed && $fid;

    die "UNIMPLEMENTED 'replicate_to' in " . (ref($self) || $self);
}



1;
