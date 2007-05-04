package MogileFS::ReplicationPolicy;
use strict;

=head1 NAME

MogileFS::ReplicationPolicy - base class for file replication policies

=head1 DESCRIPTION

A MogileFS replication policy class implements policy for how files
should be replicated around.

....

=cut

# returns:
#   0:      replication sufficient
#   undef:  no suitable recommendations currently.
#   >0:     devid to replicate to.
sub replicate_to {
    my ($class, %args) = @_;
    my $fid      = delete $args{fid};      # fid scalar to copy
    my $on_devs  = delete $args{on_devs};  # arrayref of device objects
    my $all_devs = delete $args{all_devs}; # hashref of { devid => MogileFS::Device }
    my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round
    my $min      = delete $args{min};      # configured min devcount for this class

    warn "Unknown parameters: " . join(", ", sort keys %args) if %args;
    die "Missing parameters" unless $on_devs && $all_devs && $failed && $fid;

    die "UNIMPLEMENTED 'replicate_to' in $class";
}



1;
