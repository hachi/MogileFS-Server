package MogileFS::ReplicationPolicy::MultipleDevices;
use strict;
use base 'MogileFS::ReplicationPolicy';
use MogileFS::Util qw(weighted_list);
use MogileFS::ReplicationRequest qw(ALL_GOOD TOO_GOOD TEMP_NO_ANSWER);

sub new {
    my ($class, $mindevcount) = @_;
    return bless {
        mindevcount => $mindevcount,
    }, $class;
}

sub new_from_policy_args {
    my ($class, $argref) = @_;
    # Note: "MultipleDevices()" is okay, in which case the 'mindevcount'
    # on the class is used.  (see below)
    $$argref =~ s/^\s* \( \s* (\d*) \s* \) \s*//x
        or die "$class failed to parse args: $$argref";
    return $class->new($1)
}

sub mindevcount { $_[0]{mindevcount} }

sub replicate_to {
    my ($self, %args) = @_;

    my $fid      = delete $args{fid};      # fid scalar to copy
    my $on_devs  = delete $args{on_devs};  # arrayref of device objects
    my $all_devs = delete $args{all_devs}; # hashref of { devid => MogileFS::Device }
    my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round

    # this is the per-class mindevcount (the old way), which is passed in automatically
    # from the replication worker.  but if we have our own configured mindevcount
    # in class.replpolicy, like "MultipleHosts(3)", then we use the explicit one. otherwise,
    # if blank, or zero, like "MultipleHosts()", then we use the builtin on
    my $min      = delete $args{min};
    $min         = $self->{mindevcount} || $min;

    warn "Unknown parameters: " . join(", ", sort keys %args) if %args;
    die "Missing parameters" unless $on_devs && $all_devs && $failed && $fid;

    # number of devices we currently live on
    my $already_on = @$on_devs;

    return ALL_GOOD if $min == $already_on;
    return TOO_GOOD if $already_on > $min;

    # total disks available which are candidates for having files on them
    my $total_disks = scalar grep { $_->dstate->should_have_files } values %$all_devs;

    my %on_dev = map { $_->id => 1 } @$on_devs;

    # if we have two copies and that's all the disks there are
    # anywhere, be happy enough, even if mindevcount is higher.  in
    # that case, when they add more disks later, they'll need to fsck
    # to make files replicate more.
    # this is here instead of above in case an over replication error causes
    # the file to be on all disks (where more than necessary)
    return ALL_GOOD if $already_on >= 2 && $already_on == $total_disks;

    my @all_dests = sort {
        $b->percent_free <=> $a->percent_free
     } grep {
         ! $on_dev{$_->devid} &&
         ! $failed->{$_->devid} &&
         $_->should_get_replicated_files
     } values %$all_devs;

    return TEMP_NO_ANSWER unless @all_dests;

    # Do this little dance to only weight-shuffle the top end of empty devices
    @all_dests = weighted_list(map { [$_, 100 * $_->percent_free] }
        splice(@all_dests, 0, 20));

    return MogileFS::ReplicationRequest->new(
                                             ideal => \@all_dests,
                                             desperate => [],
                                             );
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

__END__

=head1 NAME

MogileFS::ReplicationPolicy::MultipleDevices -- bare-bones replication policy

=head1 RULES

This policy only puts files onto different devices.  This is intended to be a
quieter alternative to the default MultipleHosts replication policy when hosts
are heavily-imbalanced (one host has much more storage capacity than another).
This aims to avoid the noisy "policy_no_suggestions" log messages in clusters
where one large host contains the bulk of the storage.

=head1 SEE ALSO

L<MogileFS::Worker::Replicate>

L<MogileFS::ReplicationPolicy>
