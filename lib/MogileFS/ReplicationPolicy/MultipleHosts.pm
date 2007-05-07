package MogileFS::ReplicationPolicy::MultipleHosts;
use strict;
use base 'MogileFS::ReplicationPolicy';
use MogileFS::Util qw(weighted_list);
use MogileFS::ReplicationRequest qw(ALL_GOOD TOO_GOOD TEMP_NO_ANSWER);

sub replicate_to {
    my ($class, %args) = @_;

    my $fid      = delete $args{fid};      # fid scalar to copy
    my $on_devs  = delete $args{on_devs};  # arrayref of device objects
    my $all_devs = delete $args{all_devs}; # hashref of { devid => MogileFS::Device }
    my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round
    my $min      = delete $args{min};      # configured min devcount for this class

    warn "Unknown parameters: " . join(", ", sort keys %args) if %args;
    die "Missing parameters" unless $on_devs && $all_devs && $failed && $fid;

    # number of devices we currently live on
    my $already_on = @$on_devs;

    # a silly special case, bail out early.
    return ALL_GOOD if $min == 1 && $already_on;

    # total disks available which are candidates for having files on them
    my $total_disks = scalar grep { $_->dstate->should_have_files } values %$all_devs;

    # if we have two copies and that's all the disks there are
    # anywhere, be happy enough, even if mindevcount is higher.  in
    # that case, when they add more disks later, they'll need to fsck
    # to make files replicate more.
    return ALL_GOOD if $already_on >= 2 && $already_on == $total_disks;

    # see which and how many unique hosts we're already on.
    my %on_dev;
    my %on_host;
    foreach my $dev (@$on_devs) {
        $on_host{$dev->hostid} = 1;
        $on_dev{$dev->id} = 1;
    }
    my $uniq_hosts_on    = scalar keys %on_host;
    my $total_uniq_hosts = unique_hosts($all_devs);

    return TOO_GOOD if $uniq_hosts_on >  $min;
    return TOO_GOOD if $uniq_hosts_on == $min && $already_on > $min;
    return ALL_GOOD if $uniq_hosts_on == $min;
    return ALL_GOOD if $uniq_hosts_on >= $total_uniq_hosts && $already_on >= $min;

    # if there are more hosts we're not on yet, we want to exclude devices we're already
    # on from our applicable host search.
    my %skip_host; # hostid => 1
    if ($uniq_hosts_on < $total_uniq_hosts) {
        %skip_host = %on_host;
    }

    my @all_dests = weighted_list map {
        [$_, 100 * $_->percent_free]
     } grep {
         ! $on_dev{$_->devid} &&
         ! $failed->{$_->devid} &&
         $_->should_get_replicated_files
     } MogileFS::Device->devices;

    return TEMP_NO_ANSWER unless @all_dests;

    my @ideal = grep { ! $skip_host{$_->hostid} } @all_dests;
    my @desp  = grep {   $skip_host{$_->hostid} } @all_dests;
    return MogileFS::ReplicationRequest->new(
                                             ideal => \@ideal,
                                             desperate => \@desp,
                                             );
}

sub unique_hosts {
    my $devs = shift;
    my %host;  # hostid -> 1
    foreach my $devid (keys %$devs) {
        my $dev = $devs->{$devid};
        next unless $dev->dstate->should_get_repl_files;
        $host{$dev->hostid}++;
    }
    return scalar keys %host;
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

__END__

=head1 NAME

MogileFS::ReplicationPolicy::MultipleHosts -- default replication policy

=head1 RULES

This policy tries to put files onto devices which are on different
hosts.  If you only have 1 host and 2 devices on that one host, it
obviously can't, so it'll grudgingly put it on the same device.  But
if you request a minimum replica count of 2 and have 3 devices, it'll
put 2 copies on different hosts.  If you have 4 devices on 2 hosts,
and request a minima replica count of 3, you'll get 3 copies on
different devices, but two of those devices will be on the same host,
and that's considered acceptable, since you have "multiple hosts"
covered at least.

=head1 SEE ALSO

L<MogileFS::Worker::Replicate>

L<MogileFS::ReplicationPolicy>

