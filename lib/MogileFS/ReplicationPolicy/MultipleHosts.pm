package MogileFS::ReplicationPolicy::MultipleHosts;
use strict;

# returns:
#   0:      replication sufficient
#   undef:  no suitable recommendations currently.
#   >0:     devid to replicate to.
sub replicate_to {
    my ($class, %args) = @_;

    my $fid      = delete $args{fid};      # fid scalar to copy
    my $on_devs  = delete $args{on_devs};  # arrayref of device objects
    my $all_devs = delete $args{all_devs}; # hashref of { devid => devobj }
    my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round
    my $min      = delete $args{min};      # configured min devcount for this class

    warn "Unknown parameters: " . join(", ", sort keys %args) if %args;
    die "Missing parameters" unless $on_devs && $all_devs && $failed && $fid;

    # number of devices we currently live on
    my $already_on = @$on_devs;

    # FIXME: this is NOT true.  make sure it's on 2+ hosts at least, if min > 1.

    # replication good.
    return 0 if $already_on >= $min;

    # see which and how many unique hosts we're already on.
    my %on_dev;
    my %on_host;
    foreach my $dev (@$on_devs) {
        $on_host{$dev->hostid} = 1;
        $on_dev{$dev->id} = 1;
    }
    my $uniq_hosts_on    = scalar keys %on_host;
    my $total_uniq_hosts = unique_hosts($all_devs);

    # if there are more hosts we're not on yet, we want to exclude those from
    # our applicable host search.
    my $not_on_hosts = [];
    if ($uniq_hosts_on < $total_uniq_hosts) {
        $not_on_hosts = [ keys %on_host ];
    }

    my @good_devids = grep { ! $failed->{$_} && ! $on_dev{$_} }
            MogileFS::Device->find_deviceid(
                                            random         => 1,
                                            not_on_hosts   => $not_on_hosts,
                                            weight_by_free => 1,
                                            );

    return undef unless @good_devids;
    return $good_devids[0];
}

sub unique_hosts {
    my $devs = shift;
    my %host;  # hostid -> 1
    foreach my $devid (keys %$devs) {
        my $dev = $devs->{$devid};
        next unless $dev->status =~ /^alive|readonly$/;
        $host{$dev->hostid}++;
    }
    return scalar keys %host;
}

1;
