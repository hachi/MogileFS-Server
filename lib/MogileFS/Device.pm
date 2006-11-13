package MogileFS::Device;
use strict;
use warnings;

sub of_devid {
    my ($class, $devid) = @_;
    return bless {
        devid => $devid,
    }, $class;
}

sub status {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    my $disk = $dsum->{$self->{devid}} or return;
    return $disk->{status};
}

sub is_marked_dead {
    my $self = shift;
    return $self->status eq "dead";
}

sub is_marked_readonly {
    my $self = shift;
    return $self->status eq "readonly";
}

sub exists {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    return 1 if $dsum->{$self->{devid}};
    # be damn careful to never return 0 (doesn't exist) when it could just
    # be really new and not yet in cache
    my $dbh = Mgd::get_dbh();
    my $exists = $dbh->selectall_hashref("SELECT devid FROM device", "devid");
    MogileFS::Util::dbcheck($dbh, "failed to lookup devices");
    return 0 unless $exists->{$self->{devid}};
    Mgd::invalidate_device_cache();
    return 1;
}

sub hostid {
    my $self = shift;
    my $dsum = Mgd::get_device_summary();
    my $disk = $dsum->{$self->{devid}} or return 0;
    return $disk->{hostid};
}

sub is_observed_writeable {
    my $self = shift;
    return
        MogileFS->observed_state("host", $self->hostid) eq "reachable" &&
        MogileFS->observed_state("device", $self->{devid}) eq "writeable";
}


1;
