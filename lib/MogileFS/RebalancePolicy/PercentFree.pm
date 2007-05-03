package MogileFS::RebalancePolicy::PercentFree;
use strict;
use warnings;
use base 'MogileFS::RebalancePolicy';

sub devfid_to_rebalance {
    my ($self) = @_;
    my $dbh = Mgd::get_dbh();
    my ($fid, $devid) = $dbh->selectrow_array('SELECT fid, devid FROM file_on ORDER BY rand() LIMIT 1');
    warn "gonna rebalance: fid=$fid/devid=$devid\n";
    return $fid ? MogileFS::DevFID->new($devid, $fid) : undef;
}

1;
