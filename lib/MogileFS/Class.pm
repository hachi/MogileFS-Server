package MogileFS::Class;
use strict;

sub new {
    my ($class, $hash) = @_;
    $hash->{replpolicy} ||= "MogileFS::ReplicationPolicy::MultipleHosts";
    return bless $hash, $class;
}

# return MogileFS::Class object for a given fid
sub of_fid {
    my ($class, $fid) = @_;
    my $dbh = Mgd::get_dbh();
    my $row = $dbh->selectrow_hashref("SELECT c.* FROM class c, file f ".
                                      "WHERE f.dmid=c.dmid AND f.classid=c.classid AND f.fid=?",
                                      undef, $fid);
    return $class->new($row);
}

sub domainid     { $_[0]{dmid} }
sub classid      { $_[0]{classid} }
sub mindevcount  { $_[0]{mindevcount} }
sub policy_class { $_[0]{replpolicy} }

sub foreach {
    my ($class, $cb) = @_;
    # get the min dev counts
    my %min = %{ Mgd::get_mindevcounts() };

    # iterate through each domain, replicating its contents
    foreach my $dmid (keys %min) {
        # iterate through each class, including the implicit class 0
        while (my ($classid, $min) = each %{$min{$dmid}}) {
            my $class = MogileFS::Class->new({
                classid     => $classid,
                mindevcount => $min,
                dmid        => $dmid,
                classname   => undef,
            });
            $cb->($class);
        }
    }
}

1;
