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

sub invalidate_cache {
    # FIXME: no cache yet exists
}

# legacy API quickly ported over.  probably not ideal API.
sub class_name {
    my ($pkg, $dmid, $classid) = @_;
    return undef unless $dmid > 0 && length $classid;
    # FIXME: cache this

    # lookup class
    my $dbh = Mgd::get_dbh();
    my $classname = $dbh->selectrow_array
        ("SELECT classname FROM class WHERE dmid=? AND classid=?", undef, $dmid, $classid)
            or return undef;
    return undef unless $classname;
    return $classname;
}

# legacy API quickly ported over.  probably not ideal API.
sub dmid_classes {
    my ($pkg, $dmid) = @_;
    return undef unless $dmid > 0;

    my $dbh = Mgd::get_dbh();
    my $classes = $dbh->selectall_arrayref
        ("SELECT classid, classname, mindevcount FROM class WHERE dmid=?", undef, $dmid)
            or return undef;
    return undef unless $classes;

    my $res = {};
    foreach my $row (@$classes) {
        $res->{$row->[0]} = {
            classid => $row->[0],
            classname => $row->[1],
            mindevcount => $row->[2],
        };
    }
    return $res;
}

sub foreach {
    my ($pkg, $cb) = @_;
    # get the min dev counts
    my %min = %{ MogileFS::Class->mindevcounts };

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

sub mindevcounts {
    # make sure we have good info
    MogileFS::Host->check_cache;
    my $host_ct = scalar MogileFS::Host->hosts;

    # find the classes for each domainid (including domains without explict classes)
    my %min; # dmid -> classid -> mindevcount
    Mgd::validate_dbh();
    my $dbh = Mgd::get_dbh();
    my $sth = $dbh->prepare("SELECT d.dmid, c.classid, c.mindevcount ".
                            "FROM domain d LEFT JOIN class c ON d.dmid=c.dmid");
    $sth->execute;
    while (my ($dmid, $classid, $mct) = $sth->fetchrow_array) {
        $min{$dmid} ||= {};  # note the existence of this dmid

        # classid may be NULL (undef), in which case there are no classes defined
        # and we don't note the mindevcount (yet)
        $min{$dmid}{$classid} = int($host_ct < $mct ? $host_ct : $mct) if defined $classid;
    }


    # now iterate over %min again to set the implicit class
    my $default_min = MogileFS->config('default_mindevcount');
    foreach my $dmid (keys %min) {
        # each domain's classid=0, if not defined, has an implied mindevcount of $default_mindevcount
        # which most people will probably use.
        $min{$dmid}{0} = $host_ct < $default_min ? $host_ct : $default_min
            unless exists $min{$dmid}{0};
    }

    # return ref to hash
    return \%min;
}

# legacy API quickly ported over.  probably not ideal API.
sub class_id {
    my ($pkg, $dmid, $classname) = @_;
    return undef unless $dmid > 0 && length $classname;

    my $dbh = Mgd::get_dbh();
    my $classid = $dbh->selectrow_array
        ("SELECT classid FROM class WHERE dmid=? AND classname=?", undef, $dmid, $classname);
    return $classid;
}

# --------------------------------------------------------------------------
# Instance methods:
# --------------------------------------------------------------------------

sub domainid     { $_[0]{dmid} }
sub classid      { $_[0]{classid} }
sub mindevcount  { $_[0]{mindevcount} }
sub policy_class { $_[0]{replpolicy} }


1;
