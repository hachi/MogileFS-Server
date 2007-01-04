package MogileFS::Domain;
use strict;
use warnings;

# --------------------------------------------------------------------------
# Class methods:
# --------------------------------------------------------------------------

my %singleton;  # dmid -> MogileFS::Domain

my %id2name; # dmid -> domainname(namespace)
my %name2id; # dmid -> domainname(namespace)

my $last_load = 0;

# return singleton MogileFS::Domain, given a dmid
sub of_dmid {
    my ($class, $dmid) = @_;
    return undef unless $dmid;
    return $singleton{$dmid} if $singleton{$dmid};

    my $ns = $class->name_of_id($dmid)
        or return undef;

    return $singleton{$dmid} = bless {
        dmid => $dmid,
        ns   => $ns,
    }, $class;
}

# return singleton MogileFS::Domain, given a domain(namespace)
sub of_namespace {
    my ($class, $ns) = @_;
    return undef unless $ns;
    my $dmid = $class->id_of_name($ns)
        or return undef;
    return MogileFS::Domain->of_dmid($dmid);
}

# name to dmid, reloading if not in cache
sub id_of_name {
    my ($class, $domain) = @_;
    return $name2id{$domain} if $name2id{$domain};
    $class->reload_domains;
    return $name2id{$domain};
}

# dmid to name, reloading if not in cache
sub name_of_id {
    my ($class, $dmid) = @_;
    return $id2name{$dmid} if $id2name{$dmid};
    $class->reload_domains;
    return $id2name{$dmid};
}

# force reload of cache
sub reload_domains {
    my $now = time();
    my $sto = Mgd::get_store();
    %name2id = $sto->get_all_domains;
    %id2name = ();
    while (my ($k, $v) = each %name2id) {
        $id2name{$v} = $k;
    }

    $last_load = $now;
}

sub invalidate_cache {
    $last_load = 0;
    %id2name = ();
    %name2id = ();
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("domain");
    }
}

# --------------------------------------------------------------------------
# Instance methods:
# --------------------------------------------------------------------------

sub id    { $_[0]->{dmid} }
sub name  { $_[0]->{ns}   }

1;
