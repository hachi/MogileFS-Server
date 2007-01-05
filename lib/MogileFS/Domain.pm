package MogileFS::Domain;
use strict;
use warnings;
use MogileFS::Util qw(throw);

# --------------------------------------------------------------------------
# Class methods:
# --------------------------------------------------------------------------

my %singleton;  # dmid -> MogileFS::Domain

my %id2name; # dmid -> domainname(namespace)
my %name2id; # domainname(namespace) -> dmid

my $last_load = 0;

# return singleton MogileFS::Domain, given a dmid
sub of_dmid {
    my ($pkg, $dmid) = @_;
    return undef unless $dmid;
    return $singleton{$dmid} if $singleton{$dmid};

    my $ns = $pkg->name_of_id($dmid)
        or return undef;

    return $singleton{$dmid} = bless {
        dmid => $dmid,
        ns   => $ns,
    }, $pkg;
}

# return singleton MogileFS::Domain, given a domain(namespace)
sub of_namespace {
    my ($pkg, $ns) = @_;
    return undef unless $ns;
    my $dmid = $pkg->id_of_name($ns)
        or return undef;
    return MogileFS::Domain->of_dmid($dmid);
}

# name to dmid, reloading if not in cache
sub id_of_name {
    my ($pkg, $domain) = @_;
    return $name2id{$domain} if $name2id{$domain};
    $pkg->reload_domains;
    return $name2id{$domain};
}

# dmid to name, reloading if not in cache
sub name_of_id {
    my ($pkg, $dmid) = @_;
    return $id2name{$dmid} if $id2name{$dmid};
    $pkg->reload_domains;
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

    foreach my $dmid (keys %singleton) {
        delete $singleton{$dmid} unless $id2name{$dmid};
    }

    $last_load = $now;
}

# FIXME: should probably have an invalidate_cache variant that only
# flushes locally (for things like "get_domains" or "get_hosts", where
# it needs to be locally correct for the semantics of the command, but
# no need to propogate a cache invalidation to our peers)
sub invalidate_cache {
    $last_load = 0;
    %id2name = ();
    %name2id = ();
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("domain");
    }
}

sub check_cache {
    my $pkg = shift;
    my $now = time();
    return if $last_load > $now - 5;
    MogileFS::Domain->reload_domains;
}

sub domains {
    my $pkg = shift;
    $pkg->check_cache;
    return map { $pkg->of_dmid($_) } keys %id2name;
}

# create a new domain given a name, returns MogileFS::Domain object on success.
# throws errors on failure.  error codes include:
#      "dup" -- on duplicate name
sub create {
    my ($pkg, $name) = @_;

    # throws 'dup':
    my $dmid = Mgd::get_store()->create_domain($name)
        or die "create domain didn't return a dmid";

    # return the domain id we created
    MogileFS::Domain->invalidate_cache;
    return MogileFS::Domain->of_dmid($dmid);
}

# --------------------------------------------------------------------------
# Instance methods:
# --------------------------------------------------------------------------

sub id    { $_[0]->{dmid} }
sub name  { $_[0]->{ns}   }

sub has_files {
    my $self = shift;
    return 1 if $Mgd::_T_DOM_HAS_FILES;
    return Mgd::get_store()->domain_has_files($self->id);
}

sub classes {
    my $dom = shift;
    # return a bunch of class objects for this domain
    return MogileFS::Class->classes_of_domain($dom);
}

# returns true if deleted.  throws exceptions on errors.  exception codes:
#     'has_files' if it has files.
sub delete {
    my $self = shift;
    throw("has_files") if $self->has_files;
    # TODO: delete its classes
    my $rv = Mgd::get_store()->delete_domain($self->id);
    MogileFS::Domain->invalidate_cache;
    return $rv;
}

# returns named class of domain
sub class {
    my ($dom, $clname) = @_;
    foreach my $cl (MogileFS::Class->classes_of_domain($dom)) {
        return $cl if $cl->name eq $clname;
    }
    return;
}

sub create_class {
    my ($dom, $clname) = @_;
    return MogileFS::Class->create_class($dom, $clname);
}

1;
