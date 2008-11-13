package MogileFS::Class;
use strict;

my %singleton;     # dmid -> classid -> MogileFS::Class
my $last_load = 0;

# return MogileFS::Class object for a given fid id/obj
sub of_fid {
    my ($pkg, $fid) = @_;
    return undef unless $fid;
    # make $fid into a FID object:
    $fid = MogileFS::FID->new($fid) unless ref $fid;
    return undef unless $fid->exists;
    my $cl = $pkg->of_dmid_classid($fid->dmid, $fid->classid);
    return $cl if $cl;
    # return the default class for this file, not undef.  this should
    # always return a valid class for a valid FID.  files need to
    # always have a mindevcount (default of 2), repl policy, etc.
    return $pkg->of_dmid_classid($fid->dmid, 0);
}

# return MogileFS::Class, given a dmid and classid.  or returns the
# default class, if classid is bogus.
sub of_dmid_classid {
    my ($pkg, $dmid, $classid) = @_;
    return $singleton{$dmid}{$classid} if
         $singleton{$dmid} &&
         $singleton{$dmid}{$classid} &&
         $singleton{$dmid}{$classid}->{_loaded};
    $pkg->reload_classes;
    return $singleton{$dmid}{$classid} if
        $singleton{$dmid} &&
        $singleton{$dmid}{$classid};
    return undef;
}

# marks everything dirty, triggering a reload, but doesn't actually
# reload now.  will happen later, next time somebody loads something.
sub invalidate_cache {
    my $pkg = shift;
    $last_load = 0;
    $pkg->_foreach_singleton(sub {
        my $cl = shift;
        $cl->{_loaded} = 0;
    });
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("class");
    }
}

sub check_cache {
    my $pkg = shift;
    my $now = time();
    return if $last_load > $now - 5;
    MogileFS::Class->reload_classes;
}

sub reload_classes {
    my $pkg = shift;
    my $now = time();

    # mark everything as invalid for now
    $pkg->_foreach_singleton(sub {
        my ($cl, $dmid, $clid) = @_;
        $cl->{_loaded} = 0;
    });

    # install the default classes (classid=0)
    my $default_min = MogileFS->config('default_mindevcount');
    foreach my $dom (MogileFS::Domain->domains) {
        my $dmid = $dom->id;
        my $cl =
            ($singleton{$dmid}{0} =
             bless {
                 dmid        => $dmid,
                 classid     => 0,
                 name        => "default",
                 mindevcount => $default_min,
             }, $pkg);
            $cl->{_loaded} = 1;
    }

    foreach my $row (Mgd::get_store()->get_all_classes) {
        my $cl =
            ($singleton{$row->{dmid}}{$row->{classid}} =
             bless {
                 dmid        => $row->{dmid},
                 classid     => $row->{classid},
                 name        => $row->{classname},
                 mindevcount => $row->{mindevcount},
                 replpolicy  => $row->{replpolicy}, 
             }, $pkg);
        $cl->{_loaded} = 1;
    }

    # delete any singletons that weren't just loaded
    $pkg->_foreach_singleton(sub {
        my ($cl, $dmid, $clid) = @_;
        return if $cl->{_loaded};
        delete $singleton{$dmid}{$clid};
    });

    $last_load = $now;
}

# enumerates all loaded singletons (without reloading/checked caches),
# calling the given subref with (MogileFS::Class, $dmid, $classid)
sub _foreach_singleton {
    my ($pkg, $cb) = @_;
    foreach my $dmid (keys %singleton) {
        foreach my $clid (keys %{$singleton{$dmid}}) {
            $cb->($singleton{$dmid}{$clid}, $dmid, $clid);
        }
    }
}

# enumerates all classes, (reloading if needed), calling the given
# subref with (MogileFS::Class, $dmid, $classid)
sub foreach {
    my ($pkg, $cb) = @_;
    $pkg->check_cache;
    $pkg->_foreach_singleton($cb);
}

sub class_name {
    my ($pkg, $dmid, $classid) = @_;
    my $cls = $pkg->of_dmid_classid($dmid, $classid)
        or return undef;
    return $cls->name;
}

sub class_id {
    my ($pkg, $dmid, $classname) = @_;
    return undef unless $dmid > 0 && length $classname;
    # tries to get it first from cache, then reloads and tries again.
    my $get = sub {
        foreach my $cl ($pkg->classes_of_domain($dmid)) {
            return $cl->classid if $cl->name eq $classname;
        }
        return undef;
    };
    my $id = $get->();
    return $id if $id;
    MogileFS::Class->reload_classes;
    return $get->();
}

sub classes_of_domain {
    my ($pkg, $doma) = @_;
    my $dmid = ref $doma ? $doma->id : $doma;
    $pkg->check_cache;
    return () unless $dmid && $singleton{$dmid};
    return values %{ $singleton{$dmid} };
}

# throws 'dup' on duplicate name, returns class otherwise
sub create_class {
    my ($pkg, $dom, $clname) = @_;
    my $clid = Mgd::get_store()->create_class($dom->id, $clname);
    return $pkg->of_dmid_classid($dom->id, $clid);
}

# --------------------------------------------------------------------------
# Instance methods:
# --------------------------------------------------------------------------

sub domainid     { $_[0]{dmid} }
sub classid      { $_[0]{classid} }
sub mindevcount  { $_[0]{mindevcount} }

sub repl_policy_string {
    my $self = shift;
    # if they've actually configured one, it gets used:
    return $self->{replpolicy} if $self->{replpolicy};
    # else, the historical default:
    return "MultipleHosts()";
}

sub repl_policy_obj {
    my $self = shift;
    return $self->{_repl_policy_obj} if $self->{_repl_policy_obj};
    my $polstr = $self->repl_policy_string;
    # parses it:
    my $pol = MogileFS::ReplicationPolicy->new_from_policy_string($polstr);
    return $self->{_repl_policy_obj} = $pol;
}

sub name         { $_[0]{name} }

sub domain {
    my $self = shift;
    return MogileFS::Domain->of_dmid($self->domainid);
}

# throws 'dup' (for name conflict), returns 1 otherwise
sub set_name {
    my ($self, $name) = @_;
    return 1 if $self->name eq $name;
    Mgd::get_store()->update_class_name(dmid      => $self->domainid,
                                        classid   => $self->classid,
                                        classname => $name);
    $self->{name} = $name;
    MogileFS::Class->invalidate_cache;
    return 1;
}

sub set_mindevcount {
    my ($self, $n) = @_;
    return 1 if $self->mindevcount == $n;
    Mgd::get_store()->update_class_mindevcount(dmid        => $self->domainid,
                                               classid     => $self->classid,
                                               mindevcount => $n);
    $self->{mindevcount} = $n;
    MogileFS::Class->invalidate_cache;
    return 1;
}

# throws:
#   'has_files'
sub delete {
    my $self = shift;
    throw("has_files") if $self->has_files;
    Mgd::get_store()->delete_class($self->domainid, $self->classid);
    MogileFS::Class->invalidate_cache;
    return 1;
}

sub has_files {
    my $self = shift;
    return Mgd::get_store()->class_has_files($self->domainid, $self->classid);
}

1;
