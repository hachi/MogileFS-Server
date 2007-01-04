package MogileFS::Domain;
use strict;
use warnings;

my %id2name; # dmid -> domainname
my %name2id; # dmid -> domainname

my $last_load = 0;

sub id_of_name {
    my ($class, $domain) = @_;
    return $name2id{$domain} if $name2id{$domain};
    $class->reload_domains;
    return $name2id{$domain};
}

sub name_of_id {
    my ($class, $dmid) = @_;
    return $id2name{$dmid} if $id2name{$dmid};
    $class->reload_domains;
    return $id2name{$dmid};
}

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

1;
