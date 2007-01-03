package MogileFS::Domain;
use strict;

our %domaincache; # { domainname => { domainrow } }
our $domaincachetime = 0;

# quick port of old API.  not ideal.
sub id_of_name {
    my ($class, $domain) = @_;

    # reload the cache if time is up, or if cache is empty for requested item
    my $now = time();
    if ($domaincachetime + 5 < $now || ! $domaincache{$domain}) {
        my $sto = Mgd::get_store();
        %domaincache = $sto->get_all_domains;
        $domaincachetime = $now;
    }

    # just use cached version
    return $domaincache{$domain};
}

sub name_of_id {
    my ($class, $dmid) = @_;

    my $sto = Mgd::get_store();
    return $sto->get_domain_namespace($dmid);
}

sub invalidate_cache {
    $domaincachetime = 0;
    %domaincache = ();

    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("domain");
    }
}

1;
