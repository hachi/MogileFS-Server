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
        %domaincache = ();

        # now get updated list
        my $dbh = Mgd::get_dbh();
        my $domains = $dbh->selectall_arrayref('SELECT dmid, namespace FROM domain');
        foreach my $row (@{$domains || []}) {
            # namespace -> dmid
            $domaincache{$row->[1]} = $row->[0];
        }

        $domaincachetime = $now;
    }

    # just use cached version
    return $domaincache{$domain};
}

sub name_of_id {
    my ($class, $dmid) = @_;

    my $dbh = Mgd::get_dbh();
    my $namespace = $dbh->selectrow_array
        ("SELECT namespace FROM domain WHERE dmid=?", undef, $dmid);
    return $namespace;
}

sub invalidate_cache {
    $domaincachetime = 0;
    %domaincache = ();

    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->invalidate_meta("domain");
    }
}

1;
