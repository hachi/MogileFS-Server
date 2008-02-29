#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::ReplicationPolicy::HostsPerNetwork;
require "$Bin/lib/mogtestlib.pl";

plan tests => 13;

# already good.
is(rr("min=2  h1[d1=X d2=_] h2[d3=X d4=_]"),
   "all_good", "all good");

# need to get it onto host2...
is(rr("min=2  h1[d1=X d2=_] h2[d3=_ d4=_]"),
   "ideal(3,4)", "need host2");

# still needs to be on host2, even though 2 coies on host1
is(rr("min=2  h1[d1=X d2=X] h2[d3=_ d4=_]"),
   "ideal(3,4)", "need host2, even though 2 on host1");

# anywhere will do.  (can happen on, say, rebalance)
is(rr("min=2  h1[d1=_ d2=_] h2[d3=_ d4=_]"),
   "ideal(1,2,3,4)", "anywhere");

# should desperately try d2, since host2 is down
is(rr("min=2  h1[d1=X d2=_] h2=down[d3=_ d4=_]"),
   "desperate(2)");

# should try host3, since host2 is down
is(rr("min=2  h1[d1=X d2=_] h2=down[d3=_ d4=_] h3[d5=_ d6=_]"),
   "ideal(5,6)");

# need a copy on a non-dead disk on host1
is(rr("min=2  h1[d1=_ d2=X,dead] h2=alive[d3=X d4=_]"),
   "ideal(1)");

# minimum hosts is 3, only 2 available hosts. This test differs from
# the one in multiplehosts because elavating these results to be 'ideal'
# adds complexity that is unnecessary in my eyes.
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=_]"),
   "desperate(1,4)");

# ... but if we have a 3rd host, it's gotta be there
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=_] h3[d5=_]"),
   "ideal(5)");

# ... unless that host is down, in which case it's back to 1/4,
# but desperately
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=_] h3=down[d5=_]"),
   "desperate(1,4)");

# too good, uniq hosts > min
is(rr("min=2 h1[d1=X d2=_] h2[d3=X d4=_] h3[d5=X]"),
   "too_good");

# too good, but but with uniq hosts == min
is(rr("min=2 h1[d1=X d2=X] h2[d3=X d4=_]"),
   "too_good");

# be happy with 3 copies, even though two are on same host (that's our max unique hosts)
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=X]"),
   "all_good");

sub rr {
    my ($state) = @_;
    my $ostate = $state; # original

    MogileFS::Host->t_wipe_singletons;
    MogileFS::Device->t_wipe_singletons;
    MogileFS::Config->set_config_no_broadcast("min_free_space", 100);

    my $min = 2;
    if ($state =~ s/^\bmin=(\d+)\b//) {
        $min = $1;
    }

    my $hosts   = {};
    my $devs    = {};
    my $on_devs = [];

    my $parse_error = sub {
        die "Can't parse:\n   $ostate\n"
    };
    while ($state =~ s/\bh(\d+)(?:=(.+?))?\[(.+?)\]//) {
        my ($n, $opts, $devstr) = ($1, $2, $3);
        $opts ||= "";
        die "dup host $n" if $hosts->{$n};

        my $h = $hosts->{$n} = MogileFS::Host->of_hostid($n);
        $h->t_init($opts || "alive");
        $h->{hostip} = "127.0.0.1";

        foreach my $ddecl (split(/\s+/, $devstr)) {
            $ddecl =~ /^d(\d+)=([_X])(?:,(\w+))?$/
                or $parse_error->();
            my ($dn, $on_not, $status) = ($1, $2, $3);
            die "dup device $dn" if $devs->{$dn};
            my $d = $devs->{$dn} = MogileFS::Device->of_devid($dn);
            $status ||= "alive";
            $d->t_init($h->id, $status);
            if ($on_not eq "X" && $d->dstate->should_have_files) {
                push @$on_devs, $d;
            }
        }
    }
    $parse_error->() if $state =~ /\S/;

    my $polclass = "MogileFS::ReplicationPolicy::HostsPerNetwork";

    my $pol = $polclass->new(hosts_per_zone => { zone_one => $min });

    MogileFS::Network->stuff_cache(zone_one => Net::Netmask->new('127.0.0.0/16'));
    my $rr = $pol->replicate_to(
                                fid      => 1,
                                on_devs  => $on_devs,
                                all_devs => $devs,
                                failed   => {},
                                min      => $min,
                                );
    return $rr->t_as_string;
}

