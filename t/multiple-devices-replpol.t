#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::ReplicationPolicy::MultipleDevices;
use MogileFS::Test;

# already good.
is(rr("min=2  h1[d1=X d2=_] h2[d3=X d4=_]"),
   "all_good", "all good");

# need to get it onto host2...
is(rr("min=2  h1[d1=X d2=_] h2[d3=_ d4=_]"),
   "ideal(2,3,4)", "any device will do");

# still needs to be on host2, even though 2 copies on host1
is(rr("min=2  h1[d1=X d2=X] h2[d3=_ d4=_]"),
   "all_good", "2 devices is enough");

# anywhere will do.  (can happen on, say, rebalance)
is(rr("min=2  h1[d1=_ d2=_] h2[d3=_ d4=_]"),
   "ideal(1,2,3,4)", "anywhere");

# should desperately try d2, since host2 is down
is(rr("min=2  h1[d1=X d2=_] h2=down[d3=_ d4=_]"),
   "ideal(2)");

# should try host3, since host2 is down
is(rr("min=2  h1[d1=X d2=_] h2=down[d3=_ d4=_] h3[d5=_ d6=_]"),
   "ideal(2,5,6)");

# need a copy on a non-dead disk on host1
is(rr("min=2  h1[d1=_ d2=X,dead] h2=alive[d3=X d4=_]"),
   "ideal(1,4)");

# this is an ideal move, since we only have 2 unique hosts:
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=_]"),
   "ideal(1,4)");

# ... but if we have a 3rd host, well, it could be there
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=_] h3[d5=_]"),
   "ideal(1,4,5)");

# ... unless that host is down, in which case it's back to 1/4,
# but desperately
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=_] h3=down[d5=_]"),
   "ideal(1,4)");

# too good, uniq hosts > min
is(rr("min=2 h1[d1=X d2=_] h2[d3=X d4=_] h3[d5=X]"),
   "too_good");

# too good, but but with uniq hosts == min
is(rr("min=2 h1[d1=X d2=X] h2[d3=X d4=_]"),
   "too_good");

# be happy with 3 copies, even though two are on same host (that's our max unique hosts)
is(rr("min=3 h1[d1=_ d2=X] h2[d3=X d4=X]"),
   "all_good");

# be happy with one drain copy
is(rr("min=2 h1[d3=X,drain d5=_] h2[d4=X d6=_]"),
   "all_good",
   "we are happy with one copy in a drain device");

# drain copy counts
is(rr("min=2 h1[d3=X,drain d5=X] h2[d4=X d6=_]"),
   "too_good",
   "the extra copy in drain leaves us too satisfied");

sub rr {
    my ($state) = @_;
    my $ostate = $state; # original

    MogileFS::Factory::Host->t_wipe;
    MogileFS::Factory::Device->t_wipe;
    MogileFS::Config->set_config_no_broadcast("min_free_space", 100);
    my $hfac = MogileFS::Factory::Host->get_factory;
    my $dfac = MogileFS::Factory::Device->get_factory;

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

        my $h = $hosts->{$n} = $hfac->set({ hostid => $n,
            status => ($opts || "alive"), observed_state => "reachable",
            hostname => $n });

        foreach my $ddecl (split(/\s+/, $devstr)) {
            $ddecl =~ /^d(\d+)=([_X])(?:,(\w+))?$/
                or $parse_error->();
            my ($dn, $on_not, $status) = ($1, $2, $3);
            die "dup device $dn" if $devs->{$dn};
            my $d = $devs->{$dn} = $dfac->set({ devid => $dn,
                hostid => $h->id, observed_state => "writeable",
                status => ($status || "alive"), mb_total => 1000,
                mb_used => 100, });
            if ($on_not eq "X" && $d->dstate->should_have_files) {
                push @$on_devs, $d;
            }
        }
    }
    $parse_error->() if $state =~ /\S/;

    my $polclass = "MogileFS::ReplicationPolicy::MultipleDevices";
    my $pol = $polclass->new;
    my $rr = $pol->replicate_to(
                                fid      => 1,
                                on_devs  => $on_devs,
                                all_devs => $devs,
                                failed   => {},
                                min      => $min,
                                );
    return $rr->t_as_string;
}

done_testing();
