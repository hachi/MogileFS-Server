#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use MogileFS::Util qw(weighted_list);

my %first;
for (1..100) {
    my @l = weighted_list(["A", 0.1], ["B", 0.3]);
    $first{$l[0]}++;
}

# conservative when playing with randomness
ok($first{"B"} >= ($first{"A"} * 1.8), "weightest list");

done_testing();
