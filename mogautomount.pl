#!/usr/bin/perl
#

my $base = "/var/mogdata";

use strict;
open(P, "/proc/partitions") or die "no /proc/partitions?";
my @devs;
while (<P>) {
    next unless /^(?:\s*\d+){3}\s+([hs]d.+)\s*$/;
    my $dev = $1;
    my $label = `e2label /dev/$dev`;
    chomp $label;
    next unless $label;
    next unless $label =~ /^MogileDev(\d+)$/;
    my $devid = $1;
  
    unless (-d "$base") { mkdir $base or die; }
    my $mnt = "$base/dev$devid";
    unless (-d $mnt) { mkdir $mnt or die; }

    system("mount", "-L", $label, $mnt);
    print "dev: $dev = $label\n";
}

