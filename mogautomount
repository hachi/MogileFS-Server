#!/usr/bin/perl
#

use strict;

my $base = "/var/mogdata";
my @bdevs = `/sbin/blkid -c /dev/null`;
die "Failed to run /sbin/blkid to get available block devices." if $?;

my %mounted;  # dev -> 1
open (M, "/proc/mounts") or die "Failed to open /proc/mounts for reading: $!\n";
while (<M>) {
    m!^(\S+) /var/mogdata/dev! or next;
    $mounted{$1} = 1;
}

my $exit_code = 0;

foreach my $bdev (@bdevs) {
    next unless $bdev =~ /^(.+?):.*LABEL="MogileDev(\d+)"/;
    my ($dev, $devid) = ($1, $2);
    unless (-d "$base") { mkdir $base or die "Failed to mkdir $base: $!"; }
    my $mnt = "$base/dev$devid";
    unless (-d $mnt) { mkdir $mnt or die "Failed to mkdir $mnt: $!"; }
    next if $mounted{$dev};

    if (system("mount", '-o', 'noatime', $dev, $mnt)) {
        warn "Failed to mount $dev at $mnt.\n";
        $exit_code = 1;
    }
}

exit($exit_code);


