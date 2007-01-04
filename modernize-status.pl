#!/usr/bin/perl
use strict;
use FindBin qw($Bin);

my %cmd;
my $lastcmd;
open (my $fh, "$Bin/lib/MogileFS/Worker/Query.pm");
while (<$fh>) {
    if (/^sub cmd_(\S+)/) {
        $lastcmd = $cmd{$1} = { cmd => $1, };
        next;
    }
    if (/^\}/) {
        $lastcmd = undef;
        next;
    }
    next unless $lastcmd;
    if (/\$dbh/) {
        $lastcmd->{dbh}++;
    }
}

foreach my $func (sort keys %cmd) {
    my $cmd = $cmd{$func};
    printf("%-20s dbh: %02d t: %1d\n", $func, $cmd->{'dbh'}||0, 0);
}
