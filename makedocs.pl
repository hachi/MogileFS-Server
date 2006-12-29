#!/usr/bin/perl

use strict;
my @files = "mogilefsd";
push @files, `find lib -name '*.pm'`;
chomp @files;

my $base = "/home/lj/htdocs/dev/mogdocs/";
system("find $base -type f -exec rm {} \;");

foreach my $f (@files) {
    my $outfile = $f;
    $outfile =~ s!^lib/!!;
    $outfile =~ s!\.pm$!!;
    $outfile .= ".html";
    system("install -D /dev/null $base/$outfile");
    system("pod2html --htmlroot=/dev/mogdocs $f > $base/$outfile");
    print "F = $f => $outfile\n";
}
