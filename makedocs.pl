#!/usr/bin/perl

use strict;
my @files = ("mogstored", "mogilefsd");
push @files, `find lib -name '*.pm'`;
chomp @files;

my $base = "/home/lj/htdocs/dev/mogdocs/";
#system("find $base -type f -exec rm {} \;");
my %html; # perl file -> html file

foreach my $f (@files) {
    my $outfile = $f;
    $outfile =~ s!^lib/!!;
    $outfile =~ s!\.pm$!!;
    $outfile .= ".html";
    system("install -D /dev/null $base/$outfile");
    system("pod2html --podroot=. --podpath=.:lib --htmlroot=/dev/mogdocs $f > $base/$outfile");
    print "F = $f => $outfile\n";
    $html{$f} = $outfile;
}

open (my $fh, ">$base/index.html") or die;
print $fh "<html><head><title>MogileFS server docs</title></head><body><pre>";
foreach my $f (@files) {
    if (has_pod($f)) {
        print $fh "<a href='$html{$f}'>$f</a>\n";
    } else {
        print $fh "$f\n";
    }
}
print $fh "</pre></body></html>\n";
close($fh);

sub has_pod {
    my $f = shift;
    open(my $fh, $f) or die;
    while (<$fh>) {
        return 1 if /=head1/;
    }
    return 0;
}
