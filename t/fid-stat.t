# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use Mogstored::FIDStatter;
use File::Temp qw(tempdir);

plan tests => 5;

my $td = tempdir(CLEANUP => 1);
ok($td, "got tempdir");
ok(-d $td, "tempdir is writable");

my $n_stats;
my $on_fid;

my $fs = Mogstored::FIDStatter->new(
                                    dir  => $td,
                                    from => 500,
                                    to   => 1499,
                                    t_stat => sub { $n_stats++ },
                                    on_fid => sub { $on_fid->(@_); },
                                    );
ok($fs, "made statter");

# empty directory, no stats
{
    $n_stats = 0;
    $fs->run;
    my @list;
    $on_fid = sub {
        push @list, [@_],
    };
    is($n_stats, 0, "no stats on empty directory");
    is(scalar @list, 0, "no contents on empty directory");
}


