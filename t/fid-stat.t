# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use Mogstored::FIDStatter;
use File::Temp qw(tempdir);

plan tests => 11;

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
    my @list;
    $on_fid = sub {
        push @list, [@_],
    };
    $fs->run;
    is($n_stats, 0, "no stats on empty directory");
    is(scalar @list, 0, "no contents on empty directory");
}

# make a normal (packed) directory structure
{
    for (my $n = 500; $n < 1500; $n += 2) {
        make_file($n, ($n%50) + 1);
    }

    $n_stats = 0;
    my @list;
    $on_fid = sub {
        push @list, [@_],
    };
    $fs->run;
    is($n_stats, 500, "500 stats");
    is(scalar @list, 500, "500 fids found");
}

# make a sparse directory structure, with huge (64-bit numbers)
{
    $n_stats = 0;
    my @list;
    make_file("52048709162819278", 50);
    make_file("52048709163819278", 50);
    make_file("52048809163819278", 50);
    make_file("52048819163819278", 50);
    $fs = Mogstored::FIDStatter->new(
                                     dir  => $td,
                                     from => "52048709162819278",
                                     to   => "52048819163819278",
                                     t_stat => sub { $n_stats++ },
                                     on_fid => sub {
                                         push @list, [@_];
                                     },
                                     );
    $fs->run;
    is(scalar @list, 4, "found 4 files");
    is($n_stats, 4, "and statted 4 files");
}

# trick jonathan...
{
    $n_stats = 0;
    my @list;
    make_file("3001002456", 50);
    make_file("3001002457", 50);
    make_file("30010023383333458", 50);
    make_file("3001002459", 50);
    $fs = Mogstored::FIDStatter->new(
                                     dir  => $td,
                                     from => "3001002456",
                                     to   => "3001002459",
                                     t_stat => sub { $n_stats++ },
                                     on_fid => sub {
                                         push @list, [@_];
                                     },
                                     );
    $fs->run();
    is(scalar @list, 3, "found 3 files");
    is($n_stats, 3, "and statted 3 files");
}

sub make_file {
    my ($fid, $len) = @_;
    my $pad = $fid;
    if (length($pad) < 10) {
        $pad = "0"x(10-length($pad)) . $pad;
    }
    my ($b, $mmm, $ttt, $hto) = ($pad =~ m{(\d)(\d{3})(\d{3})(\d{3})});
    my $fh;
    unless (open($fh, ">$td/$b/$mmm/$ttt/$pad.fid")) {
        if ($!{ENOENT}) {
            mkdir "$td/$b";
            mkdir "$td/$b/$mmm";
            mkdir "$td/$b/$mmm/$ttt";
        }
        open($fh, ">$td/$b/$mmm/$ttt/$pad.fid") or die
            "Error writing file: $td/$b/$mmm/$ttt/$pad.fid: $!\n";
    }
    print $fh "x" x (($len % 50) + 1);
    close($fh) or die;
}



