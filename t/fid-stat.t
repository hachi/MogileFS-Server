# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use Mogstored::FIDStatter;

plan tests => 5;

my $fs = Mogstored::FIDStatter->new;
ok($fs, "made statter");

