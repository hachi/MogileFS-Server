# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
require "$Bin/lib/mogtestlib.pl";

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 7;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $df = MogileFS::DevFID->new(100, 200);
ok($df, "made devfid");
ok($df->add_to_db, "added to db");

my $fid = $df->fid;
ok($fid, "got fid from df");
my @on = $fid->devids;
is(scalar @on, 1, "FID 200 on one device");
is($on[0], 100, "is correct number");

ok($sto->mass_insert_file_on(MogileFS::DevFID->new(1, 101),
                             MogileFS::DevFID->new(2, 101)), "did mass insert");
$fid = MogileFS::FID->new(101);
@on = $fid->devids;
is(scalar @on, 2, "FID 101 on 2 devices");




