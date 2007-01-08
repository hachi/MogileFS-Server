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
    plan tests => 12;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $dom = MogileFS::Domain->create("foo");
ok($dom, "created a domain");
my $cls = $dom->create_class("classA");
ok($cls, "created a class");

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

# create a tempfile
{
    my $fidid = $sto->register_tempfile(
                                        fid     => undef,
                                        dmid    => $dom->id,
                                        key     => "my_tempfile",
                                        classid => $cls->classid,
                                        devids  => join(',', 1,2,3),
                                        );
    ok($fidid, "got a fidid");

    my $fidid2 = eval {
        $sto->register_tempfile(
                                fid     => $fidid,
                                dmid    => $dom->id,
                                key     => "my_tempfile",
                                classid => $cls->classid,
                                devids  => join(',', 1,2,3),
                                );
    };
    my $errc = error_code($@);
    ok(!$fidid2, "didn't get fidid");
    is($errc, "dup", "got a dup into tempfile")
        or die "Got error: $@\n";
}
