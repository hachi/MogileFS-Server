# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 18;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

is(scalar MogileFS::Host->hosts, 0, "no hosts at present");
is(scalar MogileFS::Device->devices, 0, "no devices at present");

my $ha = MogileFS::Host->create("a", "10.0.0.1");
ok($ha, "made hostA");
my $hb = MogileFS::Host->create("b", "10.0.0.2");
ok($hb, "made hostB");
ok(!eval{ MogileFS::Host->create("b", "10.0.0.3") }, "can't dup hostB's name");
is(error_code($@), "dup", "yup, was a dup");
ok(!eval{ MogileFS::Host->create("c", "10.0.0.2") }, "can't dup hostB's IP");
is(error_code($@), "dup", "yup, was a dup");

ok($hb->set_ip("10.0.0.4"), "set IP");
is($hb->ip, "10.0.0.4", "IP matches");
ok(!eval{$hb->set_ip("10.0.0.1")}, "IP's taken");
is(error_code($@), "dup", "yup, was a dup");

is(scalar MogileFS::Host->hosts, 2, "2 hosts now");
ok($ha->delete, "deleted hostA");
is(scalar MogileFS::Host->hosts, 1, "1 host now");

my $da = MogileFS::Device->create(devid => 1,
                                  hostid => $hb->id,
                                  status => "alive");
ok($da, "made dev1");
ok($da->not_on_hosts($ha), "dev1 not on ha");
ok(!$da->not_on_hosts($hb), "dev1 is on hb");




