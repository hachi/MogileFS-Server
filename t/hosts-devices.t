# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Store::MySQL;
use MogileFS::Util qw(error_code);
require "$Bin/lib/mogtestlib.pl";

my $rootdbh = eval { root_dbh(); };
if ($rootdbh) {
    plan tests => 16;
} else {
    plan skip_all => "Can't connect to local MySQL as root user.";
    exit 0;
}

my $tempdb = create_temp_db();
init_store($tempdb);

my $rv;
$rv = system("$Bin/../mogdbsetup", "--yes", "--dbname=" . $tempdb->name);
ok(!$rv, "database setup proceeded without problems");

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



