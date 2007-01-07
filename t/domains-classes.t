# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Store::MySQL;
use MogileFS::Util qw(error_code);
require "$Bin/lib/mogtestlib.pl";

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 26;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

is(scalar MogileFS::Domain->domains, 0, "no domains at present");

my $dom = MogileFS::Domain->create("foo");
ok($dom, "created a domain");

my $dup = eval { MogileFS::Domain->create("foo") };
ok(!$dup, "didn't create it");
is(error_code($@), "dup", "because it was a duplicate domain");

is(scalar MogileFS::Domain->domains, 1, "one domain now");
$dom->delete;
is(scalar MogileFS::Domain->domains, 0, "back to zero domains");

$dom = MogileFS::Domain->create("foo");
ok($dom, "created foo domain again");
is(scalar MogileFS::Domain->domains, 1, "back to one domain");

{
    local $Mgd::_T_DOM_HAS_FILES = 1;
    ok(!eval{ $dom->delete; }, "failed to delete domain");
    is(error_code($@), "has_files", "because it had files");
}

my @classes = $dom->classes;
is(scalar @classes, 1, "one class in domain")
    or die;
is($classes[0]->name, "default", "is the default class");
is($classes[0]->classid, 0, ".. of classid 0");
ok(defined $classes[0]->classid, ".. which is defined");

my $cla = $dom->create_class("classA");
ok($cla, "created classA");
is(scalar($dom->classes), 2, "two classes now");

my $clb = $dom->create_class("classB");
ok($clb, "created classB");
is(scalar($dom->classes), 3, "three classes now");

{
    my $dup = eval { $dom->create_class("classA") }; # can't create this again
    ok(!$dup, "didn't create dup of A");
    is(error_code($@), "dup", "because it was a dup");
}

ok($clb->set_name("classB2"), "renamed classB to B2");
is($clb->name, "classB2", "and it renamed");

ok(!eval { $clb->set_name("classA") }, "failed to rename B2 to classA");
is(error_code($@), "dup", "because it was a dup");

ok($clb->delete, "deleted class");
is(scalar($dom->classes), 2, "two classes now");







