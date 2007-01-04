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
    plan tests => 5;
} else {
    plan skip_all => "Can't connect to local MySQL as root user.";
    exit 0;
}

my $tempdb = create_temp_db();
init_store($tempdb);

my $rv;
$rv = system("$Bin/../mogdbsetup", "--yes", "--dbname=" . $tempdb->name);
ok(!$rv, "database setup proceeded without problems");

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

