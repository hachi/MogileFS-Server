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
    plan tests => 27;
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
