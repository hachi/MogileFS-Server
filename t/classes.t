# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Store::MySQL;
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





