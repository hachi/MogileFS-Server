# -*-perl-*-

use strict;
use warnings;
use Test::More 'no_plan';
require 't/lib/mogtestlib.pl';

use FindBin qw($Bin);

# create temp mysql db,
# use mogadm to init it,
# mogstored on temp dir,
# register mogstored temp dir,
# mogilefsd startup,
# add file,
# etc

my $tempdb = create_temp_db();
isa_ok $tempdb, "DBHandle";

my $rv;
$rv = system("$Bin/../mogdbsetup", "--yes", "--dbname=" . $tempdb->name);
ok(!$rv, "database setup proceeded without problems");

$rv = system("$Bin/../mogdbsetup", "--yes", "--dbname=" . $tempdb->name);
ok(!$rv, "database setup ran again without problems");

my $tmptrack = create_temp_tracker($tempdb);

pass("done");
