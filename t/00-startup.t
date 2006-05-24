# -*-perl-*-

use strict;
use warnings;
use Test::More 'no_plan';
require 't/lib/mogtestlib.pl';

# create temp mysql db,
# use mogadm to init it,
# mogstored on temp dir,
# register mogstored temp dir,
# mogilefsd startup,
# add file,
# etc

my $tempdb = create_temp_db();
isa_ok $tempdb, "DBHandle";



ok(1);
