# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use Data::Dumper;

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;

plan tests => 31;

my $obj;

$obj = MogileFS::ReplicationPolicy->new_from_policy_string("MultipleHosts(5)");
isa_ok($obj, "MogileFS::ReplicationPolicy::MultipleHosts", "got a multiple hosts policy")
    or die "can't proceed";
is($obj->mindevcount, 5, "got correct devcount");

$obj = MogileFS::ReplicationPolicy->new_from_policy_string("MultipleHosts()");
isa_ok($obj, "MogileFS::ReplicationPolicy::MultipleHosts", "got a multiple hosts policy")
    or die "can't proceed";

foreach my $str ("Union(MultipleHosts(5), MultipleHosts(2))",
                 "Union(MultipleHosts(5), MultipleHosts(2), )",
                 "Union( MultipleHosts(5), MultipleHosts(2) )",
                 "Union(MultipleHosts(  5),MultipleHosts(2))",
                 "Union ( MultipleHosts ( 5 ) , MultipleHosts ( 2 ) )",
                 "Union ( MultipleHosts ( 5 ) ,\n MultipleHosts ( 2 ) )",
                 "Union ( MultipleHosts ( 5 ) , \n MultipleHosts ( 2 ), )",
                 )
{
    $obj = MogileFS::ReplicationPolicy->new_from_policy_string($str);
    isa_ok($obj, "MogileFS::ReplicationPolicy::Union") or die "Failed to parse: $str";
    is(scalar @{$obj->{policies}}, 2, "got 2 sub policies");
    isa_ok($obj->{policies}[0], "MogileFS::ReplicationPolicy::MultipleHosts");
    isa_ok($obj->{policies}[1], "MogileFS::ReplicationPolicy::MultipleHosts");
}


