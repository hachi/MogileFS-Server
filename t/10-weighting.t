# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use Time::HiRes qw(sleep);

use MogileFS::Server;

use lib "$Bin/../../api/perl/lib";
BEGIN {
    $ENV{PERL5LIB} = "$Bin/../../api/perl/lib" . ($ENV{PERL5LIB} ? ":$ENV{PERL5LIB}" : "");
    $ENV{TESTING} = 1;
    $ENV{T_FAKE_IOW_DEV1} = 10;
    $ENV{T_FAKE_IOW_DEV2} = 10;
    $ENV{T_FAKE_IOW_DEV3} = 70;
    $ENV{T_FAKE_IOW_DEV4} = 10;
}
use MogileFS::Client;

require 't/lib/mogtestlib.pl';

# create temp mysql db,
# use mogadm to init it,
# mogstored on temp dir,
# register mogstored temp dir,
# mogilefsd startup,
# add file,
# etc

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 55;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $dbh = $sto->dbh;
my $rv;

use File::Temp;
my %mogroot;
$mogroot{1} = File::Temp::tempdir( CLEANUP => 1 );
$mogroot{2} = File::Temp::tempdir( CLEANUP => 1 );
my $dev2host = { 1 => 1, 2 => 1,
                 3 => 2, 4 => 2, };
foreach (sort { $a <=> $b } keys %$dev2host) {
    my $root = $mogroot{$dev2host->{$_}};
    mkdir("$root/dev$_") or die "Failed to create dev$_ dir: $!";
}

my $ms1 = create_mogstored("127.0.1.1", $mogroot{1});
ok($ms1, "got mogstored1");
my $ms2 = create_mogstored("127.0.1.2", $mogroot{2});
ok($ms1, "got mogstored2");

while (! -e "$mogroot{1}/dev1/usage" &&
       ! -e "$mogroot{1}/dev2/usage" &&
       ! -e "$mogroot{2}/dev3/usage" &&
       ! -e "$mogroot{2}/dev4/usage") {
    print "Waiting on usage...\n";
    sleep(.25);
}

my $tmptrack = create_temp_tracker($sto);
ok($tmptrack);

my $mogc = MogileFS::Client->new(
                                 domain => "testdom",
                                 hosts  => [ "127.0.0.1:7001" ],
                                 );
my $be = $mogc->{backend}; # gross, reaching inside of MogileFS::Client

# test some basic commands to backend
ok($tmptrack->mogadm("domain", "add", "testdom"), "created test domain");
ok($tmptrack->mogadm("class", "add", "testdom", "4copies", "--mindevcount=4"), "created 4copies class in testdom");

ok($tmptrack->mogadm("host", "add", "hostA", "--ip=127.0.1.1", "--status=alive"), "created hostA");
ok($tmptrack->mogadm("host", "add", "hostB", "--ip=127.0.1.2", "--status=alive"), "created hostB");

ok($tmptrack->mogadm("device", "add", "hostA", 1), "created dev1 on hostA");
ok($tmptrack->mogadm("device", "add", "hostA", 2), "created dev2 on hostA");
ok($tmptrack->mogadm("device", "add", "hostB", 3), "created dev3 on hostB");
ok($tmptrack->mogadm("device", "add", "hostB", 4), "created dev4 on hostB");

# wait for monitor
{
    my $was = $be->{timeout};  # can't use local on phash :(
    $be->{timeout} = 10;
    ok($be->do_request("do_monitor_round", {}), "waited for monitor")
        or die "Failed to wait for monitor";
    $be->{timeout} = $was;
}

# create one sample file
my $fh = $mogc->new_file("file1", "4copies");
ok($fh, "got filehandle");
unless ($fh) {
    die "Error: " . $mogc->errstr;
}

my $data = "My test file.\n" x 1024;
print $fh $data;
ok(close($fh), "closed file");

# wait for it to replicate
my $tries = 1;
my @urls;
while ($tries++ < 30 && (@urls = $mogc->get_paths("file1")) < 4) {
    diag("Waiting for replication to complete: try=$tries urls=" . scalar @urls);
    sleep .25;
}
is(scalar @urls, 4, "replicated to 4 paths");
my $to_repl_rows = $dbh->selectrow_array("SELECT COUNT(*) FROM file_to_replicate");
is($to_repl_rows, 0, "no more files to replicate");

my %stats;
for (1..100) {
    @urls = $mogc->get_paths("file1");
    $stats{$urls[0]}++;
}

use Data::Dumper;
diag("Statistically got the following: " . Dumper(\%stats));

