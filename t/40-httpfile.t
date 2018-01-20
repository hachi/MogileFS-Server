# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;

find_mogclient_or_skip();

my $sto = eval { temp_store(); };
if (!$sto) {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $dbh = $sto->dbh;
my $rv;

my ($hostA_ip) = (qw/127.0.1.1/);

use File::Temp;
my %mogroot;
$mogroot{1} = File::Temp::tempdir( CLEANUP => 1 );
my $dev2host = { 1 => 1 };
foreach (sort { $a <=> $b } keys %$dev2host) {
    my $root = $mogroot{$dev2host->{$_}};
    mkdir("$root/dev$_") or die "Failed to create dev$_ dir: $!";
}

my $ms1 = create_mogstored($hostA_ip, $mogroot{1});

while (! -e "$mogroot{1}/dev1/usage") {
    print "Waiting on usage...\n";
    sleep 1;
}

my $tmptrack = create_temp_tracker($sto);
ok($tmptrack);

my $mogc = MogileFS::Client->new(
                                 domain => "testdom",
                                 hosts  => [ "127.0.0.1:7001" ],
                                 );
my $be = $mogc->{backend}; # gross, reaching inside of MogileFS::Client

# test some basic commands to backend
ok($be->do_request("test", {}), "test ping worked");

ok($tmptrack->mogadm("domain", "add", "testdom"), "created test domain");
ok($tmptrack->mogadm("class", "add", "testdom", "1copy", "--mindevcount=1"), "created 1copy class in testdom");

ok($tmptrack->mogadm("host", "add", "hostA", "--ip=$hostA_ip", "--status=alive"), "created hostA");

ok($tmptrack->mogadm("device", "add", "hostA", 1), "created dev1 on hostA");

# wait for monitor
{
    my $was = $be->{timeout};  # can't use local on phash :(
    $be->{timeout} = 10;
    ok($be->do_request("clear_cache", {}), "waited for monitor")
        or die "Failed to wait for monitor";
    ok($be->do_request("clear_cache", {}), "waited for monitor")
        or die "Failed to wait for monitor";
    $be->{timeout} = $was;
}

# create a file
my $fh = $mogc->new_file("file", "1copy")
        or die "Failed to create file: " . $mogc->errstr;
my $data = "DATA";
print $fh $data;
close($fh) or die "Failed to close file";
my @paths = $mogc->get_paths("file");

use MogileFS::Device;
use MogileFS::Host;
use MogileFS::Config;
use MogileFS::Rebalance;
use MogileFS::Factory::Host;
use MogileFS::Factory::Device;
use Digest::MD5 qw/md5/;

my $dfac = MogileFS::Factory::Device->get_factory;
my $hfac = MogileFS::Factory::Host->get_factory;

map { $hfac->set($_) } $sto->get_all_hosts;
map { $dfac->set($_) } $sto->get_all_devices;
my @devs = $dfac->get_all;

### Hacks to make tests work :/
$MogileFS::Config::skipconfig = 1;
MogileFS::Config->load_config;

my $file = MogileFS::HTTPFile->at($paths[0]);
my $md5_digest;

$md5_digest = $file->digest_mgmt("MD5", sub {});
ok($md5_digest eq md5("DATA"), "mgmt only");
my $cb_called = 0;
$md5_digest = $file->digest_http("MD5", sub { $cb_called++ });
ok(1 == $cb_called, "ping callback called");
ok($md5_digest eq md5("DATA"), "http only");

$md5_digest = $file->digest("MD5", sub {});
ok($md5_digest eq md5("DATA"), "mgmt or http");
ok(length($md5_digest) == 16, "MD5 is 16 bytes (128 bits)");

my $size = 100 * 1024 * 1024;
$fh = $mogc->new_file("largefile", "1copy")
        or die "Failed to create largefile: " . $mogc->errstr;
$data = "LARGE" x 20;
my $expect = Digest::MD5->new;
foreach my $i (1..(1024 * 1024)) {
	$expect->add($data);
	print $fh $data or die "failed to write chunk $i for largefile";
}
close($fh) or die "Failed to close largefile";
$expect = $expect->digest;
@paths = $mogc->get_paths("largefile");
$file = MogileFS::HTTPFile->at($paths[0]);
ok($size == $file->size, "big file size match $size");
ok($file->digest_mgmt('MD5', sub {}) eq $expect, "digest_mgmt('MD5') on big file");
ok($file->digest_http('MD5', sub {}) eq $expect, "digest_http('MD5') on big file");

ok($file->delete, "file deleted");
is(-1, $file->digest_http('MD5', sub {}), "digest_http detected missing");
is(-1, $file->digest_mgmt('MD5', sub {}), "digest_mgmt detected missing");

done_testing();
