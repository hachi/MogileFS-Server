# -*-perl-*-
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use MogileFS::Server;
use MogileFS::Test;
use HTTP::Request;
find_mogclient_or_skip();
use MogileFS::Admin;

my $sto = eval { temp_store(); };
if (!$sto) {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

use File::Temp;
my %mogroot;
$mogroot{1} = File::Temp::tempdir( CLEANUP => 1 );
$mogroot{2} = File::Temp::tempdir( CLEANUP => 1 );
my $dev2host = { 1 => 1, 2 => 2, };
foreach (sort { $a <=> $b } keys %$dev2host) {
    my $root = $mogroot{$dev2host->{$_}};
    mkdir("$root/dev$_") or die "Failed to create dev$_ dir: $!";
}

my $ms1 = create_mogstored("127.0.1.1", $mogroot{1});
ok($ms1, "got mogstored1");
my $ms2 = create_mogstored("127.0.1.2", $mogroot{2});
ok($ms1, "got mogstored2");

try_for(30, sub {
    print "Waiting on usage...\n";
    -e "$mogroot{1}/dev1/usage" && -e "$mogroot{2}/dev2/usage";
});

my $tmptrack = create_temp_tracker($sto);
ok($tmptrack);

my $admin = IO::Socket::INET->new(PeerAddr => '127.0.0.1:7001');
$admin or die "failed to create admin socket: $!";
my $moga = MogileFS::Admin->new(hosts => [ "127.0.0.1:7001" ]);
my $mogc = MogileFS::Client->new(
                                 domain => "testdom",
                                 hosts  => [ "127.0.0.1:7001" ],
                                 );
my $be = $mogc->{backend}; # gross, reaching inside of MogileFS::Client

# test some basic commands to backend
ok($tmptrack->mogadm("domain", "add", "testdom"), "created test domain");
ok($tmptrack->mogadm("class", "add", "testdom", "changer", "--mindevcount=2", "--hashtype=MD5"), "created changer class in testdom with hashtype=MD5");

ok($tmptrack->mogadm("host", "add", "hostA", "--ip=127.0.1.1", "--status=alive"), "created hostA");
ok($tmptrack->mogadm("host", "add", "hostB", "--ip=127.0.1.2", "--status=alive"), "created hostB");

ok($tmptrack->mogadm("device", "add", "hostA", 1), "created dev1 on hostA");
ok($tmptrack->mogadm("device", "add", "hostB", 2), "created dev2 on hostB");

sub wait_for_monitor {
    my $be = shift;
    my $was = $be->{timeout};  # can't use local on phash :(
    $be->{timeout} = 10;
    ok($be->do_request("clear_cache", {}), "waited for monitor")
        or die "Failed to wait for monitor";
    ok($be->do_request("clear_cache", {}), "waited for monitor")
        or die "Failed to wait for monitor";
    $be->{timeout} = $was;
}

wait_for_monitor($be);
want($admin, 0, "replicate");

my ($req, $rv, %opts, @paths, @fsck_log);
my $ua = LWP::UserAgent->new;

use Data::Dumper;
use Digest::MD5 qw/md5_hex/;

my $key = "foo";
{
    %opts = ( domain => "testdom", class => "changer", key => $key );
    $rv = $be->do_request("create_open", \%opts);
    %opts = %$rv;
    ok($rv && $rv->{path}, "create_open succeeded");
    $req = HTTP::Request->new(PUT => $rv->{path});
    $req->content("blah");
    $rv = $ua->request($req);
    ok($rv->is_success, "PUT successful");
    $opts{key} = $key;
    $opts{domain} = "testdom";
    $opts{checksum} = "MD5:".md5_hex('blah');
    $opts{checksumverify} = 1;
    $rv = $be->do_request("create_close", \%opts);
    ok($rv, "checksum verified successfully");
    ok($sto->get_checksum($opts{fid}), "checksum saved");
    ok($mogc->file_info($key), "file_info($key) is sane");
}

# disable MD5 checksums in "changer" class
{
    %opts = ( domain => "testdom", class => "changer",
              hashtype => "NONE", mindevcount => 2);
    ok($be->do_request("update_class", \%opts), "update class");
    wait_for_monitor($be);
}

# replicate should work even if we have, but don't need a checksum anymore
{
    want($admin, 1, "replicate");

    # wait for replicate to recreate checksum
    try_for(30, sub {
        @paths = $mogc->get_paths($key);
        scalar(@paths) != 1;
    });
    is(scalar(@paths), 2, "replicated successfully");
    want($admin, 0, "replicate");
}

# switch to SHA-1 checksums in "changer" class
{
    %opts = ( domain => "testdom", class => "changer",
              hashtype => "SHA-1", mindevcount => 2);
    ok($be->do_request("update_class", \%opts), "update class");
    wait_for_monitor($be);
}

{
    ok($tmptrack->mogadm("fsck", "stop"), "stop fsck");
    ok($tmptrack->mogadm("fsck", "clearlog"), "clear fsck log");
    ok($tmptrack->mogadm("fsck", "reset"), "reset fsck");
    ok($tmptrack->mogadm("fsck", "start"), "started fsck");

    try_for(30, sub { @fsck_log = $sto->fsck_log_rows; });
    is($fsck_log[0]->{evcode}, "BALG", "bad checksum algorithm logged");
}

done_testing();
