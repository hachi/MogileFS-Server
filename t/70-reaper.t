# -*-perl-*-
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use Time::HiRes qw(sleep);
use MogileFS::Server;
use MogileFS::Test;
find_mogclient_or_skip();
use MogileFS::Admin;
use File::Temp;

my $sto = eval { temp_store(); };
if (!$sto) {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my %mogroot;
$mogroot{1} = File::Temp::tempdir( CLEANUP => 1 );
my $dev2host = { 1 => 1, 2 => 1, 3 => 1 };
foreach (sort { $a <=> $b } keys %$dev2host) {
    my $root = $mogroot{$dev2host->{$_}};
    mkdir("$root/dev$_") or die "Failed to create dev$_ dir: $!";
}

my $ms1 = create_mogstored("127.0.1.1", $mogroot{1});
ok($ms1, "got mogstored");

while (! -e "$mogroot{1}/dev1/usage" ||
       ! -e "$mogroot{1}/dev2/usage" ||
       ! -e "$mogroot{1}/dev3/usage") {
    print "Waiting on usage...\n";
    sleep(.25);
}

my $tmptrack = create_temp_tracker($sto);
ok($tmptrack);

my $admin = IO::Socket::INET->new(PeerAddr => '127.0.0.1:7001');
$admin or die "failed to create admin socket: $!";
my $moga = MogileFS::Admin->new(hosts => [ "127.0.0.1:7001" ]);
my $mogc = MogileFS::Client->new(
                                 domain => "testdom",
                                 hosts  => [ "127.0.0.1:7001" ],
                                 );

ok($tmptrack->mogadm("host", "add", "hostA", "--ip=127.0.1.1", "--status=alive"), "created hostA");
ok($tmptrack->mogadm("device", "add", "hostA", 1), "created dev1 on hostA");
ok($tmptrack->mogadm("device", "add", "hostA", 2), "created dev2 on hostA");

ok($tmptrack->mogadm("domain", "add", "testdom"), "created test domain");
ok($tmptrack->mogadm("class", "add", "testdom", "2copies", "--mindevcount=2"), "created 2copies class in testdom");
ok($tmptrack->mogadm("settings", "set", "queue_rate_for_reaper", 123), "set queue_rate_for_reaper");

# create one sample file with 2 copies
my $fh = $mogc->new_file("file1", "2copies");
ok($fh, "got filehandle");
ok(close($fh), "closed file");

my $tries;
my @urls;

# wait for it to replicate
for ($tries = 100; $tries--; ) {
    @urls = $mogc->get_paths("file1");
    last if scalar(@urls) == 2;
    sleep .1;
}

is(scalar(@urls), 2, "replicated to 2 paths");
my $orig_urls = join("\n", sort(@urls));

# add a new device and mark an existing device as dead
ok($tmptrack->mogadm("device", "add", "hostA", 3), "created dev3 on hostA");
ok($tmptrack->mogadm("device", "mark", "hostA", 2, "dead"), "mark dev2 as dead");

# reaper should notice the dead device in 5-10s
for ($tries = 100; $tries--; ) {
    @urls = $mogc->get_paths("file1");
    last if scalar(grep(m{/dev2/}, @urls)) == 0;
    sleep 0.1;
}
is(scalar(grep(m{/dev2/}, @urls)), 0, "file1 no longer references dead dev2");

# replicator should replicate the file within 15-30s
for ($tries = 300; $tries--; ) {
    @urls = sort($mogc->get_paths("file1"));
    last if (scalar(@urls) == 2) && (join("\n", @urls) ne $orig_urls);
    sleep 0.1;
}
is(grep(m{/dev3/}, @urls), 1, "file1 got copied to dev3");
is(scalar(@urls), 2, "we have 2 paths for file1 again");

done_testing();
