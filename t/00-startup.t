# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use lib "$Bin/../../api/perl/lib";
BEGIN {
    $ENV{PERL5LIB} = "$Bin/../../api/perl/lib" . ($ENV{PERL5LIB} ? ":$ENV{PERL5LIB}" : "");
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

my $rootdbh = eval { root_dbh(); };
if ($rootdbh) {
    plan tests => 44;
} else {
    plan skip_all => "Can't connect to local MySQL as root user.";
    exit 0;
}

my $tempdb = create_temp_db();
isa_ok $tempdb, "DBHandle";
my $dbh = $tempdb->dbh;

my $rv;
$rv = system("$Bin/../mogdbsetup", "--yes", "--dbname=" . $tempdb->name);
ok(!$rv, "database setup proceeded without problems");

$rv = system("$Bin/../mogdbsetup", "--yes", "--dbname=" . $tempdb->name);
ok(!$rv, "database setup ran again without problems");

use File::Temp;
my %mogroot;
$mogroot{1} = File::Temp::tempdir( CLEANUP => 1 );
$mogroot{2} = File::Temp::tempdir( CLEANUP => 1 );
$mogroot{3} = File::Temp::tempdir( CLEANUP => 1 );
my $dev2host = { 1 => 1, 2 => 1,
                 3 => 2, 4 => 2,
                 5 => 3, 6 => 3, };
foreach (sort { $a <=> $b } keys %$dev2host) {
    my $root = $mogroot{$dev2host->{$_}};
    mkdir("$root/dev$_") or die "Failed to create dev$_ dir: $!";
}

my $ms1 = create_mogstored("127.0.1.1", $mogroot{1});
ok($ms1, "got mogstored1");
my $ms2 = create_mogstored("127.0.1.2", $mogroot{2});
ok($ms1, "got mogstored2");

while (! -e "$mogroot{1}/dev1/usage" &&
       ! -e "$mogroot{2}/dev4/usage") {
    print "Waiting on usage...\n";
    sleep 1;
}

my $tmptrack = create_temp_tracker($tempdb);
ok($tmptrack);

ok($tmptrack->mogadm("domain", "add", "testdom"), "created test domain");
ok($tmptrack->mogadm("class", "add", "testdom", "2copies", "--mindevcount=2"), "created 2copies class in testdom");

ok($tmptrack->mogadm("host", "add", "hostA", "--ip=127.0.1.1", "--status=alive"), "created hostA");
ok($tmptrack->mogadm("host", "add", "hostB", "--ip=127.0.1.2", "--status=alive"), "created hostB");

ok($tmptrack->mogadm("device", "add", "hostA", 1), "created dev1 on hostA");
ok($tmptrack->mogadm("device", "add", "hostA", 2), "created dev2 on hostA");
ok($tmptrack->mogadm("device", "add", "hostB", 3), "created dev3 on hostB");
ok($tmptrack->mogadm("device", "add", "hostB", 4), "created dev4 on hostB");

#ok($tmptrack->mogadm("device", "mark", "hostA", 1, "alive"), "dev1 alive");
#ok($tmptrack->mogadm("device", "mark", "hostA", 2, "alive"), "dev2 alive");
#ok($tmptrack->mogadm("device", "mark", "hostB", 3, "alive"), "dev3 alive");
#ok($tmptrack->mogadm("device", "mark", "hostB", 4, "alive"), "dev4 alive");

my $mogc = MogileFS::Client->new(
                                 domain => "testdom",
                                 hosts  => [ "127.0.0.1:7001" ],
                                 );

# wait for monitor
my $be = $mogc->{backend}; # gross, reaching inside of MogileFS::Client
{
    my $was = $be->{timeout};  # can't use local on phash :(
    $be->{timeout} = 10;
    ok($be->do_request("do_monitor_round", {}), "waited for monitor");
    die $be->errstr if $be->err;
    $be->{timeout} = $was;
}

# create one sample file
my $fh = $mogc->new_file("file1", "2copies");
ok($fh, "got filehandle");
unless ($fh) {
    die "Error: " . $mogc->errstr;
}

my $data = "My test file.\n" x 1024;
print $fh $data;
ok(close($fh), "closed file");

my $tries = 1;
my @urls;
while ($tries++ < 10 && (@urls = $mogc->get_paths("file1")) < 2) {
    sleep 1;
}
is(scalar @urls, 2, "replicated to 2 paths");

my $to_repl_rows = $dbh->selectrow_array("SELECT COUNT(*) FROM file_to_replicate");
is($to_repl_rows, 0, "no more files to replicate");

my $p1 = MogPath->new($urls[0]);
my $p2 = MogPath->new($urls[1]);
isnt($p1->host, $p2->host, "host1 and host2 are different");
my $path1 = $mogroot{$dev2host->{$p1->device}} . $p1->path;
my $path2 = $mogroot{$dev2host->{$p2->device}} . $p2->path;
is(-s $path1, length($data), "right length on disk for path1");
is(-s $path2, length($data), "right length on disk for path2");

ok(unlink($path1), "deleted path $path1");
my $dead_url = $urls[0];
for (1..10) {
    @urls = $mogc->get_paths("file1");
    isnt($urls[0], $dead_url, "didn't return dead url first (try $_)");
}

# create a couple hundred files now
my $n_files = 100;
for my $n (1..$n_files) {
    my $fh = $mogc->new_file("manyhundred_$n", "2copies")
        or die "Failed to create manyhundred_$n";
    my $data = "File number $n.\n" x 512;
    print $fh $data;
    close($fh) or die "Failed to close manyhundred_$n";
    diag("created $n/$n_files") if $n % 25 == 0;
}
pass("Created a ton of files");

# wait for replication to go down
{
    my $iters = 10;
    my $to_repl_rows;
    while ($iters) {
        $iters--;
        $to_repl_rows = $dbh->selectrow_array("SELECT COUNT(*) FROM file_to_replicate");
        last if ! $to_repl_rows;
        diag("Files to replicate: $to_repl_rows");
        sleep 1;
    }
    die "Failed to replicate all $n_files files" if $to_repl_rows;
    pass("Replicated all $n_files files");
}

# now let's delete a host, which should fail hard, because there are still devices attached to it
{
    die "Can't delete an active host" if
        $tmptrack->mogadm("host", "delete", "hostB");
    pass("didn't delete hostB");
}

# create a new host and device, for when we start killing some devices
my $ms3 = create_mogstored("127.0.1.3", $mogroot{3});
ok($ms3, "got mogstored3");
ok($tmptrack->mogadm("host", "add", "hostC", "--ip=127.0.1.3", "--status=alive"), "created hostC");
ok($tmptrack->mogadm("device", "add", "hostC", 5), "created dev5 on hostC");
ok($tmptrack->mogadm("device", "add", "hostC", 6), "created dev6 on hostC");

# let it be discovered
sleep(3);  # FIXME: make an explicit "rescan" or "remonitor" job to mogilefsd, just for test suite

ok($tmptrack->mogadm("device", "mark", "hostB", 3, "dead"), "marked device B/3 dead");
ok($tmptrack->mogadm("device", "mark", "hostB", 4, "dead"), "marked device B/4 dead");

ok(try_for(15, sub {
    my %has;
    my $sth = $dbh->prepare("SELECT devid, COUNT(*) FROM file_on GROUP BY devid");
    $sth->execute;
    while (my ($devid, $ct) = $sth->fetchrow_array) {
        $has{$devid} = $ct;
    }
    diag("Replication update: " . join(", ", map { "dev$_: " . sprintf("%3d", ($has{$_}||0)) } (1..6)));
    return 0 if $has{3} || $has{4};
    return $has{1} && $has{1} && $has{5} && $has{6};
}), "files replicated to hostC from hostB");

# kill hostB now
ok($tmptrack->mogadm("host", "delete", "hostB"), "killed hostB");


# enable fsck (job already running, but waiting for config update)

# do get_paths again and wait for it to go to 2, reliably.  or, wait for 1st path to be $dead_url, which is now not dead.


#$dbh->do("INSERT INTO file_to_replicate SET fid=7");
#sleep 60;


sub try_for {
    my ($tries, $code) = @_;
    for (1..$tries) {
        return 1 if $code->();
        sleep 1;
    }
    return 0;
}
