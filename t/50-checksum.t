# -*-perl-*-
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use MogileFS::Server;
use MogileFS::Test;
use HTTP::Request;
find_mogclient_or_skip();

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
ok($tmptrack->mogadm("class", "add", "testdom", "2copies", "--mindevcount=2"), "created 2copies class in testdom");
ok($tmptrack->mogadm("class", "add", "testdom", "1copy", "--mindevcount=1"), "created 1copy class in testdom");

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

sub stop_replicate {
    my ($admin) = @_;
    syswrite($admin, "!want 0 replicate\r\n"); # disable replication
    ok(<$admin> =~ /Now desiring/ && <$admin> eq ".\r\n", "disabling replicate");

    my $count;
    try_for(30, sub {
        $count = -1;
        syswrite($admin, "!jobs\r\n");
        MogileFS::Util::wait_for_readability(fileno($admin), 10);
        while (1) {
            my $line = <$admin>;
            if ($line =~ /\Areplicate count (\d+)/) {
                $count = $1;
            }
            last if $line eq ".\r\n";
        }
        $count == 0;
    });
    is($count, 0, "replicate count is zero");
}

sub full_fsck {
    my $tmptrack = shift;

    ok($tmptrack->mogadm("fsck", "stop"), "stop fsck");
    ok($tmptrack->mogadm("fsck", "clearlog"), "clear fsck log");
    ok($tmptrack->mogadm("fsck", "reset"), "reset fsck");
    ok($tmptrack->mogadm("fsck", "start"), "started fsck");
}

wait_for_monitor($be);

my ($req, $rv, %opts, @paths, @fsck_log);
my $ua = LWP::UserAgent->new;

use Data::Dumper;
use Digest::MD5 qw/md5_hex/;

# verify upload checksum
{
    my $key = "ok";
    %opts = ( domain => "testdom", class => "1copy", key => $key );
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
    is($sto->get_checksum($opts{fid}), undef, "checksum not saved");
    ok($mogc->file_info($key), "file_info($key) is sane");
}

# corrupted upload checksum fails
{
    my $key = 'corrupt';
    %opts = ( domain => "testdom", class => "1copy", key => $key );
    $rv = $be->do_request("create_open", \%opts);
    %opts = %$rv;
    ok($rv && $rv->{path}, "create_open succeeded");
    $req = HTTP::Request->new(PUT => $rv->{path});
    $req->content("blah");
    $rv = $ua->request($req);
    ok($rv->is_success, "PUT successful");
    $opts{key} = $key;
    $opts{domain} = "testdom";

    $opts{checksumverify} = 1;
    $opts{checksum} = "MD5:".md5_hex('fail');
    $rv = $be->do_request("create_close", \%opts);
    ok(!defined($rv), "checksum verify noticed mismatch");
    my $hex = md5_hex('blah');
    is('checksum_mismatch', $be->{lasterr}, "error code is correct");
    ok($be->{lasterrstr} =~ /actual: MD5:$hex;/, "error message shows actual:");
    is($sto->get_checksum($opts{fid}), undef, "checksum not saved");
    is($mogc->file_info($key), undef, "$key not uploaded");
}

# enable saving MD5 checksums in "2copies" class
{
    %opts = ( domain => "testdom", class => "2copies",
              hashtype => "MD5", mindevcount => 2 );
    ok($be->do_request("update_class", \%opts), "update class");
    wait_for_monitor($be);
}

# save new row to checksum table
{
    my $key = 'savecksum';

    stop_replicate($admin);

    %opts = ( domain => "testdom", class => "2copies", key => $key );
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
    my $row = $sto->get_checksum($opts{fid});
    ok($row, "checksum saved");
    my $info = $mogc->file_info($key);
    ok($info, "file_info($key) is sane");
    is($info->{checksum}, "MD5:".md5_hex('blah'), 'checksum shows up');
    is($sto->delete_checksum($info->{fid}), 1, "$key checksum row deleted");
    $info = $mogc->file_info($key);
    is($info->{checksum}, "MISSING", 'checksum is MISSING after delete');

    syswrite($admin, "!want 1 replicate\n"); # disable replication
    ok(<$admin> =~ /Now desiring/ && <$admin> eq ".\r\n", "enabled replicate");

    # wait for replicate to recreate checksum
    try_for(30, sub {
        @paths = $mogc->get_paths($key);
        scalar(@paths) != 1;
    });
    is(scalar(@paths), 2, "replicate successfully with good checksum");

    $info = $mogc->file_info($key);
    is($info->{checksum}, "MD5:".md5_hex('blah'), 'checksum recreated on repl');
}

# flip checksum classes around
{
    my @classes;
    %opts = ( domain => "testdom", class => "1copy", mindevcount => 1 );

    $opts{hashtype} = "NONE";
    ok($be->do_request("update_class", \%opts), "update class");
    @classes = grep { $_->{classname} eq '1copy' } $sto->get_all_classes;
    is($classes[0]->{hashtype}, undef, "hashtype unset");

    $opts{hashtype} = "MD5";
    ok($be->do_request("update_class", \%opts), "update class");
    @classes = grep { $_->{classname} eq '1copy' } $sto->get_all_classes;
    is($classes[0]->{hashtype}, 1, "hashtype is 1 (MD5)");

    $opts{hashtype} = "NONE";
    ok($be->do_request("update_class", \%opts), "update class");
    @classes = grep { $_->{classname} eq '1copy' } $sto->get_all_classes;
    is($classes[0]->{hashtype}, undef, "hashtype unset");
}

# save checksum on replicate, client didn't care to provide one
{
    my $key = 'lazycksum';

    stop_replicate($admin);

    my $fh = $mogc->new_file($key, "2copies");
    print $fh "lazy";
    ok(close($fh), "closed file");
    my $info = $mogc->file_info($key);
    is($info->{checksum}, 'MISSING', 'checksum is MISSING');

    syswrite($admin, "!want 1 replicate\n"); # disable replication
    ok(<$admin> =~ /Now desiring/ && <$admin> eq ".\r\n", "enabled replicate");

    try_for(30, sub {
        @paths = $mogc->get_paths($key);
        scalar(@paths) != 1;
    });
    is(scalar(@paths), 2, "replicate successfully with good checksum");

    $info = $mogc->file_info($key);
    is($info->{checksum}, "MD5:".md5_hex("lazy"), 'checksum is set after repl');
}

# fsck recreates checksum when missing
{
    my $key = 'lazycksum';
    my $info = $mogc->file_info($key);
    $sto->delete_checksum($info->{fid});
    $info = $mogc->file_info($key);
    is($info->{checksum}, "MISSING", "checksum is missing");
    full_fsck($tmptrack);

    try_for(30, sub {
        $info = $mogc->file_info($key);
        $info->{checksum} ne "MISSING";
    });
    is($info->{checksum}, "MD5:".md5_hex("lazy"), 'checksum is set after fsck');

    @fsck_log = $sto->fsck_log_rows;
    is(scalar(@fsck_log), 1, "fsck log has one row");
    is($fsck_log[0]->{fid}, $info->{fid}, "fid matches in fsck log");
    is($fsck_log[0]->{evcode}, "NSUM", "missing checksum logged");
}

# fsck fixes a file corrupt file
{
    my $key = 'lazycksum';
    my $info = $mogc->file_info($key);
    @paths = $mogc->get_paths($key);
    is(scalar(@paths), 2, "2 paths for lazycksum");
    $req = HTTP::Request->new(PUT => $paths[0]);
    $req->content("LAZY");
    $rv = $ua->request($req);
    ok($rv->is_success, "upload to corrupt a file successful");
    is($ua->get($paths[0])->content, "LAZY", "file successfully corrupted");
    is($ua->get($paths[1])->content, "lazy", "paths[1] not corrupted");

    full_fsck($tmptrack);

    try_for(30, sub {
        @fsck_log = $sto->fsck_log_rows;
        scalar(@fsck_log) != 0;
    });

    is(scalar(@fsck_log), 1, "fsck log has one row");
    is($fsck_log[0]->{fid}, $info->{fid}, "fid matches in fsck log");
    is($fsck_log[0]->{evcode}, "REPL", "repl for mismatched checksum logged");

    try_for(30, sub {
        @paths = $mogc->get_paths($key);
        scalar(@paths) >= 2;
    });

    is(scalar(@paths), 2, "2 paths for key after replication");
    is($ua->get($paths[0])->content, "lazy", "paths[0] is correct");
    is($ua->get($paths[1])->content, "lazy", "paths[1] is correct");
    $info = $mogc->file_info($key);
    is($info->{checksum}, "MD5:".md5_hex("lazy"), 'checksum unchanged after fsck');
}

# fsck notices when all files are corrupt
{
    my $key = 'lazycksum';
    my $info = $mogc->file_info($key);
    @paths = $mogc->get_paths($key);
    is(scalar(@paths), 2, "2 paths for lazycksum");

    $req = HTTP::Request->new(PUT => $paths[0]);
    $req->content("0000");
    $rv = $ua->request($req);
    ok($rv->is_success, "upload to corrupt a file successful");
    is($ua->get($paths[0])->content, "0000", "successfully corrupted");

    $req = HTTP::Request->new(PUT => $paths[1]);
    $req->content("1111");
    $rv = $ua->request($req);
    ok($rv->is_success, "upload to corrupt a file successful");
    is($ua->get($paths[1])->content, "1111", "successfully corrupted");

    full_fsck($tmptrack);

    try_for(30, sub {
        @fsck_log = $sto->fsck_log_rows;
        scalar(@fsck_log) != 0;
    });

    is(scalar(@fsck_log), 1, "fsck log has one row");
    is($fsck_log[0]->{fid}, $info->{fid}, "fid matches in fsck log");
    is($fsck_log[0]->{evcode}, "BSUM", "BSUM logged");

    @paths = $mogc->get_paths($key);
    is(scalar(@paths), 2, "2 paths for checksum");
    @paths = sort( map { $ua->get($_)->content } @paths);
    is(join(', ', @paths), "0000, 1111", "corrupted content preserved");
}

# reuploaded checksum row clobbers old checksum
{
    my $key = 'lazycksum';
    my $info = $mogc->file_info($key);

    ok($sto->get_checksum($info->{fid}), "old checksum row exists");

    my $fh = $mogc->new_file($key, "2copies");
    print $fh "HAZY";
    ok(close($fh), "closed replacement file (lazycksum => HAZY)");

    try_for(30, sub { ! $sto->get_checksum($info->{fid}); });
    is($sto->get_checksum($info->{fid}), undef, "old checksum is gone");
}

# completely corrupted files with no checksum row
{
    my $key = 'lazycksum';
    try_for(30, sub {
        @paths = $mogc->get_paths($key);
        scalar(@paths) >= 2;
    });
    is(scalar(@paths), 2, "replicated succesfully");

    my $info = $mogc->file_info($key);
    is($info->{checksum}, "MD5:".md5_hex("HAZY"), "checksum created on repl");

    $req = HTTP::Request->new(PUT => $paths[0]);
    $req->content("MAYB");
    $rv = $ua->request($req);
    ok($rv->is_success, "upload to corrupt a file successful");
    is($ua->get($paths[0])->content, "MAYB", "successfully corrupted (MAYB)");

    $req = HTTP::Request->new(PUT => $paths[1]);
    $req->content("CRZY");
    $rv = $ua->request($req);
    ok($rv->is_success, "upload to corrupt a file successful");
    is($ua->get($paths[1])->content, "CRZY", "successfully corrupted (CRZY)");

    is($sto->delete_checksum($info->{fid}), 1, "nuke new checksum");
    $info = $mogc->file_info($key);
    is($info->{checksum}, "MISSING", "checksum is MISSING");

    full_fsck($tmptrack);

    try_for(30, sub {
        @fsck_log = $sto->fsck_log_rows;
        scalar(@fsck_log) != 0;
    });

    is(scalar(@fsck_log), 1, "fsck log has one row");
    is($fsck_log[0]->{fid}, $info->{fid}, "fid matches in fsck log");
    is($fsck_log[0]->{evcode}, "BSUM", "BSUM logged");
}

# disable MD5 checksums in "2copies" class
{
    %opts = ( domain => "testdom", class => "2copies",
              hashtype => "NONE", mindevcount => 2 );
    ok($be->do_request("update_class", \%opts), "update class");
    wait_for_monitor($be);
}

# use fsck_checksum=MD5 instead of per-class checksums
{
    my $key = 'lazycksum';
    my $info = $mogc->file_info($key);
    $sto->delete_checksum($info->{fid});

    ok($tmptrack->mogadm("settings", "set", "fsck_checksum", "MD5"), "enable fsck_checksum=MD5");
    wait_for_monitor($be);
    full_fsck($tmptrack);

    try_for(30, sub {
        @fsck_log = $sto->fsck_log_rows;
        scalar(@fsck_log) != 0;
    });
    is(scalar(@fsck_log), 1, "fsck log has one row");
    is($fsck_log[0]->{fid}, $info->{fid}, "fid matches in fsck log");
    is($fsck_log[0]->{evcode}, "MSUM", "MSUM logged");
}

# ensure server setting is visible
use MogileFS::Admin;
{
    my $settings = $moga->server_settings;
    is($settings->{fsck_checksum}, 'MD5', "fsck_checksum server setting visible");
}

use MogileFS::Config;

# disable checksumming entirely, regardless of class setting
{
    %opts = ( domain => "testdom", class => "2copies",
              hashtype => "MD5", mindevcount => 2 );
    ok($be->do_request("update_class", \%opts), "update class");
    wait_for_monitor($be);

    ok($tmptrack->mogadm("settings", "set", "fsck_checksum", "off"), "set fsck_checksum=off");
    wait_for_monitor($be);
    my $settings = $moga->server_settings;
    is($settings->{fsck_checksum}, 'off', "fsck_checksum server setting visible");
    full_fsck($tmptrack);
    my $nr;
    try_for(1000, sub {
        $nr = $sto->file_queue_length(FSCK_QUEUE);
        $nr eq '0';
    });
    is($nr, '0', "fsck finished");
    @fsck_log = $sto->fsck_log_rows;
    is(scalar(@fsck_log), 0, "fsck log is empty with fsck_checksum=off");
}

# set fsck_checksum=class and ensure that works again
{
    my $info = $mogc->file_info('lazycksum');
    ok($tmptrack->mogadm("settings", "set", "fsck_checksum", "class"), "set fsck_checksum=class");
    wait_for_monitor($be);
    my $settings = $moga->server_settings;
    ok(! defined($settings->{fsck_checksum}), "fsck_checksum=class server setting hidden (default)");
    full_fsck($tmptrack);

    try_for(30, sub {
        @fsck_log = $sto->fsck_log_rows;
        scalar(@fsck_log) != 0;
    });

    is(scalar(@fsck_log), 1, "fsck log has one row");
    is($fsck_log[0]->{fid}, $info->{fid}, "fid matches in fsck log");
    is($fsck_log[0]->{evcode}, "BSUM", "BSUM logged");
}

done_testing();
