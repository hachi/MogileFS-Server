# -*-perl-*-
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use Time::HiRes qw(sleep);
use MogileFS::Server;
use MogileFS::Test;
use HTTP::Request;
find_mogclient_or_skip();

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 40;
} else {
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

while (! -e "$mogroot{1}/dev1/usage" &&
       ! -e "$mogroot{2}/dev2/usage") {
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

wait_for_monitor($be);

my ($req, $rv, %opts);
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
              checksumtype => "MD5", mindevcount => 2 );
    ok($be->do_request("update_class", \%opts), "update class");
    wait_for_monitor($be);
}

# save new row to checksum table
{
    my $key = 'savecksum';
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
    $sto->delete_checksum($info->{fid});
    $info = $mogc->file_info($key);
    is($info->{checksum}, "MISSING", 'checksum is MISSING after delete');
}

{
    my @classes;
    %opts = ( domain => "testdom", class => "1copy", mindevcount => 1 );

    $opts{checksumtype} = "NONE";
    ok($be->do_request("update_class", \%opts), "update class");
    @classes = grep { $_->{classname} eq '1copy' } $sto->get_all_classes;
    is($classes[0]->{checksumtype}, undef, "checksumtype unset");

    $opts{checksumtype} = "MD5";
    ok($be->do_request("update_class", \%opts), "update class");
    @classes = grep { $_->{classname} eq '1copy' } $sto->get_all_classes;
    is($classes[0]->{checksumtype}, 1, "checksumtype is 1 (MD5)");

    $opts{checksumtype} = "NONE";
    ok($be->do_request("update_class", \%opts), "update class");
    @classes = grep { $_->{classname} eq '1copy' } $sto->get_all_classes;
    is($classes[0]->{checksumtype}, undef, "checksumtype unset");
}
