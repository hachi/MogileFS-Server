# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use Time::HiRes qw(sleep);

use MogileFS::Server;

BEGIN {
    $ENV{TESTING} = 1;
}

use MogileFS::Test;
find_mogclient_or_skip();

# create temp mysql db,
# use mogadm to init it,
# mogstored on temp dir,
# register mogstored temp dir,
# mogilefsd startup,
# add file,
# etc

plan skip_all => "Filepaths plugin has been separated from the server, a bit of work is needed to make the tests run again.";
exit 0;

my $sto = eval { temp_store(); };
if (!$sto) {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $dbh = $sto->dbh;
my $rv;

use File::Temp;
my $mogroot = File::Temp::tempdir( CLEANUP => 1 );
mkdir("$mogroot/dev1") or die "Failed to create dev1 dir: #!";

my $ms = create_mogstored("127.0.1.1", $mogroot);

while (! -e "$mogroot/dev1/usage") {
    print "Waiting on usage...\n";
    sleep(.25);
}

local $ENV{PERL5OPT} = "-MMogileFS::Plugin::FilePaths";
my $tmptrack = create_temp_tracker($sto, ["--plugins=FilePaths"]);
ok($tmptrack);

my $mogc = MogileFS::Client->new(
                                 domain => "testdom",
                                 hosts  => [ "127.0.0.1:7001" ],
                                 );
my $be = $mogc->{backend}; # gross, reaching inside of MogileFS::Client

# test some basic commands to backend
ok($tmptrack->mogadm("domain", "add", "testdom"), "created test domain");
ok($tmptrack->mogadm("class", "add", "testdom", "test", "--mindevcount=1"), "created test class in testdom");
ok($tmptrack->mogadm("host", "add", "host", "--ip=127.0.1.1", "--status=alive"), "created host");
ok($tmptrack->mogadm("device", "add", "host", 1), "created dev1 on host");

ok($mogc->filepaths_enable, "Filepaths enabled successfully");

# wait for monitor
{
    my $was = $be->{timeout};  # can't use local on phash :(
    $be->{timeout} = 10;
    ok($be->do_request("clear_cache", {}), "waited for monitor")
        or die "Failed to wait for monitor";
    $be->{timeout} = $was;
}

my $data = "My test file.\n" x 1024;

# create one sample file
{
    my $fh = $mogc->new_file("/bar/file1.txt", "test");
    ok($fh, "got filehandle");
    unless ($fh) {
        die "Error: " . $mogc->errstr;
    }

    print $fh $data;
    ok(close($fh), "closed file");
}

{
    my $fh = $mogc->new_file("foo.txt", "test");
    is($fh, undef, "File without absolute path should fail to be created");
}

{
    my $dir = $mogc->filepaths_list_directory('/');
    ok($dir, "Got a directory listing for /");

    my %files;
    my $filecount = $dir->{files};

    for (my $i = 0; $i < $filecount; $i++) {
        my $prefix = "file$i";
        my %nodeinfo;
        $nodeinfo{type} = $dir->{"$prefix.type"};
        my $filename = $dir->{$prefix};
        $files{$filename} = \%nodeinfo;
    }
    ok(!$files{'foo.txt'}, "foo.txt didn't end up in the listing");
    my $bar = $files{'bar'};
    ok($bar, "/bar is in the listing");
    is($bar->{type}, "D", "/bar is a directory");
}

{
    my $dir = $mogc->filepaths_list_directory('/bar');
    ok($dir, "Got directory listing for /bar");

    my %files;
    my $filecount = $dir->{files};

    for (my $i = 0; $i < $filecount; $i++) {
        my $prefix = "file$i";
        my %nodeinfo;
        $nodeinfo{type} = $dir->{"$prefix.type"};
        $nodeinfo{size} = $dir->{"$prefix.size"};
        my $filename = $dir->{$prefix};
        $files{$filename} = \%nodeinfo;
    }

    my $file1 = $files{'file1.txt'};
    ok($file1, "/file1.txt is in the listing");
    is($file1->{type}, "F", "Type of file1.txt is correct");
    is($file1->{size}, length($data), "Size of file1.txt is correct");
}

ok($mogc->filepaths_disable, "Filepaths disabled successfully");

done_testing();

# vim: filetype=perl
