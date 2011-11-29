# -*-perl-*-

use strict;
use warnings;
use Test::More;

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;
use MogileFS::Checksum;
use Digest::MD5 qw(md5 md5_hex);

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 7;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

$sto->set_checksum(6, 1, md5("asdf"));
my $hash = $sto->get_checksum(6);
my $csum = MogileFS::Checksum->new($hash);
is(md5_hex("asdf"), $csum->hexdigest);
is("MD5", $csum->hashname);

my $zero = "MD5:d41d8cd98f00b204e9800998ecf8427e";
$csum = MogileFS::Checksum->from_string(6, $zero);
is("MogileFS::Checksum", ref($csum), "is a ref");
is("d41d8cd98f00b204e9800998ecf8427e", $csum->hexdigest, "hex matches");
is(1, $csum->save, "save successfully");
$hash = $sto->get_checksum(6);
my $reloaded = MogileFS::Checksum->new($hash);
is("d41d8cd98f00b204e9800998ecf8427e", $reloaded->hexdigest, "hex matches");
my $fid_checksum = MogileFS::FID->new(6)->checksum;
is_deeply($fid_checksum, $csum, "MogileFS::FID->checksum works");
