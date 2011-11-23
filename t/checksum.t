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
    plan tests => 2;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

$sto->set_checksum(6, 1, md5("asdf"));
my $hash = $sto->get_checksum(6);
my $csum = MogileFS::Checksum->new($hash);
is(md5_hex("asdf"), $csum->hexdigest);
is("md5", $csum->checksumname);
