# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 21;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $dom = MogileFS::Domain->create("foo");
ok($dom, "created a domain");
my $cls = $dom->create_class("classA");
ok($cls, "created a class");

my $df = MogileFS::DevFID->new(100, 200);
ok($df, "made devfid");
ok($df->add_to_db, "added to db");

my $fid = $df->fid;
ok($fid, "got fid from df");
my @on = $fid->devids;
is(scalar @on, 1, "FID 200 on one device");
is($on[0], 100, "is correct number");

ok($sto->mass_insert_file_on(MogileFS::DevFID->new(1, 101),
                             MogileFS::DevFID->new(2, 101)), "did mass insert");
$fid = MogileFS::FID->new(101);
@on = $fid->devids;
is(scalar @on, 2, "FID 101 on 2 devices");

# create a tempfile
{
    my $fidid = $sto->register_tempfile(
                                        fid     => undef,
                                        dmid    => $dom->id,
                                        key     => "my_tempfile",
                                        classid => $cls->classid,
                                        devids  => join(',', 1,2,3),
                                        );
    ok($fidid, "got a fidid");

    my $fidid2 = eval {
        $sto->register_tempfile(
                                fid     => $fidid,
                                dmid    => $dom->id,
                                key     => "my_tempfile",
                                classid => $cls->classid,
                                devids  => join(',', 1,2,3),
                                );
    };
    my $errc = error_code($@);
    ok(!$fidid2, "didn't get fidid");
    is($errc, "dup", "got a dup into tempfile")
        or die "Got error: $@\n";
}

my $ignore_replace_match = {
    base     => { pattern => undef, dies => 1 },
    MySQL    => { pattern => qr/INSERT IGNORE/, dies => 0 },
    SQLite   => { pattern => qr/REPLACE/, dies => 0 },
    Postgres => { pattern => undef, dies => 1 },
};

my $prx = eval { $sto->ignore_replace } || '';
my $sto_driver = ( split( /::/, ref($sto) ) )[2] || 'base';
my $match_spec = $ignore_replace_match->{ $sto_driver }
    or die "Test not configured for '$sto_driver' storage driver";


ok(
    ref( $match_spec->{pattern} ) eq 'Regexp'?
        ( $prx =~ $match_spec->{pattern} ) :
        ( !$prx ),
    sprintf(
        "ignore_replace %s return value for storage type '%s'",
        ref( $match_spec->{pattern} ) eq 'Regexp'?
            'should' : 'should not',
        $sto_driver
    )
) or diag "Got value: $prx";

ok(
    $match_spec->{dies}? $@ : !$@,
    sprintf(
        "ignore_replace %s die for storage type '%s'",
        $match_spec->{dies}? 'should' : 'should not',
        $sto_driver
    )
) or diag "Got exception: $@";

my $rv;

# test retry_on_deadlock using good sql
$rv = eval {
    $sto->retry_on_deadlock( sub { $sto->dbh->do("SELECT 1;"); } );
};
ok (
    $rv eq '1' || $rv eq '0E0',
    "retry_on_deadlock return value for '$sto_driver': $rv"
) or diag "Got return value: $rv";

# test retry_on_deadlock using bad sql
$rv = eval {
    $sto->retry_on_deadlock( sub { $sto->dbh->do("BADSQL;"); } );
};
ok (
    $@ =~ /BADSQL/,
    "retry_on_deadlock got an exception on bad sql '$sto_driver'"
) or diag "Got exception value: $@";

# test retry_on_deadlock using a custom exception
$rv = eval {
    $sto->retry_on_deadlock( sub { die "preempt"; } );
};
ok (
    $@ =~ /preempt/,
    "retry_on_deadlock got a non-sql exception for '$sto_driver'"
) or diag $@;

sub _do_induce_deadlock {
    my @args = @_;
    return eval {
        no strict 'refs';
        no warnings 'redefine';
        my $c = 0;
        local *{ "MogileFS\::Store\::$sto_driver\::was_deadlock_error" } = sub {
            return $c++ < 2; # unlock on third try
        };
        $sto->retry_on_deadlock( @args );
    };
}

# attempt to induce a deadlock and check iterations
my $_v = 0;
$rv = _do_induce_deadlock( sub { return $_v++; } );

ok(
   !$@,
   "no exception on retry_on_deadlock while inducing a deadlock"
) or diag $@;

ok(
    $rv == 2,
    'retry_on_deadlock returned good iteration count while inducing a deadlock'
) or diag $rv;

# induce a deadlock using badsql... should return an exemption
$rv = _do_induce_deadlock( sub { $sto->dbh->do("BADSQL;"); } );
ok (
    !$rv && $@ =~ /BADSQL/,
    "retry_on_deadlock got expected exemption inducing a deadlock with bad sql"
) or diag "Got value '$rv' with exemption: $@";

# induce a deadlock with good sql check sql return and iterations
$_v = 0;
$rv = _do_induce_deadlock(
    sub {
        return [ $sto->dbh->do("SELECT 1;"), $_v++ ];
    }
);
ok (
    ( !$@ && ref($rv) eq 'ARRAY' ) && (
        ( $rv->[0] eq '1' || $rv->[0] eq '0E0' ) &&
        $rv->[1] == 2
    ),
    "retry_on_deadlock got proper return value and iteration while inducing a deadlock"
);







