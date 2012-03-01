# -*-perl-*-
# tests for SQlite-specific features
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;
use File::Temp ();
use POSIX qw(:sys_wait_h);

my ($fh, $filename) = File::Temp::tempfile();
close($fh);
MogileFS::Config->set_config('db_dsn', "DBI:SQLite:$filename");
MogileFS::Config->set_config('db_user', '');
MogileFS::Config->set_config('db_pass', '');
MogileFS::Config->set_config('max_handles', 0xffffffff);

my ($r, $w, $pid, $buf);
my $sto = eval { MogileFS::Store->new };
if ($sto) {
    plan tests => 28;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

Mgd::set_store($sto);
is(ref($sto), "MogileFS::Store::SQLite", "store is sane");
is($sto->setup_database, 1, "setup database");

is(1, pipe($r, $w), "IPC pipe is ready");

# normal lock contention
$pid = fork;
fail("fork failed: $!") unless defined $pid;
if ($pid == 0) {
    $sto = Mgd::get_store(); # fork-safe
    $SIG{TERM} = sub {
        $sto->release_lock("test-lock") == 1 or die "released bad lock";
        exit 0;
    };
    $sto->get_lock("test-lock", 1) == 1 or die "child failed to get_lock";
    close($r);
    syswrite($w, ".") == 1 or die "child failed to wake parent";
    sleep 60;
    exit 0;
}
if ($pid > 0) {
    is(sysread($r, $buf, 1), 1, "child wakes us up");
    is($buf, ".", "child wakes parent up properly");
    ok(! $sto->get_lock("test-lock", 1), "fails to lock while child has lock");
    is(kill(TERM => $pid), 1, "kill successful");
    is(waitpid($pid, 0), $pid, "waitpid successful");
    is($?, 0, "child dies correctly");
    is($sto->get_lock("test-lock", 1), 1, "acquire lock when child dies");
}

# detects recursive lock
ok(! eval { $sto->get_lock("test-lock", 1); }, "recursion fails");
like($@, qr/Lock recursion detected/i, "proper error on failed lock");
is($sto->release_lock("test-lock"), 1, "lock release");

is($sto->get_lock("test-lock", 0), 1, "acquire lock with 0 timeout");
is($sto->release_lock("test-lock"), 1, "lock release");
is($sto->release_lock("test-lock") + 0, 0, "redundant lock release");

# waits for lock
$pid = fork;
fail("fork failed: $!") unless defined $pid;
if ($pid == 0) {
    $sto = Mgd::get_store(); # fork-safe
    $sto->get_lock("test-lock", 1) or die "child failed to get_lock";
    close($r);
    syswrite($w, ".") == 1 or die "child failed to wake parent";
    sleep 2;
    $sto->release_lock("test-lock") == 1 or die "child failed to release";
    exit 0;
}
if ($pid > 0) {
    is(sysread($r, $buf, 1), 1, "parent woken up");
    is($buf, ".", "child wakes parent up properly");
    ok($sto->get_lock("test-lock", 6), "acquire lock eventually");
    is(waitpid($pid, 0), $pid, "waitpid successful");
    is($?, 0, "child dies correctly");
    is($sto->release_lock("test-lock"), 1, "lock release");
}


# kill -9 a lock holder
$pid = fork;
fail("fork failed: $!") unless defined $pid;
if ($pid == 0) {
    $sto = Mgd::get_store(); # fork-safe
    $sto->get_lock("test-lock", 1) or die "child failed to get_lock";
    close($r);
    syswrite($w, ".") == 1 or die "child failed to wake parent";
    sleep 60;
    exit 0;
}
if ($pid > 0) {
    is(sysread($r, $buf, 1), 1, "parent woken up");
    is($buf, ".", "child wakes parent up properly");
    is(kill(KILL => $pid), 1, "kill -9 successful");
    is(waitpid($pid, 0), $pid, "waitpid successful");
    ok(WIFSIGNALED($?) && WTERMSIG($?) == 9, "child was SIGKILL-ed");
    ok($sto->get_lock("test-lock", 1), "acquire lock in parent");
}
