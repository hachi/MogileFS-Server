# this test reaches inside MogileFS::Host and MogileFS::Connection::HTTP
# internals to ensure error handling and odd corner cases are handled
# (existing tests may not exercise this in monitor)
use strict;
use warnings;
use Test::More;
use MogileFS::Server;
use MogileFS::Test;
use MogileFS::Util qw/wait_for_readability/;
use Danga::Socket;
use IO::Socket::INET;
use Socket qw(TCP_NODELAY);

# bind a random TCP port for testing
my %lopts = (
    LocalAddr => "127.0.0.1",
    LocalPort => 0,
    Proto => "tcp",
    ReuseAddr => 1,
    Listen => 1024
);
my $http = IO::Socket::INET->new(%lopts);
$http->sockopt(TCP_NODELAY, 1);
my $http_get = IO::Socket::INET->new(%lopts);
$http_get->sockopt(TCP_NODELAY, 1);

my $host_args = {
    hostid => 1,
    hostname => 'mockhost',
    hostip => $http->sockhost,
    http_port => $http->sockport,
    http_get_port => $http_get->sockport,
};
my $host = MogileFS::Host->new_from_args($host_args);

# required, defaults to 20 in normal server
MogileFS::Config->set_config("conn_pool_size", 13);

MogileFS::Host->_init_pools;

my $idle_pool = $MogileFS::Host::http_pool->{idle};
is("MogileFS::Host", ref($host), "host created");
MogileFS::Config->set_config("node_timeout", 1);

is(13, $MogileFS::Host::http_pool->{total_capacity}, "conn_pool_size took effect");

# hit the http_get_port
{
    my $resp;
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $host->http_get("GET", "/read-only", undef, sub { $resp = $_[0] });

    server_do(sub {
        my $s = $http_get->accept;
        my $buf = read_one_request($s);
        if ($buf =~ m{\AGET /read-only HTTP/1\.0\r\n})  {
            $s->syswrite("HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n");
        }
        sleep 6; # wait for SIGKILL
    },
    sub {
        Danga::Socket->EventLoop;
        ok($resp->is_success, "HTTP response is success");
        is(200, $resp->code, "got HTTP 200 response");
        my $pool = $idle_pool->{"$host->{hostip}:$host->{http_get_port}"};
        is(1, scalar @$pool, "connection placed in GET pool");
    });

    has_nothing_inflight();
    has_nothing_queued();
}

# simulate a trickled response from server
{
    my $resp;
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $host->http("GET", "/trickle", undef, sub { $resp = $_[0] });
    server_do(sub {
        my $s = $http->accept;
        my $buf = read_one_request($s);
        my $r = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
        if ($buf =~ /trickle/) {
            foreach my $x (split(//, $r)) {
                $s->syswrite($x);
                sleep 0.01;
            }
            foreach my $i (1..100) {
                $s->syswrite($i % 10);
                sleep 0.1;
            }
        }
        sleep 6;
    },
    sub {
        Danga::Socket->EventLoop;
        ok($resp->is_success, "HTTP response is successful");
        my $expect = "";
        foreach my $i (1..100) {
            $expect .= $i % 10;
        }
        is($expect, $resp->content, "response matches expected");
    });

    has_nothing_inflight();
    has_nothing_queued();
}

# simulate a differently trickled response from server
{
    my $resp;
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    my $body = "*" x 100;
    $host->http("GET", "/trickle-head-body", undef, sub { $resp = $_[0] });
    server_do(sub {
        my $s = $http->accept;
        my $buf = read_one_request($s);
        my $r = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
        if ($buf =~ /trickle-head-body/) {
            $s->syswrite($r);
            sleep 1;
            $s->syswrite($body);
        }
        sleep 6;
    },
    sub {
        Danga::Socket->EventLoop;
        ok($resp->is_success, "HTTP response is successful on trickle");
        is($resp->content, $body, "trickled response matches expected");
    });

    has_nothing_inflight();
    has_nothing_queued();
}

# simulate a server that disconnected after a (very short) idle time
# despite supporting persistent conns
{
    my $resp;
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $host->http("GET", "/foo", undef, sub { $resp = $_[0] });
    my $conn;

    server_do(sub {
        my $s = $http->accept;
        my $buf = read_one_request($s);
        if ($buf =~ m{\AGET /foo HTTP/1\.0\r\n})  {
            $s->syswrite("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        }
        sleep 6; # wait for SIGKILL
    },
    sub {
        Danga::Socket->EventLoop;
        ok($resp->is_success, "HTTP response is success");
        my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
        is(1, scalar @$pool, "connection placed in pool");
        $conn = $pool->[0];
    });

    # try again, server didn't actually keep the connection alive,
    $resp = undef;
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $host->http("GET", "/again", undef, sub { $resp = $_[0] });

    server_do(sub {
        my $s = $http->accept;
        my $buf = read_one_request($s);
        if ($buf =~ m{\AGET /again HTTP/1\.0\r\n})  {
            $s->syswrite("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        }
        sleep 6; # wait for SIGKILL
    },
    sub {
        Danga::Socket->EventLoop;
        my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
        is(1, scalar @$pool, "new connection placed in pool");
        isnt($conn, $pool->[0], "reference not reused");
    });

    has_nothing_inflight();
    has_nothing_queued();
}

# simulate persistent connection reuse
{
    my $resp;
    my $nr = 6;
    my $conn;

    my $failsafe = Danga::Socket->AddTimer(5, sub { $resp = "FAIL TIMEOUT" });
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });

    server_do(sub {
        my $s = $http->accept;
        my $buf;
        foreach my $i (1..$nr) {
            $buf = read_one_request($s);
            if ($buf =~ m{\AGET /$i HTTP/1\.0\r\n}) {
                $s->syswrite("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n$i");
            }
        }
        sleep 6; # wait for SIGKILL
    },
    sub {
        foreach my $i (1..$nr) {
            $resp = undef;
            $host->http("GET", "/$i", undef, sub { $resp = $_[0] });
            Danga::Socket->EventLoop;
            is(ref($resp), "HTTP::Response", "got HTTP response");
            ok($resp->is_success, "HTTP response is successful");
            is($i, $resp->content, "response matched");
            my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
            is(1, scalar @$pool, "connection placed in connection pool");

            if ($i == 1) {
                $conn = $pool->[0];
                is("MogileFS::Connection::HTTP", ref($conn), "got connection");
            } else {
                ok($conn == $pool->[0], "existing connection reused (#$i)");
            }
        }
    });
    $failsafe->cancel;

    has_nothing_inflight();
    has_nothing_queued();
}

# simulate a node_timeout
sub sim_node_timeout {
    my ($send_header) = @_;
    my $resp;

    # we need this timer (just to exist) to break out of the event loop
    my $t = Danga::Socket->AddTimer(1.2, sub { fail("timer should not fire") });

    my $req = "/node-time-me-out-";
    $req .= $send_header ? 1 : 0;
    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $host->http("GET", $req, undef, sub { $resp = $_[0] });

    server_do(sub {
        my $s = $http->accept;
        my $buf = read_one_request($s);
        if ($buf =~ /node-time-me-out/) {
            if ($send_header) {
                $s->syswrite("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n");
            }
            sleep 60; # wait to trigger timeout
        } else {
            # nuke the connection to _NOT_ trigger timeout
            $s->syswrite("HTTP/1.1 404 Not Found\r\n\r\n");
            close($s);
        }
    },
    sub {
        Danga::Socket->EventLoop;
        $t->cancel;
        ok(! $resp->is_success, "HTTP response is not successful");
        like($resp->message, qr/node_timeout/, "node_timeout hit");
        my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
        is(0, scalar @$pool, "connection pool is empty");
    });

    has_nothing_inflight();
    has_nothing_queued();
}

sim_node_timeout(0);
sim_node_timeout(1);

# server just drops connection
{
    my $resp;

    # we want an empty pool to avoid retries
    my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
    is(0, scalar @$pool, "connection pool is empty");

    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $host->http("GET", "/drop-me", undef, sub { $resp = $_[0] });

    server_do(sub {
        my $s = $http->accept;
        my $buf = read_one_request($s);
        close $s if ($buf =~ /drop-me/);
        sleep 6;
    },
    sub {
        Danga::Socket->EventLoop;
        ok(! $resp->is_success, "HTTP response is not successful");
        my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
        is(0, scalar @$pool, "connection pool is empty");
    });

    has_nothing_inflight();
    has_nothing_queued();
}

# server is not running
{
    my $resp;

    # we want an empty pool to avoid retries
    my $pool = $idle_pool->{"$host->{hostip}:$host->{http_port}"};
    is(0, scalar @$pool, "connection pool is empty");

    Danga::Socket->SetPostLoopCallback(sub { ! defined($resp) });
    $http->close; # $http is unusable after this
    $host->http("GET", "/fail", undef, sub { $resp = $_[0] });
    Danga::Socket->EventLoop;
    ok(! $resp->is_success, "HTTP response is not successful");
    ok($resp->header("X-MFS-Error"), "X-MFS-Error is set");
    is(0, scalar @$pool, "connection pool is empty");

    has_nothing_inflight();
    has_nothing_queued();
}

done_testing();

sub has_nothing_inflight {
    my $inflight = $MogileFS::Host::http_pool->{inflight};
    my $n = 0;
    foreach my $host_port (keys %$inflight) {
        $n += scalar keys %{$inflight->{$host_port}};
    }
    is($MogileFS::Host::http_pool->{total_inflight}, 0, "nothing is counted to be inflight");
    is($n, 0, "nothing is really inflight");
}

sub has_nothing_queued {
    is(scalar @{$MogileFS::Host::http_pool->{queue}}, 0, "connection pool task queue is empty");
}

sub server_do {
    my ($child, $parent) = @_;
    my $pid = fork;
    fail("fork failed: $!") unless defined($pid);

    if ($pid == 0) {
        $child->();
    } else {
        $parent->();
        is(1, kill(9, $pid), "child killed");
        is($pid, waitpid($pid, 0), "child reaped");
    }
}

sub read_one_request {
    my $s = shift;

    my $fd = fileno($s);
    wait_for_readability($fd, 5);
    my $buf = "";
    do {
        $s->sysread($buf, 4096, length($buf));
    } while wait_for_readability($fd, 0.1) && $buf !~ /\r\n\r\n/;
    return $buf;
}
