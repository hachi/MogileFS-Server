# a connection pool class with queueing.
# (something doesn't sound quite right with that...)
# This requires Danga::Socket to drive, but may also function without it
# via conn_get/conn_put.
package MogileFS::ConnectionPool;
use strict;
use warnings;
use Carp qw(croak confess);
use Time::HiRes;

use constant NEVER => (0xffffffff << 32) | 0xffffffff; # portable version :P

sub new {
    my ($class, $conn_class, $opts) = @_;

    $opts ||= {};
    my $self = bless {
        fdmap => {},    # { fd -> conn }
        idle => {},     # ip:port -> [ MogileFS::Connection::Poolable, ... ]
        queue => [],    # [ [ ip, port, callback ], ... ]
        timer => undef, # Danga::Socket::Timer object
        timeouts => {}, # { fd -> conn }
        inflight => {}, # ip:port -> { fd -> callback }
        total_inflight => 0, # number of inflight connections
        dest_capacity => $opts->{dest_capacity},
        total_capacity => $opts->{total_capacity},
        class => $conn_class,
        scheduled => 0, # set if we'll start tasks on next tick
        on_next_tick => [],
        next_expiry => NEVER,
    }, $class;

    # total_capacity=20 matches what we used with LWP
    $self->{total_capacity} ||= 20;

    # allow users to specify per-destination capacity limits
    $self->{dest_capacity} ||= $self->{total_capacity};

    return $self;
}

# retrieves an idle connection for the [IP, port] pair
sub _conn_idle_get {
    my ($self, $ip, $port) = @_;

    my $key = "$ip:$port";
    my $idle = $self->{idle}->{$key} or return;

    # the Danga::Socket event loop may detect hangups and close sockets,
    # However not all MFS workers run this event loop, so we need to
    # validate the connection when retrieving a connection from the pool
    while (my $conn = pop @$idle) {
        # make sure the socket is valid:

        # due to response callback ordering, we actually place connections
        # in the pool before invoking the user-supplied response callback
        # (to allow connections to get reused ASAP)
        my $sock = $conn->sock or next;

        # hope this returns EAGAIN, not using OO->sysread here since
        # Net::HTTP::NB overrides that and we _want_ to hit EAGAIN here
        my $r = sysread($sock, my $byte, 1);

        # good, connection is possibly still alive if we got EAGAIN
        return $conn if (!defined $r && $!{EAGAIN});

        my $err = $!;
        if (defined $r) {
            if ($r == 0) {
                # a common case and to be expected
                $err = "server dropped idle connection";
            } else {
                # this is a bug either on our side or the HTTP server
                Mgd::error("Bug: unexpected got $r bytes from idle conn to ". $conn->host_port. ") (byte=$byte)");
            }
        }

        # connection is bad, close the socket and move onto the
        # next idle connection if there is one.
        $conn->close($err);
    }

    return;
}

# creates a new connection if under capacity
# returns undef if we're at capacity (or on EMFILE/ENFILE)
sub _conn_new_maybe {
    my ($self, $ip, $port) = @_;
    my $key = "$ip:$port";

    # we only call this sub if we don't have idle connections, so
    # we don't check {idle} here

    # make sure we're not already at capacity for this destination
    my $nr_inflight = scalar keys %{$self->{inflight}->{$key} ||= {}};
    return if ($nr_inflight >= $self->{dest_capacity});

    # see how we're doing with regard to total capacity:
    if ($self->_total_connections >= $self->{total_capacity}) {
        # see if we have idle connections for other pools to kill
        if ($self->{total_inflight} < $self->{total_capacity}) {
            # we have idle connections to other destinations, drop one of those
            $self->_conn_drop_idle;
            # fall through to creating a new connection
        } else {
            # we're at total capacity for the entire pool
            return;
        }
    }

    # we're hopefully under capacity if we got here, create a new connection
    $self->_conn_new($ip, $port);
}

# creates new connection and registers it in our fdmap
# returns error string if resources (FDs, buffers) aren't available
sub _conn_new {
    my ($self, $ip, $port) = @_;

    # calls MogileFS::Connection::{HTTP,Mogstored}->new:
    my $conn = $self->{class}->new($ip, $port);
    if ($conn) {
        # register the connection
        $self->{fdmap}->{$conn->fd} = $conn;
        $conn->set_pool($self);

        return $conn;
    } else {
        # EMFILE/ENFILE should never happen as the capacity for this
        # pool is far under the system defaults.  Just give up on
        # EMFILE/ENFILE like any other error.
        return "failed to create socket to $ip:$port ($!)";
    }
}

# retrieves a connection, may return undef if at capacity
sub _conn_get {
    my ($self, $ip, $port) = @_;

    # if we have idle connections, always use them first
    $self->_conn_idle_get($ip, $port) || $self->_conn_new_maybe($ip, $port);
}

# Pulls a connection out of the pool for synchronous use.
# This may create a new connection (independent of pool limits).
# The connection returned by this is _blocking_.  This is currently
# only used by replicate.
sub conn_get {
    my ($self, $ip, $port) = @_;
    my $conn = $self->_conn_idle_get($ip, $port);

    if ($conn) {
        # in case the connection never comes back, let refcounting close() it:
        delete $self->{fdmap}->{$conn->fd};
    } else {
        $conn = $self->_conn_new($ip, $port);
        unless (ref $conn) {
            $! = $conn; # $conn is an error message :<
            return;
        }
        delete $self->{fdmap}->{$conn->fd};
        my $timeout = MogileFS->config("node_timeout");
        MogileFS::Util::wait_for_writeability($conn->fd, $timeout) or return;
    }

    return $conn;
}

# retrieves a connection from the connection pool and executes
# inflight_cb on it.  If the pool is at capacity, this will queue the task.
# This relies on Danga::Socket->EventLoop
sub start {
    my ($self, $ip, $port, $inflight_cb) = @_;

    my $conn = $self->_conn_get($ip, $port);
    if ($conn) {
        $self->_conn_run($conn, $inflight_cb);
    } else { # we're too busy right now, queue up
        $self->enqueue($ip, $port, $inflight_cb);
    }
}

# returns the total number of connections we have
sub _total_connections {
    my ($self) = @_;
    return scalar keys %{$self->{fdmap}};
}

# marks a connection as no longer inflight, returns the inflight
# callback if the connection was active, undef if not
sub inflight_cb_expire {
    my ($self, $conn) = @_;
    my $inflight_cb = delete $self->{inflight}->{$conn->key}->{$conn->fd};
    $self->{total_inflight}-- if $inflight_cb;

    return $inflight_cb;
}

# schedules the event loop to dequeue and run a task on the next
# tick of the Danga::Socket event loop.  Call this
# 1) whenever a task is enqueued
# 2) whenever a task is complete
sub schedule_queued {
    my ($self) = @_;

    # AddTimer(0) to avoid potential stack overflow
    $self->{scheduled} ||= Danga::Socket->AddTimer(0, sub {
        $self->{scheduled} = undef;
        my $queue = $self->{queue};

        my $total_capacity = $self->{total_capacity};
        my $i = 0;

        while ($self->{total_inflight} < $total_capacity
               && $i <= (scalar(@$queue) - 1)) {
            my ($ip, $port, $cb) = @{$queue->[$i]};

            my $conn = $self->_conn_get($ip, $port);
            if ($conn) {
                splice(@$queue, $i, 1); # remove from queue
                $self->_conn_run($conn, $cb);
            } else {
                # this queue object cannot be dequeued, skip it for now
                $i++;
            }
        }
    });
}

# Call this when done using an (inflight) connection
# This possibly places a connection in the connection pool.
# This will close the connection of the pool is already at capacity.
# This will also start the next queued callback, or retry if needed
sub conn_persist {
    my ($self, $conn) = @_;

    # schedule the next request if we're done with any connection
    $self->schedule_queued;
    $self->conn_put($conn);
}

# The opposite of conn_get, this returns a connection retrieved with conn_get
# back to the connection pool, making it available for future use.  Dead
# connections are not stored.
# This is currently only used by replicate.
sub conn_put {
    my ($self, $conn) = @_;

    my $key = $conn->key;
    # we do not store dead connections
    my $peer_addr = $conn->peer_addr_string;

    if ($peer_addr) {
        # connection is still alive, respect capacity limits
        my $idle = $self->{idle}->{$key} ||= [];

        # register it in the fdmap just in case:
        $self->{fdmap}->{$conn->fd} = $conn;

        if ($self->_dest_total($conn) < $self->{dest_capacity}) {
            $conn->mark_idle;
            push @$idle, $conn; # yay, connection is reusable
            $conn->set_timeout(undef); # clear timeout
            return 1; # success
        }
    }

    # we have too many connections or the socket is dead, caller
    # should close after returning from this function.
    return 0;
}

# enqueues a request (inflight_cb) and schedules it to run ASAP
# This must be used with Danga::Socket->EventLoop
sub enqueue {
    my ($self, $ip, $port, $inflight_cb) = @_;

    push @{$self->{queue}}, [ $ip, $port, $inflight_cb ];

    # we have something in the queue,  make sure it's run soon
    $self->schedule_queued;
}

# returns the total connections to the host of a given connection
sub _dest_total {
    my ($self, $conn) = @_;
    my $key = $conn->key;
    my $inflight = scalar keys %{$self->{inflight}->{$key}};
    my $idle = scalar @{$self->{idle}->{$key}};
    return $idle + $inflight;
}

# only call this from the event_hup/event_err callbacks used by Danga::Socket
sub conn_drop {
    my ($self, $conn, $close_reason) = @_;

    my $fd = $conn->fd;
    my $key = $conn->key;

    # event_read must handle errors anyways, so hand off
    # error handling to the event_read callback if inflight.
    return $conn->event_read if $self->{inflight}->{$key}->{$fd};

    # we get here if and only if the socket is idle, we can drop it ourselves
    # splice out the socket we're closing from the idle pool
    my $idle = $self->{idle}->{$key};
    foreach my $i (0 .. (scalar(@$idle) - 1)) {
        my $old = $idle->[$i];
        if ($old->sock) {
            if ($old->fd == $fd) {
                splice(@$idle, $i, 1);
                $conn->close($close_reason);
                return;
            }
        } else {
            # some connections may have expired but not been spliced out, yet
            # splice it out here since we're iterating anyways
            splice(@$idle, $i, 1);
        }
    }
}

# unregisters and prepares connection to be closed
# Returns the inflight callback if there was one
sub conn_close_prepare {
    my ($self, $conn, $close_reason) = @_;

    if ($conn->sock) {
        my $fd = $conn->fd;

        my $valid = delete $self->{fdmap}->{$fd};
        delete $self->{timeouts}->{$fd};

        my $inflight_cb = $self->inflight_cb_expire($conn);

        # $valid may be undef in replicate worker which removes connections
        # from fdmap.  However, valid==undef connections should never have
        # an inflight_cb
        if ($inflight_cb && !$valid) {
            croak("BUG: dropping unregistered conn with callback: $conn");
        }
        return $inflight_cb;
    }
}

# schedules cb to run on the next tick of the event loop,
# (immediately after this tick runs)
sub on_next_tick {
    my ($self, $cb) = @_;
    my $on_next_tick = $self->{on_next_tick};
    push @$on_next_tick, $cb;

    if (scalar(@$on_next_tick) == 1) {
        Danga::Socket->AddTimer(0, sub {
            # prevent scheduled callbacks from being called on _this_ tick
            $on_next_tick = $self->{on_next_tick};
            $self->{on_next_tick} = [];

            while (my $sub = shift @$on_next_tick) {
                $sub->()
            }
        });
    }
}

# marks a connection inflight and invokes cb on it
# $conn may be a error string, in which case we'll invoke the user-supplied
# callback with a mock error (this mimics how LWP fakes an HTTP response
# even if the socket could not be created/connected)
sub _conn_run {
    my ($self, $conn, $cb) = @_;

    if (ref $conn) {
        my $inflight = $self->{inflight}->{$conn->key} ||= {};
        $inflight->{$conn->fd} = $cb; # stash callback for retrying
        $self->{total_inflight}++;
        $cb->($conn);
    } else {
        # fake an error message on the response callback
        $self->on_next_tick(sub {
            # fatal error creating the socket, do not queue
            my $mfs_err = $conn;
            $self->{class}->new_err($mfs_err, $cb);

            # onto the next request
            $self->schedule_queued;
        });
    }
}

# drops an idle connection from the idle connection pool (so we can open
# another socket without incurring out-of-FD errors)
# Only call when you're certain there's a connection to drop
# XXX This is O(destinations), unfortunately
sub _conn_drop_idle {
    my ($self) = @_;
    my $idle = $self->{idle};

    # using "each" on the hash since it preserves the internal iterator
    # of the hash across invocations of this sub.  This should preserve
    # the balance of idle connections in a big pool with many hosts.
    # Thus we loop twice to ensure we scan the entire idle connection
    # pool if needed
    foreach (1..2) {
        while (my (undef, $val) = each %$idle) {
            my $conn = shift @$val or next;

            $conn->close("idle_expire") if $conn->sock;
            return;
        }
    }

    confess("BUG: unable to drop an idle connection");
}

# checks for expired connections, this can be expensive if there
# are many concurrent connections waiting on timeouts, but still
# better than having AddTimer create a Danga::Socket::Timer object
# every time a timeout is reset.
sub check_timeouts {
    my ($self) = @_;
    my $timeouts = $self->{timeouts};
    my @fds = keys %$timeouts;
    my $next_expiry = NEVER;
    my $now = Time::HiRes::time();

    # this is O(n) where n is concurrent connections
    foreach my $fd (@fds) {
        my $conn = $timeouts->{$fd};
        if ($conn->expired($now)) {
            delete $timeouts->{$fd};
        } else {
            # look for the next timeout
            my $expiry = $conn->expiry;
            if ($expiry) {
                $next_expiry = $expiry if $expiry < $next_expiry;
            } else {
                # just in case, this may not happen...
                delete $timeouts->{$fd};
            }
        }
    }

    # schedule the wakeup for the next timeout
    if ($next_expiry == NEVER) {
        $self->{timer} = undef;
    } else {
        my $timeout = $next_expiry - $now;
        $timeout = 0 if $timeout <= 0;
        $self->{timer} = Danga::Socket->AddTimer($timeout, sub {
            $self->check_timeouts;
        });
    }
    $self->{next_expiry} = $next_expiry;
}

# registers a timeout for a given connection, each connection may only
# have one pending timeout.  Timeout may be undef to cancel the current
# timeout.
sub register_timeout {
    my ($self, $conn, $timeout) = @_;

    if ($conn->sock) {
        my $fd = $conn->fd;
        if ($timeout) {
            $self->{timeouts}->{$fd} = $conn;
            my $next_expiry = $self->{next_expiry};
            my $old_timer = $self->{timer};
            my $expiry = $timeout + Time::HiRes::time();

            if (!$old_timer || $expiry < $next_expiry) {
                $self->{next_expiry} = $expiry;
                $self->{timer} = Danga::Socket->AddTimer($timeout, sub {
                    $self->check_timeouts;
                });
                $old_timer->cancel if $old_timer;
            }
        } else {
            delete $self->{timeouts}->{$fd};
        }
    } elsif ($timeout) { # this may never happen...
        # no FD, so we must allocate a new Danga::Socket::Timer object
        # add 1msec to avoid FP rounding problems leading to missed
        # expiration when calling conn->expired
        Danga::Socket->AddTimer($timeout + 0.001, sub { $conn->expired });
    }
}

1;
