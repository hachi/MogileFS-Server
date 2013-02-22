package MogileFS::Connection::HTTP;
use strict;
use warnings;
use MogileFS::Connection::Poolable;
use HTTP::Response;
use base qw(MogileFS::Connection::Poolable);
use MogileFS::Util qw/debug/;

use fields (
    'read_size_hint',     # bytes to read for body
    'http_response',      # HTTP::Response object
    'http_req',           # HTTP request ("GET $URL")
    'http_res_cb',        # called on HTTP::Response (_after_ body is read)
    'http_res_body_read',# number of bytes read in the response body
    'http_res_content_cb' # filter for the response body (success-only)
);
use Net::HTTP::NB;

sub new {
    my ($self, $ip, $port) = @_;
    my %opts = ( Host => "$ip:$port", Blocking => 0, KeepAlive => 300 );
    my $sock = Net::HTTP::NB->new(%opts) or return;

    $self = fields::new($self) unless ref $self;
    $self->SUPER::new($sock, $ip, $port); # MogileFS::Connection::Poolable->new

    return $self;
}

# starts an HTTP request, returns immediately and relies on Danga::Socket
# to schedule the run the callback.
sub start {
    my ($self, $method, $path, $opts, $http_res_cb) = @_;
    $opts ||= {};

    my $err = delete $self->{mfs_err};
    return $self->err_response($err, $http_res_cb) if $err;

    $self->{http_res_cb} = $http_res_cb;
    $self->{http_res_content_cb} = $opts->{content_cb};
    $self->{read_size_hint} = $opts->{read_size_hint} || 4096;

    my $h = $opts->{headers} || {};
    $h->{'User-Agent'} = ref($self) . "/$MogileFS::Server::VERSION";
    my $content = $opts->{content};
    if (defined $content) {
        # Net::HTTP::NB->format_request will set Content-Length for us
        $h->{'Content-Type'} = 'application/octet-stream'
    } else {
        $content = "";
    }

    # support full URLs for LWP compatibility
    # some HTTP daemons don't support Absolute-URIs, so we only give
    # them the HTTP/1.0-compatible path
    if ($path =~ m{\Ahttps?://[^/]+(/.*)\z}) {
        $path = $1;
    }

    $self->set_timeout("node_timeout");

    # Force HTTP/1.0 to avoid potential chunked responses and force server
    # to set Content-Length: instead.  In practice, we'll never get chunked
    # responses anyways as all known DAV servers will set Content-Length
    # for static files...
    $self->sock->http_version($method eq "GET" ? "1.0" : "1.1");
    $h->{Connection} = "keep-alive";

    # format the request here since it sets the reader up to read
    my $req = $self->sock->format_request($method, $path, %$h, $content);
    $self->{http_req} = "$method http://" . $self->key . $path;

    # we'll start watching for writes here since it's unlikely the
    # 3-way handshake for new TCP connections is done at this point
    $self->write($req);

    # start reading once we're done writing
    $self->write(sub {
        # we're connected after writing $req is successful, so
        # change the timeout and wait for readability
        $self->set_timeout("node_timeout");
        $self->watch_read(1);
    });
}

# called by Danga::Socket upon readability
sub event_read {
    my ($self) = @_;

    my $content_cb = $self->{http_res_content_cb};
    my Net::HTTP::NB $sock = $self->sock;
    my $res = $self->{http_response};

    # read and cache HTTP response headers
    unless ($res) {
        my ($code, $mess, @headers) = eval { $sock->read_response_headers };

        # wait for readability on EAGAIN
        unless (defined $code) {
            my $err = $@;
            if ($err) {
                $err =~ s/ at .*\z//s; # do not expose source file location
                $err =~ s/\r?\n/\\n/g; # just in case
                return $self->err("read_response_headers: $err");
            }

            # assume EAGAIN, though $! gets clobbered by Net::HTTP::*
            return;
        }

        # hold onto response object until the response body is processed
        $res = HTTP::Response->new($code, $mess, \@headers, "");
        $res->protocol("HTTP/" . $sock->peer_http_version);
        $self->{http_response} = $res;
        $self->{http_res_body_read} = $content_cb ? 0 : undef;
    }

    my $body_read = sub {
        $content_cb ? $self->{http_res_body_read} : length($res->content);
    };

    # continue reading the response body if we have a header
    my $rsize = $self->{read_size_hint};
    my $buf;

    my $clen = $res->header("Content-Length");
    while (1) {
        my $n = $sock->read_entity_body($buf, $rsize);
        if (!defined $n) {
            if ($!{EAGAIN}) {
                # workaround a bug in Net::HTTP::NB
                # ref: https://rt.cpan.org/Ticket/Display.html?id=78233
                if (defined($clen) && $clen == $body_read->()) {
                    return $self->_http_done;
                }

                # reset the timeout if we got any body bytes
                $self->set_timeout("node_timeout");
                return;
            }
            next if $!{EINTR};
            return $self->err("read_entity_body: $!");
        }
        if ($n == 0) {
            # EOF, call the response header callback
            return $self->_http_done;
        }
        if ($n > 0) {
            if ($content_cb && $res->is_success) {
                $self->{http_res_body_read} += length($buf);

                # filter the buffer through content_cb, no buffering.
                # This will be used by tracker-side checksumming
                # replicate does NOT use this code path for performance
                # reasons (tracker-side checksumming is already a slow path,
                # so there's little point in optimizing).
                # $buf may be empty on EOF (n == 0)
                $content_cb->($buf, $self, $res);

                if (defined($clen) && $clen == $body_read->()) {
                    return $self->_http_done;
                }
            } else {
                # append to existing buffer, this is only used for
                # PUT/DELETE/HEAD and small GET responses (monitor)
                $res->content($res->content . $buf);
            }
            # continue looping until EAGAIN or EOF (n == 0)
        }
    }
}

# this does cleanup as an extra paranoid step to prevent circular refs
sub close {
    my ($self, $close_reason) = @_;

    delete $self->{http_res_cb};
    delete $self->{http_res_content_cb};

    $self->SUPER::close($close_reason); # MogileFS::Connection::Poolable->close
}

# This is only called on a socket-level error (e.g. disconnect, timeout)
# bad server responses (500, 403) do not trigger this
sub err {
    my ($self, $reason) = @_;

    # Fake an HTTP response like LWP does on errors.
    # delete prevents http_res_cb from being invoked twice, as event_read
    # will delete http_res_cb on success, too
    my $http_res_cb = delete $self->{http_res_cb};

    # don't retry if we already got a response header nor if we got a timeout
    if ($self->retryable($reason) && $http_res_cb && !$self->{http_response}) {
        # do not call inflight_expire here, since we need inflight_cb
        # for retrying

        $self->close(":retry"); # trigger a retry in MogileFS::ConnectionPool
    } else {
        # ensure we don't call new_err on close()
        $self->inflight_expire;

        # free the FD before invoking the callback
        $self->close($reason);
        $self->err_response($reason, $http_res_cb) if $http_res_cb;
    }
}

# Fakes an HTTP response like LWP does on errors.
sub err_response {
    my ($self, $err, $http_res_cb) = @_;

    my $res = HTTP::Response->new(500, $err);
    $err ||= "(unspecified error)";
    my $req = $self->{http_req} || "no HTTP request made";
    Mgd::error("$err: $req");
    $res->header("X-MFS-Error", $err);
    $res->protocol("HTTP/1.0");
    $http_res_cb->($res);
}

# returns true if the HTTP connection is persistent/reusable, false if not.
sub _http_persistent {
    my ($self, $res) = @_;

    # determine if this connection is reusable:
    my $connection = $res->header("Connection");
    my $persist;

    # Connection: header takes precedence over protocol version
    if ($connection) {
        if ($connection =~ /\bkeep-alive\b/i) {
            $persist = 1;
        } elsif ($connection =~ /\bclose\b/i) {
            $persist = 0;
        }

        # if we can't make sense of the Connection: header, fall through
        # and decided based on protocol version
    }

    # HTTP/1.1 is persistent-by-default, HTTP/1.0 is not.
    # Will there be HTTP/1.2?
    $persist = $res->protocol eq "HTTP/1.1" unless defined $persist;

    # we're not persistent if the pool is full, either
    return ($persist && $self->persist);
}

# Called on successfully read HTTP response (it could be a server-side
# error (404,403,500...), but not a socket error between client<->server).
sub _http_done {
    my ($self) = @_;

    # delete ensures we only fire the callback once
    my $http_res_cb = delete $self->{http_res_cb};
    my $res = delete $self->{http_response};
    delete $self->{http_req};

    # ensure we don't call new_err on eventual close()
    $self->inflight_expire;

    # free up the FD if possible
    $self->close('http_close') unless $self->_http_persistent($res);

    # finally, invoke the user-supplied callback
    $http_res_cb->($res);
}

1;
