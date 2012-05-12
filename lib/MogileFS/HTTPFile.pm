package MogileFS::HTTPFile;
use strict;
use warnings;
use Carp qw(croak);
use Socket qw(PF_INET IPPROTO_TCP SOCK_STREAM);
use Digest;
use MogileFS::Server;
use MogileFS::Util qw(error undeferr wait_for_readability wait_for_writeability);

# (caching the connection used for HEAD requests)
my $user_agent;

my %size_check_retry_after; # host => $hirestime.
my %size_check_failcount;   # host => $count.

# create a new MogileFS::HTTPFile instance from a URL.  not called
# "new" because I don't want to imply that it's creating anything.
sub at {
    my ($class, $url) = @_;
    my $self = bless {}, $class;

    unless ($url =~ m!^http://([^:/]+)(?::(\d+))?(/.+)$!) {
        croak "Bogus URL.\n";
    }

    $self->{url}  = $url;
    $self->{host} = $1;
    $self->{port} = $2;
    $self->{uri}  = $3;
    return $self;
}

sub device_id {
    my $self = shift;
    return $self->{devid} if $self->{devid};
    $self->{url} =~ /\bdev(\d+)\b/
        or die "Can't find device from URL: $self->{url}\n";
    return $self->{devid} = $1;
}

sub host_id {
    my $self = shift;
    return $self->device->hostid;
}

# return MogileFS::Device object
sub device {
    my $self = shift;
    return Mgd::device_factory()->get_by_id($self->device_id);
}

# return MogileFS::Host object
sub host {
    my $self = shift;
    return $self->device->host;
}

# returns true on success, dies on failure
sub delete {
    my $self = shift;
    my %opts = @_;
    my ($host, $port) = ($self->{host}, $self->{port});

    my $httpsock = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Timeout => 2)
        or die "can't connect to $host:$port in 2 seconds";

    $httpsock->write("DELETE $self->{uri} HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");

    my $keep_alive = 0;
    my $did_del    = 0;

    while (defined (my $line = <$httpsock>)) {
        $line =~ s/[\s\r\n]+$//;
        last unless length $line;
        if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            my $rescode = $1;
            # make sure we get a good response
            if ($rescode == 404 && $opts{ignore_missing}) {
                $did_del = 1;
                next;
            }
            unless ($rescode == 204) {
                die "Bad response from $host:$port: [$line]";
            }
            $did_del = 1;
            next;
        }
        die "Unexpected HTTP response line during DELETE from $host:$port: [$line]" unless $did_del;
    }
    die "Didn't get valid HTTP response during DELETE from $host:port" unless $did_del;

    return 1;
}

# returns size of file, (doing a HEAD request and looking at content-length)
# returns -1 on file missing (404),
# returns undef on connectivity error
use constant FILE_MISSING => -1;
sub size {
    my $self = shift;

    return $self->{_size} if defined $self->{_size};

    my ($host, $port, $uri, $path) = map { $self->{$_} } qw(host port uri url);

    return undef if (exists $size_check_retry_after{$host}
        && $size_check_retry_after{$host} > Time::HiRes::time());

    my $node_timeout = MogileFS->config("node_timeout");
    # Hardcoded connection cache size of 20 :(
    $user_agent ||= LWP::UserAgent->new(timeout => $node_timeout, keep_alive => 20);
    my $res = $user_agent->head($path);
    if ($res->is_success) {
        delete $size_check_failcount{$host} if exists $size_check_failcount{$host};
        my $size = $res->header('content-length');
        if (! defined $size &&
            $res->header('server') =~ m/^lighttpd/) {
            # lighttpd 1.4.x (main release) does not return content-length for
            # 0 byte files.
            $self->{_size} = 0;
            return 0;
        }
        $self->{_size} = $size;
        return $size;
    } else {
        if ($res->code == 404) {
            delete $size_check_failcount{$host} if exists $size_check_failcount{$host};
            return FILE_MISSING;
        }
        if ($res->message =~ m/connect:/) {
            my $count = $size_check_failcount{$host};
            $count ||= 1;
            $count *= 2 unless $count > 360;
            $size_check_retry_after{$host} = Time::HiRes::time() + $count;
            $size_check_failcount{$host}   = $count;
        }
        return undeferr("Failed HEAD check for $path (" . $res->code . "): "
            . $res->message); 
    }
}

sub digest_mgmt {
    my ($self, $alg, $ping_cb, $reason) = @_;
    my $mogconn = $self->host->mogstored_conn;
    my $node_timeout = MogileFS->config("node_timeout");
    my $sock;
    my $rv;
    my $expiry;

    $reason = defined($reason) ? " $reason" : "";
    my $uri = $self->{uri};
    my $req = "$alg $uri$reason\r\n";
    my $reqlen = length $req;

    # a dead/stale socket may not be detected until we try to recv on it
    # after sending a request
    my $retries = 2;

    # assuming the storage node can checksum at >=2MB/s, low expectations here
    my $response_timeout = $self->size / (2 * 1024 * 1024);

retry:
    $sock = $mogconn->sock($node_timeout) or return;
    $rv = send($sock, $req, 0);
    if ($! || $rv != $reqlen) {
        my $err = $!;
        $mogconn->mark_dead;
        if ($retries-- <= 0) {
            $req =~ tr/\r\n//d;
            $err = $err ? "send() error ($req): $err" :
                          "short send() ($req): $rv != $reqlen";
            $err = $mogconn->{ip} . ":" . $mogconn->{port} . " $err";
            return undeferr($err);
        }
        goto retry;
    }

    $expiry = Time::HiRes::time() + $response_timeout;
    while (!wait_for_readability(fileno($sock), 1.0) &&
           (Time::HiRes::time() < $expiry)) {
        $ping_cb->();
    }

    $rv = <$sock>;
    if (! $rv) {
        $mogconn->mark_dead;
        return undeferr("EOF from mogstored") if ($retries-- <= 0);
        goto retry;
    } elsif ($rv =~ /^\Q$uri\E \Q$alg\E=([a-f0-9]{32,128})\r\n/) {
        my $hexdigest = $1;

        if ($hexdigest eq FILE_MISSING) {
            # FIXME, this could be another error like EMFILE/ENFILE
            return FILE_MISSING;
        }
        my $checksum = eval {
            MogileFS::Checksum->from_string(0, "$alg:$hexdigest")
        };
        return undeferr("$alg failed for $uri: $@") if $@;
        return $checksum->{checksum};
    } elsif ($rv =~ /^ERROR /) {
        return; # old server, fallback to HTTP
    }
    return undeferr("mogstored failed to handle ($alg $uri)");
}

sub digest_http {
    my ($self, $alg, $ping_cb) = @_;

    # TODO: refactor
    my $node_timeout = MogileFS->config("node_timeout");
    # Hardcoded connection cache size of 20 :(
    $user_agent ||= LWP::UserAgent->new(timeout => $node_timeout, keep_alive => 20);
    my $digest = Digest->new($alg);

    my %opts = (
        # default (4K) is tiny, use 1M like replicate
        ':read_size_hint' => 0x100000,
        ':content_cb' => sub {
            $digest->add($_[0]);
            $ping_cb->();
        }
    );

    my $path = $self->{url};
    my $res = $user_agent->get($path, %opts);

    return $digest->digest if $res->is_success;
    return FILE_MISSING if $res->code == 404;
    return undeferr("Failed $alg (GET) check for $path (" . $res->code . "): "
                    . $res->message);
}

sub digest {
    my ($self, $alg, $ping_cb, $reason) = @_;
    my $digest = $self->digest_mgmt($alg, $ping_cb, $reason);

    return $digest if ($digest && $digest ne FILE_MISSING);

    $self->digest_http($alg, $ping_cb);
}

1;
