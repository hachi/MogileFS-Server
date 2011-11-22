package MogileFS::HTTPFile;
use strict;
use warnings;
use Carp qw(croak);
use Socket qw(PF_INET IPPROTO_TCP SOCK_STREAM);
use Digest::MD5;
use MIME::Base64;
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
    my ($host, $port, $uri, $path) = map { $self->{$_} } qw(host port uri url);

    return undef if (exists $size_check_retry_after{$host}
        && $size_check_retry_after{$host} > Time::HiRes::time());

    # don't SIGPIPE us
    my $flag_nosignal = MogileFS::Sys->flag_nosignal;
    local $SIG{'PIPE'} = "IGNORE" unless $flag_nosignal;

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
            return 0;
        }
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

sub md5_mgmt {
    my ($self, $ping_cb) = @_;
    my $mogconn = $self->host->mogstored_conn;
    my $node_timeout = MogileFS->config("node_timeout");
    my $sock;
    my $rv;
    my $expiry;

    my $uri = $self->{uri};
    my $req = "md5 $uri\r\n";
    my $reqlen = length $req;

    # a dead/stale socket may not be detected until we try to recv on it
    # after sending a request
    my $retries = 2;

    # assuming the storage node can MD5 at >=2MB/s, low expectations here
    my $response_timeout = $self->size / (2 * 1024 * 1024);

    my $flag_nosignal = MogileFS::Sys->flag_nosignal;
    local $SIG{'PIPE'} = "IGNORE" unless $flag_nosignal;

retry:
    $sock = $mogconn->sock($node_timeout) or return;
    $rv = send($sock, $req, $flag_nosignal);
    if ($! || $rv != $reqlen) {
        my $err = $!;
        $mogconn->mark_dead;
        if ($retries-- <= 0) {
            $err = $err ? "send() error (md5 $uri): $err" :
                          "short send() (md5 $uri): $rv != $reqlen";
            $err = $mogconn->{ip} . ":" . $mogconn->{port} . " $err";
            return undeferr($err);
        }
        goto retry;
    }

    $expiry = Time::HiRes::time() + $response_timeout;
    while (!wait_for_readability(fileno($sock), 1.0) &&
           (Time::HiRes::time() > $expiry)) {
        $ping_cb->();
    }

    $rv = <$sock>;
    if (! $rv) {
        $mogconn->mark_dead;
        return undeferr("EOF from mogstored") if ($retries-- <= 0);
        goto retry;
    } elsif ($rv =~ /^\Q$uri\E md5=(\S+)\r\n/) {
        my $md5 = $1;

        if ($md5 eq FILE_MISSING) {
            # FIXME, this could be another error like EMFILE/ENFILE
            return FILE_MISSING;
        }
        if (length($md5) == 22) {
            # Digest::MD5->b64digest on mogstored does not pad
            return decode_base64("$md5==");
        }
    } elsif ($rv =~ /^ERROR /) {
        return; # old server, fallback to HTTP
    }
    return undeferr("mogstored failed to handle (md5 $uri)");
}

sub md5_http {
    my ($self, $ping_cb) = @_;

    # don't SIGPIPE us (why don't we just globally ignore SIGPIPE?)
    my $flag_nosignal = MogileFS::Sys->flag_nosignal;
    local $SIG{'PIPE'} = "IGNORE" unless $flag_nosignal;

    # TODO: refactor
    my $node_timeout = MogileFS->config("node_timeout");
    # Hardcoded connection cache size of 20 :(
    $user_agent ||= LWP::UserAgent->new(timeout => $node_timeout, keep_alive => 20);
    my $digest = Digest::MD5->new;

    my %opts = (
        # default (4K) is tiny, we can try bigger, maybe 1M like replicate
        ':read_size_hint' => 0x4000,
        ':content_cb' => sub {
            $digest->add($_[0]);
            $ping_cb->();
        }
    );

    my $path = $self->{url};
    my $res = $user_agent->get($path, %opts);

    return $digest->digest if $res->is_success;
    return FILE_MISSING if $res->code == 404;
    return undeferr("Failed MD5 (GET) check for $path (" . $res->code . "): "
                    . $res->message);
}

sub md5 {
    my ($self, $ping_cb) = @_;
    my $md5 = $self->md5_mgmt($ping_cb);

    return $md5 if ($md5 && $md5 ne FILE_MISSING);

    $self->md5_http($ping_cb);
}

1;
