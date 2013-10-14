package MogileFS::Host;
use strict;
use warnings;
use MogileFS::Util qw(throw);
use Net::Netmask;
use Carp qw(croak);
use MogileFS::Connection::Mogstored;
use MogileFS::Connection::HTTP;
use MogileFS::ConnectionPool;
our $http_pool;

=head1

MogileFS::Host - host class

=cut

# Centralized here instead of three places.
my @observed_fields = qw/observed_state/;
my @fields = (qw/hostid hostname hostip status http_port http_get_port altip altmask/,
    @observed_fields);

# TODO: Validate a few things: state, observed state.
sub new_from_args {
    my ($class, $args, $dev_factory) = @_;
    my $self = bless {
        dev_factory => $dev_factory,
        %{$args},
    }, $class;

    $self->{mask} = ($self->{altip} && $self->{altmask}) ?
        Net::Netmask->new2($self->{altmask}) : undef;

    return $self;
}

sub valid_state {
    my ($class, $state) = @_;
    return $state && $state =~ /\A(?:alive|dead|down|readonly)\z/;
}

# Instance methods:

sub id        { $_[0]{hostid} }
sub name      { $_[0]{hostname} }
sub hostname  { $_[0]{hostname} }
sub hostip    { $_[0]{hostip} }
sub status    { $_[0]{status} }
sub http_port { $_[0]{http_port} }

sub http_get_port {
    return $_[0]->{http_get_port} || $_[0]->{http_port};
}

sub ip {
    my $self = shift;
    if ($self->{mask} && $self->{altip} &&
        ($MogileFS::REQ_altzone || ($MogileFS::REQ_client_ip &&
         $self->{mask}->match($MogileFS::REQ_client_ip)))) {
        return $self->{altip};
    } else {
        return $self->{hostip};
    }
}

sub fields {
    my $self = shift;
    my @tofetch = @_ ? @_ : @fields;
    return { map { $_ => $self->{$_} } @tofetch };
}

sub observed_fields {
    return $_[0]->fields(@observed_fields);
}

sub alive {
    return $_[0]->status eq 'alive';
}

sub readonly {
    return $_[0]->status eq 'readonly';
}

sub should_read_from {
    return $_[0]->alive || $_[0]->readonly;
}

sub observed_reachable {
    my $self = shift;
    return $self->{observed_state} && $self->{observed_state} eq 'reachable';
}

sub observed_unreachable {
    my $self = shift;
    return $self->{observed_state} && $self->{observed_state} eq 'unreachable';
}

# returns/creates a MogileFS::Connection::Mogstored object to the
# host's mogstored management/side-channel port (which starts
# unconnected, and only connects when you ask it to, with its sock
# method)
sub mogstored_conn {
    my $self = shift;
    return $self->{mogstored_conn} ||=
      MogileFS::Connection::Mogstored->new($self->ip, $self->sidechannel_port);
}

sub sidechannel_port {
    # TODO: let this be configurable per-host?  currently it's configured
    # once for all machines.
    MogileFS->config("mogstored_stream_port");
}

# starts an HTTP request on the given $port with $method to $path
# Calls cb with an HTTP::Response object when done
sub _http_conn {
    my ($self, $port, $method, $path, $opts, $cb) = @_;
    _init_pools();

    $http_pool->start($opts->{ip} || $self->ip, $port, sub {
        $_[0]->start($method, $path, $opts, $cb);
    });
}

# Returns a ready, blocking HTTP connection
# This is only used by replicate
sub http_conn_get {
    my ($self, $opts) = @_;
    my $ip = $opts->{ip} || $self->ip;
    my $port = $opts->{port} || $self->http_port;

    _init_pools();
    my $conn = $http_pool->conn_get($ip, $port);
    $conn->sock->blocking(1) if $conn;
    return $conn;
}

# Returns a blocking HTTP connection back to the pool.
# This is the inverse of http_conn_get, and should be called when
# done using a connection (iff the connection is really still alive)
# (and makes it non-blocking for future use)
# This is only used by replicate.
sub http_conn_put {
    my ($self, $conn) = @_;
    $conn->sock->blocking(0);
    $http_pool->conn_put($conn);
}

sub http_get {
    my ($self, $method, $path, $opts, $cb) = @_;
    $opts ||= {};
    $self->_http_conn($self->http_get_port, $method, $path, $opts, $cb);
}

sub http {
    my ($self, $method, $path, $opts, $cb) = @_;
    $opts ||= {};
    my $port = delete $opts->{port} || $self->http_port;
    $self->_http_conn($port, $method, $path, $opts, $cb);
}

# FIXME - make these customizable
sub _init_pools {
    return if $http_pool;
    my $opts = {
        total_capacity => MogileFS->config("conn_pool_size"),
    };

    $http_pool = MogileFS::ConnectionPool->new("MogileFS::Connection::HTTP", $opts);
}

1;
