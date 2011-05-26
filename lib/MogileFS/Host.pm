package MogileFS::Host;
use strict;
use warnings;
use MogileFS::Util qw(throw);
use Net::Netmask;
use Carp qw(croak);
use MogileFS::Connection::Mogstored;

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
    return $state && $state =~ /^alive|dead|down$/;
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

sub should_get_new_files {
    return $_[0]->status eq 'alive';
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

1;
