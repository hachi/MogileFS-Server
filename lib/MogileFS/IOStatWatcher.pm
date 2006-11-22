package MogileFS::IOStatWatcher;
use strict;
use Sys::Syscall 0.22; # We use it indirectly, and trigger bugs in earlier versions.
use Danga::Socket;
use IO::Socket::INET;

sub new {
    my ($class) = @_;
    return bless {}, $class;
}

sub set_hosts {
    my ($self, @ips) = @_;
    # TODO: start/remove new Danga::Socket-based client sockets to
    # new/old @ips.
    my $old_hosts = $self->{hosts};
    my $new_hosts = {};
    foreach my $host (@ips) {
        $new_hosts->{$host} = $old_hosts->{$host} || MogileFS::IOStatWatch::Client->new($host, $self);
    }
    $self->{hosts} = $new_hosts;
}

sub got_stats {
    my ($self, $host, $stats) = @_;
    $self->{on_stats}->($host, $stats);
}

sub got_error {
    my ($self, $host) = @_;
    Danga::Socket->AddTimer(60, sub {
        $self->{hosts}->{$host} = MogileFS::IOStatWatch::Client->new($host, $self);
    });
}

sub got_disconnect {
    my ($self, $host) = @_;
    $self->{hosts}->{$host} = MogileFS::IOStatWatch::Client->new($host, $self);
}

sub on_stats {
    my ($self, $cb) = @_;
    $self->{on_stats} = $cb;
}

package MogileFS::IOStatWatch::Client;

use strict;
use warnings;

use base 'Danga::Socket';
use fields qw(host watcher buffer active);

sub new {
    my MogileFS::IOStatWatch::Client $self = shift;
    my $hostspec = shift;
    my $watcher = shift;

    my $sock = IO::Socket::INET->new(
                                     PeerAddr => $hostspec,
                                     PeerPort => 7501,
                                     Proto    => 'tcp',
                                     Blocking => 0,
                                     );
    return unless $sock;

    $self = fields::new($self) unless ref $self;
    $self->SUPER::new($sock);
    $self->watch_write(1);
    $self->watch_read(1);

    $self->{watcher} = $watcher;
    $self->{buffer} = '';
    $self->{host} = $hostspec;

    return $self;
}

sub event_write {
    my MogileFS::IOStatWatch::Client $self = shift;
    $self->{active} = 1;
    $self->write("watch\n");
    $self->watch_write(0); # I hope I can safely assume that 6 characters will write properly.
}

sub event_read {
    my MogileFS::IOStatWatch::Client $self = shift;

    my $bref = $self->read(10240);
    return $self->close unless defined $bref;

    $self->{buffer} .= $$bref;

    if ($self->{buffer} =~ m/^ERR\s+(.*?)\s* $ /x) {
        # There was an error on the way to watching this machine, close it and stay quiet.
        $self->close;
    }

    # If we can yank off lines till there is one by itself with a . on it, we've gotten a full set of stats.
    while ($self->{buffer} =~ s/^(.*?\n)\.\n//s) {
        my %stats;
        foreach my $line (split /\n+/, $1) {
            my ($devnum, $device, $util) = split /\s+/, $line;
            $stats{$devnum} = $util;
        }
        $self->{watcher}->got_stats($self->{host}, \%stats);
    }
}

sub event_err {
    my MogileFS::IOStatWatch::Client $self = shift;
    $self->{watcher}->got_error($self->{host});
}

sub event_hup {
    my MogileFS::IOStatWatch::Client $self = shift;
    $self->{watcher}->got_error($self->{host});
}

sub close {
    my MogileFS::IOStatWatch::Client $self = shift;
    if ($self->{active}) {
        $self->{watcher}->got_disconnect($self->{host});
    } else {
        $self->{watcher}->got_error($self->{host});
    }
    $self->SUPER::close(@_);
}
1;

