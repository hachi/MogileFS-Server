package MogileFS::Worker;
use strict;
use fields ('psock',              # socket for parent/child communications
            'last_bcast_state'    # "{device|host}-$devid" => [$time, {alive|dead}]
            );

use MogileFS::Util qw(error);
use vars (
          '$got_live_vs_die',    # local'ized scalarref flag for whether we've 
                                 # gotten a live-vs-die instruction from parent
          );

sub new {
    my ($self, $psock) = @_;
    $self = fields::new($self) unless ref $self;

    $self->{psock}            = $psock;
    $self->{last_bcast_state} = {};
    return $self;
}

sub validate_dbh {
    return Mgd::validate_dbh();
}

sub get_dbh {
    return Mgd::get_dbh();
}

sub send_to_parent {
    my $self = shift;
    $self->{psock}->write("$_[0]\r\n");
}

# should be overridden by workers to process worker-specific directives
# from the parent process.  return 1 if you recognize the command, 0 otherwise.
sub process_line {
    my ($self, $lineref) = @_;
    return 0;
}

sub read_from_parent {
    my $self = shift;
    my $psock = $self->{psock};

    #warn "$self reading from parent...\n";
    # while things are immediately available,
    my $buf;
    while (Mgd::wait_for_readability(fileno($psock), 0)) {
        # FIXME: ghetto single byte line-reading.  need to make this
        # non-blocket socketpair.  doing this for now to get the interface
        # down, even if implementation sucks.
        my $byte;
        my $rv = sysread($psock, $byte, 1);
        die "Didn't read a byte, got rv=$rv ($!)" unless $rv == 1;
        $buf .= $byte;

        next unless $buf =~ s/^(.+?)\r?\n//;
        my $line = $1;
        #warn "  $self got line: [$line]\n";

        next if $self->process_generic_command(\$line);
        my $ok = $self->process_line(\$line);
        unless ($ok) {
            error("Unrecognized command from parent: $line");
        }
    }
}

sub parent_ping {
    my $self = shift;
    my $psock = $self->{psock};
    $self->send_to_parent('still_alive');

    my $got_reply = 0;
    die "recursive parent_ping!" if $got_live_vs_die;
    local $got_live_vs_die = \$got_reply;

    my $loops = 0;

    while (!$got_reply) {
        $self->read_from_parent;
        return if $got_reply;

        $loops++;
        select undef, undef, undef, 0.20;
        if ($loops > 5) {
            warn "No simple reply from parent in $loops 0.2second loops.\n";
            die "No answer in 4 seconds from parent" if $loops > 20;
        }
    }
}

sub broadcast_device_error {
    $_[0]->_broadcast_state("device", $_[1], "dead");
}
sub broadcast_device_alive {
    $_[0]->_broadcast_state("device", $_[1], "alive");
}
sub broadcast_host_error {
    $_[0]->_broadcast_state("host", $_[1], "dead");
}
sub broadcast_host_alive {
    $_[0]->_broadcast_state("host", $_[1], "alive");
}

sub _broadcast_state {
    my ($self, $what, $whatid, $state) = @_;
    MogileFS->set_observed_state($what, $whatid, $state);
    my $key = "$what-$whatid";
    my $laststate = $self->{last_bcast_state}{$key};
    my $now = time();
    # broadcast on initial discovery, state change, and every 10 seconds
    if (!$laststate || $laststate->[1] ne $state || $laststate->[0] < $now - 10) {
        $self->send_to_parent(":state_change $what $whatid $state");
        $self->{last_bcast_state}{$key} = [$now, $state];
    }
}

# tries to parse generic (not job-specific) commands sent from parent
# to child.  returns 1 on success, or 0 if comman given isn't generic,
# and child should parse.
# lineref doesn't have \r\n at end.
sub process_generic_command {
    my ($self, $lineref) = @_;
    return 0 unless $$lineref =~ /^:/;  # all generic commands start with colon

    if ($$lineref =~ /^:state_change (\w+) (\d+) (\w+)/) {
        # {"host"|"device"} <id> {"alive"|"dead"}
        MogileFS->set_observed_state($1, $2, $3);
        return 1;
    }

    if ($$lineref =~ /^:shutdown/) {
        $$got_live_vs_die = 1 if $got_live_vs_die;
        exit 0;
    }

    if ($$lineref =~ /^:stay_alive/) {
        $$got_live_vs_die = 1 if $got_live_vs_die;
        return 1;
    }

    # TODO: warn on unknown commands?

    return 0;
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

