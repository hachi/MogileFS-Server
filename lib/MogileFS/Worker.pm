package MogileFS::Worker;
use strict;
use fields ('psock',              # socket for parent/child communications
            'last_bcast_state',   # "{device|host}-$devid" => [$time, {alive|dead}]
            'readbuf',            # unparsed data from parent
            'monitor_has_run',    # true once we've heard of the monitor job being alive
            'last_ping',          # time we last said we're alive
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
    $self->{readbuf}          = '';
    $self->{last_bcast_state} = {};
    $self->{monitor_has_run}  = 0;
    $self->{last_ping}        = 0;

    IO::Handle::blocking($psock, 0);
    return $self;
}

sub validate_dbh {
    return Mgd::validate_dbh();
}

sub get_dbh {
    return Mgd::get_dbh();
}

sub monitor_has_run {
    my $self = shift;
    return $self->{monitor_has_run} ? 1 : 0;
}

# method that workers can call just to write something to the parent, so worker
# doesn't get killed.  (during idle/slow operation, say)
sub still_alive {
    my $self = shift;
    my $now = time();
    if ($now > $self->{last_ping}) {
        $self->send_to_parent(":still_alive");  # a no-op, just for the watchdog
        $self->{last_ping} = $now;
    }
}

sub send_to_parent {
    my $self = shift;
    my $write = "$_[0]\r\n";
    my $totallen = length $write;
    my $rv = syswrite($self->{psock}, $write);
    return 1 if $rv == $totallen;
    die "Error writing: $!" if $!;

    my $remain = $totallen - $rv;
    my $offset = $rv;
    my $rout = '';
    vec($rout, fileno($self->{psock}), 1) = 1;
    while ($remain > 0) {
        select(undef, $rout, undef, undef) or next;
        $rv = syswrite($self->{psock}, $write, $remain, $offset);
        $remain -= $rv;
        $offset += $rv;
    }
    die "remain is negative:  $remain" if $remain < 0;
    return 1;
}

# override in children
sub watchdog_timeout {
    return 10;
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

    # while things are immediately available,
    while (Mgd::wait_for_readability(fileno($psock), 0)) {
        my $buf;
        my $rv = sysread($psock, $buf, 1024);
        $self->{readbuf} .= $buf;

        while ($self->{readbuf} =~ s/^(.+?)\r?\n//) {
            my $line = $1;

            next if $self->process_generic_command(\$line);
            my $ok = $self->process_line(\$line);
            unless ($ok) {
                error("Unrecognized command from parent: $line");
            }
        }
    }
}

sub parent_ping {
    my $self = shift;
    my $psock = $self->{psock};
    $self->send_to_parent(':ping');

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
            warn "No simple reply from parent to child $self [$$] in $loops 0.2second loops.\n";
            die "No answer in 4 seconds from parent to child $self [$$], dying" if $loops > 20;
        }
    }
}

sub broadcast_device_writeable {
    $_[0]->_broadcast_state("device", $_[1], "writeable");
}
sub broadcast_device_readable {
    $_[0]->_broadcast_state("device", $_[1], "readable");
}
sub broadcast_device_unreachable {
    $_[0]->_broadcast_state("device", $_[1], "unreachable");
}
sub broadcast_host_reachable {
    $_[0]->_broadcast_state("host", $_[1], "reachable");
}
sub broadcast_host_unreachable {
    $_[0]->_broadcast_state("host", $_[1], "unreachable");
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

sub invalidate_meta {
    my ($self, $what) = @_;
    return if $Mgd::INVALIDATE_NO_PROPOGATE;  # anti recursion
    $self->send_to_parent(":invalidate_meta $what");
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

    if ($$lineref =~ /^:invalidate_meta_once (\w+)/) {
        local $Mgd::INVALIDATE_NO_PROPOGATE = 1;
        eval("Mgd::invalidate_${1}_cache()");
        return 1;
    }

    if ($$lineref =~ /^:monitor_has_run/) {
        $self->{monitor_has_run} = 1;
        return 1;
    }

    if ($$lineref =~ /^:set_config_from_parent (\S+) (.+)/) {
        # the 'no_broadcast' API keeps us from looping forever.
        MogileFS::Config->set_config_no_broadcast($1, $2);
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

