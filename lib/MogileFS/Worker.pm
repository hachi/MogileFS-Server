package MogileFS::Worker;
use strict;
use fields ('psock',              # socket for parent/child communications
            'last_bcast_state'    # "{device|host}-$devid" => [$time, {alive|dead}]
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

sub get_orders_from_parent {
    my $self = shift;
    my $psock = $self->{psock};

    $self->send_to_parent('request_orders');
    while (defined (my $line = <$psock>)) {
        $line =~ s/\r?\n$//;
        last if $line eq '.';
        if ($line eq 'shutdown') {
            exit 0;
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
    my $key = "$what-$whatid";
    my $laststate = $self->{last_bcast_state}{$key};
    my $now = time();
    # broadcast on initial discovery, state change, and every 10 seconds
    if (!$laststate || $laststate->[1] ne $state || $laststate->[0] < $now - 10) {
        $self->send_to_parent("state_change $what $whatid $state");
    }
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

