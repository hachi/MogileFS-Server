package MogileFS::Worker;
use strict;
use fields qw(psock);

sub new {
    my ($self, $psock) = @_;
    $self = fields::new($self) unless ref $self;

    $self->{psock} = $psock;
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

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

