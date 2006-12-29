package MogileFS::Worker::Fsck;

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw(every error);

sub watchdog_timeout {
    30;
}

sub work {
    my $self = shift;

    every(10, sub {
        $self->parent_ping;

        # get db and note we're starting a run
        error("Monitor running; scanning usage files")
            if $Mgd::DEBUG >= 1;



    });
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
