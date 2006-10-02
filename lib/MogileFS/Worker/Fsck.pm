package MogileFS::Worker::Fsck;

use strict;
use base 'MogileFS::Worker';
use MogileFS::Util qw(every error);

use POSIX;

sub watchdog_timeout {
    30;
}

sub work {
    my $self = shift;

    my $update_db_every = 15;
    my %last_db_update;  # devid -> time.  update db less often than poll interval.

    every(10, sub {
        $self->parent_ping;

        # get db and note we're starting a run
        error("Monitor running; scanning usage files")
            if $Mgd::DEBUG >= 1;
        $self->validate_dbh;
        my $dbh = $self->get_dbh or return 0;



    });
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
