package MogileFS::Worker::Reaper;
# deletes files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Server;
use MogileFS::Util qw(every error debug);
use MogileFS::Config qw(DEVICE_SUMMARY_CACHE_TIMEOUT);

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub watchdog_timeout {
    return 240;
}

my %all_empty;  # devid -> bool, if all empty of files in file_on

# order is important here:
#
# first, add fid to file_to_replicate table.  it
# shouldn't matter if the replicator gets to this
# before the subsequent 'forget_about' method, as the
# replicator will treat dead file_on devices as
# non-existent anyway.  however, it is important that
# we enqueue it for replication first, before we
# forget about that file_on row, otherwise a failure
# after/during 'forget_about' could leave a stranded
# file on a dead device and we'd never fix it.
#
# and just for extra safety, in case replication happened
# on another machine after 'enqueue_for_replication' but
# before 'forget_about', and that other machine hadn't yet
# re-read the device table to learn that this device
# was dead, we delay the replication for the amount of time
# that the device summary table is valid for (presumably
# the other trackers are running identical software, or
# at least have the same timeout value)
sub reap_fid {
    my ($self, $fid, $dev) = @_;

    $fid->enqueue_for_replication(in => DEVICE_SUMMARY_CACHE_TIMEOUT + 1);
    $dev->forget_about($fid);
    $fid->update_devcount;
}

# this returns 1000 by default
sub reaper_inject_limit {
    my ($self) = @_;

    my $sto = Mgd::get_store();
    my $max = MogileFS::Config->server_setting_cached('queue_size_for_reaper');
    my $limit = MogileFS::Config->server_setting_cached('queue_rate_for_reaper') || 1000;

    # max defaults to zero, meaning we inject $limit every wakeup
    if ($max) {
        # if a queue size limit is configured for reaper, prevent too many
        # files from entering the repl queue:
        my $len = $sto->deferred_repl_queue_length;
        my $space_left = $max - $len;

        $limit = $space_left if ($limit > $space_left);

        # limit may end up being negative here since other processes
        # can inject into the deferred replication queue, reaper is
        # the only one which can respect this queue size
        $limit = 0 if $limit < 0;
    }

    return $limit;
}

sub work {
    my $self = shift;

    every(5, sub {
        # get db and note we're starting a run
        debug("Reaper running; looking for dead devices");

        foreach my $dev (grep { $_->dstate->is_perm_dead }
                         Mgd::device_factory()->get_all)
        {
            my $devid = $dev->id;
            next if $all_empty{$devid};
            my $limit = $self->reaper_inject_limit or next;

            my $sto = Mgd::get_store();
            my $lock = "mgfs:reaper";
            my $lock_timeout = $self->watchdog_timeout / 4;
            if ($sto->get_lock($lock, $lock_timeout)) {
                my @fids = $dev->fid_list(limit => $limit);
                if (@fids) {
                    $self->still_alive;
                    foreach my $fid (@fids) {
                        $self->reap_fid($fid, $dev);
                    }
                } else {
                    $all_empty{$devid} = 1;
                }
                $sto->release_lock($lock);
            } else {
                debug("get_lock($lock, $lock_timeout) failed");
            }
        }
    });
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
