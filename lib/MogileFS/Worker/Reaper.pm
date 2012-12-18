package MogileFS::Worker::Reaper;
# deletes files

use strict;
use base 'MogileFS::Worker';
use MogileFS::Server;
use MogileFS::Util qw(error debug);
use MogileFS::Config qw(DEVICE_SUMMARY_CACHE_TIMEOUT);
use constant REAP_INTERVAL => 5;
use constant REAP_BACKOFF_MIN => 60;

# completely forget about devices we've reaped after 2 hours of idleness
use constant REAP_BACKOFF_MAX => 7200;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub watchdog_timeout {
    return 240;
}

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
sub reap_fid {
    my ($self, $fid, $dev) = @_;

    $fid->enqueue_for_replication(in => 1);
    $dev->forget_about($fid);
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

# we pass the $devid here (instead of a Device object) to avoid
# potential memory leaks since this sub reschedules itself to run
# forever.   $delay is the current delay we were scheduled at
sub reap_dev {
    my ($self, $devid, $delay) = @_;

    # ensure the master DB is up, retry in REAP_INTERVAL if down
    unless ($self->validate_dbh) {
        $delay = REAP_INTERVAL;
        Danga::Socket->AddTimer($delay, sub { $self->reap_dev($devid, $delay) });
        return;
    }

    my $limit = $self->reaper_inject_limit;

    # just in case a user mistakenly nuked a devid from the device table:
    my $dev = Mgd::device_factory()->get_by_id($devid);
    unless ($dev) {
        error("No device row for dev$devid, cannot reap");
        $delay = undef;
    }

    # limit == 0 if we hit the queue size limit, we'll just reschedule
    if ($limit && $dev) {
        my $sto = Mgd::get_store();
        my $lock = "mgfs:reaper";
        my $lock_timeout = $self->watchdog_timeout / 4;
        my @fids;

        if ($sto->get_lock($lock, $lock_timeout)) {
            @fids = $dev->fid_list(limit => $limit);
            if (@fids) {
                $self->still_alive;
                foreach my $fid (@fids) {
                    $self->reap_fid($fid, $dev);
                }
            }
            $sto->release_lock($lock);

            # if we've found any FIDs (perhaps even while backing off)
            # ensure we try to find more soon:
            if (@fids) {
                $delay = REAP_INTERVAL;
            } else {
                $delay = $self->reap_dev_backoff_delay($delay);
            }
        } else {
            # No lock after a long lock_timeout?  Try again soon.
            # We should never get here under MySQL, and rarely for other DBs.
            debug("get_lock($lock, $lock_timeout) failed");
            $delay = REAP_INTERVAL;
        }
    }

    return unless defined $delay;

    # schedule another update, delay could be REAP_BACKOFF_MAX
    Danga::Socket->AddTimer($delay, sub { $self->reap_dev($devid, $delay) });
}

# called when we're hopefully all done with a device, but reschedule
# into the future in case the replicator had an out-of-date cache and the
# "dead" device was actually writable.
sub reap_dev_backoff_delay {
    my ($self, $delay) = @_;

    return REAP_BACKOFF_MIN if ($delay < REAP_BACKOFF_MIN);

    $delay *= 2;
    return $delay > REAP_BACKOFF_MAX ? undef : $delay;
}

# looks for dead devices
sub work {
    my $self = shift;

    # we just forked from our parent process, also using Danga::Socket,
    # so we need to lose all that state and start afresh.
    Danga::Socket->Reset;

    # ensure we get monitor updates
    Danga::Socket->AddOtherFds($self->psock_fd, sub{ $self->read_from_parent });

    my %devid_seen;
    my $reap_check;
    $reap_check = sub {
        # get db and note we're starting a run
        debug("Reaper running; looking for dead devices");
        $self->still_alive;

        foreach my $dev (grep { $_->dstate->is_perm_dead }
                         Mgd::device_factory()->get_all)
        {
            next if $devid_seen{$dev->id};

            # delay the initial device reap in case any replicator cache
            # thinks the device is still alive
            Danga::Socket->AddTimer(DEVICE_SUMMARY_CACHE_TIMEOUT + 1, sub {
                $self->reap_dev($dev->id, REAP_INTERVAL);
            });

            # once we've seen a device, reap_dev will takeover scheduling
            # reaping for the given device.
            $devid_seen{$dev->id} = 1;
        }

        Danga::Socket->AddTimer(REAP_INTERVAL, $reap_check);
    };

    # kick off the reaper and loop forever
    $reap_check->();
    Danga::Socket->EventLoop;
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
