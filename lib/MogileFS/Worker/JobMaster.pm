package MogileFS::Worker::JobMaster;
# manages/monitors the internal queues for various workers.
# decided to have one of these per tracker instead of have workers
# all elect one per job type... should be able to reuse more code, and avoid
# relying on too many database locks.

use strict;
use base 'MogileFS::Worker';
use fields (
            'fsck_queue_limit',
            'repl_queue_limit',
            'dele_queue_limit',
            );
use MogileFS::Util qw(every error debug eurl);

use constant FSCK_QUEUE => 1;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub watchdog_timeout { 120; }

# heartbeat all of the queues constantly.
# if a queue drops below a watermark, check for more work.
# NOTE: Uh. now that I think about it, should queue_check just return
# the status for all queues in one roundtrip? :(
# It's separate in case future workers want to manage their own queues, or
# this gets split up...
sub work {
    my $self = shift;

    $self->{fsck_queue_limit} = 100;
    $self->{repl_queue_limit} = 100;
    $self->{dele_queue_limit} = 100;

    every(1, sub {
        # 'pings' parent and populates all queues.
        $self->send_to_parent("queue_depth all");
        my $sto = Mgd::get_store();
        $self->read_from_parent(1);
        $self->_check_replicate_queues($sto);
        $self->_check_delete_queues($sto);
        $self->_check_fsck_queues($sto);
    });
}

sub _check_delete_queues {
    my $self = shift;
    my $sto  = shift;
    my ($need_fetch, $new_limit) =
        queue_depth_check($self->queue_depth('delete'),
        $self->{dele_queue_limit});
    return unless $need_fetch;
    my @to_del = $sto->grab_files_to_delete2($new_limit);
    $self->{dele_queue_limit} = @to_del ? $new_limit : 100;
    return unless @to_del;
    for my $todo (@to_del) {
        $self->send_to_parent("queue_todo delete " .
            _eurl_encode_args($todo));
    }
}

# NOTE: we only maintain one queue per worker, but we can easily
# give specialized work per worker by tagging the $todo href.
# in the case of replication, we want a normal "replication" queue,
# but also "drain" and "rebalance" queues. So use $todo->{type} or something.
# Drain/rebalance will be way awesomer with a queue attached:
# "drain 5% of devid 5" or "drain 10G off devids 7,8,9"
# hell, drain barely works if you encounter errors. Using a work queue
# should fix that.
# FIXME: Don't hardcode the min queue depth.
sub _check_replicate_queues {
    my $self = shift;
    my $sto  = shift;
    my ($need_fetch, $new_limit) =
        queue_depth_check($self->queue_depth('replicate'),
        $self->{repl_queue_limit});
    return unless $need_fetch;
    my @to_repl = $sto->grab_files_to_replicate($new_limit);
    $self->{repl_queue_limit} = @to_repl ? $new_limit : 100;
    return unless @to_repl;
    # don't need to shuffle or sort, since we're the only tracker to get this
    # list.
    for my $todo (@to_repl) {
        $todo->{_type} = 'replicate'; # could be 'drain', etc.
        $self->send_to_parent("queue_todo replicate " .
            _eurl_encode_args($todo));
    }
}

# FSCK is going to be a little odd... We still need a single "global"
# fsck worker to do the queue injection, but need to locally poll data.
sub _check_fsck_queues {
    my $self = shift;
    my $sto  = shift;
    my $fhost = MogileFS::Config->server_setting('fsck_host');
    if ($fhost && $fhost eq MogileFS::Config->hostname) {
        $self->_inject_fsck_queues($sto);
    }

    # Queue depth algorithm:
    # if internal queue is less than 30% full, fetch more.
    # if internal queue bottomed out, increase fetch limit by 50.
    # fetch more work
    # if no work fetched, reset limit to 100 (default)
    my ($need_fetch, $new_limit) =
        queue_depth_check($self->queue_depth('fsck'),
        $self->{fsck_queue_limit});
    return unless $need_fetch;
    my @to_fsck = $sto->grab_files_to_queued(FSCK_QUEUE, $new_limit);
    $self->{fsck_queue_limit} = @to_fsck ? $new_limit : 100;
    return unless @to_fsck;
    for my $todo (@to_fsck) {
        $self->send_to_parent("queue_todo fsck " . _eurl_encode_args($todo));
    }
}

sub _inject_fsck_queues {
    my $self = shift;
    my $sto  = shift;

    my $max_checked = MogileFS::Config->server_setting('fsck_highest_fid_checked') || 0;
    my $to_inject   =
        MogileFS::Config->server_setting_cached('queue_rate_for_fsck', 30) || 1000;
    my @fids        = $sto->get_fid_hrefs_above_id($max_checked, $to_inject);
    unless (@fids) {
        $sto->set_server_setting("fsck_host", undef);
        $sto->set_server_setting("fsck_stop_time", $sto->get_db_unixtime);
        MogileFS::Config->set_server_setting('fsck_highest_fid_checked',
            $max_checked);
        return;
    }

    $sto->enqueue_many_for_todo(\@fids, FSCK_QUEUE, 0);

    my $nmax = $fids[-1]->{fid};
    MogileFS::Config->set_server_setting('fsck_highest_fid_checked', $nmax);
}

# takes the current queue depth and fetch limit
# returns whether or not to fetch, and new fetch limit.
# TODO: make the limit cap configurable.
# TODO: separate a fetch limit from a queue limit...
# so we don't hammer the DB with giant transactions, but loop
# fast trying to keep the queue full.
sub queue_depth_check {
    my ($depth, $limit) = @_;
    if ($depth == 0) {
        $limit += 50 unless $limit >= 1000;
        return (1, $limit);
    } elsif ($depth / $limit < 0.70) {
        return (1, $limit);
    }
    return (0, $limit);
}

# TODO: Move this into Util.pm?
sub _eurl_encode_args {
    my $args = shift;
    return join('&', map { eurl($_) . "=" . eurl($args->{$_}) } keys %$args);
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
