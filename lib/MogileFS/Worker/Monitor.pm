package MogileFS::Worker::Monitor;
use strict;
use warnings;

use base 'MogileFS::Worker';
use fields (
            'last_test_write', # devid -> time.  time we last tried writing to a device.
            'monitor_start',   # main monitor start time
            'skip_host',       # hostid -> 1 if already noted dead (reset every loop)
            'seen_hosts',      # IP -> 1 (reset every loop)
            'iow',             # MogileFS::IOStatWatcher object
            'prev_data',       # DB data from previous run
            'devutil',         # Running tally of device utilization
            'events',          # Queue of state events
            'refresh_state',   # devid -> { used, total, callbacks }, temporary data in each refresh run
            'have_masterdb',   # Hint flag for if the master DB is available
            'updateable_devices', # devid -> Device, avoids device table updates
            'parent',          # socketpair to parent process
            'refresh_pending', # set if there was a manually-requested refresh
            'db_monitor_ran',  # We announce "monitor_just_ran" every time the
                               # device checks are run, but only if the DB has
                               # been checked inbetween.
            );

use Danga::Socket 1.56;
use MogileFS::Config;
use MogileFS::Util qw(error debug encode_url_args apply_state_events_list);
use MogileFS::IOStatWatcher;
use MogileFS::Server;
use MogileFS::Connection::Parent;
use Digest::MD5 qw(md5_base64);

use constant UPDATE_DB_EVERY => 15;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    $self->{last_test_write} = {};
    $self->{iow}             = MogileFS::IOStatWatcher->new;
    $self->{prev_data}       = { domain => {}, class => {}, host => {},
        device => {} };
    $self->{devutil}         = { cur => {}, prev => {}, tmp => {} };
    $self->{events}          = [];
    $self->{have_masterdb}   = 0;
    return $self;
}

sub watchdog_timeout {
    30;
}

# returns 1 if a DB update was attempted
# returns 0 immediately if the (device) monitor is already running
sub cache_refresh {
    my $self = shift;

    if ($self->{refresh_state}) {
        debug("Monitor run in progress, will not check for DB updates");
        return 0;
    }

    debug("Monitor running; checking DB for updates");
    # "Fix" our local cache of this flag, so we always check the master DB.
    MogileFS::Config->cache_server_setting('_master_db_alive', 1);
    my $have_dbh = $self->validate_dbh;
    if ($have_dbh && !$self->{have_masterdb}) {
        $self->{have_masterdb} = 1;
        $self->set_event('srvset', '_master_db_alive', { value => 1 });
    } elsif (!$have_dbh) {
        $self->{have_masterdb} = 0;
        $self->set_event('srvset', '_master_db_alive', { value => 0 });
        error("Cannot connect to master database!");
    }

    if ($have_dbh) {
        my $db_data   = $self->grab_all_data;

        # Stack diffs to ship back later
        $self->diff_data($db_data);
    }

    $self->send_events_to_parent;
    $self->{db_monitor_ran} = 1;

    return 1;
}

sub usage_refresh {
    my ($self) = @_;

    # prevent concurrent refresh
    return if $self->{refresh_state};

    debug("Monitor running; scanning usage files");

    $self->{refresh_state} = {}; # devid -> ...
    $self->{monitor_start} = Time::HiRes::time();

    my $have_dbh = $self->validate_dbh;

    # See if we should be allowed to update the device table rows.
    if ($have_dbh && Mgd::get_store()->get_lock('mgfs:device_update', 0)) {
        # Fetch the freshlist list of entries, to avoid excessive writes.
        $self->{updateable_devices} = { map { $_->{devid} => $_ }
            Mgd::get_store()->get_all_devices };
    } else {
        $self->{updateable_devices} = undef;
    }

    $self->{skip_host}  = {};  # hostid -> 1 if already noted dead.
    $self->{seen_hosts} = {}; # IP -> 1

    my $dev_factory = MogileFS::Factory::Device->get_factory();
    my $devutil = $self->{devutil};

    $devutil->{tmp} = {};
    # kick off check_device to test host/devs. diff against old values.
    for my $dev ($dev_factory->get_all) {
        if (my $state = $self->is_iow_diff($dev)) {
            $self->state_event('device', $dev->id, {utilization => $state});
        }
        $devutil->{tmp}->{$dev->id} = $devutil->{cur}->{$dev->id};

        $dev->can_read_from or next;
        $self->check_device_begin($dev);
    }
    # we're done if we didn't schedule any work
    $self->usage_refresh_done unless keys %{$self->{refresh_state}};
}

sub usage_refresh_done {
    my ($self) = @_;

    if ($self->{updateable_devices}) {
        Mgd::get_store()->release_lock('mgfs:device_update');
        $self->{updateable_devices} = undef;
    }

    $self->{devutil}->{prev} = $self->{devutil}->{tmp};
    # Set the IOWatcher hosts (once old monitor code has been disabled)

    $self->send_events_to_parent;

    $self->{iow}->set_hosts(keys %{$self->{seen_hosts}});

    foreach my $devid (keys %{$self->{refresh_state}}) {
        error("device check incomplete for dev$devid");
    }

    my $start = delete $self->{monitor_start};
    my $elapsed = Time::HiRes::time() - $start;
    debug("device refresh finished after $elapsed");

    $self->{refresh_state} = undef;
    my $pending_since = $self->{refresh_pending};

    # schedule another usage_refresh immediately if somebody requested it
    # Don't announce :monitor_just_ran if somebody requested a refresh
    # while we were running, we could've been refreshing on a stale DB
    if ($pending_since && $pending_since > $start) {
        # using AddTimer to schedule the refresh to avoid stack overflow
        # since usage_refresh can call usage_refresh_done directly if
        # there are no devices
        Danga::Socket->AddTimer(0, sub {
            $self->cache_refresh;
            $self->usage_refresh;
        });
    }

    # announce we're done if we ran on schedule, or we had a
    # forced refresh that was requested before we started.
    if (!$pending_since || $pending_since <= $start) {
        # totally done refreshing, accept manual refresh requests again
        $self->{parent}->watch_read(1);
        delete $self->{refresh_pending};
        if (delete $self->{db_monitor_ran} || $pending_since) {
            $self->send_to_parent(":monitor_just_ran");
        }
    }
}

sub work {
    my $self = shift;

    # It makes sense to have monitor use a shorter timeout
    # (conn_timeout) across the board to skip slow hosts.  Other workers
    # are less tolerant, and may use a higher value in node_timeout.
    MogileFS::Config->set_config_no_broadcast("node_timeout", MogileFS::Config->config("conn_timeout"));

    my $iow = $self->{iow};
    $iow->on_stats(sub {
        my ($hostname, $stats) = @_;

        while (my ($devid, $util) = each %$stats) {
            # Lets not propagate devices that we accidentally find.
            my $dev = Mgd::device_factory()->get_by_id($devid);
            next unless $dev;
            $self->{devutil}->{cur}->{$devid} = $util;
        }
    });

    my $db_monitor;
    $db_monitor = sub {
        $self->still_alive;

        # reschedule immediately if we were blocked by main_monitor.
        # setting refresh_pending will call cache_refresh again
        if (!$self->cache_refresh) {
            $self->{refresh_pending} ||= Time::HiRes::time();
        }

        # always reschedule in 4 seconds, regardless
        Danga::Socket->AddTimer(4, $db_monitor);
    };

    $db_monitor->();
    $self->read_from_parent;

    my $main_monitor;
    $main_monitor = sub {
        $self->{parent}->ping;
        $self->usage_refresh;
        Danga::Socket->AddTimer(2.5, $main_monitor);
    };

    $self->parent_ping; # ensure we get the initial DB state back
    $self->{parent} = MogileFS::Connection::Parent->new($self);
    Danga::Socket->AddTimer(0, $main_monitor);
    Danga::Socket->EventLoop;
}

sub process_line {
    my MogileFS::Worker::Monitor $self = shift;
    my $lineref = shift;
    if ($$lineref =~ /^:refresh_monitor$/) {
        if ($self->cache_refresh) {
            $self->usage_refresh;
        } else {
            $self->{refresh_pending} ||= Time::HiRes::time();
        }
        # try to stop processing further refresh_monitor requests
        # if we're acting on a manual refresh
        $self->{parent}->watch_read(0);
        return 1;
    }
    return 0;
}

# --------------------------------------------------------------------------

# Flattens and flips events up to the parent. Can be huge on startup!
# Events: set type foo=bar&baz=quux
# remove type id
# setstate type id foo=bar&baz=quux
# Combined: ev_mode=set&ev_type=device&foo=bar
# ev_mode=setstate&ev_type=device&ev_id=1&foo=bar
sub send_events_to_parent {
    my $self = shift;
    my @flat = ();
    for my $ev (@{$self->{events}}) {
        my ($mode, $type, $args) = @$ev;
        $args->{ev_mode} = $mode;
        $args->{ev_type} = $type;
        push(@flat, encode_url_args($args));
    }
    return unless @flat;
    $self->{events} = [];

    {
        # $events can be several MB, so let it go out-of-scope soon:
        my $events = join(' ', ':monitor_events', @flat);
        debug("sending state changes $events", 2);
        $self->send_to_parent($events);
    }

    apply_state_events_list(@flat);
}

sub add_event {
    push(@{$_[0]->{events}}, $_[1]);
}

sub set_event { 
    # Allow callers to use shorthand
    $_[3]->{ev_id} = $_[2];
    $_[0]->add_event(['set', $_[1], $_[3]]); 
}
sub remove_event { $_[0]->add_event(['remove', $_[1], { ev_id => $_[2] }]); }
sub state_event {
    $_[3]->{ev_id} = $_[2];
    $_[0]->add_event(['setstate', $_[1], $_[3]]);
}

sub is_iow_diff {
    my ($self, $dev) = @_;
    my $devid = $dev->id;
    my $p = $self->{devutil}->{prev}->{$devid};
    my $c = $self->{devutil}->{cur}->{$devid};
    if ( ! defined $p || $p ne $c ) {
        return $c;
    }
    return undef;
}

sub diff_data {
    my ($self, $db_data) = @_;

    my $new_data  = {};
    my $prev_data = $self->{prev_data};
    for my $type (keys %{$db_data}) {
        my $d_data = $db_data->{$type};
        my $p_data = $prev_data->{$type};
        my $n_data = {};

        for my $item (@{$d_data}) {
            my $id = $type eq 'domain' ? $item->{dmid}
                : $type eq 'class'     ? $item->{dmid} . '-' . $item->{classid}
                : $type eq 'host'      ? $item->{hostid}
                : $type eq 'device'    ? $item->{devid}
                : $type eq 'srvset'    ? $item->{field}
                : die "Unknown type";
            my $old = delete $p_data->{$id};
            # Special case: for devices, we don't care if mb_asof changes.
            # FIXME: Change the grab routine (or filter there?).
            delete $item->{mb_asof} if $type eq 'device';
            if (!$old || $self->diff_hash($old, $item)) {
                $self->set_event($type, $id, { %$item });
            }
            $n_data->{$id} = $item;
        }
        for my $id (keys %{$p_data}) {
            $self->remove_event($type, $id);
        }

        $new_data->{$type} = $n_data;
    }
    $self->{prev_data} = $new_data;
}

# returns 1 if the hashes are different.
sub diff_hash {
    my ($self, $old, $new) = @_;

    my %keys = ();
    map { $keys{$_}++ } keys %$old, keys %$new;
    for my $k (keys %keys) {
        return 1 if (exists $old->{$k} && ! exists $new->{$k});
        return 1 if (exists $new->{$k} && ! exists $old->{$k});
        return 1 if (defined $old->{$k} && ! defined $new->{$k});
        return 1 if (defined $new->{$k} && ! defined $old->{$k});
        next     if (! defined $new->{$k} && ! defined $old->{$k});
        return 1 if ($old->{$k} ne $new->{$k});
    }
    return 0;
}

sub grab_all_data {
    my $self = shift;
    my $sto  = Mgd::get_store();

    # Normalize the domain data to the rest to simplify the differ.
    # FIXME: Once new objects are swapped in, fix the original
    my %dom = $sto->get_all_domains;
    my @fixed_dom = ();
    while (my ($name, $id) = each %dom) {
        push(@fixed_dom, { namespace => $name, dmid => $id });
    }

    my $set = $sto->server_settings;
    my @fixed_set = ();
    while (my ($field, $value) = each %$set) {
        push(@fixed_set, { field => $field, value => $value });
    }

    my %ret = ( domain => \@fixed_dom,
        class  => [$sto->get_all_classes],
        host   => [$sto->get_all_hosts],
        device => [$sto->get_all_devices],
        srvset => \@fixed_set, );
    return \%ret;
}

# returns true on success, false on failure
sub check_usage_response {
    my ($self, $dev, $response) = @_;
    my $devid = $dev->id;

    my %stats;
    my $data = $response->content;
    foreach (split(/\r?\n/, $data)) {
        next unless /^(\w+)\s*:\s*(.+)$/;
        $stats{$1} = $2;
    }

    my ($used, $total) = ($stats{used}, $stats{total});
    unless ($used && $total) {
        $used  = "<undef>" unless defined $used;
        $total = "<undef>" unless defined $total;
        my $clen = length($data || "");
        error("dev$devid reports used = $used, total = $total, content-length: $clen, error?");
        return 0;
    }

    my $rstate = $self->{refresh_state}->{$devid};
    ($rstate->{used}, $rstate->{total}) = ($used, $total);

    # only update database every ~15 seconds per device
    if ($self->{updateable_devices}) {
        my $devrow = $self->{updateable_devices}->{$devid};
        my $last = ($devrow && $devrow->{mb_asof}) ? $devrow->{mb_asof} : 0;
        if ($last + UPDATE_DB_EVERY < time()) {
            Mgd::get_store()->update_device_usage(mb_total => int($total / 1024),
                                                  mb_used  => int($used / 1024),
                                                  devid    => $devid);
        }
    }
    return 1;
}

sub dev_debug {
    my ($self, $dev, $writable) = @_;
    return unless $Mgd::DEBUG >= 1;
    my $devid = $dev->id;
    my $rstate = $self->{refresh_state}->{$devid};
    my ($used, $total) = ($rstate->{used}, $rstate->{total});

    debug("dev$devid: used = $used, total = $total, writeable = $writable");
}

sub check_write {
    my ($self, $dev) = @_;
    my $rstate = $self->{refresh_state}->{$dev->id};
    my $test_write = $rstate->{test_write};

    if (!$test_write || $test_write->{tries} > 0) {
        # this was "$$-$now" before, but we don't yet have a cleaner in
        # mogstored for these files
        my $num = int(rand 100);
        $test_write = $rstate->{test_write} ||= {};
        $test_write->{path} = "/dev${\$dev->id}/test-write/test-write-$num";
        $test_write->{content} = "time=" . time . " rand=$num";
        $test_write->{tries} ||= 2;
    }
    $test_write->{tries}--;

    my $opts = { content => $test_write->{content} };
    $dev->host->http("PUT", $test_write->{path}, $opts, sub {
        my ($response) = @_;
        $self->on_check_write_response($dev, $response);
    });
}

# starts the lengthy device check process
sub check_device_begin {
    my ($self, $dev) = @_;
    $self->{refresh_state}->{$dev->id} = {};

    $self->check_device($dev);
}

# the lengthy device check process
sub check_device {
    my ($self, $dev) = @_;
    return $self->check_device_done($dev) if $self->{skip_host}{$dev->hostid};

    my $devid = $dev->id;
    my $url = $dev->usage_url;
    my $host = $dev->host;

    $self->{seen_hosts}{$host->ip} = 1;

    # now try to get the data with a short timeout
    my $start_time = Time::HiRes::time();
    $host->http_get("GET", $dev->usage_url, undef, sub {
        my ($response) = @_;
        if (!$self->on_usage_response($dev, $response, $start_time)) {
            return $self->check_device_done($dev);
        }
        # next if we're not going to try this now
        my $now = time();
        if (($self->{last_test_write}{$devid} || 0) + UPDATE_DB_EVERY > $now) {
            return $self->check_device_done($dev);
        }
        $self->{last_test_write}{$devid} = $now;

        unless ($dev->can_delete_from) {
            # we should not try to write on readonly devices because it can be
            # mounted as RO.
            return $self->dev_observed_readonly($dev);
        }
        # now we want to check if this device is writeable

        # first, create the test-write directory.  this will return
        # immediately after the first time, as the 'create_directory'
        # function caches what it's already created.
        $dev->create_directory("/dev$devid/test-write", sub {
            $self->check_write($dev);
        });
    });
}

# called on a successful PUT, ensure the data we get back is what we uploaded
sub check_reread {
    my ($self, $dev) = @_;
    # now let's get it back to verify; note we use the get_port to
    # verify that the distinction works (if we have one)
    my $test_write = $self->{refresh_state}->{$dev->id}->{test_write};
    $dev->host->http_get("GET", $test_write->{path}, undef, sub {
        my ($response) = @_;
        $self->on_check_reread_response($dev, $response);
    });
}

sub on_check_reread_response {
    my ($self, $dev, $response) = @_;
    my $test_write = $self->{refresh_state}->{$dev->id}->{test_write};

    # if success and the content matches, mark it writeable
    if ($response->is_success) {
        if ($response->content eq $test_write->{content}) {
            if (!$dev->observed_writeable) {
                my $event = { observed_state => 'writeable' };
                $self->state_event('device', $dev->id, $event);
            }
            $self->dev_debug($dev, 1);

            return $self->check_bogus_md5($dev); # onto the final check...
        }

        # content didn't match due to race, retry and hope we're lucky
        return $self->check_write($dev) if ($test_write->{tries} > 0);
    }

    return $self->dev_observed_readonly($dev); # it's read-only at least
}

sub on_check_write_response {
    my ($self, $dev, $response) = @_;
    return $self->check_reread($dev) if $response->is_success;
    return $self->dev_observed_readonly($dev);
}

# returns true on success, false on failure
sub on_usage_response {
    my ($self, $dev, $response, $start_time) = @_;
    my $host = $dev->host;
    my $hostip = $host->ip;

    if ($response->is_success) {
        # at this point we can reach the host
        if (!$host->observed_reachable) {
            my $event = { observed_state => 'reachable' };
            $self->state_event('host', $dev->hostid, $event);
        }
        $self->{iow}->restart_monitoring_if_needed($hostip);

        return $self->check_usage_response($dev, $response);
    }

    my $url = $dev->usage_url;
    my $failed_after = Time::HiRes::time() - $start_time;
    if ($failed_after < 0.5) {
        if (!$dev->observed_unreachable) {
            my $event = { observed_state => 'unreachable' };
            $self->state_event('device', $dev->id, $event);
        }
        my $get_port = $host->http_get_port;
        error("Port $get_port not listening on $hostip ($url)?  Error was: " . $response->status_line);
    } else {
        $failed_after = sprintf("%.02f", $failed_after);
        if (!$host->observed_unreachable) {
            my $event = { observed_state => 'unreachable' };
            $self->state_event('host', $dev->hostid, $event);
        }
        $self->{skip_host}{$dev->hostid} = 1;
    }
    return 0; # failure
}

sub check_bogus_md5 {
    my ($self, $dev) = @_;
    my $put_path = "/dev${\$dev->id}/test-write/test-md5";
    my $opts = {
        headers => { "Content-MD5" => md5_base64("!") . "==", },
        content => '.',
    };

    # success is bad here, it means the server doesn't understand how to
    # verify and reject corrupt bodies from Content-MD5 headers.
    # most servers /will/ succeed here :<
    $dev->host->http("PUT", $put_path, $opts, sub {
        my ($response) = @_;
        $self->on_bogus_md5_response($dev, $response);
    });
}

sub on_bogus_md5_response {
    my ($self, $dev, $response) = @_;
    my $rej = $response->is_success ? 0 : 1;
    my $prev = $dev->reject_bad_md5;

    if (!defined($prev) || $prev != $rej) {
        debug("dev${\$dev->id}: reject_bad_md5 = $rej");
        $self->state_event('device', $dev->id, { reject_bad_md5 => $rej });
    }
    return $self->check_device_done($dev);
}

# if we fall through to here, then we know that something is not so
# good, so mark it readable which is guaranteed given we even tested
# writeability
sub dev_observed_readonly {
    my ($self, $dev) = @_;

    if (!$dev->observed_readable) {
        my $event = { observed_state => 'readable' };
        $self->state_event('device', $dev->id, $event);
    }
    $self->dev_debug($dev, 0);
    return $self->check_device_done($dev);
}

# called when all checks are done for a particular device
sub check_device_done {
    my ($self, $dev) = @_;

    $self->still_alive; # Ping parent if needed so we don't time out
                        # given lots of devices.
    delete $self->{refresh_state}->{$dev->id};

    # if refresh_state is totally empty, we're done
    if ((scalar keys %{$self->{refresh_state}}) == 0) {
        $self->usage_refresh_done;
    }
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
