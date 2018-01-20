package MogileFS::Worker::Fsck;

use strict;
use base 'MogileFS::Worker';
use fields (
            'opt_nostat',          # bool: do we trust mogstoreds? skipping size stats?
            'opt_checksum',        # (class|off|MD5) checksum mode
            );
use MogileFS::Util qw(every error debug);
use MogileFS::Config;
use MogileFS::Server;
use List::Util ();
use Time::HiRes ();

use constant SUCCESS => 0;
use constant TEMPORARY => 1;
use constant PERMANENT => 2;
use constant REPLICATE => 3;

use constant EV_NO_PATHS         => "NOPA";
use constant EV_POLICY_VIOLATION => "POVI";
use constant EV_FILE_MISSING     => "MISS";
use constant EV_BAD_LENGTH       => "BLEN";
use constant EV_CANT_FIX         => "GONE";
use constant EV_START_SEARCH     => "SRCH";
use constant EV_FOUND_FID        => "FOND";
use constant EV_RE_REPLICATE     => "REPL";
use constant EV_BAD_COUNT        => "BCNT";
use constant EV_BAD_CHECKSUM     => "BSUM";
use constant EV_NO_CHECKSUM      => "NSUM";
use constant EV_MULTI_CHECKSUM   => "MSUM";
use constant EV_BAD_HASHTYPE     => "BALG";

use POSIX ();

my $nowish;  # approximate unixtime, updated once per loop.

sub watchdog_timeout { 120 }

sub work {
    my $self = shift;

    # this can be CPU-intensive.  let's nice ourselves down.
    POSIX::nice(10);

    my $sto         = Mgd::get_store();
    my $max_checked = 0;

    every(2.0, sub {
        my $sleep_set = shift;
        $nowish = time();
        local $Mgd::nowish = $nowish;

        my $queue_todo = $self->queue_todo('fsck');
        # This counts the same as a $self->still_alive;
        $self->send_to_parent('worker_bored 50 fsck');
        return unless @{$queue_todo};
        return unless $self->validate_dbh;

        my @fids = ();
        while (my $todo = shift @{$queue_todo}) {
            my $fid = MogileFS::FID->new($todo->{fid});
            if ($fid->exists) {
                push(@fids, $fid);
            } else {
                # FID stopped existing before being checked.
                $sto->delete_fid_from_file_to_queue($fid->id, FSCK_QUEUE);
            }
        }
        return unless @fids;

        $self->{opt_nostat} = MogileFS::Config->server_setting('fsck_opt_policy_only')     || 0;
        my $alg = MogileFS::Config->server_setting_cached("fsck_checksum");
        if (defined($alg) && $alg eq "off") {
            $self->{opt_checksum} = "off";
        } else {
            $self->{opt_checksum} = MogileFS::Checksum->valid_alg($alg) ? $alg : 0;
        }
        MogileFS::FID->mass_load_devids(@fids);

        # don't sleep in loop, next round, since we found stuff to work on
        # this round...
        $sleep_set->(0);

        my $new_max;
        my $hit_problem = 0;

        foreach my $fid (@fids) {
            if (!$self->check_fid($fid)) {
                # some connectivity problem... retry this fid later.
                # (don't dequeue it)
                $self->still_alive;
                next;
            }
            $sto->delete_fid_from_file_to_queue($fid->id, FSCK_QUEUE);
        }
    });
}

# given a $fid (MogileFS::FID, with pre-populated ->devids data)
# return 0 if reachability problems.
# return 1 if fid was checked (regardless of there being problems or not)
#   if no problems, no action.
#   if problems, log & enqueue fixes
use constant STALLED => 0;
use constant HANDLED => 1;
sub check_fid {
    my ($self, $fid) = @_;

    my $fix = sub {
        my ($reason, $recheck) = @_;
        my $fixed;

        # we cached devids without locking for the fast path,
        # ensure we get an up-to-date list in the slow path.
        $fid->forget_cached_devids;

        my $sto = Mgd::get_store();
        unless ($sto->should_begin_replicating_fidid($fid->id)) {
            error("Fsck stalled for fid $fid: failed to acquire lock");
            return STALLED;
        }

        unless ($fid->exists) {
            # FID stopped existing while doing (or waiting on)
            # the fast check, give up on this fid
            $sto->note_done_replicating($fid->id);
            return HANDLED;
        }

        # we may have a lockless check which failed, retry the check
        # with the lock and see if it succeeds here:
        if ($recheck) {
            $fixed = $recheck->();
            if (!$fixed) {
                $fid->fsck_log($reason);
            }
        }

        $fixed ||= eval { $self->fix_fid($fid) };
        my $err = $@;
        $sto->note_done_replicating($fid->id);
        if (! defined $fixed) {
            error("Fsck stalled for fid $fid: $err");
            return STALLED;
        }
        $fid->fsck_log(EV_CANT_FIX) if ! $fixed;

        # that might've all taken awhile, let's update our approximate time
        $nowish = $self->still_alive;
        return HANDLED;
    };

    # first obvious fucked-up case:  no devids even presumed to exist.
    unless ($fid->devids) {
        # weird, recheck with a lock and then log it if it fails
        # and attempt a fix (which will do a search over all
        # devices as a last-ditch effort to locate it)
        return $fix->(EV_NO_PATHS, sub { $fid->devids });
    }

    # first, see if the assumed devids meet the replication policy for
    # the fid's class.
    unless ($fid->devids_meet_policy) {
        # recheck for policy violation under a lock, logging the violation
        # if we failed.
        return $fix->(EV_POLICY_VIOLATION, sub { $fid->devids_meet_policy });
    }

    # This is a simple fixup case
    # If we got here, we already know we have no policy violation and
    # don't need to call $fix->() to just fix a devcount
    $self->maybe_fix_devcount($fid);

    # missing checksum row
    if ($fid->class->hashtype && ! $fid->checksum) {
        return $fix->();
    }

    # in the fast case, do nothing else (don't check if assumed file
    # locations are actually there).  in the fast case, all we do is
    # check the replication policy, which is already done, so finish.
    return HANDLED if $self->{opt_nostat};

    if ($self->{opt_checksum} && $self->{opt_checksum} ne "off") {
        return $fix->();
    }

    # stat each device to see if it's still there.  on first problem,
    # stop and go into the slow(er) fix function.
    my $err;
    my $rv = $self->parallel_check_sizes([ $fid->devfids ], sub {
        my ($dfid, $disk_size) = @_;
        if (! defined $disk_size) {
            my $dev  = $dfid->device;
            # We end up checking is_perm_dead twice, but that's the way the
            # flow goes...
            if ($dev->dstate->is_perm_dead) {
                $err = "needfix";
                return 0;
            }
            error("Connectivity problem reaching device " . $dev->id . " on host " . $dev->host->ip . "\n");
            $err = "stalled";
            return 0;
        }
        return 1 if $disk_size == $fid->length;
        $err = "needfix";
        # Note: not doing fsck_log, as fix_fid will log status for each device.
        return 0;
    });

    if ($rv) {
        return ($fid->class->hashtype && !($self->{opt_checksum} && $self->{opt_checksum} eq "off"))
            ? $fix->() : HANDLED;
    } elsif ($err eq "stalled") {
        return STALLED;
    } elsif ($err eq "needfix") {
        return $fix->();
    } else {
        die "Unknown error checking fid sizes in parallel.\n";
    }
}

# returns true if all size checks succeeded, false otherwise
sub parallel_check_sizes {
    my ($self, $dflist, $cb) = @_;
    my $expect = scalar @$dflist;
    my ($good, $done) = (0, 0);

    foreach my $df (@$dflist) {
        $df->size_on_disk(sub {
            my ($size) = @_;
            $done++;
            if ($cb->($df, $size)) {
                $good++;
            } else {
                # use another timer to force PostLoopCallback to run
                Danga::Socket->AddTimer(0, sub { $self->still_alive });
            }
        });
    }

    Danga::Socket->SetPostLoopCallback(sub { $done != $expect });
    Danga::Socket->EventLoop;

    return $good == $expect;
}

# this is the slow path.  if something above in check_fid finds
# something amiss in any way, we went the slow path on a fid and try
# really hard to fix the situation.
#
# return true if situation handled, 0 if nothing could be done.
# die on errors (like connectivity problems).
use constant CANT_FIX => 0;
sub fix_fid {
    my ($self, $fid) = @_;
    debug(sprintf("Fixing FID %d", $fid->id));

    # make devfid objects from the devids that this fid is on,
    my @dfids = map { MogileFS::DevFID->new($_, $fid) } $fid->devids;

    # track all known good copies (dev objects), as well as all bad
    # copies (places it should've been, but isn't)
    my @good_devs;
    my @bad_devs;
    my %already_checked;  # devid -> 1.
    my $alg = $fid->class->hashname || $self->{opt_checksum};
    my $checksums = {};
    my $ping_cb = sub { $self->still_alive };

    my $check_dfids = sub {
        my $is_desperate_mode = shift;

        # stat all devices.
        foreach my $dfid (@dfids) {
            my $dev = $dfid->device;
            next if $already_checked{$dev->id}++;

            # Got a dead link, but reaper hasn't cleared it yet?
            if ($dev->dstate->is_perm_dead) {
                push @bad_devs, $dev;
                next;
            }

            my $disk_size = $dfid->size_on_disk;
            die "dev " . $dev->id . " unreachable" unless defined $disk_size;

            if ($disk_size == $fid->length) {
                if ($alg && $alg ne "off") {
                    my $digest = $self->checksum_on_disk($dfid, $alg, $ping_cb);
                    unless (defined $digest) {
                        die "dev " . $dev->id . " unreachable";
                    }

                    # DELETE could've hit right after size check
                    if ($digest eq "-1") {
                        unless ($is_desperate_mode) {
                            $fid->fsck_log(EV_FILE_MISSING, $dev);
                        }
                        push @bad_devs, $dfid->device;
                        next;
                    }
                    push @{$checksums->{$digest} ||= []}, $dfid->device;
                }

                push @good_devs, $dfid->device;
                # if we were doing a desperate search, one is enough, we can stop now!
                return if $is_desperate_mode;
                next;
            }

            # don't log in desperate mode, as we'd have "file missing!" log entries
            # for every device in the normal case, which is expected.
            unless ($is_desperate_mode) {
                if ($disk_size == -1) {
                    $fid->fsck_log(EV_FILE_MISSING, $dev);
                } else {
                    $fid->fsck_log(EV_BAD_LENGTH, $dev);
                }
            }

            push @bad_devs, $dfid->device;
        }
    };

    $check_dfids->();

    # if we didn't find it anywhere, let's go do an exhaustive search over
    # all devices, looking for it...
    unless (@good_devs) {
        # replace @dfids with list of all (alive) devices.  dups will be ignored by
        # check_dfids
        $fid->fsck_log(EV_START_SEARCH);
        @dfids = List::Util::shuffle(
                                     map  { MogileFS::DevFID->new($_, $fid)  }
                                     grep { $_->dstate->should_fsck_search_on }
                                     Mgd::device_factory()->get_all
                                     );
        $check_dfids->("desperate");

        # still can't fix it?
        unless (@good_devs) {
            $self->forget_bad_devs($fid, @bad_devs);
            $fid->update_devcount;
            return CANT_FIX;
        }

        # wow, we actually found it!
        $fid->note_on_device($good_devs[0]); # at least one good one.
        $fid->fsck_log(EV_FOUND_FID);

        # fall through to check policy (which will most likely be
        # wrong, with only one file_on record...) and re-replicate
    }

    $self->forget_bad_devs($fid, @bad_devs);
    # in case the devcount or similar was fixed.
    $fid->want_reload;

    $self->fix_checksums($fid, $alg, $checksums) if $alg && $alg ne "off";

    # Note: this will reload devids, if they called 'note_on_device'
    # or 'forget_about_device'
    unless ($fid->devids_meet_policy) {
        $fid->enqueue_for_replication(in => 1);
        $fid->fsck_log(EV_RE_REPLICATE);
        return HANDLED;
    }
    
    # Clean up the device count if it's wrong
    $self->maybe_fix_devcount($fid);

    return HANDLED;
}

sub forget_file_on_with_bad_checksums {
    my ($self, $fid, $checksums) = @_;
    foreach my $bdevs (values %$checksums) {
        foreach my $bdev (@$bdevs) {
            error("removing file_on mapping for fid=" . $fid->id . ", dev=" . $bdev->id);
            $fid->forget_about_device($bdev);
        }
    }
}

# returns -1 on missing,
# undef on connectivity error,
# else checksum of file on disk (after HTTP GET or mogstored read)
sub checksum_on_disk {
    my ($self, $dfid, $alg, $ping_cb) = @_;
    return $dfid->checksum_on_disk($alg, $ping_cb, "fsck");
}

sub bad_checksums_errmsg {
    my ($self, $alg, $checksums) = @_;
    my @err;

    foreach my $checksum (keys %$checksums) {
        my $bdevs = join(",", map { $_->id } @{$checksums->{$checksum}});
        $checksum = unpack("H*", $checksum);
        push @err, "$alg:$checksum on devids=[$bdevs]"
    }

    return join('; ', @err);
}

# we don't now what checksum the file is supposed to be, but some
# of the devices had checksums that didn't match the other(s).
sub auto_checksums_bad {
    my ($self, $fid, $checksums) = @_;
    my $alg = $self->{opt_checksum};
    my $err = $self->bad_checksums_errmsg($alg, $checksums);

    error("$fid has multiple checksums: $err");
    $fid->fsck_log(EV_MULTI_CHECKSUM);
}

sub all_checksums_bad {
    my ($self, $fid, $checksums) = @_;
    my $alg = $fid->class->hashname or return; # class could've changed
    my $cur_checksum = $fid->checksum;
    my $err = $self->bad_checksums_errmsg($alg, $checksums);
    my $cur = $cur_checksum ? "Expected: $cur_checksum"
                            : "No known valid checksum";
    error("all checksums bad: $err. $cur");
    $fid->fsck_log(EV_BAD_CHECKSUM);
}

sub fix_checksums {
    my ($self, $fid, $alg, $checksums) = @_;
    my $cur_checksum = $fid->checksum;
    my @all_checksums = keys(%$checksums);

    if (scalar(@all_checksums) == 1) { # all checksums match, good!
        my $disk_checksum = $all_checksums[0];
        if ($cur_checksum) {
            if ($cur_checksum->{checksum} ne $disk_checksum) {
                my $expect = $cur_checksum->info;
                my $actual = "$alg:" . unpack("H*", $disk_checksum);
                error("$cur_checksum does not match disk: $actual");
                if ($alg ne $cur_checksum->hashname) {
                    $fid->fsck_log(EV_BAD_HASHTYPE);
                } else {
                    $fid->fsck_log(EV_BAD_CHECKSUM);
                }
            }
        } else { # fresh row to checksum
            my $hashtype = $fid->class->hashtype;

            # we store this in the database
            if ($hashtype) {
                my %row = (
                    fid => $fid->id,
                    checksum => $disk_checksum,
                    hashtype => $hashtype,
                );
                my $new_checksum = MogileFS::Checksum->new(\%row);
                debug("creating new checksum=$new_checksum");
                $fid->fsck_log(EV_NO_CHECKSUM);
                $new_checksum->save;
            } else {
                my $hex_checksum = unpack("H*", $disk_checksum);
                my $alg = $self->{opt_checksum};
                debug("fsck_checksum=auto good: $fid $alg:$hex_checksum");
            }
        }
    } elsif ($cur_checksum) {
        my $good = delete($checksums->{$cur_checksum->{checksum}});
        if ($good && (scalar(@$good) > 0)) {
            $self->forget_file_on_with_bad_checksums($fid, $checksums);
            # will fail $fid->devids_meet_policy and re-replicate
        } else {
            $self->all_checksums_bad($fid, $checksums);
        }
    } elsif ($self->{opt_checksum}) {
        $self->auto_checksums_bad($fid, $checksums);
    } else {
        $self->all_checksums_bad($fid, $checksums);
    }
}

# remove the file_on mappings for devices that were bogus/missing.
sub forget_bad_devs {
    my ($self, $fid, @bad_devs) = @_;
    foreach my $bdev (@bad_devs) {
        error("removing file_on mapping for fid=" . $fid->id . ", dev=" . $bdev->id);
        $fid->forget_about_device($bdev);
    }
}

sub maybe_fix_devcount {
    # don't even log BCNT errors if skip_devcount is enabled
    return if MogileFS::Config->server_setting_cached('skip_devcount');

    my ($self, $fid) = @_;
    return if scalar($fid->devids) == $fid->devcount;
    # log a bad count
    $fid->fsck_log(EV_BAD_COUNT);
    $fid->update_devcount();
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
