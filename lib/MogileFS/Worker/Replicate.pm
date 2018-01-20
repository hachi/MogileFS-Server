package MogileFS::Worker::Replicate;
# replicates files around

use strict;
use base 'MogileFS::Worker';
use fields (
            'fidtodo',   # hashref { fid => 1 }
            );

use List::Util ();
use MogileFS::Server;
use MogileFS::Util qw(error every debug wait_for_readability);
use MogileFS::Config;
use MogileFS::ReplicationRequest qw(rr_upgrade);
use Digest;
use MIME::Base64 qw(encode_base64);

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);
    $self->{fidtodo} = {};
    return $self;
}

# replicator wants
sub watchdog_timeout { 90; }
use constant SOCK_TIMEOUT => 45;

sub work {
    my $self = shift;

    every(1.0, sub {
        $self->send_to_parent("worker_bored 100 replicate rebalance");

        my $queue_todo  = $self->queue_todo('replicate');
        my $queue_todo2 = $self->queue_todo('rebalance');
        return unless (@$queue_todo || @$queue_todo2);

        return unless $self->validate_dbh;
        my $sto = Mgd::get_store();

        while (my $todo = shift @$queue_todo) {
            my $fid = $todo->{fid};
            $self->replicate_using_torepl_table($todo);
        }
        while (my $todo = shift @$queue_todo2) {
            $self->still_alive;
            # deserialize the arg :/
            $todo->{arg} = [split /,/, $todo->{arg}];
            my $devfid =
                MogileFS::DevFID->new($todo->{devid}, $todo->{fid});
            $self->rebalance_devfid($devfid, 
                { target_devids => $todo->{arg} });

            # If files error out, we want to send the error up to syslog
            # and make a real effort to chew through the queue. Users may
            # manually re-run rebalance to retry.
            $sto->delete_fid_from_file_to_queue($todo->{fid}, REBAL_QUEUE);
        }
        $_[0]->(0); # don't sleep.
    });
}

# return 1 if we did something (or tried to do something), return 0 if
# there was nothing to be done.
sub replicate_using_torepl_table {
    my $self = shift;
    my $todo = shift;

    # find some fids to replicate, prioritize based on when they should be tried
    my $sto = Mgd::get_store();

    my $fid = $todo->{fid};
    $self->still_alive;

    my $errcode;

    my %opts;
    $opts{errref}       = \$errcode;
    $opts{no_unlock}    = 1; # to make it return an $unlock subref
    $opts{source_devid} = $todo->{fromdevid} if $todo->{fromdevid};

    my ($status, $unlock) = replicate($fid, %opts);

    if ($status) {
        # $status is either 0 (failure, handled below), 1 (success, we actually
        # replicated this file), or 2 (success, but someone else replicated it).

        # when $staus eq "lost_race", this delete is unnecessary normally
        # (somebody else presumably already deleted it if they
        # also replicated it), but in the case of running with old
        # replicators from previous versions, -or- simply if the
        # other guy's delete failed, this cleans it up....
        $sto->delete_fid_from_file_to_replicate($fid);
        $unlock->() if $unlock;
        next;
    }

    debug("Replication of fid=$fid failed with errcode=$errcode") if $Mgd::DEBUG >= 2;

    # ERROR CASES:

    # README: please keep this up to date if you update the replicate() function so we ensure
    # that this code always does the right thing
    #
    # -- HARMLESS --
    # failed_getting_lock        => harmless.  skip.  somebody else probably doing.
    #
    # -- ACTIONABLE --
    # too_happy                  => too many copies, attempt to rebalance.
    #
    # -- TEMPORARY; DO EXPONENTIAL BACKOFF --
    # source_down                => only source available is observed down.
    # policy_error_doing_failed  => policy plugin fucked up.  it's looping.
    # policy_error_already_there => policy plugin fucked up.  it's dumb.
    # policy_no_suggestions      => no copy was attempted.  policy is just not happy.
    # copy_error                 => policy said to do 1+ things, we failed, it ran out of suggestions.
    #
    # -- FATAL; DON'T TRY AGAIN --
    # no_source                  => it simply exists nowhere.  not that something's down, but file_on is empty.

    # bail if we failed getting the lock, that means someone else probably
    # already did it, so we should just move on
    if ($errcode eq 'failed_getting_lock') {
        $unlock->() if $unlock;
        next;
    }

    # logic for setting the next try time appropriately
    my $update_nexttry = sub {
        my ($type, $delay) = @_;
        my $sto = Mgd::get_store();
        if ($type eq 'end_of_time') {
            # special; update to a time that won't happen again,
            # as we've encountered a scenario in which case we're
            # really hosed
            $sto->reschedule_file_to_replicate_absolute($fid, $sto->end_of_time);
        } elsif ($type eq "offset") {
            $sto->reschedule_file_to_replicate_relative($fid, $delay+0);
        } else {
            $sto->reschedule_file_to_replicate_absolute($fid, $delay+0);
        }
    };

    # now let's handle any error we want to consider a total failure; do not
    # retry at any point.  push this file off to the end so someone has to come
    # along and figure out what went wrong.
    if ($errcode eq 'no_source') {
        $update_nexttry->( end_of_time => 1 );
        $unlock->() if $unlock;
        next;
    }

    # try to shake off extra copies. fall through to the backoff logic
    # so we don't flood if it's impossible to properly weaken the fid.
    # there's a race where the fid could be checked again, but the
    # exclusive locking prevents replication clobbering.
    if ($errcode eq 'too_happy') {
        $unlock->() if $unlock;
        $unlock = undef;
        my $f = MogileFS::FID->new($fid);
        my @devs = List::Util::shuffle($f->devids);
        my $devfid;
        # First one we can delete from, we try to rebalance away from.
        for (@devs) {
            my $dev = Mgd::device_factory()->get_by_id($_);
            # Not positive 'should_read_from' needs to be here.
            # We must be able to delete off of this dev so the fid can
            # move.
            if ($dev->can_delete_from && $dev->should_read_from) {
                $devfid = MogileFS::DevFID->new($dev, $f);
                last;
            }
        }
        if ($devfid) {
            if ($self->rebalance_devfid($devfid)) {
                # disable exponential backoff below if we rebalanced due to
                # excessive replication:
                $todo->{failcount} = 0;
            }
        }
    }

    # at this point, the rest of the errors require exponential backoff.  define what this means
    # as far as failcount -> delay to next try.
    # 15s, 1m, 5m, 30m, 1h, 2h, 4h, 8h, 24h, 24h, 24h, 24h, ...
    my @backoff = qw( 15 60 300 1800 3600 7200 14400 28800 );
    $update_nexttry->( offset => int(($backoff[$todo->{failcount}] || 86400) * (rand(0.4) + 0.8)) );
    $unlock->() if $unlock;
    return 1;
}

# Return 1 on success, 0 on failure or no-op.
sub rebalance_devfid {
    my ($self, $devfid, $opts) = @_;
    $opts ||= {};
    MogileFS::Util::okay_args($opts, qw(avoid_devids target_devids));

    my $fid = $devfid->fid;

    # bail out early if this FID is no longer in the namespace (weird
    # case where file is in file_on because not yet deleted, but
    # has been replaced/deleted in 'file' table...).  not too harmful
    # (just noisy) if this line didn't exist, but whatever... it
    # makes stuff cleaner on my intentionally-corrupted-for-fsck-testing
    # dev machine...
    return 1 if ! $fid->exists;

    my $errcode;
    my ($ret, $unlock) = replicate($fid,
                                   mask_devids  => { $devfid->devid => 1 },
                                   no_unlock    => 1,
                                   target_devids => $opts->{target_devids},
                                   errref       => \$errcode,
                                   );

    my $fail = sub {
        my $error = shift;
        $unlock->();
        error("Rebalance for $devfid (" . $devfid->url . ") failed: $error");
        return 0;
    };

    unless ($ret || $errcode eq "too_happy") {
        return $fail->("Replication failed");
    }

    my $should_delete = 0;
    my $del_reason;

    if ($errcode eq "too_happy" || $ret eq "lost_race") {
        # for some reason, we did no work. that could be because
        # either 1) we lost the race, as the error code implies,
        # and some other process rebalanced this first, or 2)
        # the file is over-replicated, and everybody just thinks they
        # lost the race because the replication policy said there's
        # nothing to do, even with this devfid masked away.
        # so let's figure it out... if this devfid still exists,
        # we're over-replicated, else we just lost the race.
        if ($devfid->exists) {
            # over-replicated

            # see if some copy, besides this one we want
            # to delete, is currently alive & of right size..
            # just as extra paranoid check before we delete it
            foreach my $test_df ($fid->devfids) {
                next if $test_df->devid == $devfid->devid;
                if ($test_df->size_matches) {
                    $should_delete = 1;
                    $del_reason = "over_replicated";
                    last;
                }
            }
        } else {
            # lost race
            $should_delete = 0;  # no-op
        }
    } elsif ($ret eq "would_worsen") {
        # replication has indicated we would be making ruining this fid's day
        # if we delete an existing copy, so lets not do that.
        # this indicates a condition where there're no suitable devices to
        # copy new data onto, so lets be loud about it.
        return $fail->("no suitable destination devices available");
    } else {
        $should_delete = 1;
        $del_reason = "did_rebalance;ret=$ret";
    }

    my %destroy_opts;

    $destroy_opts{ignore_missing} = 1
        if MogileFS::Config->config("rebalance_ignore_missing");

    if ($should_delete) {
        eval { $devfid->destroy(%destroy_opts) };
        if ($@) {
            return $fail->("HTTP delete (due to '$del_reason') failed: $@");
        }
    }

    $unlock->();
    return $should_delete;
}

# replicates $fid to make sure it meets its class' replicate policy.
#
# README: if you update this sub to return a new error code, please update the
# appropriate callers to know how to deal with the errors returned.
#
# returns either:
#    $rv
#    ($rv, $unlock_sub)    -- when 'no_unlock' %opt is used. subref to release lock.
# $rv is one of:
#    0 = failure  (failure written to ${$opts{errref}})
#    1 = success
#    "lost_race" = skipping, we did no work and policy was already met.
#    "nofid" => fid no longer exists. skip replication.
sub replicate {
    my ($fid, %opts) = @_;
    $fid = MogileFS::FID->new($fid) unless ref $fid;
    my $fidid = $fid->id;

    debug("Replication for $fidid called, opts=".join(',',keys(%opts))) if $Mgd::DEBUG >= 2;

    my $errref    = delete $opts{'errref'};
    my $no_unlock = delete $opts{'no_unlock'};
    my $fixed_source = delete $opts{'source_devid'};
    my $mask_devids  = delete $opts{'mask_devids'}  || {};
    my $avoid_devids = delete $opts{'avoid_devids'} || {};
    my $target_devids = delete $opts{'target_devids'} || []; # inverse of avoid_devids.
    die "unknown_opts" if %opts;
    die unless ref $mask_devids eq "HASH";

    my $sdevid;

    my $sto = Mgd::get_store();
    my $unlock = sub {
        $sto->note_done_replicating($fidid);
    };

    my $retunlock = sub {
        my $rv = shift;
        my ($errmsg, $errcode);
        if (@_ == 2) {
            ($errcode, $errmsg) = @_;
            $errmsg = "$errcode: $errmsg"; # include code with message
        } else {
            ($errmsg) = @_;
        }
        $$errref = $errcode if $errref;

        my $ret;
        if ($errcode && $errcode eq "failed_getting_lock") {
            # don't emit a warning with error() on lock failure.  not
            # a big deal, don't scare people.
            $ret = 0;
        } else {
            $ret = $rv ? $rv : error($errmsg);
        }
        if ($no_unlock) {
            die "ERROR: must be called in list context w/ no_unlock" unless wantarray;
            return ($ret, $unlock);
        } else {
            die "ERROR: must not be called in list context w/o no_unlock" if wantarray;
            $unlock->();
            return $ret;
        }
    };

    # hashref of devid -> MogileFS::Device
    my $devs = Mgd::device_factory()->map_by_id
        or die "No device map";

    return $retunlock->(0, "failed_getting_lock", "Unable to obtain lock for fid $fidid")
        unless $sto->should_begin_replicating_fidid($fidid);

    # if the fid doesn't even exist, consider our job done!  no point
    # replicating file contents of a file no longer in the namespace.
    return $retunlock->("nofid") unless $fid->exists;

    my $cls = $fid->class;
    my $polobj = $cls->repl_policy_obj;

    # learn what this devices file is already on
    my @on_devs;         # all devices fid is on, reachable or not.
    my @on_devs_tellpol; # subset of @on_devs, to tell the policy class about
    my @on_up_devid;     # subset of @on_devs:  just devs that are readable

    foreach my $devid ($fid->devids) {
        my $d = Mgd::device_factory()->get_by_id($devid)
            or next;
        push @on_devs, $d;
        if ($d->dstate->should_have_files && ! $mask_devids->{$devid}) {
            push @on_devs_tellpol, $d;
        }
        if ($d->should_read_from) {
            push @on_up_devid, $devid;
        }
    }

    return $retunlock->(0, "no_source",   "Source is no longer available replicating $fidid") if @on_devs == 0;
    return $retunlock->(0, "source_down", "No alive devices available replicating $fidid") if @on_up_devid == 0;

    if ($fixed_source && ! grep { $_ == $fixed_source } @on_up_devid) {
        error("Fixed source dev$fixed_source requested for $fidid but not available. Trying other devices");
    }

    my %dest_failed;    # devid -> 1 for each devid we were asked to copy to, but failed.
    my %source_failed;  # devid -> 1 for each devid we had problems reading from.
    my $got_copy_request = 0;  # true once replication policy asks us to move something somewhere
    my $copy_err;

    my $dest_devs = $devs;
    if (@$target_devids) {
        $dest_devs = {map { $_ => $devs->{$_} } @$target_devids};
    }

    my $rr;  # MogileFS::ReplicationRequest
    while (1) {
        $rr = rr_upgrade($polobj->replicate_to(
                                               fid       => $fidid,
                                               on_devs   => \@on_devs_tellpol, # all device objects fid is on, dead or otherwise
                                               all_devs  => $dest_devs,
                                               failed    => \%dest_failed,
                                               min       => $cls->mindevcount,
                                               ));

        last if $rr->is_happy;

        my @ddevs;  # dest devs, in order of preference
        my $ddevid; # dest devid we've chosen to copy to
        if (@ddevs = $rr->copy_to_one_of_ideally) {
            if (my @not_masked_ids = (grep { ! $mask_devids->{$_} &&
                                             ! $avoid_devids->{$_}
                                         }
                                      map { $_->id } @ddevs)) {
                $ddevid = $not_masked_ids[0];
            } else {
                # once we masked devids away, there were no
                # ideal suggestions.  this is the case of rebalancing,
                # which without this check could 'worsen' the state
                # of the world.  consider the case:
                #    h1[ d1 d2 ] h2[ d3 ]
                # and files are on d1 & d3, an ideal layout.
                # if d3 is being rebalanced, and masked away, the
                # replication policy could presumably say to put
                # the file on d2, even though d3 isn't dead.
                # so instead, when masking is in effect, we don't
                # use non-ideal placement, just bailing out.

                # this used to return "lost_race" as a lie, but rebalance was
                # happily deleting the masked fid if at least one other fid
                # existed... because it assumed it was over replicated.
                # now we tell rebalance that touching this fid would be
                # stupid.
                return $retunlock->("would_worsen");
            }
        } elsif (@ddevs = $rr->copy_to_one_of_desperate) {
            # TODO: reschedule a replication for 'n' minutes in future, or
            # when new hosts/devices become available or change state
            $ddevid = $ddevs[0]->id;
        } else {
            last;
        }

        $got_copy_request = 1;

        # replication policy shouldn't tell us to put a file on a device
        # we've already told it that we've failed at.  so if we get that response,
        # the policy plugin is broken and we should terminate now.
        if ($dest_failed{$ddevid}) {
            return $retunlock->(0, "policy_error_doing_failed",
                                "replication policy told us to do something we already told it we failed at while replicating fid $fidid");
        }

        # replication policy shouldn't tell us to put a file on a
        # device that it's already on.  that's just stupid.
        if (grep { $_->id == $ddevid } @on_devs) {
            return $retunlock->(0, "policy_error_already_there",
                                "replication policy told us to put fid $fidid on dev $ddevid, but it's already there!");
        }

        # find where we're replicating from
        {
            # TODO: use an observed good device+host as source to start.
            my @choices = grep { ! $source_failed{$_} } @on_up_devid;
            return $retunlock->(0, "source_down", "No devices available replicating $fidid") unless @choices;
            if ($fixed_source && grep { $_ == $fixed_source } @choices) {
                $sdevid = $fixed_source;
            } else {
                @choices = List::Util::shuffle(@choices);
                MogileFS::run_global_hook('replicate_order_final_choices', $devs, \@choices);
                $sdevid = shift @choices;
            }
        }

        my $worker = MogileFS::ProcManager->is_child or die;
        my $digest;
        my $fid_checksum = $fid->checksum;
        $digest = Digest->new($fid_checksum->hashname) if $fid_checksum;
        $digest ||= Digest->new($cls->hashname) if $cls->hashtype;

        my $rv = http_copy(
                           sdevid       => $sdevid,
                           ddevid       => $ddevid,
                           fid          => $fid,
                           errref       => \$copy_err,
                           callback     => sub { $worker->still_alive; },
                           digest       => $digest,
                           );
        die "Bogus error code: $copy_err" if !$rv && $copy_err !~ /^(?:src|dest)_error$/;

        unless ($rv) {
            error("Failed copying fid $fidid from devid $sdevid to devid $ddevid (error type: $copy_err)");
            if ($copy_err eq "src_error") {
                $source_failed{$sdevid} = 1;

                if ($fixed_source && $fixed_source == $sdevid) {
                    error("Fixed source dev$fixed_source was requested for $fidid but failed: will try other sources");
                }

            } else {
                $dest_failed{$ddevid} = 1;
            }
            next;
        }

        my $dfid = MogileFS::DevFID->new($ddevid, $fid);
        $dfid->add_to_db;
        if ($digest && !$fid->checksum) {
            $sto->set_checksum($fidid, $cls->hashtype, $digest->digest);
        }

        push @on_devs, $devs->{$ddevid};
        push @on_devs_tellpol, $devs->{$ddevid};
        push @on_up_devid, $ddevid;
    }

    # We are over replicated. Let caller decide if it should rebalance.
    if ($rr->too_happy) {
        return $retunlock->(0, "too_happy", "fid $fidid is on too many devices");
    }

    if ($rr->is_happy) {
        return $retunlock->(1) if $got_copy_request;
        return $retunlock->("lost_race");  # some other process got to it first.  policy was happy immediately.
    }

    return $retunlock->(0, "policy_no_suggestions",
                        "replication policy ran out of suggestions for us replicating fid $fidid");
}

# Returns a hashref with the following:
# {
#   code => HTTP status code integer,
#   keep => boolean, whether to keep the connection after reading
#   len =>  value of the Content-Length header (integer)
# }
# Returns undef on timeout
sub read_headers {
    my ($sock, $intercopy_cb) = @_;
    my $head = '';

    do {
        wait_for_readability(fileno($sock), SOCK_TIMEOUT) or return;
        $intercopy_cb->();
        my $r = sysread($sock, $head, 1024, length($head));
        if (defined $r) {
            return if $r == 0; # EOF
        } elsif ($!{EAGAIN} || $!{EINTR}) {
            # loop again
        } else {
            return;
        }
    } until ($head =~ /\r?\n\r?\n/);

    my $data;
    ($head, $data) = split(/\r?\n\r?\n/, $head, 2);
    my @head = split(/\r?\n/, $head);
    $head = shift(@head);
    $head =~ m!\AHTTP/(\d+\.\d+)\s+(\d+)! or return;
    my %rv = ( keep => $1 >= 1.1, code => $2 );

    foreach my $line (@head) {
        if ($line =~ /\AConnection:\s*keep-alive\s*\z/is) {
            $rv{keep} = 1;
        } elsif ($line =~ /\AConnection:\s*close\s*\z/is) {
            $rv{keep} = 0;
        } elsif ($line =~ /\AContent-Length:\s*(\d+)\s*\z/is) {
            $rv{len} = $1;
        }
    }
    return (\%rv, $data);
}

# copies a file from one Perlbal to another utilizing HTTP
sub http_copy {
    my %opts = @_;
    my ($sdevid, $ddevid, $fid, $intercopy_cb, $errref, $digest) =
        map { delete $opts{$_} } qw(sdevid
                                    ddevid
                                    fid
                                    callback
                                    errref
                                    digest
                                    );
    die if %opts;

    $fid = MogileFS::FID->new($fid) unless ref($fid);
    my $fidid = $fid->id;
    my $expected_clen = $fid->length;
    my $clen;
    my $content_md5 = '';
    my ($sconn, $dconn);
    my $fid_checksum = $fid->checksum;
    if ($fid_checksum && $fid_checksum->hashname eq "MD5") {
        # some HTTP servers may be able to verify Content-MD5 on PUT
        # and reject corrupted requests.  no HTTP server should reject
        # a request for an unrecognized header
        my $b64digest = encode_base64($fid_checksum->{checksum}, "");
        $content_md5 = "\r\nContent-MD5: $b64digest";
    }

    $intercopy_cb ||= sub {};

    my $err_common = sub {
        my ($err, $msg) = @_;
        $$errref = $err if $errref;
        $sconn->close($err) if $sconn;
        $dconn->close($err) if $dconn;
        return error($msg);
    };

    # handles setting unreachable magic; $error->(reachability, "message")
    my $error_unreachable = sub {
        return $err_common->("src_error", "Fid $fidid unreachable while replicating: $_[0]");
    };

    my $dest_error = sub {
        return $err_common->("dest_error", $_[0]);
    };

    my $src_error = sub {
        return $err_common->("src_error", $_[0]);
    };

    # get some information we'll need
    my $sdev = Mgd::device_factory()->get_by_id($sdevid);
    my $ddev = Mgd::device_factory()->get_by_id($ddevid);

    return error("Error: unable to get device information: source=$sdevid, destination=$ddevid, fid=$fidid")
        unless $sdev && $ddev;

    my $s_dfid = MogileFS::DevFID->new($sdev, $fid);
    my $d_dfid = MogileFS::DevFID->new($ddev, $fid);

    my ($spath, $dpath) = (map { $_->uri_path } ($s_dfid, $d_dfid));
    my ($shost, $dhost) = (map { $_->host     } ($sdev, $ddev));

    my ($shostip, $sport) = ($shost->ip, $shost->http_port);
    if (MogileFS::Config->config("repl_use_get_port")) {
        $sport = $shost->http_get_port;
    }
    my ($dhostip, $dport) = ($dhost->ip, $dhost->http_port);
    unless (defined $spath && defined $dpath && defined $shostip && defined $dhostip && $sport && $dport) {
        # show detailed information to find out what's not configured right
        error("Error: unable to replicate file fid=$fidid from device id $sdevid to device id $ddevid");
        error("       http://$shostip:$sport$spath -> http://$dhostip:$dport$dpath");
        return 0;
    }

    my $put = "PUT $dpath HTTP/1.0\r\nConnection: keep-alive\r\n" .
              "Content-length: $expected_clen$content_md5\r\n\r\n";

    # need by webdav servers, like lighttpd...
    $ddev->vivify_directories($d_dfid->url);

    # call a hook for odd casing completely different source data
    # for specific files.
    my $shttphost;
    MogileFS::run_global_hook('replicate_alternate_source',
                              $fid, \$shostip, \$sport, \$spath, \$shttphost);

    my $durl = "http://$dhostip:$dport$dpath";
    my $surl = "http://$shostip:$sport$spath";
    # okay, now get the file
    my %sopts = ( ip => $shostip, port => $sport );

    my $get = "GET $spath HTTP/1.0\r\nConnection: keep-alive\r\n";
    # plugin set a custom host.
    $get .= "Host: $shttphost\r\n" if $shttphost;

    my ($sock, $dsock);
    my ($wcount, $bytes_to_read, $written, $remain);
    my ($stries, $dtries) = (0, 0);
    my ($sres, $data, $bytes);

retry:
    $sconn->close("retrying") if $sconn;
    $dconn->close("retrying") if $dconn;
    $dconn = undef;
    $stries++;
    $sconn = $shost->http_conn_get(\%sopts)
        or return $src_error->("Unable to create source socket to $shostip:$sport for $spath");
    $sock = $sconn->sock;
    unless ($sock->write("$get\r\n")) {
        goto retry if $sconn->retryable && $stries == 1;
        return $src_error->("Pipe closed retrieving $spath from $shostip:$sport");
    }

    # we just want a content length
    ($sres, $data) = read_headers($sock, $intercopy_cb);
    unless ($sres) {
        goto retry if $sconn->retryable && $stries == 1;
        return $error_unreachable->("Error: Resource $surl failed to return an HTTP response");
    }
    unless ($sres->{code} >= 200 && $sres->{code} <= 299) {
        return $error_unreachable->("Error: Resource $surl failed: HTTP $sres->{code}");
    }
    $clen = $sres->{len};

    return $error_unreachable->("File $spath has unexpected content-length of $clen, not $expected_clen")
        if $clen != $expected_clen;

    # open target for put
    $dtries++;
    $dconn = $dhost->http_conn_get
        or return $dest_error->("Unable to create dest socket to $dhostip:$dport for $dpath");
    $dsock = $dconn->sock;

    unless ($dsock->write($put)) {
        goto retry if $dconn->retryable && $dtries == 1;
        return $dest_error->("Pipe closed during write to $dpath on $dhostip:$dport");
    }

    # now read data and print while we're reading.
    $bytes = length($data);
    ($written, $remain) = (0, $clen);
    $bytes_to_read = 1024*1024;  # read 1MB at a time until there's less than that remaining
    $bytes_to_read = $remain if $remain < $bytes_to_read;
    $wcount = 0;

    while ($bytes_to_read) {
        unless (defined $bytes) {
read_again:
            $bytes = sysread($sock, $data, $bytes_to_read);
            unless (defined $bytes) {
                if ($!{EAGAIN} || $!{EINTR}) {
                    wait_for_readability(fileno($sock), SOCK_TIMEOUT) and
                        goto read_again;
                }
                return $src_error->("error reading midway through source: $!");
            }
            if ($bytes == 0) {
                return $src_error->("EOF reading midway through source: $!");
            }
        }

        # now we've read in $bytes bytes
        $remain -= $bytes;
        $bytes_to_read = $remain if $remain < $bytes_to_read;
        $digest->add($data) if $digest;

        my $data_len = $bytes;
        $bytes = undef;
        my $data_off = 0;
        while (1) {
            my $wbytes = syswrite($dsock, $data, $data_len, $data_off);
            unless (defined $wbytes) {
                # it can take two writes to determine if a socket is dead
                # (TCP_NODELAY and TCP_CORK are (and must be) zero here)
                goto retry if (!$wcount && $dconn->retryable && $dtries == 1);
                return $dest_error->("Error: syswrite failed after $written bytes with: $!; failed putting to $dpath");
            }
            $wcount++;
            $written += $wbytes;
            $intercopy_cb->();
            last if ($data_len == $wbytes);

            $data_len -= $wbytes;
            $data_off += $wbytes;
        }

        die if $bytes_to_read < 0;
    }

    # source connection drained, return to pool
    if ($sres->{keep}) {
        $shost->http_conn_put($sconn);
        $sconn = undef;
    } else {
        $sconn->close("http_close");
    }

    # callee will want this digest, too, so clone as "digest" is destructive
    $digest = $digest->clone->digest if $digest;

    if ($fid_checksum) {
        if ($digest ne $fid_checksum->{checksum}) {
            my $expect = $fid_checksum->hexdigest;
            $digest = unpack("H*", $digest);
            return $src_error->("checksum mismatch on GET: expected: $expect actual: $digest");
        }
    }

    # now read in the response line (should be first line)
    my ($dres, $ddata) = read_headers($dsock, $intercopy_cb);
    unless ($dres) {
        goto retry if (!$wcount && $dconn->retryable && $dtries == 1);
        return $dest_error->("Error: HTTP response line not recognized writing to $durl");
    }

    # drain the response body if there is one
    # there may be no dres->{len}/Content-Length if there is no body
    my $dlen = ($dres->{len} || 0) - length($ddata);
    if ($dlen > 0) {
        my $r = $dsock->read($data, $dlen); # dres->{len} should be tiny
        if (defined $r) {
            if ($r != $dlen) {
                Mgd::error("Failed to read $r of Content-Length:$dres->{len} bytes for PUT response on $durl");
                $dres->{keep} = 0;
            }
        } else {
            Mgd::error("Failed to read Content-Length:$dres->{len} bytes for PUT response on $durl ($!)");
            $dres->{keep} = 0;
        }
    } elsif ($dlen < 0) {
        Mgd::error("strange response Content-Length:$dres->{len} with ".
                    length($ddata) .
                    " extra bytes for PUT response on $durl ($!)");
        $dres->{keep} = 0;
    }

    # return the connection back to the connection pool
    if ($dres->{keep}) {
        $dhost->http_conn_put($dconn);
        $dconn = undef;
    } else {
        $dconn->close("http_close");
    }

    if ($dres->{code} >= 200 && $dres->{code} <= 299) {
        if ($digest) {
            my $alg = ($fid_checksum && $fid_checksum->hashname) || $fid->class->hashname;

            if ($ddev->{reject_bad_md5} && ($alg eq "MD5")) {
                # dest device would've rejected us with a error,
                # no need to reread the file
                return 1;
            }
            my $httpfile = MogileFS::HTTPFile->at($durl);
            my $actual = $httpfile->digest($alg, $intercopy_cb);
            if ($actual ne $digest) {
                my $expect = unpack("H*", $digest);
                $actual = unpack("H*", $actual);
                return $dest_error->("checksum mismatch on PUT, expected: $expect actual: $digest");
            }
        }
        return 1;
    }
    return $dest_error->("Got HTTP status code $dres->{code} PUTing to $durl");
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

__END__

=head1 NAME

MogileFS::Worker::Replicate -- replicates files

=head1 OVERVIEW

This process replicates files enqueued in B<file_to_replicate> table.

The replication policy (which devices to replicate to) is pluggable,
but only one policy comes with the server.  See
L<MogileFS::ReplicationPolicy::MultipleHosts>

=head1 SEE ALSO

L<MogileFS::Worker>

L<MogileFS::ReplicationPolicy>

L<MogileFS::ReplicationPolicy::MultipleHosts>

