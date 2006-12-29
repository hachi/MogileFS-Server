package MogileFS::Worker::Replicate;
# replicates files around

use strict;
use base 'MogileFS::Worker';
use fields (
            'fidtodo',   # hashref { fid => 1 }
            );

use List::Util ();
use MogileFS::Util qw(error every debug);
use MogileFS::Class;

# setup the value used in a 'nexttry' field to indicate that this item will never
# actually be tried again and require some sort of manual intervention.
use constant ENDOFTIME => 2147483647;

# { fid => lastcheck }; instructs us not to replicate this fid... we will clear
# out fids from this list that are expired
my %fidfailure;

# { fid => 1 }; used to keep track of fids we find in the unreachable_fids table
my %unreachable;
my $dbh;

sub end_of_time { ENDOFTIME; }

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);
    $self->{fidtodo} = {};
    return $self;
}

sub process_line {
    my ($self, $lineref) = @_;

    if ($$lineref =~ /^repl_was_done (\d+)/) {
        delete $self->{fidtodo}{$1};
        return 1;
    }

    if ($$lineref =~ /^repl_unreachable (\d+)/) {
        $unreachable{$1} = 1;
        return 1;
    }

    # telnet to main port and do:
    #    !to replicate repl_compat {0,1}
    # to change it in realtime, without restarting.
    if ($$lineref =~ /^repl_compat (\d+)/) {
        MogileFS::Config->set_config("old_repl_compat", $1);
        return 1;
    }

    return 0;
}

# replicator wants
sub watchdog_timeout { 30; }

sub work {
    my $self = shift;

    # give the monitor job 15 seconds to give us an update
    my $warn_after = time() + 15;

    every(2.0, sub {
        $self->parent_ping;

        # replication doesn't go well if the monitor job hasn't actively started
        # marking things as being available
        unless ($self->monitor_has_run) {
            error("waiting for monitor job to complete a cycle before beginning replication")
                if time() > $warn_after;
            return;
        }

        $self->validate_dbh;
        $dbh = $self->get_dbh or return 0;

        # update our unreachable fid list... we consider them good for 15 minutes
        my $urfids = $dbh->selectall_arrayref('SELECT fid, lastupdate FROM unreachable_fids');
        die $dbh->errstr if $dbh->err;
        foreach my $r (@{$urfids || []}) {
            my $nv = $r->[1] + 900;
            unless ($fidfailure{$r->[0]} && $fidfailure{$r->[0]} < $nv) {
                # given that we might have set it below to a time past the unreachable
                # 15 minute timeout, we want to only overwrite %fidfailure's idea of
                # the expiration time if we are extending it
                $fidfailure{$r->[0]} = $nv;
            }
            $unreachable{$r->[0]} = 1;
        }

        # this finds stuff to replicate based on its record in the needs_replication table
        $self->replicate_using_torepl_table;

        # this finds stuff to replicate based on the devcounts.  (old style)
        if (MogileFS::Config->config("old_repl_compat")) {
            $self->replicate_using_devcounts;
        }

    });
}

sub replicate_using_torepl_table {
    my $self = shift;

    # find some fids to replicate, prioritize based on when they should be tried
    my $LIMIT = 1000;
    my $to_repl_map = $dbh->selectall_hashref(qq{
        SELECT fid, fromdevid, failcount, flags, nexttry
        FROM file_to_replicate
        WHERE nexttry <= UNIX_TIMESTAMP()
        ORDER BY nexttry
        LIMIT $LIMIT
    }, "fid");
    if ($dbh->err) {
        error("Database error selecting fids to replicate: " . $dbh->errstr);
        return;
    }

    # get random list of hashref of things to do:
    my $to_repl = [ List::Util::shuffle(values %$to_repl_map) ];
    return unless @$to_repl;

    # sort our priority list in terms of 0s (immediate, only 1 copy), 1s (immediate replicate,
    # but we already have 2 copies), and big numbers (unixtimestamps) of things that failed.
    # but because sort is stable, these are random within their 0/1/big classes.
    @$to_repl = sort {
        ($a->{nexttry} < 1000 || $b->{nexttry} < 1000) ? ($a->{nexttry} <=> $b->{nexttry}) : 0
    } @$to_repl;

    foreach my $todo (@$to_repl) {
        my $fid = $todo->{fid};

        my $errcode;
        my ($status, $unlock) = replicate($dbh, $fid,
                                          errref       => \$errcode,
                                          no_unlock    => 1,   # to make it return an $unlock subref
                                          source_devid => $todo->{fromdevid},
                                          );
        if ($status) {
            # $status is either 0 (failure, handled below), 1 (success, we actually
            # replicated this file), or 2 (success, but someone else replicated it).

            # when $staus == 2, this delete is unnecessary normally
            # (somebody else presumably already deleted it if they
            # also replicated it), but in the case of running with old
            # replicators from previous versions, -or- simply if the
            # other guy's delete failed, this cleans it up....
            $dbh->do("DELETE FROM file_to_replicate WHERE fid=?", undef, $fid);
            $unlock->() if $unlock;
            next;
        }

        # ERROR CASES:

        # README: please keep this up to date if you update the replicate() function so we ensure
        # that this code always does the right thing
        #
        # -- HARMLESS --
        # failed_getting_lock        => harmless.  skip.  somebody else probably doing.
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
        # no_devices                 => no devices are configured.  at all.  why are we replicating something?
        #                               how did something come into being since you can't delete devices?

        # bail if we failed getting the lock, that means someone else probably
        # already did it, so we should just move on
        if ($errcode eq 'failed_getting_lock') {
            $unlock->() if $unlock;
            next;
        }

        # logic for setting the next try time appropriately
        my $update_nexttry = sub {
            my ($type, $delay) = @_;
            if ($type eq 'end_of_time') {
                # special; update to a time that won't happen again, as we've encountered a scenario
                # in which case we're really hosed
                $dbh->do("UPDATE file_to_replicate SET nexttry = " . ENDOFTIME . ", failcount = failcount + 1 WHERE fid = ?",
                         undef, $fid);
            } else {
                my $extra = $type eq 'offset' ? 'UNIX_TIMESTAMP() +' : '';
                $dbh->do("UPDATE file_to_replicate SET nexttry = $extra ?, failcount = failcount + 1 WHERE fid = ?",
                         undef, $delay+0, $fid);
            }
            error("Failed setting nexttry of fid $fid to $type $delay: " . $dbh->errstr)
                if $dbh->err;
        };

        # now let's handle any error we want to consider a total failure; do not
        # retry at any point.  push this file off to the end so someone has to come
        # along and figure out what went wrong.
        if ($errcode eq 'no_source' || $errcode eq 'no_devices') {
            $update_nexttry->( end_of_time => 1 );
            $unlock->() if $unlock;
            next;
        }

        # at this point, the rest of the errors require exponential backoff.  define what this means
        # as far as failcount -> delay to next try.
        # 15s, 1m, 5m, 30m, 1h, 2h, 4h, 8h, 24h, 24h, 24h, 24h, ...
        my @backoff = qw( 15 60 300 1800 3600 7200 14400 28800 );
        $update_nexttry->( offset => int(($backoff[$todo->{failcount}] || 86400) * (rand(0.4) + 0.8)) );
        $unlock->() if $unlock;
    }

}

sub replicate_using_devcounts {
    my $self = shift;

    MogileFS::Class->foreach(sub {
        my $mclass = shift;
        my ($dmid, $classid, $min, $policy_class) = map { $mclass->$_ } qw(domainid classid mindevcount policy_class);

        debug("Checking replication for dmid=$dmid, classid=$classid, min=$min");

        my $LIMIT = 1000;

        # try going from devcount of 1 up to devcount of $min-1
        $self->{fidtodo} = {};
        my $fixed = 0;
        my $attempted = 0;
        my $devcount = 1;
        while ($fixed < $LIMIT && $devcount < $min) {
            my $now = time();
            $self->still_alive;

            my $fids = $dbh->selectcol_arrayref("SELECT fid FROM file WHERE dmid=? AND classid=? ".
                                                "AND devcount = ? AND length IS NOT NULL ".
                                                "LIMIT $LIMIT", undef, $dmid, $classid, $devcount);
            die $dbh->errstr if $dbh->err;
            $self->{fidtodo}{$_} = 1 foreach @$fids;

            # increase devcount so we try to replicate the files at the next devcount
            $devcount++;

            # see if we have any files to replicate
            my $count = $fids ? scalar @$fids : 0;
            debug("  found $count for dmid=$dmid/classid=$classid/min=$min");
            next unless $count;

            # randomize the list so multiple daemons/threads working on
            # replicate at the same time don't all fight over the
            # same fids to move
            my @randfids = List::Util::shuffle(@$fids);

            debug("Need to replicate: $dmid/$classid: @$fids") if $Mgd::DEBUG >= 2;
            foreach my $fid (@randfids) {
                # now replicate this fid
                $attempted++;
                next unless $self->{fidtodo}{$fid};

                if ($fidfailure{$fid}) {
                    if ($fidfailure{$fid} < $now) {
                        delete $fidfailure{$fid};
                    } else {
                        next;
                    }
                }

                $self->read_from_parent;
                $self->still_alive;

                if (my $status = replicate($dbh, $fid, class => $mclass)) {
                    # $status is either 0 (failure, handled below), 1 (success, we actually
                    # replicated this file), or 2 (success, but someone else replicated it).
                    # so if it's 2, we just want to go to the next fid.  this file is done.
                    next if $status == 2;

                    # if it was no longer reachable, mark it reachable
                    if (delete $unreachable{$fid}) {
                        $dbh->do("DELETE FROM unreachable_fids WHERE fid = ?", undef, $fid);
                        die $dbh->errstr if $dbh->err;
                    }

                    # housekeeping
                    $fixed++;
                    $self->send_to_parent("repl_i_did $fid");

                    # status update
                    if ($Mgd::DEBUG >= 1 && $fixed % 20 == 0) {
                        my $ratio = $fixed/$attempted*100;
                        error(sprintf("replicated=$fixed, attempted=$attempted, ratio=%.2f%%", $ratio))
                            if $fixed % 20 == 0;
                    }
                } else {
                    # failed in replicate, don't retry for a minute
                    $fidfailure{$fid} = $now + 60;
                }
            }
        }
    });
}

# replicates $fid if its devcount is less than $min.  (eh, not quite)
#
# $policy_class is optional (perl classname representing replication policy).  if present, used.  if not, looked up based on $fid.
#
# README: if you update this sub to return a new error code, please update the
# appropriate callers to know how to deal with the errors returned.
sub replicate {
    my ($dbh, $fid, %opts) = @_;
    my $errref    = delete $opts{'errref'};
    my $mclass    = delete $opts{'class'};
    my $no_unlock = delete $opts{'no_unlock'};
    my $sdevid    = delete $opts{'source_devid'};
    die if %opts;

    # bool:  if source was explicitly requested by caller
    my $fixed_source = $sdevid ? 1 : 0;

    $mclass ||= MogileFS::Class->of_fid($fid);

    my $policy_class = $mclass->policy_class;
    eval "use $policy_class; 1;";
    if ($@) {
        return error("Failed to load policy class: $policy_class: $@");
    }

    my $lock;  # bool: whether we got the lock or not
    my $lockname = "mgfs:fid:$fid:replicate";
    my $unlock = sub {
        $dbh->selectrow_array("SELECT RELEASE_LOCK(?)", undef, $lockname)
            if $lock;
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

        my $ret = $rv ? $rv : error($errmsg);
        if ($no_unlock) {
            return ($ret, $unlock);
        } else {
            $unlock->();
            return $ret;
        }
    };

    # hashref of devid -> $device_row_href  (where devid is alive)
    my $devs = MogileFS::Device->map;
    return $retunlock->(0, "no_devices", "Device information from get_device_summary is empty")
        unless $devs && %$devs;

    $lock = $dbh->selectrow_array("SELECT GET_LOCK(?, 1)", undef, $lockname);
    return $retunlock->(0, "failed_getting_lock", "Unable to obtain lock $lockname")
        unless $lock;

    # learn what this devices file is already on
    my @on_devs;       # all devices fid is on, reachable or not.
    my @dead_devid;    # list of dead devids.  FIXME: do something with this?
    my @on_up_devid;   # subset of @on_devs:  just devs that are alive or readonly

    my $sth = $dbh->prepare("SELECT devid FROM file_on WHERE fid=?");
    $sth->execute($fid);
    die $dbh->errstr if $dbh->err;
    while (my ($devid) = $sth->fetchrow_array) {
        my $d = $devs->{$devid};
        push @on_devs, $d;
        unless ($d && $d->status =~ /^alive|readonly$/) {
            push @dead_devid, $devid;
            next;
        }
        push @on_up_devid, $devid;
    }

    return $retunlock->(0, "no_source",   "Source is no longer available replicating $fid") if @on_devs == 0;
    return $retunlock->(0, "source_down", "No alive devices available replicating $fid") if @on_up_devid == 0;

    # if they requested a specific source, that source must be up.
    if ($sdevid && ! grep { $_ == $sdevid} @on_up_devid) {
        return $retunlock->(0, "source_down", "Requested replication source device $sdevid not available");
    }

    my $ddevid;
    my %dest_failed;    # devid -> 1 for each devid we were asked to copy to, but failed.
    my %source_failed;  # devid -> 1 for each devid we had problems reading from.
    my $got_copy_request = 0;  # true once replication policy asks us to move something somewhere
    my $copy_err;

    while ($ddevid = $policy_class->replicate_to(
                                                 fid       => $fid,
                                                 on_devs   => \@on_devs, # all device objects fid is on, dead or otherwise
                                                 all_devs  => $devs,
                                                 failed    => \%dest_failed,
                                                 min       => $mclass->mindevcount,
                                                 ))
    {
        # they can return either a dev hashref/object or a devid number.  we want the number.
        $ddevid = $ddevid->{devid} if ref $ddevid;
        $got_copy_request = 1;

        # replication policy shouldn't tell us to put a file on a device
        # we've already told it that we've failed at.  so if we get that response,
        # the policy plugin is broken and we should terminate now.
        if ($dest_failed{$ddevid}) {
            return $retunlock->(0, "policy_error_doing_failed",
                                "replication policy told us to do something we already told it we failed at while replicating fid $fid");
        }

        # replication policy shouldn't tell us to put a file on a
        # device that it's already on.  that's just stupid.
        if (grep { $_->id == $ddevid } @on_devs) {
            return $retunlock->(0, "policy_error_already_there",
                                "replication policy told us to put fid $fid on dev $ddevid, but it's already there!");
        }

        # find where we're replicating from
        unless ($fixed_source) {
            # TODO: use an observed good device+host as source to start.
            my @choices = grep { ! $source_failed{$_} } @on_up_devid;
            return $retunlock->(0, "source_down", "No devices available replicating $fid") unless @choices;
            $sdevid = @choices[int(rand(scalar @choices))];
        }

        my $worker = MogileFS::ProcManager->is_child or die;
        my $rv = http_copy(
                           sdevid       => $sdevid,
                           ddevid       => $ddevid,
                           fid          => $fid,
                           expected_len => undef,  # FIXME: get this info to pass along
                           errref       => \$copy_err,
                           callback     => sub { $worker->still_alive; },
                           );
        die "Bogus error code: $copy_err" if !$rv && $copy_err !~ /^(?:src|dest)_error$/;

        unless ($rv) {
            error("Failed copying fid $fid from devid $sdevid to devid $ddevid (error type: $copy_err)");
            if ($copy_err eq "src_error") {
                $source_failed{$sdevid} = 1;

                if ($fixed_source) {
                    # there can't be any more retries, as this source
                    # is busted and is the only one we wanted.
                    return $retunlock->(0, "copy_error", "error copying fid $fid from devid $sdevid during replication");
                }

            } else {
                $dest_failed{$ddevid} = 1;
            }
            next;
        }

        my $dfid = MogileFS::DevFID->new($ddevid, $fid);
        $dfid->add_to_db;

        push @on_devs, $devs->{$ddevid};
    }

    # returning 0, not undef, means replication policy is happy and we're done.
    if (defined $ddevid && ! $ddevid) {
        return $retunlock->(1) if $got_copy_request;
        return $retunlock->(2);  # some other process got to it first.  policy was happy immediately.
    }

    # TODO: if we're on only 1 device and they returned undef, let's
    # try and put it SOMEWHERE just to make ourselves happy, even if
    # it it doesn't obey policy?  or is that decision itself policy?
    # unfortunately, there's no way for the replication policy to say
    # "replicate to 6, but I don't like that, so don't count it as good"

    if ($got_copy_request) {
        return $retunlock->(0, "copy_error", "errors copying fid $fid during replication");
    } else {
        return $retunlock->(0, "policy_no_suggestions", "replication policy ran out of suggestions for us replicating fid $fid");
    }
}

# copies a file from one Perlbal to another utilizing HTTP
sub http_copy {
    my %opts = @_;
    my ($sdevid, $ddevid, $fid, $expected_clen, $intercopy_cb, $errref) =
        map { delete $opts{$_} } qw(sdevid
                                    ddevid
                                    fid
                                    expected_len
                                    callback
                                    errref
                                    );
    die if %opts;


    $intercopy_cb ||= sub {};

    # handles setting unreachable magic; $error->(reachability, "message")
    my $error_unreachable = sub {
        my $worker = MogileFS::ProcManager->is_child;
        $worker->send_to_parent(":repl_unreachable $fid");

        # update database table
        Mgd::validate_dbh();
        my $dbh = Mgd::get_dbh();
        $dbh->do("REPLACE INTO unreachable_fids VALUES ($fid, UNIX_TIMESTAMP())");

        $$errref = "src_error" if $errref;
        return error("Fid $fid unreachable while replicating: $_[0]");
    };

    my $dest_error = sub {
        $$errref = "dest_error" if $errref;
        error($_[0]);
        return 0;
    };

    my $src_error = sub {
        $$errref = "src_error" if $errref;
        error($_[0]);
        return 0;
    };

    # get some information we'll need
    my $sdev = MogileFS::Device->of_devid($sdevid);
    my $ddev = MogileFS::Device->of_devid($ddevid);

    return error("Error: unable to get device information: source=$sdevid, destination=$ddevid, fid=$fid")
        unless $sdev && $ddev && $sdev->exists && $ddev->exists;

    my $s_dfid = MogileFS::DevFID->new($sdev, $fid);
    my $d_dfid = MogileFS::DevFID->new($ddev, $fid);

    my ($spath, $dpath) = (map { $_->uri_path } ($s_dfid, $d_dfid));
    my ($shost, $dhost) = (map { $_->host     } ($sdev, $ddev));

    my ($shostip, $sport) = ($shost->ip, $shost->http_port);
    my ($dhostip, $dport) = ($dhost->ip, $dhost->http_port);
    unless (defined $spath && defined $dpath && defined $shostip && defined $dhostip && $sport && $dport) {
        # show detailed information to find out what's not configured right
        error("Error: unable to replicate file fid=$fid from device id $sdevid to device id $ddevid");
        error("       http://$shostip:$sport$spath -> http://$dhostip:$dport$dpath");
        return 0;
    }

    # setup our pipe error handler, in case we get closed on
    my $pipe_closed = 0;
    local $SIG{PIPE} = sub { $pipe_closed = 1; };

    # okay, now get the file
    my $sock = IO::Socket::INET->new(PeerAddr => $shostip, PeerPort => $sport, Timeout => 2)
        or return $src_error->("Unable to create source socket to $shostip:$sport for $spath");
    $sock->write("GET $spath HTTP/1.0\r\n\r\n");
    return error("Pipe closed retrieving $spath from $shostip:$sport")
        if $pipe_closed;

    # we just want a content length
    my $clen;
    # FIXME: this can block.  needs to timeout.
    while (defined (my $line = <$sock>)) {
        $line =~ s/[\s\r\n]+$//;
        last unless length $line;
        if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            # make sure we get a good response
            return $error_unreachable->("Error: Resource http://$shostip:$sport$spath failed: HTTP $1")
                unless $1 >= 200 && $1 <= 299;
        }
        next unless $line =~ /^Content-length:\s*(\d+)\s*$/i;
        $clen = $1;
    }
    return $error_unreachable->("File $spath has a content-length of 0; unable to replicate")
        unless $clen;
    return $error_unreachable->("File $spath has unexpected content-length of $clen, not $expected_clen")
        if defined $expected_clen && $clen != $expected_clen;

    # open target for put
    my $dsock = IO::Socket::INET->new(PeerAddr => $dhostip, PeerPort => $dport, Timeout => 2)
        or return $dest_error->("Unable to create dest socket to $dhostip:$dport for $dpath");
    $dsock->write("PUT $dpath HTTP/1.0\r\nContent-length: $clen\r\n\r\n")
        or return $dest_error->("Unable to write data to $dpath on $dhostip:$dport");
    return $dest_error->("Pipe closed during write to $dpath on $dhostip:$dport")
        if $pipe_closed;

    # now read data and print while we're reading.
    my ($data, $written, $remain) = ('', 0, $clen);
    my $bytes_to_read = 1024*1024;  # read 1MB at a time until there's less than that remaining
    $bytes_to_read = $remain if $remain < $bytes_to_read;
    my $finished_read = 0;

    while (!$pipe_closed && (my $bytes = $sock->read($data, $bytes_to_read))) {
        # now we've read in $bytes bytes
        $remain -= $bytes;
        $bytes_to_read = $remain if $remain < $bytes_to_read;

        my $wbytes = $dsock->send($data);
        $written  += $wbytes;
        return $dest_error->("Error: wrote $wbytes; expected to write $bytes; failed putting to $dpath")
            unless $wbytes == $bytes;
        $intercopy_cb->();

        die if $bytes_to_read < 0;
        next if $bytes_to_read;
        $finished_read = 1;
        last;
    }
    return $dest_error->("closed pipe writing to destination")     if $pipe_closed;
    return $src_error->("error reading midway through source: $!") unless $finished_read;

    # now read in the response line (should be first line)
    my $line = <$dsock>;
    if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
        return 1 if $1 >= 200 && $1 <= 299;
        return $dest_error->("Got HTTP status code $1 PUTing to http://$dhostip:$dport$dpath");
    } else {
        return $dest_error->("Error: HTTP response line not recognized writing to http://$dhostip:$dport$dpath: $line");
    }
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
