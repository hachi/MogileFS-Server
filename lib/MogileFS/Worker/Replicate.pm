package MogileFS::Worker::Replicate;
# replicates files around

use strict;
use base 'MogileFS::Worker';
use fields (
            'fidtodo',   # hashref { fid => 1 }
            );

use List::Util ();
use MogileFS::Util qw(error every);
use POSIX ":sys_wait_h"; # argument for waitpid
use POSIX;

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

    return 0;
}

sub work {
    my $self = shift;

    # { fid => lastcheck }; instructs us not to replicate this fid... we will clear
    # out fids from this list that are expired
    my %fidfailure;

    # { fid => 1 }; used to keep track of fids we find in the unreachable_fids table
    my %unreachable;

    every(2.0, sub {
        $self->parent_ping;

        $self->validate_dbh;
        my $dbh = $self->get_dbh or return 0;

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

        foreach_class(sub {
            my ($dmid, $classid, $min, $policy_class) = @_;

            error("Checking replication for dmid=$dmid, classid=$classid, min=$min")
                if $Mgd::DEBUG >= 1;

            my $LIMIT = 1000;

            # try going from devcount of 1 up to devcount of $min-1
            $self->{fidtodo} = {};
            my $fixed = 0;
            my $attempted = 0;
            my $devcount = 1;
            while ($fixed < $LIMIT && $devcount < $min) {
                my $now = time();
                my $fids = $dbh->selectcol_arrayref("SELECT fid FROM file WHERE dmid=? AND classid=? ".
                                                    "AND devcount = ? AND length IS NOT NULL ".
                                                    "LIMIT $LIMIT", undef, $dmid, $classid, $devcount);
                die $dbh->errstr if $dbh->err;
                $self->{fidtodo}{$_} = 1 foreach @$fids;

                # increase devcount so we try to replicate the files at the next devcount
                $devcount++;

                # see if we have any files to replicate
                my $count = $fids ? scalar @$fids : 0;
                error("  found $count for dmid=$dmid/classid=$classid/min=$min")
                    if $Mgd::DEBUG >= 1;
                next unless $count;

                # randomize the list so multiple daemons/threads working on
                # replicate at the same time don't all fight over the
                # same fids to move
                my @randfids = List::Util::shuffle(@$fids);

                error("Need to replicate: $dmid/$classid: @$fids") if $Mgd::DEBUG >= 2;
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

                    if (my $status = replicate($dbh, $fid, $min, $policy_class)) {
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
                        if ($fixed % 20 == 0) {
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
    });
}

# replicates $fid if its devcount is less than $min.
sub replicate {
    my ($dbh, $fid, $min, $policy_class) = @_;

    eval "use $policy_class; 1;";
    if ($@) {
        return error("Failed to load policy class: $policy_class: $@");
    }

    my $lockname = "mgfs:fid:$fid:replicate";
    my $lock = $dbh->selectrow_array("SELECT GET_LOCK(?, 1)", undef,
                                     $lockname);
    return error("Unable to obtain lock $lockname")
        unless $lock;

    # hashref of devid -> $device_row_href  (where devid is alive)
    my $devs = Mgd::get_device_summary();
    return error("Device information from get_device_summary is empty")
        unless $devs && %$devs;

    # learn what devices this file is already on
    my $on_count = 0;
    my @dead_devid;   # list of dead devids.  FIXME: do something with this?
    my @exist_devid;  # list of existing devids

    my $sth = $dbh->prepare("SELECT devid FROM file_on WHERE fid=?");
    $sth->execute($fid);
    die $dbh->errstr if $dbh->err;
    my @on_devs;
    while (my ($devid) = $sth->fetchrow_array) {
        my $d = $devs->{$devid};
        push @on_devs, $d;
        unless ($d && $d->{status} =~ /^alive|readonly$/) {
            push @dead_devid, $devid;
            next;
        }
        $on_count++;
        push @exist_devid, $devid;
    }

    my $retunlock = sub {
        my $rv = shift;
        $dbh->selectrow_array("SELECT RELEASE_LOCK(?)", undef, $lockname);
        return $rv ? $rv : error($_[0]);
    };

    # maybe somebody else got to it already?  let's double-check.
    if ($on_count >= $min) {
        my $devcount = $dbh->selectrow_array("SELECT devcount FROM file WHERE fid=?",
                                              undef, $fid);
        if ($devcount != $on_count) {
            return Mgd::update_fid_devcount($fid);
        }
        return $retunlock->(2);
    }

    return $retunlock->(0, "Source is no longer available replicating $fid") if $on_count == 0;
    return $retunlock->(0, "No eligible devices available replicating $fid") if @exist_devid == 0;

    my $ddevid;
    my $sdevid;
    my %failed;  # devid -> 1 for each devid we were asked to copy to, but failed.
    while ($ddevid = $policy_class->replicate_to(
                                                 fid       => $fid,
                                                 on_devs   => \@on_devs, # all device objects fid is on, dead or otherwise
                                                 all_devs  => $devs,
                                                 failed    => \%failed,
                                                 min       => $min,
                                                 ))
    {
        # they can return either a dev hashref/object or a devid number.  we want the number.
        $ddevid = $ddevid->{devid} if ref $ddevid;

        # replication policy shouldn't tell us to put a file on a device
        # we've already told it that we've failed at.  so if we get that response,
        # the policy plugin is broken and we should terminate now.
        if ($failed{$ddevid}) {
            return $retunlock->(0, "replication policy told us to do something we already told it we failed at while replicating fid $fid");
        }

        # replication policy shouldn't tell us to put a file on a
        # device that it's already on.  that's just stupid.
        if (grep { $_->{devid} == $ddevid } @on_devs) {
            return $retunlock->(0, "replication policy told us to put fid $fid on dev $ddevid, but it's already there!");
        }

        # TODO: use an observed good device as source.
        $sdevid ||= @exist_devid[int(rand(scalar @exist_devid))];

        my $rv = undef;
        if (MogileFS::Config->http_mode) {
            my $lastping = time();
            my $worker = MogileFS::ProcManager->is_child or die;
            $rv = http_copy($sdevid, $ddevid, $fid, sub {
                my $now = time();
                return if $now == $lastping; # ping once per second
                $worker->still_alive;
                $lastping = $now;
            });
        } else {
            my $root = Mgd::mog_root();
            my $dst_path = $root . "/" . make_path($ddevid, $fid);
            my $src_path = $root . "/" . make_path($sdevid, $fid);
            $rv = File::Copy::copy($src_path, $dst_path);
        }

        unless ($rv) {
            error("Failed copying fid $fid from devid $sdevid to devid $ddevid");
            $failed{$ddevid} = 1;
            next;
        }

        add_file_on($fid, $ddevid, 1);
        push @on_devs, $devs->{$ddevid};
    }

    # returning 0, not undef, means replication policy is happy and we're done.
    if (defined $ddevid && ! $ddevid) {
        return $retunlock->(1);
    }

    return $retunlock->(0, "replication policy ran out of suggestions for us replicating fid $fid");
}

# copies a file from one Perlbal to another utilizing HTTP
sub http_copy {
    my ($sdevid, $ddevid, $fid, $intercopy_cb) = @_;

    $intercopy_cb ||= sub {};

    # handles setting unreachable magic; $error->(reachability, "message")
    my $error = sub {
        if ($_[0]) {
            my $worker = MogileFS::ProcManager->is_child;
            $worker->send_to_parent("repl_unreachable $fid");

            # update database table
            Mgd::validate_dbh();
            my $dbh = Mgd::get_dbh();
            $dbh->do("REPLACE INTO unreachable_fids VALUES ($fid, UNIX_TIMESTAMP())");
        }
        return error($_[1]);
    };

    # get some information we'll need
    my $devs = Mgd::get_device_summary();
    my ($sdev, $ddev) = ($devs->{$sdevid}, $devs->{$ddevid});
    return error("Error: unable to get device information: source=$sdevid, destination=$ddevid, fid=$fid")
        unless ref $sdev && ref $ddev;
    my ($spath, $dpath) = (Mgd::make_http_path($sdevid, $fid),
                           Mgd::make_http_path($ddevid, $fid));
    my ($shost, $sport) = (Mgd::hostid_ip($sdev->{hostid}), Mgd::hostid_http_port($sdev->{hostid}));
    my ($dhost, $dport) = (Mgd::hostid_ip($ddev->{hostid}), Mgd::hostid_http_port($ddev->{hostid}));
    unless (defined $spath && defined $dpath && defined $shost && defined $dhost && $sport && $dport) {
        # show detailed information to find out what's not configured right
        error("Error: unable to replicate file fid=$fid from device id $sdevid to device id $ddevid");
        error("       http://$shost:$sport$spath -> http://$dhost:$dport$dpath");
        return 0;
    }

    # setup our pipe error handler, in case we get closed on
    my $pipe_closed = 0;
    local $SIG{PIPE} = sub { $pipe_closed = 1; };

    # okay, now get the file
    my $sock = IO::Socket::INET->new(PeerAddr => $shost, PeerPort => $sport, Timeout => 2)
        or return error("Unable to create socket to $shost:$sport for $spath");
    $sock->write("GET $spath HTTP/1.0\r\n\r\n");
    return error("Pipe closed retrieving $spath from $shost:$sport")
        if $pipe_closed;

    # we just want a content length
    my $clen;
    # FIXME: this can block.  needs to timeout.
    while (defined (my $line = <$sock>)) {
        $line =~ s/[\s\r\n]+$//;
        last unless length $line;
        if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            # make sure we get a good response
            return $error->(1, "Error: Resource http://$shost:$sport$spath failed: HTTP $1")
                unless $1 >= 200 && $1 <= 299;
        }
        next unless $line =~ /^Content-length:\s*(\d+)\s*$/i;
        $clen = $1;
    }
    return $error->(1, "File $spath has a content-length of 0; unable to replicate")
        unless $clen;

    # open target for put
    my $dsock = IO::Socket::INET->new(PeerAddr => $dhost, PeerPort => $dport, Timeout => 2)
        or return error("Unable to create socket to $dhost:$dport for $dpath");
    $dsock->write("PUT $dpath HTTP/1.0\r\nContent-length: $clen\r\n\r\n")
        or return error("Unable to write data to $dpath on $dhost:$dport");
    return error("Pipe closed during write to $dpath on $dhost:$dport")
        if $pipe_closed;

    # now read data and print while we're reading.
    my ($data, $written, $remain) = ('', 0, $clen);
    my $bytes_to_read = 1024*1024;  # read 1MB at a time until there's less than that remaining
    $bytes_to_read = $remain if $remain < $bytes_to_read;

    while (!$pipe_closed && (my $bytes = $sock->read($data, $bytes_to_read))) {
        # now we've read in $bytes bytes
        $remain -= $bytes;
        $bytes_to_read = $remain if $remain < $bytes_to_read;

        my $wbytes = $dsock->send($data);
        $written  += $wbytes;
        return error("Error: wrote $wbytes; expected to write $bytes; failed putting to $dpath")
            unless $wbytes == $bytes;
        $intercopy_cb->();
    }
    return error("Error: wrote $written bytes, expected to write $clen")
        unless $written == $clen;

    # now read in the response line (should be first line)
    my $line = <$dsock>;
    if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
        return 1 if $1 >= 200 && $1 <= 299;
        warn "Error: got a 404 in put: device not on host?: http://$dhost:$dport$dpath"
            if $1 == 404;
    } else {
        warn "Error: HTTP response line not recognized: $line";
    }
    return 0;
}

sub add_file_on {
    my ($fid, $devid, $no_lock) = @_;

    my $dbh = Mgd::get_dbh() or return 0;

    my $rv = $dbh->do("INSERT IGNORE INTO file_on SET fid=?, devid=?",
                      undef, $fid, $devid);
    if ($rv > 0) {
        return Mgd::update_fid_devcount($fid, $no_lock);
    } else {
        # was already on that device
        return 1;
    }
}

sub foreach_class {
    my $cb = shift;

    # get the min dev counts
    my %min = %{ Mgd::get_mindevcounts() };

    # iterate through each domain, replicating its contents
    foreach my $dmid (keys %min) {
        # iterate through each class, including the implicit class 0
        while (my ($classid, $min) = each %{$min{$dmid}}) {
            $cb->($dmid, $classid, $min, "MogileFS::ReplicationPolicy::MultipleHosts");
        }
    }

}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
