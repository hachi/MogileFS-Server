package MogileFS::Worker::Replicate;
# deletes files

use strict;
use base 'MogileFS::Worker';

use POSIX ":sys_wait_h"; # argument for waitpid
use POSIX;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    return $self;
}

sub work {
    my $self = shift;
    my $psock = $self->{psock};

    my $parse_parent_response = sub {
        # now see what was in our message queue
        while (defined (my $line = <$psock>)) {
            $line =~ s/\r?\n$//;
            last if $line eq '.';

            # now find out what command this is?
            if ($line =~ /^repl_was_done (\d+)/ && $_[0]) {
                delete $_[0]->{$1};
            } elsif ($line eq 'shutdown') {
                exit 0;
            }
        }
    };

    # { fid => lastcheck }; instructs us not to replicate this fid... we will clear
    # out fids from this list that are expired
    my %fidfailure;

    # { fid => 1 }; used to keep track of fids we find in the unreachable_fids table
    my %unreachable;

    my $sleep = 2;
    while (1) {
        sleep $sleep;
        $self->validate_dbh;
        my $dbh = $self->get_dbh or return 0;

        # general report in to parent
        $self->send_to_parent('repl_ping');
        $parse_parent_response->(undef);

        # start off assuming that we're going to get everything replicated and then take a break
        $sleep = 2;

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

        # get the min dev counts
        my %min = %{ Mgd::get_mindevcounts() };

        # iterate through each domain, replicating its contents
        foreach my $dmid (keys %min) {
            # iterate through each class, including the implicit class 0
            while (my ($classid, $min) = each %{$min{$dmid}}) {
                error("Checking replication for dmid=$dmid, classid=$classid, min=$min")
                    if $Mgd::DEBUG >= 1;

                my $LIMIT = 1000;

                # try going from devcount of 1 up to devcount of $min-1
                my %fidtodo; # fid => 1
                my $fixed = 0;
                my $attempted = 0;
                my $devcount = 1;
                while ($fixed < $LIMIT && $devcount < $min) {
                    my $now = time();
                    my $fids = $dbh->selectcol_arrayref("SELECT fid FROM file WHERE dmid=? AND classid=? ".
                                                        "AND devcount = ? AND length IS NOT NULL ".
                                                        "LIMIT $LIMIT", undef, $dmid, $classid, $devcount);
                    die $dbh->errstr if $dbh->err;
                    $fidtodo{$_} = 1 foreach @$fids;

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
                    my @randfids = randlist(@$fids);

                    error("Need to replicate: $dmid/$classid: @$fids") if $Mgd::DEBUG >= 2;
                    foreach my $fid (@randfids) {
                        # now replicate this fid
                        $attempted++;
                        next unless $fidtodo{$fid};

                        if ($fidfailure{$fid}) {
                            if ($fidfailure{$fid} < $now) {
                                delete $fidfailure{$fid};
                            } else {
                                next;
                            }
                        }

                        if (my $status = replicate($dbh, $fid, $min)) {
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
                            $parse_parent_response->(\%fidtodo);

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

                # if we did 1000, we just want to jump to the next pass through all domains and classes without pausing
                $sleep = 0 if $fixed >= $LIMIT;
            }
        }
    }
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
