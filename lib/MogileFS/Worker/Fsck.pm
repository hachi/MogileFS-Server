package MogileFS::Worker::Fsck;

use strict;
use base 'MogileFS::Worker';
use fields (
            'last_stop_check',  # unixtime 'should_stop_running' last called
            );
use MogileFS::Util qw(every error);
use List::Util ();
use Time::HiRes ();

use constant SUCCESS => 0;
use constant TEMPORARY => 1;
use constant PERMANENT => 2;
use constant REPLICATE => 3;

sub watchdog_timeout { 30 }

sub work {
    my $self = shift;

    my $run_count = 0;

    every(5.0, sub {
        my $sleep_set = shift;
        $self->parent_ping;

        # see if we're even enabled for this host.
        return unless $self->should_be_running;

        # checking doesn't go well if the monitor job hasn't actively started
        # marking things as being available
        unless ($self->monitor_has_run) {
            # only warn on runs after the first.  gives the monitor job some time to work
            # before we throw a message.
            error("waiting for monitor job to complete a cycle before beginning checking")
                if $run_count++ > 0;
            return;
        }

        my $sto        = Mgd::get_store();
        my $max_check  = MogileFS::Config->server_setting('fsck_highest_fid_checked') || 0;
        my $opt_nostat = MogileFS::Config->server_setting('fsck_opt_skip_stat')       || 0;
        my @fids       = $sto->get_fids_above_id($max_check, 100);

        unless (@fids) {
            warn "[fsck] no fids to check...\n";
            return;
        }

        MogileFS::FID->mass_load_devids(@fids);

        my $new_max;
        foreach my $fid (@fids) {
            $self->still_alive;
            last if $self->should_stop_running;
            last unless $self->check_fid($fid, no_stat => $opt_nostat);
            $new_max = $fid->id;
        }

        MogileFS::Config->set_server_setting('fsck_highest_fid_checked', $new_max) if $new_max;

        $sleep_set->(0); # don't sleep in next round.
    });
}

# this version is accurate,
sub should_be_running {
    my $self = shift;
    my $fhost = MogileFS::Config->server_setting('fsck_host')
        or return;
    return $fhost eq MogileFS::Config->hostname;
}

# this version is sloppy, optimized for speed.  only checks db every 5 seconds.
sub should_stop_running {
    my $self = shift;
    my $now  = time();
    return 0 if $now < ($self->{last_stop_check} || 0) + 5;
    $self->{last_stop_check} = $now;
    return ! $self->should_be_running;
}

# given a $fid (MogileFS::FID, with pre-populated ->devids data)
# return 0 if reachability problems.
# return 1 if fid was checked (regardless of there being problems or not)
#   if no problems, no action.
#   if problems, log & enqueue fixes
sub check_fid {
    my ($self, $fid, %opts) = @_;
    my $opt_no_stat = delete $opts{no_stat};
    die "badopts" if %opts;

    # first, see if the assumed devids meet the replication policy for
    # the fid's class.
    unless ($fid->devids_meet_policy) {
        printf("FID %d (len=%d; cldid=%d) doesn't meet repl policy (on: %s)\n",
               $fid->id,
               $fid->length,
               $fid->classid,
               join(",", $fid->devids),
               );

        # TODO: does this do it immediately, or later? should decide
        # on whether there's 1 copy or more.  or do we check number of
        # good copies first, in this case?:
        $fid->enqueue_for_replication;

        # TODO: log to fsck_log
        # ....

        return 1;
    }

    # in the fast case, do nothing else (don't check if assumed file
    # locations are actually there).  in the fast case, all we do is
    # check the replication policy, which is already done, so finish.
    return 1 if $opt_no_stat;


    # **********************************************************************
    # FIXME: temporary, until statting is done...
    # **********************************************************************

    # printf("fid %d is good.\n", $fid->id);
    return 1;

    # iterate and do HEAD requests to determine some basic information about the file
    my %devs;
    foreach my $devid ($fid->devids) {
        # setup and do the request.  these failures are total failures in that we expect
        # them to work again later, as it's probably transient and will persist no matter
        # how many paths we try.
        my $dfid = MogileFS::DevFID->new($devid, $fid);
        my $path = $dfid->get_url
            or die "FIXME";
        # TODO: use side-channel?  eh, why?  LWP + ConnCache good enough for now.
        my $ua = LWP::UserAgent->new(timeout => 3)
            or die "FIXME";
        my $resp = $ua->head($path);

        # at this point we're going to assume that any error is based on the device alone
        # so we want to store the status and not return
        if ($resp->is_success) {
            # great, check the size against what's in the database
            if ($resp->header('Content-Length') == $fid->length) {
                #yay
            } else {
                #shit.
            }

        } else {
            # easy one, the request failed for some reason, 500 would tend to imply that the
            # mogstored is having issues so we should try again later, whereas a 404 is a
            # total and permanent failure
            #error("check_fid($fid, $level): " . $resp->code . " on device $devid");

            if ($resp->code == 404) {
                #fucked!
            } else {
                # just unreachable
                warn "TODO: foo is unreachable\n";
                return 0;
            }
        }
    }

    return 1;
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
