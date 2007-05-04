package Mogstored::ChildProcess::FIDSizes;
use strict;
use base 'Mogstored::ChildProcess';
use warnings;
use Errno qw(ENOENT);

# Gearman version 1.06: bug fix with closing pipe to parent.  we don't actually use Gearman::Client
# object, but it comes with the updated Gearman::Worker which we do care about
use Gearman::Client 1.06;
use Gearman::Worker;

use Storable ();

# Note: in this case, this module is loaded *before* the fork (which
# happens in Gearman::Server's start_worker) in the parent mogstored's
# process, so be careful nothing heavy/gross is added to the FIDSizes
# worker.

my $docroot;
my $worker;

sub pre_exec_init {
    $ENV{MOG_DOCROOT} = Perlbal->service('mogstored')->{docroot};

}

sub run {
    $0 = "mogstored [fidsizes]";
    $worker = Gearman::Worker->new;
    $worker->register_function(fid_sizes => \&gw_fidsizes);
    $docroot = $ENV{MOG_DOCROOT}
    or die "MOG_DOCROOT environment variable not set";
    while (1) {
        $worker->work();
    }
}

sub gw_fidsizes {
    my $job = shift;
    my $args = Storable::thaw($job->arg);
    my ($start, $end, $devices) = @$args;

    my @output;
    foreach my $device (@$devices) {
        my $entries = do_one_device($start, $end, "$docroot/dev$device");
        push @output, [$device, $entries];
    }
    return \Storable::nfreeze(\@output);
}

sub do_one_device {
    my ($start, $end, $base) = @_;

    die "'$base' isn't a directory"
        unless -d $base;

    die "start and end are not both defined"
        unless length $start && length $end;

    die "end cannot come before start"
        unless $end >= $start;

    die "start out of range"
        if $start > 9_999_999_999;

    die "end out of range"
        if $end   > 9_999_999_999;

    die "start less than 0"
        if $start < 0;

    die "end less than 0"
        if $end   < 0;

    my $hdir_start = int($start / 1000);
    my $hdir_end = int($end / 1000);

    my $file_start = $start % 1000;
    my $file_end   = $end   % 1000;

    my @files;

    for (my $hdir = $hdir_start; $hdir <= $hdir_end; $hdir++) {
        my $nfid = sprintf '%07d', $hdir;
        my ($b, $mmm, $ttt) = ( $nfid =~ m{(\d)(\d{3})(\d{3})} );

        my $hdir_path = "$base/$b/$mmm/$ttt";

        my $rv = opendir(my $dh, "$base/$b/$mmm/$ttt");
        unless ($rv) {
            if ($! != ENOENT) {
                die "Unable to read directory $hdir_path: $!\n";
            }
            next;
        }

        foreach my $file (sort readdir($dh)) {
            next if ($file eq '.' || $file eq '..');
            unless ($file =~ m/\Q$nfid\E(\d{3})\.fid/) {
                warn "Spurious file during readdir: $hdir_path/$file\n";
                next;
            }

            my $hhh = $1;
            my ($fid) = ($nfid . $hhh) =~ /^0*(\d+)$/;

            next if (($hdir == $hdir_start && $hhh < $file_start) ||
                     ($hdir == $hdir_end && $hhh > $file_end));

            my $filepath = "$hdir_path/$file";
            my $size = (stat($filepath))[7];

            push @files, [$fid, $size];
            die "Results too large" if @files > 10_000;
        }
    }

    return \@files;
}

1;
