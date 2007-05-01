package Mogstored::ChildProcess::DiskUsage;
use strict;
use base 'Mogstored::ChildProcess';

my $docroot;

sub pre_exec_init {
    my $class = shift;
    $SIG{TERM} = 'DEFAULT'; # override custom one from earlier
    $ENV{MOG_DOCROOT} = Perlbal->service('mogstored')->{docroot};
}

sub run {
    $docroot = $ENV{MOG_DOCROOT};
    die "\$ENV{MOG_DOCROOT} not set"                unless $docroot;
    die "\$ENV{MOG_DOCROOT} not set to a directory" unless -d $docroot;

    # (runs in exec'd child process)
    $0 = "mogstored [diskusage]";
    select((select(STDOUT), $|++)[0]);

    while (1) {
        look_at_disk_usage();
        sleep 10;
    }
}

sub look_at_disk_usage {
    my $err = sub { warn "$_[0]\n"; };
    my $path = $ENV{MOG_DOCROOT};
    $path =~ s!/$!!;

    # find all devices below us
    my @devnum;
    if (opendir(D, $path)) {
        @devnum = grep { /^dev\d+$/ } readdir(D);
        closedir(D);
    } else {
        return $err->("Failed to open $path: $!");
    }

    foreach my $devnum (@devnum) {
        my $rval = `df -P -l -k $path/$devnum`;
        my $uperK = ($rval =~ /512-blocks/i) ? 2 : 1; # units per kB
        foreach my $l (split /\r?\n/, $rval) {
            next unless $l =~ /^(.+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.+)\s+(.+)$/;
            my ($dev, $total, $used, $avail, $useper, $disk) = ($1, $2, $3, $4, $5, $6);

            unless ($disk =~ m!$devnum/?$!) {
                $disk = "$path/$devnum";
            }

            # create string to print
            my $now = time;
            my $output = {
                time      => time(),
                device    => $dev,    # /dev/sdh1
                total     => int($total / $uperK), # integer: total KiB blocks
                used      => int($used  / $uperK), # integer: used KiB blocks
                available => int($avail / $uperK),  # integer: available KiB blocks
                'use'     => $useper, # "45%"
                disk      => $disk,   # mount point of disk (/var/mogdata/dev8), or path if not a mount
            };

            # open a file on that disk location called 'usage'
            my $rv = open(FILE, ">$disk/usage");
            unless ($rv) {
                return $err->("Unable to open '$disk/usage' for writing: $!");
                next;
            }
            foreach (sort keys %$output) {
                print FILE "$_: $output->{$_}\n";
            }
            close FILE;
        }
    }
}


1;
