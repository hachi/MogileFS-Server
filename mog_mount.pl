#!/usr/bin/perl
#

use strict;
use Getopt::Long;
use Pod::Usage;
use File::Path;
use MogileFS;
$| = 1;

die "Must be run as root\n" if $<;

my $opt = { host => [] }; # host can be multiple options
GetOptions($opt, 
           qw(host|h=s port|p=i type|t=s at|a=s opts|o=s no-mount remount unmount verbose help));

if ($opt->{help}) {
    pod2usage($0);
    exit;
}

my @mog_hosts  = @{$opt->{host}} ? @{$opt->{host}} : qw(127.0.0.1);
my $mog_port   = $opt->{port} || 7001;
my $mount_at   = $opt->{at}   || "/mnt/mogilefs";
my $mount_opts = $opt->{opts} || "defaults,noatime,timeo=1,retrans=1,soft";
my $mount_type = $opt->{type} || "nfs";
my $unmount    = $opt->{unmount};
my $remount    = $opt->{remount};
my $no_mount   = $opt->{'no-mount'};
my $verbose    = $opt->{verbose};

die "Mount root '$mount_at' does not exist\n"
    unless -e $mount_at;

die "Mount root '$mount_at' is not a directory\n"
    unless -d _;

# validate MogileFS hosts
foreach my $host (@mog_hosts) {

    # passed an IP address?
    my $port = $mog_port;
    if ($host =~ /^((\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})):?(\d*)/) {

        die "Invalid host IP: $1\n" 
            unless (($2 | $3 | $4 | $5) & ~255) == 0;

        $host = $1;
        $port = int($6) if $6 > 0;

    # passed a hostname
    } elsif ($host =~ /^([\w-]+):?(\d*)$/) {

        $host = $1;
        $port = int($2) if $2 > 0;

    # who knows
    } else {
        die "Invalid host: $host\n";
    }

    $host = "$host:$port";
}

my $mg = new MogileFS::Admin ( hosts  => \@mog_hosts )
    or die "Couldn't initialize MogileFS\n";

my $hosts = $mg->get_hosts
    or die "Couldn't get MogileFS hosts";

my $devices = $mg->get_devices
    or die "Couldn't get MogileFS devices";

# indexes into arrays
my %dev_by_devid = map { $_->{devid}  => $_ } @$devices;
my %host_by_ip   = map { $_->{hostip} => $_ } @$hosts;
my %host_by_id   = map { $_->{hostid} => $_ } @$hosts;

# see what is currently mounted
my @mount_res = `mount` or die "Couldn't run 'mount'";
my $did_umount = 0;
foreach my $line (@mount_res) {

    # parse line of 'mount' output
    # - not all of these variables are used
    next unless my ($ip, $export, $mnt, $type, $opts) = 
        $line =~ /
            ^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}): # ip address, colon
            ([\w\/]+)\s+                           # remote export, space
            on\s+([\w\/]+)\s+                      # local mount point, space
            type\s+(\w+)\s+                        # mount type, space
            \((.+)\)                               # extra options
        /x;

    # valid host?
    my $hostid = $host_by_ip{$ip}->{hostid} or next;

    # valid device?
    next unless $mnt =~ m!$mount_at/\w+/dev(\d+)!;
    my $devid = $1;

    # does device belong to host?
    next unless $dev_by_devid{$devid}->{hostid} == $hostid;

    # if we're unmounting or remounting existing mounts, we'll do that now
    if ($unmount || $remount) {

        my $res = `umount $mnt 2>&1`;
        my $doing = " - unmounting $mnt...";

        # successful unmount
        unless ($?) {
            print "$doing OK\n" if $verbose;
            $did_umount++;
            next;
        }

        # error unmounting?
        # -- alert user then proceed to delete from %dev_by_devid
        print "$doing FAIL\n";

        # need to do this hack because unmount seems to print duplicate
        # error messages on unmount errors?
        my $lasterr = "";
        foreach (split("\n", $res)) {
            next if $_ eq $lasterr;
            print "    * $_\n";
            $lasterr = $_;
        }
    }

    # don't try to remount already mounted devices
    delete $dev_by_devid{$devid};
}
@$devices = grep { $dev_by_devid{$_->{devid}} } @$devices;

# if our goal is just to umount all devices, that should be done at this point
if ($unmount) {
    if ($verbose) {
        die "\n$did_umount devices unmounted.  Done\n" if $did_umount;
        die "No devices to unmount.  Done\n";
    }
    exit 0;
}

# separator between unmounts and mounts
print "\n" if $verbose && $remount;

# is there anything to do?
if (! @$devices) {
    die "All devices already mounted. Nothing to do.\n" if $verbose;
    exit 0;
}

# mount all devices
my $did_mount = 0;
my $to_mount = @$devices;
foreach my $dev (@$devices) {

    # calculate source/destination paths
    my $host       = $host_by_id{$dev->{hostid}};
    my $hostname   = $host->{hostname};
    my $hostip     = $host->{hostip};
    my $remoteroot = $host->{remoteroot};
    my $devid      = $dev->{devid};

    my $src        = "$hostip:$remoteroot/dev$devid";
    my $host_dest  = "$mount_at/$hostname";
    my $dest       = "$host_dest/dev$devid";

    my $doing = " - mounting $src => $dest ...";

    my $ok = sub {
        print "$doing OK\n" if $verbose;
        $did_mount++;
        next;
    };

    my $fail = sub {
        print "$doing FAIL\n";
        print join("\n", map { "   * $_" } @_) . "\n";
        next;
    };

    # if the destination is already a symlink, then we presume that is the 
    # current machine, with the directory already symlinked... so no actual
    # mounting should be done.
    $ok->() if -l $host_dest; # host   is a symlink
    $ok->() if -l $dest;      # device is a symlink

    # create directory if necessary
    unless (-d _) {
        eval { mkpath($dest) };
        $fail->("error creating $dest: $@") if $@;
    }

    # don't actually mount the device if this is a dry run,
    # just make directories.
    $ok->() if $no_mount;

    # actually attempt to mount device
    my $res = `mount -t $mount_type -o $mount_opts $src $dest 2>&1`;
    chomp($res);

    # check return code of mount
    if (my $errcode = $? >> 8) {

        # bit => err string
        my @errmap = ( "incorrect invocation or permissions",
                       "system error (out of memory, cannot fork, no more loop devices)",
                       "internal mount bug or missing nfs support in mount",
                       "user interrupt",
                       "problems writing or locking /etc/mtab",
                       "mount failure",
                       "some mount succeeded" );

        my $errstr = "unknown error code";
        foreach my $bit (0..6) {
            $errstr = $errmap[$bit] if $errcode & 1 << $bit;
        }

        $fail->("error: $errstr", $res);
    }

    # one down...
    $ok->();
}

if ($verbose) {
    print "\n";
    print "$did_umount devices unmounted, " if $remount;
    print "$did_mount of $to_mount devices sucessfully mounted.\n";
}

__END__

=head1 SYNOPSIS

./mog_mount.pl [OPTIONS]

Connect to one of a list of specified MogileFS servers, mounting any unmounted devices in the
directory specified by the --at option.

On success, no output is printed unless the --verbose option is specified.

Must be run as root.

=head2 OPTIONS

=over

=item -h, --host=HOSTNAME[:PORT]

Specify one or more MogileFS servers to use. Port is optional.

For multiple hosts, repeat the --host option.

Default is '127.0.0.1'.

=item -p, --port=PORT

Override the port used for hosts with no port specified.

Default is port 7001.

=item -a, --at=PATH

Parent directory for mounts. A hostname/devX directory will be made for each mount. 

Default is '/mnt/mogilefs'.

=item -t, --type=VFSTYPE

Vfstype to pass to 'mount'.  Default is 'nfs'.

=item -o, --opts=VFSOPTIONS

Comma separated list of vfs options to pass to 'mount'.  Available options vary depending on the vfstype in use.

Default is 'defaults,noatime,timeo=1,retrans=1,soft'.

=item -n, --no-mount

Don't actually do any mounting, only create destination directories

=item -r, --remount

Unmount all mounted devices, then remount them all.

=item -u, --unmount

Unmount all mounted devices, mounting nothing.

=item -v, --verbose

Print verbose output.  Displays status of each successful mount.

=item --help

Display usage information.

=back

=cut
