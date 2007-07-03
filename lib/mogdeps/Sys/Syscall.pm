# LICENSE: You're free to distribute this under the same terms as Perl itself.

package Sys::Syscall;
use strict;
use POSIX qw(ENOSYS SEEK_CUR);
use Config;

require Exporter;
use vars qw(@ISA @EXPORT_OK %EXPORT_TAGS $VERSION);

$VERSION     = "0.22";
@ISA         = qw(Exporter);
@EXPORT_OK   = qw(sendfile epoll_ctl epoll_create epoll_wait
                  EPOLLIN EPOLLOUT EPOLLERR EPOLLHUP EPOLLRDBAND
                  EPOLL_CTL_ADD EPOLL_CTL_DEL EPOLL_CTL_MOD);
%EXPORT_TAGS = (epoll => [qw(epoll_ctl epoll_create epoll_wait
                             EPOLLIN EPOLLOUT EPOLLERR EPOLLHUP EPOLLRDBAND
                             EPOLL_CTL_ADD EPOLL_CTL_DEL EPOLL_CTL_MOD)],
                sendfile => [qw(sendfile)],
                );

use constant EPOLLIN       => 1;
use constant EPOLLOUT      => 4;
use constant EPOLLERR      => 8;
use constant EPOLLHUP      => 16;
use constant EPOLLRDBAND   => 128;
use constant EPOLL_CTL_ADD => 1;
use constant EPOLL_CTL_DEL => 2;
use constant EPOLL_CTL_MOD => 3;

our $loaded_syscall = 0;

sub _load_syscall {
    # props to Gaal for this!
    return if $loaded_syscall++;
    my $clean = sub {
        delete @INC{qw<syscall.ph asm/unistd.ph bits/syscall.ph
                        _h2ph_pre.ph sys/syscall.ph>};
    };
    $clean->(); # don't trust modules before us
    my $rv = eval { require 'syscall.ph'; 1 } || eval { require 'sys/syscall.ph'; 1 };
    $clean->(); # don't require modules after us trust us
    return $rv;
}

our ($sysname, $nodename, $release, $version, $machine) = POSIX::uname();

our (
     $SYS_epoll_create,
     $SYS_epoll_ctl,
     $SYS_epoll_wait,
     $SYS_sendfile,
     $SYS_readahead,
     );

if ($^O eq "linux") {
    # whether the machine requires 64-bit numbers to be on 8-byte
    # boundaries.
    my $u64_mod_8 = 0;

    # if we're running on an x86_64 kernel, but a 32-bit process,
    # we need to use the i386 syscall numbers.
    if ($machine eq "x86_64" && $Config{ptrsize} == 4) {
        $machine = "i386";
    }

    if ($machine =~ m/^i[3456]86$/) {
        $SYS_epoll_create = 254;
        $SYS_epoll_ctl    = 255;
        $SYS_epoll_wait   = 256;
        $SYS_sendfile     = 187;  # or 64: 239
        $SYS_readahead    = 225;
    } elsif ($machine eq "x86_64") {
        $SYS_epoll_create = 213;
        $SYS_epoll_ctl    = 233;
        $SYS_epoll_wait   = 232;
        $SYS_sendfile     =  40;
        $SYS_readahead    = 187;
    } elsif ($machine eq "ppc64") {
        $SYS_epoll_create = 236;
        $SYS_epoll_ctl    = 237;
        $SYS_epoll_wait   = 238;
        $SYS_sendfile     = 186;  # (sys32_sendfile).  sys32_sendfile64=226  (64 bit processes: sys_sendfile64=186)
        $SYS_readahead    = 191;  # both 32-bit and 64-bit vesions
        $u64_mod_8        = 1;
    } elsif ($machine eq "ppc") {
        $SYS_epoll_create = 236;
        $SYS_epoll_ctl    = 237;
        $SYS_epoll_wait   = 238;
        $SYS_sendfile     = 186;  # sys_sendfile64=226
        $SYS_readahead    = 191;
        $u64_mod_8        = 1;
    } elsif ($machine eq "ia64") {
        $SYS_epoll_create = 1243;
        $SYS_epoll_ctl    = 1244;
        $SYS_epoll_wait   = 1245;
        $SYS_sendfile     = 1187;
        $SYS_readahead    = 1216;
        $u64_mod_8        = 1;
    } elsif ($machine eq "alpha") {
        # natural alignment, ints are 32-bits
        $SYS_sendfile     = 370;  # (sys_sendfile64)
        $SYS_epoll_create = 407;
        $SYS_epoll_ctl    = 408;
        $SYS_epoll_wait   = 409;
        $SYS_readahead    = 379;
        $u64_mod_8        = 1;
    } else {
        # as a last resort, try using the *.ph files which may not
        # exist or may be wrong
        _load_syscall();
        $SYS_epoll_create = eval { &SYS_epoll_create; } || 0;
        $SYS_epoll_ctl    = eval { &SYS_epoll_ctl;    } || 0;
        $SYS_epoll_wait   = eval { &SYS_epoll_wait;   } || 0;
        $SYS_readahead    = eval { &SYS_readahead;    } || 0;
    }

    if ($u64_mod_8) {
        *epoll_wait = \&epoll_wait_mod8;
        *epoll_ctl = \&epoll_ctl_mod8;
    } else {
        *epoll_wait = \&epoll_wait_mod4;
        *epoll_ctl = \&epoll_ctl_mod4;
    }
}

elsif ($^O eq "freebsd") {
    if ($ENV{FREEBSD_SENDFILE}) {
        # this is still buggy and in development
        $SYS_sendfile = 393;  # old is 336
    }
}

############################################################################
# sendfile functions
############################################################################

unless ($SYS_sendfile) {
    _load_syscall();
    $SYS_sendfile = eval { &SYS_sendfile; } || 0;
}

sub sendfile_defined { return $SYS_sendfile ? 1 : 0; }

if ($^O eq "linux" && $SYS_sendfile) {
    *sendfile = \&sendfile_linux;
} elsif ($^O eq "freebsd" && $SYS_sendfile) {
    *sendfile = \&sendfile_freebsd;
} else {
    *sendfile = \&sendfile_noimpl;
}

sub sendfile_noimpl {
    $! = ENOSYS;
    return -1;
}

# C: ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
# Perl:  sendfile($write_fd, $read_fd, $max_count) --> $actually_sent
sub sendfile_linux {
    return syscall(
                   $SYS_sendfile,
                   $_[0] + 0,  # fd
                   $_[1] + 0,  # fd
                   0,          # don't keep track of offset.  callers can lseek and keep track.
                   $_[2] + 0   # count
                   );
}

sub sendfile_freebsd {
    my $offset = POSIX::lseek($_[1]+0, 0, SEEK_CUR) + 0;
    my $ct = $_[2] + 0;
    my $sbytes_buf = "\0" x 8;
    my $rv = syscall(
                     $SYS_sendfile,
                     $_[1] + 0,   # fd     (from)
                     $_[0] + 0,   # socket (to)
                     $offset,
                     $ct,
                     0,           # struct sf_hdtr *hdtr
                     $sbytes_buf, # off_t *sbytes
                     0);          # flags
    return $rv if $rv < 0;


    my $set = unpack("L", $sbytes_buf);
    POSIX::lseek($_[1]+0, SEEK_CUR, $set);
    return $set;
}


############################################################################
# epoll functions
############################################################################

sub epoll_defined { return $SYS_epoll_create ? 1 : 0; }

# ARGS: (size) -- but in modern Linux 2.6, the
# size doesn't even matter (radix tree now, not hash)
sub epoll_create {
    return -1 unless defined $SYS_epoll_create;
    my $epfd = eval { syscall($SYS_epoll_create, ($_[0]||100)+0) };
    return -1 if $@;
    return $epfd;
}

# epoll_ctl wrapper
# ARGS: (epfd, op, fd, events_mask)
sub epoll_ctl_mod4 {
    syscall($SYS_epoll_ctl, $_[0]+0, $_[1]+0, $_[2]+0, pack("LLL", $_[3], $_[2], 0));
}
sub epoll_ctl_mod8 {
    syscall($SYS_epoll_ctl, $_[0]+0, $_[1]+0, $_[2]+0, pack("LLLL", $_[3], 0, $_[2], 0));
}

# epoll_wait wrapper
# ARGS: (epfd, maxevents, timeout (milliseconds), arrayref)
#  arrayref: values modified to be [$fd, $event]
our $epoll_wait_events;
our $epoll_wait_size = 0;
sub epoll_wait_mod4 {
    # resize our static buffer if requested size is bigger than we've ever done
    if ($_[1] > $epoll_wait_size) {
        $epoll_wait_size = $_[1];
        $epoll_wait_events = "\0" x 12 x $epoll_wait_size;
    }
    my $ct = syscall($SYS_epoll_wait, $_[0]+0, $epoll_wait_events, $_[1]+0, $_[2]+0);
    for (0..$ct-1) {
        @{$_[3]->[$_]}[1,0] = unpack("LL", substr($epoll_wait_events, 12*$_, 8));
    }
    return $ct;
}

sub epoll_wait_mod8 {
    # resize our static buffer if requested size is bigger than we've ever done
    if ($_[1] > $epoll_wait_size) {
        $epoll_wait_size = $_[1];
        $epoll_wait_events = "\0" x 16 x $epoll_wait_size;
    }
    my $ct = syscall($SYS_epoll_wait, $_[0]+0, $epoll_wait_events, $_[1]+0, $_[2]+0);
    for (0..$ct-1) {
        # 16 byte epoll_event structs, with format:
        #    4 byte mask [idx 1]
        #    4 byte padding (we put it into idx 2, useless)
        #    8 byte data (first 4 bytes are fd, into idx 0)
        @{$_[3]->[$_]}[1,2,0] = unpack("LLL", substr($epoll_wait_events, 16*$_, 12));
    }
    return $ct;
}


1;
__END__

=head1 NAME

Sys::Syscall - access system calls that Perl doesn't normally provide access to

=head1 SYNOPSIS

  use Sys::Syscall;

=head1 DESCRIPTION

Use epoll, sendfile, from Perl.  Mostly Linux-only support now, but
more syscalls/OSes planned for future.

=head1 Exports

Nothing by default.

May export: sendfile epoll_ctl epoll_create epoll_wait EPOLLIN EPOLLOUT EPOLLERR EPOLLHUP EPOLL_CTL_ADD  EPOLL_CTL_DEL EPOLL_CTL_MOD

Export tags:  :epoll and :sendfile

=head1 Functions

=head2 epoll support

=over 4

=item $ok = epoll_defined()

Returns true if epoll might be available.  (caller must still test with epoll_create)

=item $epfd = epoll_create([ $start_size ])

Create a new epoll filedescriptor.  Returns -1 if epoll isn't available.

=item $rv = epoll_ctl($epfd, $op, $fd, $events)

See manpage for epoll_ctl

=item $count = epoll_wait($epfd, $max_events, $timeout, $arrayref)

See manpage for epoll_wait.  $arrayref is an arrayref to be modified
with the items returned.  The values put into $arrayref are arrayrefs
of [$fd, $state].

=back

=head2 sendfile support

=over 4

=item $ok = sendfile_defined()

Returns true if sendfile should work on this operating system.

=item $sent = sendfile($sock_fd, $file_fd, $max_send)

Sends up to $max_send bytes from $file_fd to $sock_fd.  Returns bytes
actually sent, or -1 on error.

=back

=head1 COPYRIGHT

This module is Copyright (c) 2005, 2006 Six Apart, Ltd.

All rights reserved.

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file.
If you need more liberal licensing terms, please contact the
maintainer.

=head1 WARRANTY

This is free software. IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 AUTHORS

Brad Fitzpatrick <brad@danga.com>

