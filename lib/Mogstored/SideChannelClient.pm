### simple package for handling the stream request port
package Mogstored::SideChannelClient;

use strict;
use base qw{Perlbal::Socket};
use fields (
            'count',      # how many requests we've serviced
            'read_buf',   # unprocessed read buffer
            'mogsvc',     # the mogstored Perlbal::Service object
            );
use Digest;
use POSIX qw(O_RDONLY);
use Mogstored::TaskQueue;

# TODO: interface to make this tunable
my %digest_queues;

# needed since we're pretending to be a Perlbal::Socket... never idle out
sub max_idle_time { return 0; }

sub new {
    my Mogstored::SideChannelClient $self = shift;
    $self = fields::new($self) unless ref $self;
    $self->SUPER::new(@_);
    $self->{count} = 0;
    $self->{read_buf} = '';
    $self->{mogsvc} = Perlbal->service('mogstored');
    return $self;
}

sub validate_uri {
    my ($self, $uri) = @_;
    if ($uri =~ /\.\./) {
        $self->write("ERROR: uri invalid (contains ..)\r\n");
        return;
    }
    $uri;
}

sub event_read {
    my Mogstored::SideChannelClient $self = shift;

    my $bref = $self->read(1024);
    return $self->close unless defined $bref;
    $self->{read_buf} .= $$bref;
    $self->read_buf_consume;
}

sub read_buf_consume {
    my $self = shift;
    my $path = $self->{mogsvc}->{docroot};

    while ($self->{read_buf} =~ s/^(.+?)\r?\n//) {
        my $cmd = $1;
        if ($cmd =~ /^size (\S+)$/) {
            # increase our count
            $self->{count}++;

            my $uri = $self->validate_uri($1);
            return unless defined($uri);

            # now stat the file to get the size and such
            Perlbal::AIO::aio_stat("$path$uri", sub {
                return if $self->{closed};
                my $size = -e _ ? -s _ : -1;
                $self->write("$uri $size\r\n");
            });
        } elsif ($cmd =~ /^watch$/i) {
            unless (Mogstored->iostat_available) {
                $self->write("ERR iostat unavailable\r\n");
                next;
            }
            $self->watch_read(0);
            Mogstored->iostat_subscribe($self);
        } elsif ($cmd =~ /^(MD5|SHA-1) (\S+)(?: (\w+))?$/) {
            # we can easily enable other hash algorithms with the above
            # regexp, but we won't for now (see MogileFS::Checksum)
            my $alg = $1;
            my $uri = $self->validate_uri($2);
            my $reason = $3;
            return unless defined($uri);

            return $self->digest($alg, $path, $uri, $reason);
        } else {
            # we don't understand this so pass it on to manage command interface
            my @out;
            Perlbal::run_manage_command($cmd, sub { push @out, $_[0]; });
            $self->write(join("\r\n", @out) . "\r\n");
        }
    }
}

# stop watching writeability if we've nothing else to
# write to them.  else just kick off more writes.
sub event_write {
    my $self = shift;
    $self->watch_write(0) if $self->write(undef);
}

# override Danga::Socket's event handlers which die
sub event_err { $_[0]->close; }
sub event_hup { $_[0]->close; }

# as_string handler
sub as_string {
    my Mogstored::SideChannelClient $self = shift;

    my $ret = $self->SUPER::as_string;
    $ret .= "; size_requests=$self->{count}";

    return $ret;
}

sub close {
    my Mogstored::SideChannelClient $self = shift;
    Mogstored->iostat_unsubscribe($self);
    $self->SUPER::close;
}

sub die_gracefully {
    Mogstored->on_sidechannel_die_gracefully;
}

sub digest {
    my ($self, $alg, $path, $uri, $reason) = @_;

    $self->watch_read(0);

    Perlbal::AIO::aio_open("$path$uri", O_RDONLY, 0, sub {
        my $fh = shift;

        if ($self->{closed}) {
            CORE::close($fh) if $fh;
            return;
        }
        if ($fh) {
            my $queue;

            if ($reason && $reason eq "fsck") {
                # fstat(2) should return immediately, no AIO needed
                my $devid = (stat($fh))[0];
                $queue = $digest_queues{$devid} ||= Mogstored::TaskQueue->new;
                $queue->run(sub { $self->digest_fh($alg, $fh, $uri, $queue) });
            } else {
                $self->digest_fh($alg, $fh, $uri);
            }
        } else {
            $self->write("$uri $alg=-1\r\n");
            $self->after_long_request;
        }
    });
}

sub digest_fh {
    my ($self, $alg, $fh, $uri, $queue) = @_;
    my $offset = 0;
    my $data = '';
    my $digest = Digest->new($alg);
    my $cb;

    $cb = sub {
        my $retval = shift;
        if ($retval > 0) {
            my $bytes = length($data);
            $offset += $bytes;
            $digest->add($data);
            Perlbal::AIO::aio_read($fh, $offset, 0x100000, $data, $cb);
        } elsif ($retval == 0) { # EOF
            $cb = undef;
            CORE::close($fh);
            $digest = $digest->hexdigest;
            $self->write("$uri $alg=$digest\r\n");
            $queue->task_done if $queue;
            $self->after_long_request;
        } else {
            $cb = undef;
            CORE::close($fh);
            $self->write("ERR read $uri at $offset failed\r\n");
            $queue->task_done if $queue;
            $self->after_long_request; # should we try to continue?
        }
    };
    Perlbal::AIO::aio_read($fh, $offset, 0x100000, $data, $cb);
}

sub after_long_request {
    my $self = shift;

    if ($self->{read_buf} =~ /^(.+?)\r?\n/) {
        $self->read_buf_consume;
    } else {
        $self->watch_read(1);
    }
}

1;
