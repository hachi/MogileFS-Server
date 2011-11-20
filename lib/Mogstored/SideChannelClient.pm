### simple package for handling the stream request port
package Mogstored::SideChannelClient;

use strict;
use base qw{Perlbal::Socket};
use fields (
            'count',      # how many requests we've serviced
            'read_buf',   # unprocessed read buffer
            'mogsvc',     # the mogstored Perlbal::Service object
            );
use Digest::MD5;
use POSIX qw(O_RDONLY);

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
        } elsif ($cmd =~ /^md5 (\S+)$/) {
            my $uri = $self->validate_uri($1);
            return unless defined($uri);

            $self->watch_read(0);
            $self->md5($path, $uri);
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

sub md5 {
    my ($self, $path, $uri) = @_;

    Perlbal::AIO::aio_open("$path$uri", O_RDONLY, 0, sub {
        my $fh = shift;

        if ($self->{closed}) {
           CORE::close($fh) if $fh;
           return;
        }
        $fh or return $self->close('aio_open_failure');
        $self->md5_fh($fh, $uri);
    });
}

sub md5_fh {
    my ($self, $fh, $uri) = @_;
    my $offset = 0;
    my $data = '';
    my $md5 = Digest::MD5->new;
    my $total = -s $fh;
    my $cb;

    $cb = sub {
        unless ($_[0] > 0) {
            $cb = undef;
            CORE::close($fh);
            return $self->write("ERR read $uri at $offset failed\r\n");
        }
        my $bytes = length($data);
        $offset += $bytes;
        $md5->add($data);
        if ($offset >= $total) {
            my $content_md5 = $md5->b64digest;
            $self->write("$uri md5=$content_md5==\r\n");
            $cb = undef;
            CORE::close($fh);
            $self->watch_read(1);
        } else {
            Perlbal::AIO::aio_read($fh, $offset, 0x4000, $data, $cb);
        }
    };
    Perlbal::AIO::aio_read($fh, $offset, 0x4000, $data, $cb);
}

1;
