package Mogstored::HTTPServer::Apache;
use strict;
use base 'Mogstored::HTTPServer';
use File::Temp ();

sub start {
    my $self = shift;
    my $exe = $self->{bin};

    if ($exe && ! -x $exe) {
        die "Provided apache path $exe not valid.\n";
    }
    unless ($exe) {
        # TODO: not sure where else common locations are... just guessing
        my @loc = qw(/usr/sbin/apache
                     /usr/sbin/httpd
                     );
        foreach my $loc (@loc) {
            $exe = $loc;
            last if -x $exe;
        }
        unless (-x $exe) {
            die "Can't find apache in @loc\n";
        }
    }

    my $pid = fork();
    die "Can't fork: $!" unless defined $pid;

    if ($pid) {
        $self->{pid} = $pid;
        Mogstored->on_pid_death($pid => sub {
            die "apache died";
        });
        return;
    }

    my ($fh, $filename) = File::Temp::tempfile();
    $self->{temp_conf_file} = $filename;

    my $portnum = $self->listen_port;
    my $bind_ip = $self->bind_ip;

    print $fh qq{
ServerType standalone
ErrorLog /dev/null
LoadModule dav_module  /usr/lib/apache/1.3/libdav.so

Listen 7500
<VirtualHost *:7500>
  DocumentRoot $self->{docroot}

  <Directory $self->{docroot}>
    Options +Indexes +FollowSymLinks
  </Directory>

  <Location />
    DAV On
  </Location>
</VirtualHost>

};

    exec $exe, "-F", "-f", $filename;
}

sub DESTROY {
    my $self = shift;
    unlink $self->{temp_conf_file} if $self->{temp_conf_file};
}

1;
