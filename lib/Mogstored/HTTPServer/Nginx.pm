package Mogstored::HTTPServer::Nginx;
use strict;
use base 'Mogstored::HTTPServer';
use File::Temp ();
my $nginxpidfile;

sub start {

    my $self = shift;
    my $exe = $self->{bin};

    if ($exe && ! -x $exe) {
        die "Provided nginx path $exe not valid.\n";
    }
    unless ($exe) {
        my @loc = qw(/usr/sbin/nginx
                     /usr/local/bin/nginx
                     /usr/bin/nginx
                     );
        foreach my $loc (@loc) {
            $exe = $loc;
            last if -x $exe;
        }
        unless (-x $exe) {
            die "Can't find nginx in @loc\n";
        }
    }

    my $prefixDir = $self->{docroot} . '/.tmp';
    $nginxpidfile = $self->{docroot} . "/nginx.pid";

    my $nginxpid = _getpid();
    # TODO: Support reloading of nginx instead?
    if ($nginxpid) {
        my $killed = kill 15,$nginxpid;
        if ($killed > 0) {
    	    print "Killed nginx on PID # $nginxpid";
        }
    }

    my ($fh, $filename) = File::Temp::tempfile();
    $self->{temp_conf_file} = $filename;

    my $portnum = $self->listen_port;
    my $bind_ip = $self->bind_ip;

    my $client_max_body_size = "0";
    $client_max_body_size = $self->{client_max_body_size}
        if $self->{client_max_body_size};

    # TODO: Pull from config file?
    #print "client_max_body_size = $client_max_body_size\n";

    my @devdirs = _disks($self->{docroot});
    my $devsection = '';

    foreach my $devid (@devdirs) {
    	my $devseg = qq{
        location /$devid {
            client_body_temp_path $self->{docroot}/$devid/.tmp;
            dav_methods put delete;
            dav_access user:rw group:rw all:r;
            create_full_put_path on;
        }
	};
	$devsection = $devsection . $devseg;
    }
    
    print $fh qq{
pid $nginxpidfile;
worker_processes 15;
error_log /dev/null crit;
events {
    worker_connections 1024;
}
http {
    default_type application/octet-stream;
    sendfile on;
    keepalive_timeout 0;
    client_max_body_size $client_max_body_size;
    server_tokens off;
	access_log off;
    server {
        listen $bind_ip:$portnum;
        root $self->{docroot};
        charset utf-8;
	$devsection
        location /.tmp {
            deny all;
        }
	location / {
	    autoindex on;
        }
    }
}
};
   close $fh;

    # create prefix directory and start server
    mkdir $prefixDir;
    mkdir $prefixDir.'/logs';
    system $exe, '-p', $prefixDir, "-c", $filename;

   return 1;
}

sub _disks {
    my $root = shift;
    opendir(my $dh, $root) or die "Failed to open docroot: $root: $!";
    return grep { /^dev\d+$/ } readdir($dh);
}

sub _getpid {
  local $/ = undef;
  open FILE, $nginxpidfile or return;
  binmode FILE;
  my $string = <FILE>;
  close FILE;
  return $string;
}

sub DESTROY {
    my $self = shift;
    unlink $self->{temp_conf_file} if $self->{temp_conf_file};
}

1;
