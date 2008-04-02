package MogileFS::Network;

use strict;
use warnings;

use Net::Netmask;
use MogileFS::Config;

my %cache; # '192.168.0.0/24' => Net::Netmask->new2('192.168.0.0/24');
my $age;   # increments everytime we look

sub zone_for_ip {
    my $class = shift;
    my $ip = shift;

    return unless $ip;

    # clear the cache occasionally
    if ((!defined $age) or ($age == 0) or ($age++ > 500)) {
        clear_and_build_cache();
        $age = 1;
    }

    foreach my $zone (keys %cache) {
        if ($cache{$zone}->match($ip)) {
            return $zone;
        }
    }
    return;
}

sub clear_and_build_cache {
    undef %cache;

    my @zones = split(/\s*,\s*/,MogileFS::Config->server_setting("network_zones"));

    foreach my $zone (@zones) {
        my $netmask = MogileFS::Config->server_setting("zone_$zone");

        if (not $netmask) {
            warn "couldn't find network_zone <<zone_$zone>> check your server settings";
            next;
        }

        if ($cache{$zone}) {
            warn "duplicate netmask <$netmask> in network zones. check your server settings";
        }

        $cache{$zone} = Net::Netmask->new2($netmask);

        if (Net::Netmask::errstr()) {
            warn "couldn't parse <$zone> as a netmask. error was <" . Net::Netmask::errstr().
                 ">. check your server settings";
        }
    }
}

sub stuff_cache { # for testing, or it'll try the db
    my ($self, $ip, $netmask) = @_;

    $cache{$ip} = $netmask;
    $age = 1;
}

1;
