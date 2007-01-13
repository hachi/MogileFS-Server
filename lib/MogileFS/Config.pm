package MogileFS::Config;
use strict;
require Exporter;
use MogileFS::ProcManager;
use Getopt::Long;
use MogileFS::Store;

our @ISA = qw(Exporter);
our @EXPORT = qw($DEBUG config set_config);
our @EXPORT_OK = qw(DEVICE_SUMMARY_CACHE_TIMEOUT);

our ($DEFAULT_CONFIG, $DEFAULT_MOG_ROOT, $MOG_ROOT, $MOGSTORED_STREAM_PORT, $DEBUG);
$DEBUG = 0;
$DEFAULT_CONFIG = "/etc/mogilefs/mogilefsd.conf";
$DEFAULT_MOG_ROOT = "/mnt/mogilefs";
$MOGSTORED_STREAM_PORT = 7501;

use constant DEVICE_SUMMARY_CACHE_TIMEOUT => 15;

my %conf;
sub set_config {
    shift if @_ == 3;
    my ($k, $v) = @_;

    # if a child, propogate to parent
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->send_to_parent(":set_config_from_child $k $v");
    } else {
        MogileFS::ProcManager->send_to_all_children(":set_config_from_parent $k $v");
    }

    return set_config_no_broadcast($k, $v);
}

sub set_config_no_broadcast {
    shift if @_ == 3;
    my ($k, $v) = @_;
    return $conf{$k} = $v;
}

set_config("mogstored_stream_port" => $MOGSTORED_STREAM_PORT);
set_config('default_mindevcount', 2);

our (
    %cmdline,
    %cfgfile,
    $config,
    $skipconfig,
    $daemonize,
    $db_dsn,
    $db_user,
    $db_pass,
    $conf_port,
    $query_jobs,
    $delete_jobs,
    $replicate_jobs,
    $reaper_jobs,
    $monitor_jobs,
    $checker_jobs,
    $mog_root,
    $min_free_space,
    $max_disk_age,
    $node_timeout,          # time in seconds to wait for storage node responses
    $old_repl_compat,
    $pidfile,
   );

my $default_mindevcount;

sub load_config {
    my $dummy_workerport;

    # Command-line options will override
    Getopt::Long::Configure( "bundling" );
    Getopt::Long::GetOptions(
                             'c|config=s'    => \$config,
                             's|skipconfig'  => \$skipconfig,
                             'd|debug+'      => \$cmdline{debug},
                             'D|daemonize'   => \$cmdline{daemonize},
                             'dsn=s'         => \$cmdline{db_dsn},
                             'dbuser=s'      => \$cmdline{db_user},
                             'dbpass:s'      => \$cmdline{db_pass},
                             'user=s'        => \$cmdline{user},
                             'r|mogroot=s'   => \$cmdline{mog_root},
                             'p|confport=i'  => \$cmdline{conf_port},
                             'w|workers=i'   => \$cmdline{query_jobs},
                             'no_http'       => \$cmdline{no_http},  # OLD, we just eat it to shut it up.
                             'workerport=i'  => \$dummy_workerport,  # eat it for backwards compat
                             'maxdiskage=i'  => \$cmdline{max_disk_age},
                             'minfreespace=i' => \$cmdline{min_free_space},
                             'default_mindevcount=i' => \$cmdline{default_mindevcount},
                             'node_timeout=i' => \$cmdline{node_timeout},
                             'pidfile=s'      => \$cmdline{pidfile},
                             'no_schema_check' => \$cmdline{no_schema_check},
                             'old_repl_compat=i' => \$cmdline{old_repl_compat},
                             );

    # warn of old/deprecated options
    warn "The command line option --workerport is no longer needed (and has no necessary replacement)\n"
        if $dummy_workerport;

    $config = $DEFAULT_CONFIG if !$config && -r $DEFAULT_CONFIG;

    # Read the config file if one was specified
    if ($config && !$skipconfig) {
        open my $cf, "<$config" or die "open: $config: $!";

        my $configLine = qr{
            ^\s*                        # Leading space
                ([\w.]+)                # Key
                \s+ =? \s*              # space + optional equal + optional space
                (.+?)                   # Value
                \s*$                    # Trailing space
            }x;

        my $linecount = 0;
        while (defined( my $line = <$cf> )) {
            $linecount++;
            next if $line =~ m!^\s*(\#.*)?$!;
            die "Malformed config file (line $linecount)" unless $line =~ $configLine;

            my ( $key, $value ) = ( $1, $2 );
            print STDERR "Setting '$key' to '$value'\n" if $cmdline{debug};
            $cfgfile{$key} = $value;
        }

        close $cf;
    }

    # Fill in defaults for those values which were either loaded from config or
    # specified on the command line. Command line takes precendence, then values in
    # the config file, then the defaults.
    $daemonize      = choose_value( 'daemonize', 0 );
    $db_dsn         = choose_value( 'db_dsn', "DBI:mysql:mogilefs" );
    $db_user        = choose_value( 'db_user', "mogile" );
    $db_pass        = choose_value( 'db_pass', "", 1 );
    $conf_port      = choose_value( 'conf_port', 7001 );
    $MOG_ROOT       = set_config('root',
                                 choose_value( 'mog_root', $DEFAULT_MOG_ROOT )
                                 );
    $query_jobs     = set_config("query_jobs",
                                 choose_value( 'listener_jobs', undef) || # undef if not present, then we
                                 choose_value( 'query_jobs', 20 ));       # fall back to query_jobs, new name
    $delete_jobs    = choose_value( 'delete_jobs', 1 );
    $replicate_jobs = choose_value( 'replicate_jobs', 1 );
    $reaper_jobs    = choose_value( 'reaper_jobs', 1 );
    $monitor_jobs   = choose_value( 'monitor_jobs', 1 );
    $checker_jobs   = choose_value( 'checker_jobs', 1 );
    $min_free_space = choose_value( 'min_free_space', 100 );
    $max_disk_age   = choose_value( 'max_disk_age', 5 );
    $DEBUG          = choose_value( 'debug', $ENV{DEBUG} || 0 );
    $pidfile        = choose_value( 'pidfile', "" );

    choose_value( 'default_mindevcount', 2 );
    $node_timeout   = choose_value( 'node_timeout', 2 );

    $old_repl_compat = choose_value( 'old_repl_compat', 1 );

    choose_value( 'no_schema_check', 0 );

    # now load plugins
    load_plugins($cfgfile{plugins}) if $cfgfile{plugins};

    choose_value('user', '');
}

### FUNCTION: choose_value( $name, $default )
sub choose_value {
    my ( $name, $default ) = @_;
    return set_config($name, $cmdline{$name}) if defined $cmdline{$name};
    return set_config($name, $cfgfile{$name}) if defined $cfgfile{$name};
    return set_config($name, $default);
}

sub load_plugins {
    my $plugins = shift;
    foreach my $plugin (split(/\s*,\s*/, $plugins)) {
        my $rv = eval "use MogileFS::Plugin::$plugin; MogileFS::Plugin::$plugin->load; 1;";
        die "Unable to load $plugin: $@\n" unless $rv;
    }
}

sub config {
    shift if @_ == 2;
    my $k = shift;
    die "No config variable '$k'" unless defined $conf{$k};
    return $conf{$k};
}

sub check_database {
    my $sto = eval { Mgd::get_store() };
    unless ($sto && $sto->ping) {
        die qq{
Error: unable to establish connection with your MogileFS database.

Please verify that you have correctly setup a configuration file or are
providing the correct information in order to reach the database and try
running the MogileFS server again.  If you haven\'t setup your database yet,
run 'mogdbsetup'.

Details: [sto=$sto, err=$@]
}
    }

    my $sversion = MogileFS::Config->server_setting('schema_version') || 0;
    my $expect_ver = MogileFS::Store->latest_schema_version;
    unless ($sversion == $expect_ver || MogileFS::Config->config('no_schema_check')) {
        die "Server's database schema version of $sversion doesn't match expected value of $expect_ver.  Halting.\n\n".
            "Please run mogdbsetup to upgrade your schema.\n";
    }

    $sto->pre_daemonize_checks;
}

# set_server_setting( key, value )
#   set value to undef to remove whatever is presently stored; returns 1 on success or
#   undef on error
sub set_server_setting {
    my ($class, $key, $val) = @_;
    return unless $key;

    my $sto = Mgd::get_store();
    $sto->set_server_setting($key, $val);
    return 1;
}

# get_server_setting( key )
#   get value of server setting, undef on error (or no result)
sub server_setting {
    my ($class, $key) = @_;
    return Mgd::get_store()->server_setting($key);
}


1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
