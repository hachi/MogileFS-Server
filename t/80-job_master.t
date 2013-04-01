# -*-perl-*-
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use MogileFS::Server;
use MogileFS::Test;

my @jm_jobs = qw(fsck delete replicate);
my $jobs;

my $sto = eval { temp_store(); };
if (!$sto) {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

my $tmptrack = create_temp_tracker($sto, ["--no-job_master"]);
my $admin = IO::Socket::INET->new(PeerAddr => '127.0.0.1:7001');
$admin or die "failed to create admin socket: $!";

sub jobs {
    my ($admin) = @_;
    my %ret;

    syswrite($admin, "!jobs\r\n");
    MogileFS::Util::wait_for_readability(fileno($admin), 10);
    while (1) {
        my $line = <$admin>;
        $line =~ s/\r\n//;
        last if $line eq ".";
        $line =~ /^(\w+ \w+)\s*(.*)$/ or die "Failed to parse $line\n";
        $ret{$1} = $2;
    }
    return \%ret;
}

ok(try_for(30, sub { jobs($admin)->{"queryworker count"} }), "wait for queryworker");

$jobs = jobs($admin);
foreach my $job (@jm_jobs) {
    ok(!$jobs->{"$job count"}, "no $job workers");
}

# enable job master
want($admin, 1, "job_master");

ok(try_for(30, sub { jobs($admin)->{"queryworker count"} }), "wait for queryworker");

foreach my $job (@jm_jobs) {
    ok(try_for(30, sub { jobs($admin)->{"$job count"} }), "wait for $job");
}

# disable job_master again
want($admin, 0, "job_master");

foreach my $job (@jm_jobs) {
    ok(try_for(30, sub { !jobs($admin)->{"$job count"} }), "wait for $job to die");
}

done_testing();
