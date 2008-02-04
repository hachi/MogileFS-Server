# -*-perl-*-

use strict;
use warnings;
use Test::More tests => 11;
use Data::Dumper;
use Carp qw(croak);

use MogileFS::ReplicationPolicy::MultipleHosts;

{
    my %all_devs;

    for my $i (1..3) {
        $all_devs{$i} = MogileFS::Test::Device->new(
            hostid          => 1,
            id              => $i,
            state           => "alive",
            percent_free    => 0,
            should_get_replicated_files => 0,
        );
    }
    for my $i (4..6) {
        $all_devs{$i} = MogileFS::Test::Device->new(
            hostid          => 2,
            id              => $i,
            state           => "alive",
            percent_free    => 0,
            should_get_replicated_files => 0,
        );
    }
    for my $i (7..9) {
        $all_devs{$i} = MogileFS::Test::Device->new(
            hostid          => 3,
            id              => $i,
            state           => "alive",
            percent_free    => .2,
            should_get_replicated_files => 1,
        );
    }
    for my $i (10..12) {
        $all_devs{$i} = MogileFS::Test::Device->new(
            hostid          => 4,
            id              => $i,
            state           => "alive",
            percent_free    => .2,
            should_get_replicated_files => 1,
        );
    }

    # Now for the actual tests

    {
        my $res = run(
            mindevcount  => 3,
            policy_class => "MogileFS::ReplicationPolicy::MultipleHosts",
            on_devs      => [ $all_devs{4}, $all_devs{7}, $all_devs{10} ],
            all_devs     => \%all_devs,
        );

        ok($res->is_happy, "Expected happiness");
        ok(!$res->too_happy, "... but not too happy");
    }

    {
        my $res = run(
            mindevcount  => 3,
            policy_class => "MogileFS::ReplicationPolicy::MultipleHosts",
            on_devs      => [ $all_devs{1}, $all_devs{4}, $all_devs{7}, $all_devs{10} ],
            all_devs     => \%all_devs,
        );

        ok($res->is_happy, "Expected happiness");
        ok($res->too_happy, "... and too happy too");
    }

    {
        my $res = run(
            mindevcount  => 3,
            policy_class => "MogileFS::ReplicationPolicy::MultipleHosts",
            on_devs      => [ $all_devs{1}, $all_devs{2}, $all_devs{4} ],
            all_devs     => \%all_devs,
        );

        ok(!$res->is_happy, "Expected unhappiness");

        my @ideals = $res->copy_to_one_of_ideally;
        ok(@ideals, "List of ideal devices");

        my @desperate = $res->copy_to_one_of_desperate;
        is(@desperate, 0, "Empty list of desperate devices");
    }

    {
        my $res = run(
            mindevcount  => 3,
            policy_class => "MogileFS::ReplicationPolicy::MultipleHosts",
            on_devs      => [ $all_devs{7}, $all_devs{10} ],
            all_devs     => \%all_devs,
        );

        ok(!$res->is_happy, "Expected unhappiness");

        my @ideals = $res->copy_to_one_of_ideally;
        is(@ideals, 0, "No ideal devices");

        my @desperate = $res->copy_to_one_of_desperate;
        ok(@desperate, "List of desperate devices");
    }

    {
        my $res = run(
            mindevcount  => 3,
            policy_class => "MogileFS::ReplicationPolicy::MultipleHosts",
            on_devs      => [ $all_devs{7}, $all_devs{10}, $all_devs{11} ],
            all_devs     => \%all_devs,
        );

        ok($res->temp_fail, "Expected temporary failure");
    }
}

sub run {
    my %args = @_;

    my $fidid        = delete($args{fidid})        || 1;
    my $mindevcount  = delete($args{mindevcount})  || croak "mindevcount arg required";
    my $policy_class = delete($args{policy_class}) || croak "policy_class arg required";
    my $on_devs      = delete($args{on_devs})      || croak "on_devs arg required";
    my $all_devs     = delete($args{all_devs})     || croak "all_devs arg required";
    my $failed      = delete($args{failed})        || {};

    eval "use $policy_class";
    my $policy = $policy_class->new;
    my $class = MogileFS::Test::Class->new(
        repl_policy_obj => $policy,
        mindevcount => $mindevcount,
    );
    my $devfid = MogileFS::Test::DevFID->new(
        id    => $fidid,
        class => $class,
    );

    my $polobj = $class->repl_policy_obj;

    return $polobj->replicate_to(
        fid      => $fidid,
        on_devs  => $on_devs,
        all_devs => $all_devs,
        failed   => $failed,
        min      => $mindevcount,
    );
}

package MogileFS::Test::Device;

use MogileFS::DeviceState;
use Carp qw(croak);

sub new {
    my $class = shift;

    my %opts = @_;

    my $self = bless {}, (ref $class || $class);

    foreach my $optkey (qw(id hostid state should_get_replicated_files percent_free)) {
        croak "$optkey argument not supplied" unless exists $opts{$optkey};
        $self->{$optkey} = delete $opts{$optkey};
    }

    croak "Extra args:" if (keys %opts);

    $self->{dstate} = MogileFS::DeviceState->of_string($self->{state});

    return $self;
}

sub hostid {
    return $_[0]->{hostid};
}

sub id {
    return $_[0]->{id};
}

sub devid {
    return $_[0]->{id};
}

sub dstate {
    return $_[0]->{dstate};
}

sub should_get_replicated_files {
    return $_[0]->{should_get_replicated_files};
}

sub percent_free {
    return $_[0]->{percent_free};
}

package MogileFS::Test::DevFID;

use strict;
use warnings;

sub new {
    my $class = shift;
    my %opts = @_;

    my $self = bless {}, (ref $class || $class);

    foreach my $optkey (qw(id class)) {
        $self->{$optkey} = delete $opts{$optkey} || die("$optkey argument not supplied");
    }

    die "Extra args:" if (keys %opts);

    return $self;
}

sub id {
    return $_[0]->{id};
}

sub class {
    return $_[0]->{class};
}

package MogileFS::Test::Class;

use strict;
use warnings;

sub new {
    my $class = shift;

    my %opts = @_;

    my $self = bless {}, (ref $class || $class);

    foreach my $optkey (qw(repl_policy_obj mindevcount)) {
        $self->{$optkey} = delete $opts{$optkey} || die("$optkey argument not supplied");
    }

    die "Extra args:" if (keys %opts);

    return $self;
}

sub repl_policy_obj {
    return $_[0]->{repl_policy_obj};
}

sub mindevcount {
    return $_[0]->{mindevcount};
}

