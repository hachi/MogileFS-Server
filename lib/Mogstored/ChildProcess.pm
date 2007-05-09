package Mogstored::ChildProcess;
use strict;

sub run {
    my $class = shift;
    die "run not implemented for $class\n";
}

sub pre_exec_init {
    my $class = shift;
    # override to setup environment ...
}

sub exec {
    my $class = shift;
    if (_running_under_par()) {
        # then we can't exec, as we'll lose magic @INC
        # ghetto:
        #for (3..100) { POSIX::close($_); }
        my $rv = eval "use $class; 1" or die "Failed to load $class: $@\n";
        $class->run;
    } else {
        exec $^X, "-M$class", "-e", "$class->run;";
    }
    die "$class run loop ended!\n";
}

sub _running_under_par {
    # not the best test in the world, but works.
    return (grep { ref $_ eq "CODE" } @INC) ? 1 : 0;
}

1;
