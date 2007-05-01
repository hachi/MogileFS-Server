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
    exec $^X, "-M$class", "-e", "$class->run;";
}

1;
