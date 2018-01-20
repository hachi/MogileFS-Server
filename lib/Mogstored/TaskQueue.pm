# low priority task queue which limits jobs (currently MD5 digest requests)
package Mogstored::TaskQueue;
use fields (
            'active',  # number of active tasks
            'max',     # maximum active tasks before deferring to pending
            'pending', # pending code refs for execution
           );

sub new {
    my Mogstored::TaskQueue $self = shift;
    $self = fields::new($self) unless ref $self;
    $self->{active} = 0;
    $self->{max} = 1;
    $self->{pending} = [];
    $self;
}

sub run {
    my ($self, $task) = @_;

    if ($self->{active} < $self->{max}) {
        $self->{active}++;
        $task->();
    } else {
        push @{$self->{pending}}, $task;
    }
}

sub task_done {
    my $self = shift;

    $self->{active}--;
    if ($self->{active} < $self->{max}) {
        my $task = shift @{$self->{pending}};
        if ($task) {
            $self->{active}++;
            $task->();
        }
    }
}

1;
