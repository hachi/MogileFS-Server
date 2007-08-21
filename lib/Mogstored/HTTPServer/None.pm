package Mogstored::HTTPServer::None;
use strict;
use base 'Mogstored::HTTPServer';

# Allow the use of an existing backend DAV server not managed by mogstored

sub start {
    my $self = shift;
    return 1;
}

1;
