package MogileFS::Sys;
use strict;
use Socket qw(MSG_NOSIGNAL);
use vars qw($FLAG_NOSIGNAL);

# used in send() calls to request not to get SIGPIPEd
eval { $FLAG_NOSIGNAL = MSG_NOSIGNAL };

sub flag_nosignal {
    return $FLAG_NOSIGNAL;
}

1;
