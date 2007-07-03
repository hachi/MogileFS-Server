
package Gearman::Util;
use strict;

# I: to jobserver
# O: out of job server
# W: worker
# C: client of job server
# J : jobserver
our %cmd = (
            1 =>  [ 'I', "can_do" ],     # from W:  [FUNC]
            23 => [ 'I', "can_do_timeout" ], # from W: FUNC[0]TIMEOUT
            2 =>  [ 'I', "cant_do" ],    # from W:  [FUNC]
            3 =>  [ 'I', "reset_abilities" ],  # from W:  ---
            22 => [ 'I', "set_client_id" ],    # W->J: [RANDOM_STRING_NO_WHITESPACE]
            4 =>  [ 'I', "pre_sleep" ],  # from W: ---

            6 =>  [ 'O', "noop" ],        # J->W  ---
            7 =>  [ 'I', "submit_job" ],    # C->J  FUNC[0]UNIQ[0]ARGS
            21 =>  [ 'I', "submit_job_high" ],    # C->J  FUNC[0]UNIQ[0]ARGS
            18 => [ 'I', "submit_job_bg" ], # C->J     " "   "  " "

            8 =>  [ 'O', "job_created" ], # J->C HANDLE
            9 =>  [ 'I', "grab_job" ],    # W->J --
            10 => [ 'O', "no_job" ],      # J->W --
            11 => [ 'O', "job_assign" ],  # J->W HANDLE[0]FUNC[0]ARG

            12 => [ 'IO',  "work_status" ],   # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
            13 => [ 'IO',  "work_complete" ], # W->J/C: HANDLE[0]RES
            14 => [ 'IO',  "work_fail" ],     # W->J/C: HANDLE

            15 => [ 'I',  "get_status" ],  # C->J: HANDLE
            20 => [ 'O',  "status_res" ],  # C->J: HANDLE[0]KNOWN[0]RUNNING[0]NUM[0]DENOM

            16 => [ 'I',  "echo_req" ],    # ?->J TEXT
            17 => [ 'O',  "echo_res" ],    # J->? TEXT

            19 => [ 'O',  "error" ],       # J->? ERRCODE[0]ERR_TEXT

            # for worker to declare to the jobserver that this worker is only connected
            # to one jobserver, so no polls/grabs will take place, and server is free
            # to push "job_assign" packets back down.
            24 => [ 'I', "all_yours" ],    # W->J ---
            );

our %num;  # name -> num
while (my ($num, $ary) = each %cmd) {
    die if $num{$ary->[1]};
    $num{$ary->[1]} = $num;
}

sub cmd_name {
    my $num = shift;
    my $c = $cmd{$num};
    return $c ? $c->[1] : undef;
}

sub pack_req_command {
    my $type_arg = shift;
    my $type = $num{$type_arg} || $type_arg;
    die "Bogus type arg of '$type_arg'" unless $type;
    my $arg = $_[0] || '';
    my $len = length($arg);
    return "\0REQ" . pack("NN", $type, $len) . $arg;
}

sub pack_res_command {
    my $type_arg = shift;
    my $type = $num{$type_arg} || int($type_arg);
    die "Bogus type arg of '$type_arg'" unless $type;

    # If they didn't pass in anything to send, make it be an empty string.
    $_[0] = '' unless defined $_[0];
    my $len = length($_[0]);
    return "\0RES" . pack("NN", $type, $len) . $_[0];
}

# returns undef on closed socket or malformed packet
sub read_res_packet {
    my $sock = shift;
    my $err_ref = shift;

    my $buf;
    my $rv;

    my $err = sub {
        my $code = shift;
        $$err_ref = $code if ref $err_ref;
        return undef;
    };

    # read the header
    $rv = sysread($sock, $buf, 12);

    return $err->("read_error")       unless defined $rv;
    return $err->("eof")              unless $rv;
    return $err->("malformed_header") unless $rv == 12;

    my ($magic, $type, $len) = unpack("a4NN", $buf);
    return $err->("malformed_magic") unless $magic eq "\0RES";

    if ($len) {
        $rv = sysread($sock, $buf, $len);
        return $err->("short_body") unless $rv == $len;
    }

    $type = $cmd{$type};
    return $err->("bogus_command") unless $type;
    return $err->("bogus_command_type") unless index($type->[0], "O") != -1;

    return {
        'type' => $type->[1],
        'len' => $len,
        'blobref' => \$buf,
    };
}

sub send_req {
    my ($sock, $reqref) = @_;
    return 0 unless $sock;

    my $len = length($$reqref);
    local $SIG{PIPE} = 'IGNORE';
    my $rv = $sock->syswrite($$reqref, $len);
    return 0 unless $rv == $len;
    return 1;
}

# given a file descriptor number and a timeout, wait for that descriptor to
# become readable; returns 0 or 1 on if it did or not
sub wait_for_readability {
    my ($fileno, $timeout) = @_;
    return 0 unless $fileno && $timeout;

    my $rin = '';
    vec($rin, $fileno, 1) = 1;
    my $nfound = select($rin, undef, undef, $timeout);

    # nfound can be undef or 0, both failures, or 1, a success
    return $nfound ? 1 : 0;
}

1;
