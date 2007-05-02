package Mogstored::FIDStatter;
use strict;
use warnings;
use Carp qw(croak);
use File::Find;

# on_fid => sub { my ($fidid, $size) = @_; ... }
sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;
    foreach (qw(dir from to on_fid t_stat t_readdir)) {
        $self->{$_} = delete $opts{$_};
    }
    croak("unknown opts") if %opts;
    $self->{on_fid} ||= sub {};
    $self->{t_stat} ||= sub {};
    return $self;
}

sub run {
    my $self = shift;

    my $base  = $self->{dir};
    my $start = $self->{from};
    my $end   = $self->{to};
    my $on_fid = $self->{on_fid};

    die "'$base' isn't a directory"
        unless -d $base;

    die "start and end are not both defined"
        unless length $start && length $end;

    # Example fid, and where the variables fall inside it.
    # Note that b, mmm, ttt don't actually align to billion,
    # million, and thousand all the time.
    #
    # Big fid, left aligned no padding necessary.
    # 52048709972319950
    # -------           hdir
    # -                 b
    #  ---              mmm
    #     ---           ttt
    #        ---------- file
    #
    # Small fid, right aligned and padded to 10 digits.
    #        0000023077
    #        -------    hdir
    #        -          b
    #         ---       mmm
    #            ---    ttt
    #               --- file
    #
    # start refers to the first fid to check, end refers to
    # the last fid to check.

    my ($start_hdir, $start_file, $end_hdir, $end_file);

    if ($start =~ m/^ (\d{0,6}?) (\d{1,3}) $/x) {
        ($start_hdir, $start_file) = ($1, $2);
        $start_hdir ||= 0;
    } elsif ($start =~ m/^ (\d{7}) (\d{3,}) $/x) {
        ($start_hdir, $start_file) = ($1, $2);
    } else {
        die "Couldn't parse start '$start' into dir and file parts\n";
    }

    my ($start_b, $start_mmm, $start_ttt) =
        sprintf('%07d', $start_hdir) =~ m{(\d)(\d{3})(\d{3})};

    my @start_parts = ($start_b, $start_mmm, $start_ttt, $start_file);

    if ($end =~ m/^ (\d{0,6}?) (\d{1,3}) $/x) {
        ($end_hdir, $end_file) = ($1, $2);
        $end_hdir ||= 0;
    } elsif ($end =~ m/^ (\d{7}) (\d{3,}) $/x) {
        ($end_hdir, $end_file) = ($1, $2);
    } else {
        die "Couldn't parse end '$end' into dir and file parts\n";
    }

    my ($end_b, $end_mmm, $end_ttt) =
        sprintf('%07d', $end_hdir) =~ m{(\d)(\d{3})(\d{3})};

    my @end_parts = ($end_b, $end_mmm, $end_ttt, $end_file);

    # Contains flags of whether a particular depth needs to check the start or end
    # for range on the fids/dirs. Set by the previous depth's checks, and we need
    # to see it with a pair of 1's because we want to check both bounds on the first
    # pass.
    my @start_matches = (1);
    my @end_matches   = (1);

    my $depth = -1;

    my $preprocess = sub {
        my @dirs;
        $depth++;

        die if $depth >= 4;

        my $check_regex = $depth < 3 ? qr/^(\d+)$/ : qr/^\d{7}(\d+)\.fid$/;
        my $start_part = $start_parts[$depth];
        my $end_part   = $end_parts[$depth];
        my $start_match = $start_matches[$depth];
        my $end_match   = $end_matches[$depth];

        @dirs = grep { $_ =~ $check_regex &&
                       (!$start_match || $1 >= $start_part) &&
                       (!$end_match   || $1 <= $end_part)
                     } @_;

        push @start_matches, 0;
        push @end_matches, 0;

        return sort @dirs;
    };

    my $postprocess = sub {
        $depth--;
    };

    my $wanted = sub {
        my $name = $_;

        return if $depth < 0;

        if ($depth < 3) {
            my $start_part = $start_parts[$depth];
            my $end_part   = $end_parts[$depth];

            # Set the match flags for the next depth.
            $start_matches[$depth+1] = $name eq $start_part ? 1 : 0;
            $end_matches[$depth+1]   = $name eq $end_part   ? 1 : 0;
            return;
        }

        my $size = (stat($File::Find::name))[9];
        $name =~ s/^0+//;
        $self->{on_fid}->($name, $size);
        $self->{t_stat}->($name);
    };

    find( {
        wanted      => $wanted,
        preprocess  => $preprocess,
        postprocess => $postprocess,
    }, $base);
}

1;
