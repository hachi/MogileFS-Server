package Mogstored::FIDStatter;
use strict;
use warnings;
use Carp qw(croak);

# on_fid => sub { my ($fidid, $size) = @_; ... }
# t_stat => sub { my $fid = shift }
sub new {
    my ($class, %opts) = @_;
    my $self = bless {}, $class;
    foreach (qw(dir from to on_fid t_stat)) {
        $self->{$_} = delete $opts{$_};
    }
    croak("unknown opts") if %opts;
    $self->{on_fid} ||= sub {};
    $self->{t_stat} ||= sub {};
    return $self;
}

sub run {
    my $self = shift;

    # min/max dirs we could possibly care about format: "n/nnn/nnn/"
    my $min_dir = dir($self->{from});
    my $max_dir = dir($self->{to});

    # our start/end fid ranges, zero-padded to 25 or so digits, to be
    # string-comparable, avoiding integer math (this might be a 32-bit
    # machine, with a 64-bit mogilefsd/clients)
    my $min_zpad = zeropad($self->{from});
    my $max_zpad = zeropad($self->{to});

    my $dir_in_range = sub {
        my $dir = shift; # "n/[nnn/[nnnn/]]"
        return 0 if max_subdir($dir) lt $min_dir;
        return 0 if min_subdir($dir) gt $max_dir;
        return 1;
    };

    my $file_in_range = sub {
        my $fid = zeropad(shift);
        return $fid ge $min_zpad && $fid le $max_zpad;
    };

    foreach_dentry($self->{dir}, qr/^\d$/, sub {
        my ($bdir, $dir) = @_;
        return unless $dir_in_range->("$bdir/");

        foreach_dentry($dir, qr/^\d{3}$/, sub {
            my ($mdir, $dir) = @_;
            return unless $dir_in_range->("$bdir/$mdir/");

            foreach_dentry($dir, qr/^\d{3}$/, sub {
                my ($tdir, $dir) = @_;
                return unless $dir_in_range->("$bdir/$mdir/$tdir/");

                foreach_dentry($dir, qr/^\d+\.fid$/, sub {
                    my ($file, $fullfile) = @_;
                    my ($fid) = ($file =~ /^0*(\d+)\.fid$/);
                    return unless $file_in_range->($fid);

                    $self->{t_stat}->($fid);
                    my $size = (stat($fullfile))[9];
                    $self->{on_fid}->($fid, $size) if $size;
                });
            });
        });
    });
}

sub zeropad {
    my $fid = shift;
    return "0"x(25-length($fid)) . $fid;
}

sub foreach_dentry {
    my ($dir, $re, $code) = @_;
    opendir(my $dh, $dir) or die "Failed to open $dir: $!";
    $code->($_, "$dir/$_") foreach sort grep { /$re/ } readdir($dh);
}

# returns directory that a fid will be in
# $fid may or may not have leading zeroes.
sub dir {
    my $fid = shift;
    $fid =~ s!^0*!!;
    $fid = "0"x(10-length($fid)) . $fid if length($fid) < 10;
    my ($b, $mmm, $ttt) = $fid =~ m{^(\d)(\d{3})(\d{3})};
    return "$b/$mmm/$ttt/";
}

sub max_subdir { pad_dir($_[0], "999"); }
sub min_subdir { pad_dir($_[0], "000"); }

sub pad_dir {
    my ($dir, $pad) = @_;
    if (length($dir) ==  2) { return "$dir$pad/$pad/" }
    if (length($dir) ==  6) { return "$dir$pad/"      }
    if (length($dir) == 10) { return $dir             }
    Carp::confess("how do I pad '$dir' ?");
}

1;
