package Mogstored::FIDStatter;
use strict;
use warnings;
use Carp qw(croak);

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
    for (my $fid = $self->{from}; $fid <= $self->{to}; $fid++) {
        my $pad = sprintf("%010d", $fid);
        my ($b, $mmm, $ttt, $hto) = ($pad =~ m{(\d)(\d{3})(\d{3})(\d{3})});
        my $path = "$self->{dir}/$b/$mmm/$ttt/$pad.fid";
        $self->{t_stat}->($fid);
        my $size = (stat($path))[9];
        $self->{on_fid}->($fid, $size) if $size;
    }
}

1;
