
package Net::Netmask;

use vars qw($VERSION);
$VERSION = 1.9011;

require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(findNetblock findOuterNetblock findAllNetblock
	cidrs2contiglists range2cidrlist sort_by_ip_address
	dumpNetworkTable sort_network_blocks cidrs2cidrs
	cidrs2inverse);
@EXPORT_OK = (@EXPORT, qw(int2quad quad2int %quadmask2bits 
	%quadhostmask2bits imask sameblock cmpblocks contains));

my $remembered = {};
my %imask2bits;
my %size2bits;
my @imask;

# our %quadmask2bits;
# our %quadhostmask2bits;

use vars qw($error $debug %quadmask2bits %quadhostmask2bits);
$debug = 1;

use strict;
use warnings;
use Carp;
use overload
	'""' => \&desc,
	'<=>' => \&cmp_net_netmask_block,
	'cmp' => \&cmp_net_netmask_block,
	'fallback' => 1; 

sub new
{
	my ($package, $net, $mask) = @_;

	$mask = '' unless defined $mask;

	my $base;
	my $bits;
	my $ibase;
	undef $error;

	if ($net =~ m,^(\d+\.\d+\.\d+\.\d+)/(\d+)$,) {
		($base, $bits) = ($1, $2);
	} elsif ($net =~ m,^(\d+\.\d+\.\d+\.\d+)[:/](\d+\.\d+\.\d+\.\d+)$,) {
		$base = $1;
		my $quadmask = $2;
		if (exists $quadmask2bits{$quadmask}) {
			$bits = $quadmask2bits{$quadmask};
		} else {
			$error = "illegal netmask: $quadmask";
		}
	} elsif ($net =~ m,^(\d+\.\d+\.\d+\.\d+)[#](\d+\.\d+\.\d+\.\d+)$,) {
		$base = $1;
		my $hostmask = $2;
		if (exists $quadhostmask2bits{$hostmask}) {
			$bits = $quadhostmask2bits{$hostmask};
		} else {
			$error = "illegal hostmask: $hostmask";
		}
	} elsif (($net =~ m,^\d+\.\d+\.\d+\.\d+$,)
		&& ($mask =~ m,\d+\.\d+\.\d+\.\d+$,)) 
	{
		$base = $net;
		if (exists $quadmask2bits{$mask}) {
			$bits = $quadmask2bits{$mask};
		} else {
			$error = "illegal netmask: $mask";
		}
	} elsif (($net =~ m,^\d+\.\d+\.\d+\.\d+$,) &&
		($mask =~ m,0x[a-z0-9]+,i)) 
	{
		$base = $net;
		my $imask = hex($mask);
		if (exists $imask2bits{$imask}) {
			$bits = $imask2bits{$imask};
		} else {
			$error = "illegal netmask: $mask ($imask)";
		}
	} elsif ($net =~ /^\d+\.\d+\.\d+\.\d+$/ && ! $mask) {
		($base, $bits) = ($net, 32);
	} elsif ($net =~ /^\d+\.\d+\.\d+$/ && ! $mask) {
		($base, $bits) = ("$net.0", 24);
	} elsif ($net =~ /^\d+\.\d+$/ && ! $mask) {
		($base, $bits) = ("$net.0.0", 16);
	} elsif ($net =~ /^\d+$/ && ! $mask) {
		($base, $bits) = ("$net.0.0.0", 8);
	} elsif ($net =~ m,^(\d+\.\d+\.\d+)/(\d+)$,) {
		($base, $bits) = ("$1.0", $2);
	} elsif ($net =~ m,^(\d+\.\d+)/(\d+)$,) {
		($base, $bits) = ("$1.0.0", $2);
	} elsif ($net eq 'default' || $net eq 'any') {
		($base, $bits) = ("0.0.0.0", 0);
	} elsif ($net =~ m,^(\d+\.\d+\.\d+\.\d+)\s*-\s*(\d+\.\d+\.\d+\.\d+)$,) {
		# whois format
		$ibase = quad2int($1);
		my $end = quad2int($2);
		$error = "illegal dotted quad: $net" 
			unless defined($ibase) && defined($end);
		my $diff = ($end || 0) - ($ibase || 0) + 1;
		$bits = $size2bits{$diff};
		$error = "could not find exact fit for $net"
			if ! defined $error && (
				! defined $bits
				|| ($ibase & ~$imask[$bits]));
	} else {
		$error = "could not parse $net";
		$error .= " $mask" if $mask;
	}

	carp $error if $error && $debug;

	$ibase = quad2int($base || 0) unless defined $ibase;
	unless (defined($ibase) || defined($error)) {
		$error = "could not parse $net";
		$error .= " $mask" if $mask;
	}
	$ibase &= $imask[$bits]
		if defined $ibase && defined $bits;

	$bits = 0 unless $bits;
	if ($bits > 32) { 
		$error = "illegal number of bits: $bits"
			unless $error;
		$bits = 32;
	}

	return bless { 
		'IBASE' => $ibase,
		'BITS' => $bits, 
		( $error ? ( 'ERROR' => $error ) : () ),
	};
}

sub new2
{
	local($debug) = 0;
	my $net = new(@_);
	return undef if $error;
	return $net;
}

sub errstr { return $error; }
sub debug  { my $this = shift; return (@_ ? $debug = shift : $debug) }

sub base { my ($this) = @_; return int2quad($this->{'IBASE'}); }
sub bits { my ($this) = @_; return $this->{'BITS'}; }
sub size { my ($this) = @_; return 2**(32- $this->{'BITS'}); }
sub next { my ($this) = @_; int2quad($this->{'IBASE'} + $this->size()); }

sub broadcast 
{
	my($this) = @_;
	int2quad($this->{'IBASE'} + $this->size() - 1);
}

*first = \&base;
*last = \&broadcast;

sub desc 
{ 
	return int2quad($_[0]->{'IBASE'}).'/'.$_[0]->{'BITS'};
}

sub imask 
{
	return (2**32 -(2** (32- $_[0])));
}

sub mask 
{
	my ($this) = @_;

	return int2quad ( $imask[$this->{'BITS'}]);
}

sub hostmask
{
	my ($this) = @_;

	return int2quad ( ~ $imask[$this->{'BITS'}]);
}

sub nth
{
	my ($this, $index, $bitstep) = @_;
	my $size = $this->size();
	my $ibase = $this->{'IBASE'};
	$bitstep = 32 unless $bitstep;
	my $increment = 2**(32-$bitstep);
	$index *= $increment;
	$index += $size if $index < 0;
	return undef if $index < 0;
	return undef if $index >= $size;
	return int2quad($ibase+$index);
}

sub enumerate
{
	my ($this, $bitstep) = @_;
	$bitstep = 32 unless $bitstep;
	my $size = $this->size();
	my $increment = 2**(32-$bitstep);
	my @ary;
	my $ibase = $this->{'IBASE'};
	for (my $i = 0; $i < $size; $i += $increment) {
		push(@ary, int2quad($ibase+$i));
	}
	return @ary;
}

sub inaddr
{
	my ($this) = @_;
	my $ibase = $this->{'IBASE'};
	my $blocks = int($this->size()/256);
	return (join('.',unpack('xC3', pack('V', $ibase))).".in-addr.arpa",
		$ibase%256, $ibase%256+$this->size()-1) if $blocks == 0;
	my @ary;
	for (my $i = 0; $i < $blocks; $i++) {
		push(@ary, join('.',unpack('xC3', pack('V', $ibase+$i*256)))
			.".in-addr.arpa", 0, 255);
	}
	return @ary;
}

sub tag
{
	my $this = shift;
	my $tag = shift;
	my $val = $this->{'T'.$tag};
	$this->{'T'.$tag} = $_[0] if @_;
	return $val;
}

sub quad2int
{
	my @bytes = split(/\./,$_[0]);

	return undef unless @bytes == 4 && ! grep {!(/\d+$/ && $_<256)} @bytes;

	return unpack("N",pack("C4",@bytes));
}

sub int2quad
{
	return join('.',unpack('C4', pack("N", $_[0])));
}

sub storeNetblock
{
	my ($this, $t) = @_;
	$t = $remembered unless $t;

	my $base = $this->{'IBASE'};

	$t->{$base} = [] unless exists $t->{$base};

	my $mb = maxblock($this);
	my $b = $this->{'BITS'};
	my $i = $b - $mb;

	$t->{$base}->[$i] = $this;
}

sub deleteNetblock
{
	my ($this, $t) = @_;
	$t = $remembered unless $t;

	my $base = $this->{'IBASE'};

	my $mb = maxblock($this);
	my $b = $this->{'BITS'};
	my $i = $b - $mb;

	return unless defined $t->{$base};

	undef $t->{$base}->[$i];

	for my $x (@{$t->{$base}}) {
		return if $x;
	}
	delete $t->{$base};
}

sub findNetblock
{
	my ($ipquad, $t) = @_;
	$t = $remembered unless $t;

	my $ip = quad2int($ipquad);
	my %done;

	for (my $b = 32; $b >= 0; $b--) {
		my $nb = $ip & $imask[$b];
		next unless exists $t->{$nb};
		my $mb = imaxblock($nb, 32);
		next if $done{$mb}++;
		my $i = $b - $mb;
		confess "$mb, $b, $ipquad, $nb" if ($i < 0 or $i > 32);
		while ($i >= 0) {
			return $t->{$nb}->[$i]
				if defined $t->{$nb}->[$i];
			$i--;
		}
	}
}

sub findOuterNetblock
{
	my ($ipquad, $t) = @_;
	$t = $remembered unless $t;

	my $ip;
	my $mask;
	if (ref($ipquad)) {
		$ip = $ipquad->{IBASE};
		$mask = $ipquad->{BITS};
	} else {
		$ip = quad2int($ipquad);
		$mask = 32;
	}

	for (my $b = 0; $b <= $mask; $b++) {
		my $nb = $ip & $imask[$b];;
		next unless exists $t->{$nb};
		my $mb = imaxblock($nb, $mask);
		my $i = $b - $mb;
		confess "$mb, $b, $ipquad, $nb" if $i < 0;
		confess "$mb, $b, $ipquad, $nb" if $i > 32;
		while ($i >= 0) {
			return $t->{$nb}->[$i]
				if defined $t->{$nb}->[$i];
			$i--;
		}
	}
}

sub findAllNetblock
{
	my ($ipquad, $t) = @_;
	$t = $remembered unless $t;
	my @ary ;
	my $ip = quad2int($ipquad);
	my %done;

	for (my $b = 32; $b >= 0; $b--) {
		my $nb = $ip & $imask[$b];
		next unless exists $t->{$nb};
		my $mb = imaxblock($nb, 32);
		next if $done{$mb}++;
		my $i = $b - $mb;
		confess "$mb, $b, $ipquad, $nb" if $i < 0;
		confess "$mb, $b, $ipquad, $nb" if $i > 32;
		while ($i >= 0) {
			push(@ary,  $t->{$nb}->[$i])
				if defined $t->{$nb}->[$i];
			$i--;
		}
	}
	return @ary;
}

sub dumpNetworkTable
{
	my ($t) = @_;
	$t = $remembered unless $t;

	my @ary;
	foreach my $base (keys %$t) {
		push(@ary, grep (defined($_), @{$t->{base}}));
		for my $x (@{$t->{$base}}) {
			push(@ary, $x)
				if defined $x;
		}
	}
	return sort @ary;
}

sub checkNetblock
{
	my ($this, $t) = @_;
	$t = $remembered unless $t;

	my $base = $this->{'IBASE'};

	my $mb = maxblock($this);
	my $b = $this->{'BITS'};
	my $i = $b - $mb;

	return defined $t->{$base}->[$i];
}

sub match
{
	my ($this, $ip) = @_;
	my $i = quad2int($ip);
	my $imask = $imask[$this->{BITS}];
	if (($i & $imask) == $this->{IBASE}) {
		return (($i & ~ $imask) || "0 ");
	} else {
		return 0;
	}
}

sub maxblock 
{ 
	my ($this) = @_;
	return imaxblock($this->{'IBASE'}, $this->{'BITS'});
}

sub imaxblock
{
	my ($ibase, $tbit) = @_;
	confess unless defined $ibase;
	while ($tbit > 0) {
		my $im = $imask[$tbit-1];
		last if (($ibase & $im) != $ibase);
		$tbit--;
	}
	return $tbit;
}

sub range2cidrlist
{
	my ($startip, $endip) = @_;

	my $start = quad2int($startip);
	my $end = quad2int($endip);

	($start, $end) = ($end, $start)
		if $start > $end;
	return irange2cidrlist($start, $end);
}

sub irange2cidrlist
{
	my ($start, $end) = @_;
	my @result;
	while ($end >= $start) {
		my $maxsize = imaxblock($start, 32);
		my $maxdiff = 32 - int(log($end - $start + 1)/log(2));
		$maxsize = $maxdiff if $maxsize < $maxdiff;
		push (@result, bless {
			'IBASE' => $start,
			'BITS' => $maxsize
		});
		$start += 2**(32-$maxsize);
	}
	return @result;
}

sub cidrs2contiglists
{
	my (@cidrs) = sort_network_blocks(@_);
	my @result;
	while (@cidrs) {
		my (@r) = shift(@cidrs);
		my $max = $r[0]->{IBASE} + $r[0]->size;
		while ($cidrs[0] && $cidrs[0]->{IBASE} <= $max) {
			my $nm = $cidrs[0]->{IBASE} + $cidrs[0]->size;
			$max = $nm if $nm > $max;
			push(@r, shift(@cidrs));
		}
		push(@result, [@r]);
	}
	return @result;
}

sub cidrs2cidrs
{
	my (@cidrs) = sort_network_blocks(@_);
	my @result;
	while (@cidrs) {
		my (@r) = shift(@cidrs);
		my $max = $r[0]->{IBASE} + $r[0]->size;
		while ($cidrs[0] && $cidrs[0]->{IBASE} <= $max) {
			my $nm = $cidrs[0]->{IBASE} + $cidrs[0]->size;
			$max = $nm if $nm > $max;
			push(@r, shift(@cidrs));
		}
		my $start = $r[0]->{IBASE};
		my $end = $max - 1;
		push(@result, irange2cidrlist($start, $end));
	}
	return @result;
}

sub cidrs2inverse
{
	my $outer = shift;
	$outer = __PACKAGE__->new($outer) unless ref($outer);
	my (@cidrs) = cidrs2cidrs(@_);
	my $first = $outer->{IBASE};
	my $last = $first + $outer->size() -1;
	shift(@cidrs) while $cidrs[0] && $cidrs[0]->{IBASE} + $cidrs[0]->size < $first;
	my @r;
	while (@cidrs && $first < $last) {
		if ($first < $cidrs[0]->{IBASE}) {
			if ($last <= $cidrs[0]->{IBASE}-1) {
				return (@r, irange2cidrlist($first, $last));
			}
			push(@r, irange2cidrlist($first, $cidrs[0]->{IBASE}-1));
		}
		last if $cidrs[0]->{IBASE} > $last;
		$first = $cidrs[0]->{IBASE} + $cidrs[0]->size;
		shift(@cidrs);
	}
	if ($first < $last) {
		push(@r, irange2cidrlist($first, $last));
	}
	return @r;
}

sub by_net_netmask_block
{
	$a->{'IBASE'} <=> $b->{'IBASE'}
		|| $a->{'BITS'} <=> $b->{'BITS'};
}

sub sameblock
{
	return ! cmpblocks(@_);
}

sub cmpblocks
{
	my $this = shift;
	my $class = ref $this;
	my $other = (ref $_[0]) ? shift : $class->new(@_);
	return cmp_net_netmask_block($this, $other);
}

sub contains
{
	my $this = shift;
	my $class = ref $this;
	my $other = (ref $_[0]) ? shift : $class->new(@_);
	return 0 if $this->{IBASE} > $other->{IBASE};
	return 0 if $this->{BITS} > $other->{BITS};
	return 0 if $other->{IBASE} > $this->{IBASE} + $this->size -1;
	return 1;
}

sub cmp_net_netmask_block
{
	return ($_[0]->{IBASE} <=> $_[1]->{IBASE} 
		|| $_[0]->{BITS} <=> $_[1]->{BITS});
}

sub sort_network_blocks
{
	return
		map $_->[0],
		sort { $a->[1] <=> $b->[1] || $a->[2] <=> $b->[2] }
		map [ $_, $_->{IBASE}, $_->{BITS} ], @_;

}

sub sort_by_ip_address
{
	return
		map $_->[0],
		sort { $a->[1] cmp $b->[1] }
		map [ $_, pack("C4",split(/\./,$_)) ], @_;

}

BEGIN {
	for (my $i = 0; $i <= 32; $i++) {
		$imask[$i] = imask($i);
		$imask2bits{$imask[$i]} = $i;
		$quadmask2bits{int2quad($imask[$i])} = $i;
		$quadhostmask2bits{int2quad(~$imask[$i])} = $i;
		$size2bits{ 2**(32-$i) } = $i;
	}
}
1;
