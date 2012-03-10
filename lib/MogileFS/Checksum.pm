package MogileFS::Checksum;
use strict;
use warnings;
use overload '""' => \&as_string;

my %TYPE = (
    "MD5"     => { type => 1, bytelen => 128 / 8 },
    "SHA-1"   => { type => 2, bytelen => 160 / 8 },
    "SHA-224" => { type => 3, bytelen => 224 / 8 },
    "SHA-256" => { type => 4, bytelen => 256 / 8 },
    "SHA-384" => { type => 5, bytelen => 384 / 8 },
    "SHA-512" => { type => 6, bytelen => 512 / 8 },
);

our %NAME2TYPE = map { $_ => $TYPE{$_}->{type} } keys(%TYPE);
our %TYPE2NAME = map { $NAME2TYPE{$_} => $_ } keys(%NAME2TYPE);

sub valid_alg {
    my ($class, $alg) = @_;

    defined($alg) && defined($TYPE{$alg});
}

sub new {
    my ($class, $row) = @_;
    my $self = bless {
        fidid => $row->{fid},
        checksum => $row->{checksum},
        hashtype => $row->{hashtype}
    }, $class;

    return $self;
}

# $string = "MD5:d41d8cd98f00b204e9800998ecf8427e"
sub from_string {
    my ($class, $fidid, $string) = @_;
    $string =~ /\A([\w-]+):([a-fA-F0-9]{32,128})\z/ or
        die "invalid checksum string";
    my $hashname = $1;
    my $hexdigest = $2;
    my $ref = $TYPE{$hashname} or
        die "invalid checksum name ($hashname) from $string";
    my $checksum = pack("H*", $hexdigest);
    my $len = length($checksum);
    $len == $ref->{bytelen} or
        die "invalid checksum length=$len (expected $ref->{bytelen})";

    bless {
        fidid => $fidid,
        checksum => $checksum,
        hashtype => $NAME2TYPE{$hashname},
    }, $class;
}

sub hashname {
    my $self = shift;
    my $type = $self->{hashtype};
    my $name = $TYPE2NAME{$type} or die "hashtype=$type unknown";

    return $name;
}

sub save {
    my $self = shift;
    my $sto = Mgd::get_store();

    $sto->set_checksum($self->{fidid}, $self->{hashtype}, $self->{checksum});
}

sub maybe_save {
    my ($self, $dmid, $classid) = @_;
    my $class = eval { Mgd::class_factory()->get_by_id($dmid, $classid) };

    # $class may be undef as it could've been deleted between
    # create_open and create_close, we've never verified this before...
    if ($class && $self->{hashtype} eq $class->{hashtype}) {
        $self->save;
    }
}

sub hexdigest {
    my $self = shift;

    unpack("H*", $self->{checksum});
}

sub as_string {
    my $self = shift;
    my $name = $self->hashname;
    my $hexdigest = $self->hexdigest;

    "Checksum[f=$self->{fidid};$name=$hexdigest]"
}

sub info {
    my $self = shift;

    $self->hashname . ':' . $self->hexdigest;
}

1;
