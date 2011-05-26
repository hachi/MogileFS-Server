# -*-perl-*-

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

use MogileFS::Server;
use MogileFS::Util qw(error_code);
use MogileFS::Test;
use MogileFS::Factory;
use MogileFS::Factory::Domain;
use MogileFS::Factory::Class;
use MogileFS::Domain;
use MogileFS::Class;

use Data::Dumper qw/Dumper/;

my $sto = eval { temp_store(); };
if ($sto) {
    plan tests => 33;
} else {
    plan skip_all => "Can't create temporary test database: $@";
    exit 0;
}

# Fetch the factories.
my $domfac = MogileFS::Factory::Domain->get_factory;
ok($domfac, "got a domain factory");
my $classfac = MogileFS::Factory::Class->get_factory;
ok($classfac, "got a class factory");

# Ensure the inherited singleton is good.
ok($domfac != $classfac, "factories are not the same singleton");

{
    # Add in a test domain.
    my $dom = $domfac->set({ dmid => 1, namespace => 'toast'});
    ok($dom, "made a new domain object");
    is($dom->id, 1, "domain id is 1");
    is($dom->name, 'toast', 'domain namespace is toast');

    # Add in a test class.
    my $cls = $classfac->set({ classid => 1, dmid => 1, mindevcount => 3,
        replpolicy => '', classname => 'fried'});
    ok($cls, "got a class object");
    is($cls->id, 1, "class id is 1");
    is($cls->name, 'fried', 'class name is fried');
    is(ref($cls->domain), 'MogileFS::Domain',
        'class can find a domain object');
}

# Add a few more classes and domains.
{
    my $dom2 = $domfac->set({ dmid => 2, namespace => 'harro' });
    $classfac->set({ classid => 1, dmid => 2, mindevcount => 2,
        replpolicy => '', classname => 'red' });
    $classfac->set({ classid => 2, dmid => 2, mindevcount => 3,
        replpolicy => 'MultipleHosts(2)', classname => 'green' });
    $classfac->set({ classid => 3, dmid => 2, mindevcount => 4,
        replpolicy => 'MultipleHosts(5)', classname => 'blue' });
}

# Ensure the select and remove factory methods work.
{
    my $dom = $domfac->get_by_id(1);
    is($dom->name, 'toast', 'got the right domain from get_by_id');
}

{
    my $dom = $domfac->get_by_name('harro');
    is($dom->id, 2, 'got the right domain from get_by_name');
}

{
    my @doms = $domfac->get_all;
    is(scalar(@doms), 2, 'got two domains back from get_all');
    for (@doms) {
        is(ref($_), 'MogileFS::Domain', 'and both are domains');
    }
    isnt($doms[0]->id, $doms[1]->id, 'and both are not the same');
}

{
    my $dom    = $domfac->get_by_name('harro');
    my $clsmap = $classfac->map_by_id($dom);
    is(ref($clsmap), 'HASH', 'got a mapped class hash');
    is($clsmap->{2}->name, 'green', 'got the right class set');

    $classfac->remove($clsmap->{2});

    my $cls = $classfac->get_by_name($dom, 'green');
    ok(!$cls, "class removed from factory");
}

# Test the domain routines harder.
{
    my $dom = $domfac->get_by_name('harro');
    my @classes = $dom->classes;
    # Magic "default" class is included
    is(scalar(@classes), 3, 'found three classes');

    ok($dom->class('blue'), 'found the blue class');
    ok(!$dom->class('fried'), 'did not find the fried class');
}

# Test the class routines harder.
{
    my $dom = $domfac->get_by_name('harro');
    my $cls = $dom->class('blue');
    my $polobj = $cls->repl_policy_obj;
    ok($polobj, 'class can create policy object');
}

# Add a domain and two classes to the DB.
{
    my $domid = $sto->create_domain('foo');
    ok($domid, 'new domain stored in database: ' . $domid);

    my $clsid1 = $sto->create_class($domid, 'bar');
    my $clsid2 = $sto->create_class($domid, 'baz');
    is($clsid1, 1, 'new class1 stored in database');
    is($clsid2, 2, 'new class2 stored in database');

    ok($sto->update_class_mindevcount(dmid => $domid, classid => $clsid2,
        mindevcount => 3), 'can set mindevcount');
    ok($sto->update_class_replpolicy(dmid => $domid, classid => $clsid2,
        replpolicy => 'MultipleHosts(6)'), 'can set replpolicy');
    ok($sto->update_class_name(dmid => $domid, classid => $clsid2,
        classname => 'boo'), 'can rename class');
}

{
    # Reload from the DB and confirm they came back the way they went in.
    my %domains = $sto->get_all_domains;
    ok(exists $domains{foo}, 'domain foo exists');
    is($domains{foo}, 1, 'and the id is 1');
    my @classes = $sto->get_all_classes;
    is_deeply($classes[0], {
        'replpolicy' => undef,
        'dmid' => '1',
        'classid' => '1',
        'mindevcount' => '2',
        'classname' => 'bar'
    }, 'class bar came back');
    # We edited class2 a bunch, make sure that all stuck. 
    is_deeply($classes[1], {
        'replpolicy' => 'MultipleHosts(6)',
        'dmid' => '1',
        'classid' => '2',
        'mindevcount' => '3',
        'classname' => 'boo'
    }, 'class baz came back as boo');
}
