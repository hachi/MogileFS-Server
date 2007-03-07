# FilePaths plugin for MogileFS, by xb95

#
# This plugin enables full pathing support within MogileFS, for creating files,
# listing files in a directory, deleting files, etc.
#
# Supports most functionality you'd expect.

package MogileFS::Plugin::FilePaths;

use strict;
use warnings;

use MogileFS::Worker::Query;

# FIXME: need to add in the configuration options for domain only

# called when this plugin is loaded, this sub must return a true value in order for
# MogileFS to consider the plugin to have loaded successfully.  if you return a
# non-true value, you MUST NOT install any handlers or other changes to the system.
# if you install something here, you MUST uninstall it in the unload sub.
sub load {

    # we want to remove the key being passed to create_open, as it is going to contain
    # only a path, and we want to ignore that for now
    MogileFS::register_global_hook( 'cmd_create_open', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});
        delete $args->{key};
    });

    # when people try to create new files, we need to intercept it and rewrite the
    # request a bit in order to do the right footwork to support paths.
    MogileFS::register_global_hook( 'cmd_create_close', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # the key is the path, so we need to move that into the logical_path argument
        # and then set the key to be something more reasonable
        $args->{logical_path} = $args->{key};
        $args->{key} = "fid:$args->{fid}";
    });

    # called when we know a file has successfully been uploaded to the system, it's
    # a done deal, we don't have to worry about anything else
    MogileFS::register_global_hook( 'file_stored', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # we need a path or this plugin is moot
        return 0 unless $args->{logical_path};

        # ensure we got a valid seeming path and filename
        my ($path, $filename) =
            ($args->{logical_path} =~ m!^(/(?:[\w\-\.]+/)*)([\w\-\.]+)$!) ? ($1, $2) : (undef, undef);

        return 0 unless defined($path) && length($path) && defined($filename) && length($filename);

        # great, let's vivify that path and get the node to it
        my $parentnodeid = MogileFS::Plugin::FilePaths::vivify_path( $args->{dmid}, $path );
        return 0 unless defined $parentnodeid;

        # see if this file exists already
        my $oldfid = MogileFS::Plugin::FilePaths::get_file_mapping( $args->{dmid}, $parentnodeid, $filename );
        if (defined $oldfid && $oldfid) {
            my $dbh = Mgd::get_dbh();
            $dbh->do("DELETE FROM file WHERE fid=?", undef, $oldfid);
            $dbh->do("REPLACE INTO file_to_delete SET fid=?", undef, $oldfid);
        }

        # and now, setup the mapping
        my $nodeid = MogileFS::Plugin::FilePaths::set_file_mapping( $args->{dmid}, $parentnodeid, $filename, $args->{fid} );
        return 0 unless $nodeid;

        # we're successful, let's keep the file
        return 1;
    });

    # and now magic conversions that make the rest of the MogileFS commands work
    # without having to understand how the path system works
    MogileFS::register_global_hook( 'cmd_get_paths', \&_path_to_key );
    MogileFS::register_global_hook( 'cmd_delete', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # ensure we got a valid seeming path and filename
        my ($path, $filename) =
            ($args->{key} =~ m!^(/(?:[\w\-\.]+/)*)([\w\-\.]+)$!) ? ($1, $2) : (undef, undef);
        return 0 unless $path && $filename;

        # now try to get the end of the path
        my $parentnodeid = MogileFS::Plugin::FilePaths::load_path( $args->{dmid}, $path );
        return 0 unless defined $parentnodeid;

        # get the fid of the file, bail out if it doesn't have one (directory nodes)
        my $fid = MogileFS::Plugin::FilePaths::get_file_mapping( $args->{dmid}, $parentnodeid, $filename );
        return 0 unless $fid;

        # great, delete this file
        delete_file_mapping( $args->{dmid}, $parentnodeid, $filename );
        # FIXME What should happen if this delete fails?

        # now pretend they asked for it and continue
        $args->{key} = "fid:$fid";
    });

    MogileFS::register_worker_command( 'filepaths_enable', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        my $dbh = Mgd::get_dbh();
        return undef unless $dbh;

        $dbh->do("REPLACE INTO plugin_filepaths_domains (dmid) VALUES (?)", undef, $dmid);

        return $self->err_line('unable_to_enable', "Unable to enable the filepaths plugin: " . $dbh->errstr)
            if $dbh->err;

        return $self->ok_line;
    });

    MogileFS::register_worker_command( 'filepaths_disable', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        my $dbh = Mgd::get_dbh();
        return undef unless $dbh;

        $dbh->do("DELETE FROM plugin_filepaths_domains WHERE dmid = ?", undef, $dmid);

        return $self->err_line('unable_to_disable', "Unable to enable the filepaths plugin: " . $dbh->errstr)
            if $dbh->err;

        return $self->ok_line;
    });

    # now let's define the extra plugin commands that we allow people to interact with us
    # just like with a regular MogileFS command
    MogileFS::register_worker_command( 'list_directory', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        # verify arguments - only one expected, make sure it starts with a /
        my $path = $args->{arg1};
        return $self->err_line('bad_params')
            unless $args->{argcount} == 1 && $path && $path =~ /^\//;

        # now find the id of the path
        my $nodeid = MogileFS::Plugin::FilePaths::load_path( $dmid, $path );
        return $self->err_line('path_not_found', 'Path provided was not found in database')
            unless defined $nodeid;

        # get files in path, return as an array
        my %res;
        my $ct = 0;
        my @nodes = MogileFS::Plugin::FilePaths::list_directory( $dmid, $nodeid );
        my $dbh = Mgd::get_dbh();

        foreach my $node (@nodes) {
            my ($nodename, $fid) = @$node;
            if ($fid) { # thjs object is a file
                my $length = $dbh->selectrow_array("SELECT length FROM file WHERE fid=?", undef, $fid);
                $res{$nodename} = "F:$length";
            } else { # This is a directory
                $res{$nodename} = "D";
            }
        }

        return $self->ok_line( \%res );
    });

    return 1;
}

# this sub is called at the end or when the module is being unloaded, this needs to
# unregister any registered methods, etc.  you MUST uninstall everything that the
# plugin has previously installed.
sub unload {

    # remove our hooks
    MogileFS::unregister_global_hook( 'cmd_create_open' );
    MogileFS::unregister_global_hook( 'cmd_create_close' );
    MogileFS::unregister_global_hook( 'file_stored' );

    return 1;
}

# called when you want to create a path, this will break down the given argument and
# create any elements needed, returning the nodeid of the final node.  returns undef
# on error, else, 0-N is valid.
sub vivify_path {
    my ($dmid, $path) = @_;
    return undef unless $dmid && $path;
    return _traverse_path($dmid, $path, 1);
}

# called to load the nodeid of the final element in a path, which is useful for finding
# out if a path exists.  does NOT automatically create path elements that don't exist.
sub load_path {
    my ($dmid, $path) = @_;
    return undef unless $dmid && $path;
    return _traverse_path($dmid, $path, 0);
}

# does the internal work of traversing a path
sub _traverse_path {
    my ($dmid, $path, $vivify) = @_;
    return undef unless $dmid && $path;

    my @paths = grep { $_ } split /\//, $path;
    return 0 unless @paths; #toplevel

    # FIXME: validate_dbh()? or not needed? assumed done elsewhere? bleh.
    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $parentnodeid = 0;
    foreach my $node (@paths) {
        # try to get the id for this node
        my $nodeid = _find_node($dbh, $dmid, $parentnodeid, $node, $vivify);
        return undef unless $nodeid;

        # this becomes the new parent
        $parentnodeid = $nodeid;
    }

    # we're done, so the parentnodeid is what we return
    return $parentnodeid;
}

# checks to see if a node exists, and if not, creates it if $vivify is set
sub _find_node {
    my ($dbh, $dmid, $parentnodeid, $node, $vivify) = @_;
    return undef unless $dbh && $dmid && defined $parentnodeid && $node;

    my $nodeid = $dbh->selectrow_array('SELECT nodeid FROM plugin_filepaths_paths ' .
                                       'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                       undef, $dmid, $parentnodeid, $node);
    return undef if $dbh->err;
    return $nodeid if $nodeid;

    if ($vivify) {
        $dbh->do('INSERT INTO plugin_filepaths_paths (nodeid, dmid, parentnodeid, nodename, fid) ' .
                 'VALUES (NULL, ?, ?, ?, NULL)', undef, $dmid, $parentnodeid, $node);
        return undef if $dbh->err;

        $nodeid = $dbh->{mysql_insertid}+0;
    }

    return undef unless $nodeid && $nodeid > 0;
    return $nodeid;
}

# sets the mapping of a file from a name to a fid
sub set_file_mapping {
    my ($dmid, $parentnodeid, $filename, $fid) = @_;
    return undef unless $dmid && defined $parentnodeid && $filename && $fid;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $nodeid = _find_node($dbh, $dmid, $parentnodeid, $filename, 1);
    return undef unless $nodeid;

    $dbh->do("UPDATE plugin_filepaths_paths SET fid = ? WHERE nodeid = ?", undef, $fid, $nodeid);
    return undef if $dbh->err;
    return $nodeid;
}

# given a domain and parent node and filename, return the fid
sub get_file_mapping {
    my ($dmid, $parentnodeid, $filename,) = @_;
    return undef unless $dmid && defined $parentnodeid && $filename;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $fid = $dbh->selectrow_array('SELECT fid FROM plugin_filepaths_paths ' .
                                    'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                    undef, $dmid, $parentnodeid, $filename);
    return undef if $dbh->err;
    return undef unless $fid && $fid > 0;
    return $fid;
}

sub delete_file_mapping {
    my ($dmid, $parentnodeid, $filename,) = @_;
    return undef unless $dmid && defined $parentnodeid && $filename;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    $dbh->do('DELETE FROM plugin_filepaths_paths WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
             undef, $dmid, $parentnodeid, $filename);

    return undef if $dbh->err;
    return 1;
}

sub list_directory {
    my ($dmid, $nodeid) = @_;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $sth = $dbh->prepare('SELECT nodename, fid FROM plugin_filepaths_paths ' .
                            'WHERE dmid = ? AND parentnodeid = ?');

    $sth->execute($dmid, $nodeid);

    my @return;

    while (my ($nodename, $fid) = $sth->fetchrow_array) {
        push @return, [$nodename, $fid];
    }

    return @return;
}

# generic sub that converts a file path to a key name that
# MogileFS will understand
sub _path_to_key {
    my $args = shift;

    # ensure we got a valid seeming path and filename
    my ($path, $filename) =
        ($args->{key} =~ m!^(/(?:[\w\-\.]+/)*)([\w\-\.]+)$!) ? ($1, $2) : (undef, undef);
    return 0 unless $path && $filename;

    # now try to get the end of the path
    my $parentnodeid = MogileFS::Plugin::FilePaths::load_path( $args->{dmid}, $path );
    return 0 unless defined $parentnodeid;

    # great, find this file
    my $fid = MogileFS::Plugin::FilePaths::get_file_mapping( $args->{dmid}, $parentnodeid, $filename );
    return 0 unless defined $fid && $fid > 0;

    # now pretend they asked for it and continue
    $args->{key} = "fid:$fid";
    return 1;
}

my %active_dmids;
my $last_dmid_check = 0;

sub _check_dmid {
    my $dmid = shift;

    return unless defined $dmid;

    my $time = time();
    if ($time >= $last_dmid_check + 15) {
        $last_dmid_check = $time;

        unless (_load_dmids()) {
            warn "Unable to load active domains list for filepaths plugin, using old list";
        }
    }

    return $active_dmids{$dmid};
}

sub _load_dmids {
    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $sth = $dbh->prepare('SELECT dmid FROM plugin_filepaths_domains');
    $sth->execute();

    return undef if $sth->err;

    %active_dmids = ();

    while (my $dmid = $sth->fetchrow_array) {
        $active_dmids{$dmid} = 1;
    }
    return 1;
}

package MogileFS::Store;

use strict;
use warnings;

sub TABLE_plugin_filepaths_paths {
    "CREATE TABLE plugin_filepaths_paths (
        nodeid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        dmid SMALLINT UNSIGNED NOT NULL,
        parentnodeid BIGINT UNSIGNED NOT NULL,
        nodename VARCHAR(255) BINARY NOT NULL,
        fid BIGINT UNSIGNED,
        PRIMARY KEY (nodeid),
        UNIQUE KEY (dmid, parentnodeid, nodename)
)"
}

sub TABLE_plugin_filepaths_domains {
    "CREATE TABLE plugin_filepaths_domains (
        dmid SMALLINT UNSIGNED NOT NULL,
        PRIMARY KEY (dmid)
)"
}

__PACKAGE__->add_extra_tables("plugin_filepaths_paths", "plugin_filepaths_domains");

1;
