package MogileFS::Worker::Query;
# responds to queries from Mogile clients

use strict;
use warnings;

use base 'MogileFS::Worker';
use fields qw(querystarttime reqid);
use MogileFS::Util qw(error error_code first weighted_list
                      device_state eurl decode_url_args);
use MogileFS::HTTPFile;

sub new {
    my ($class, $psock) = @_;
    my $self = fields::new($class);
    $self->SUPER::new($psock);

    $self->{querystarttime} = undef;
    $self->{reqid}          = undef;
    return $self;
}

# no query should take 30 seconds, and we check in every 5 seconds.
sub watchdog_timeout { 30 }

# called by plugins to register a command in the namespace
sub register_command {
    my ($cmd, $sub) = @_;

    # validate the command, then convert it to the actual thing the user
    # will be calling
    return 0 unless $cmd =~ /^[\w\d]+$/;
    $cmd = "plugin_$cmd";

    # register in namespace with 'cmd_' which we will automatically find
    no strict 'refs';
    *{"cmd_$cmd"} = $sub;

    # all's well
    return 1;
}

sub work {
    my $self = shift;
    my $psock = $self->{psock};
    my $rin = '';
    vec($rin, fileno($psock), 1) = 1;
    my $buf;

    while (1) {
        my $rout;
        unless (select($rout=$rin, undef, undef, 5.0)) {
            $self->still_alive;
            next;
        }

        my $newread;
        my $rv = sysread($psock, $newread, 1024);
        if (!$rv) {
            if (defined $rv) {
                die "While reading pipe from parent, got EOF.  Parent's gone.  Quitting.\n";
            } else {
                die "Error reading pipe from parent: $!\n";
            }
        }
        $buf .= $newread;

        while ($buf =~ s/^(.+?)\r?\n//) {
            my $line = $1;
            if ($self->process_generic_command(\$line)) {
                $self->still_alive;  # no-op for watchdog
            } else {
                $self->validate_dbh;
                $self->process_line(\$line);
            }
        }
    }
}

sub process_line {
    my MogileFS::Worker::Query $self = shift;
    my $lineref = shift;

    # see what kind of command this is
    return $self->err_line('unknown_command')
        unless $$lineref =~ /^(\d+-\d+)?\s*(\S+)\s*(.*)/;

    $self->{reqid} = $1 || undef;
    my ($client_ip, $line) = ($2, $3);

    # set global variables for zone determination
    local $MogileFS::REQ_client_ip = $client_ip;

    # Use as array here, otherwise we get a string which breaks usage of
    # Time::HiRes::tv_interval further on.
    $self->{querystarttime} = [ Time::HiRes::gettimeofday() ];

    # fallback to normal command handling
    if ($line =~ /^(\w+)\s*(.*)/) {
        my ($cmd, $args) = ($1, $2);
        $cmd = lc($cmd);

        no strict 'refs';
        my $cmd_handler = *{"cmd_$cmd"}{CODE};
        if ($cmd_handler) {
            my $args = decode_url_args(\$args);
            local $MogileFS::REQ_altzone = ($args->{zone} && $args->{zone} eq 'alt');
            eval {
                $cmd_handler->($self, $args);
            };
            if ($@) {
                my $errc = error_code($@);
                if ($errc eq "dup") {
                    return $self->err_line("dup");
                } else {
                    warn "Error: $@\n";
                    error("Error running command '$cmd': $@");
                    return $self->err_line("failure");
                }
            }
            return;
        }
    }

    return $self->err_line('unknown_command');
}

# this is a half-finished command.  in particular, errors tend to
# crash the parent or child or something.  it's a quick hack for a quick
# ops task that needs done.  note in particular how it reaches across
# package boundaries into an API that the Replicator probably doesn't
# want exposed.
sub cmd_httpcopy {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    my $sdevid = $args->{sdevid};
    my $ddevid = $args->{ddevid};
    my $fid    = $args->{fid};

    my $err;
    my $rv = MogileFS::Worker::Replicate::http_copy(sdevid => $sdevid,
                                                    ddevid => $ddevid,
                                                    fid    => $fid,
                                                    errref => \$err);
    if ($rv) {
        my $dfid = MogileFS::DevFID->new($ddevid, $fid);
        $dfid->add_to_db
            or return $self->err_line("copy_err", "failed to add link to database");
        return $self->ok_line;
    } else {
        return $self->err_line("copy_err", $err);
    }
}

# returns 0 on error, or dmid of domain
sub check_domain {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $domain = $args->{domain};

    return $self->err_line("no_domain") unless defined $domain && length $domain;

    # validate domain
    my $dmid = MogileFS::Domain->id_of_name($domain) or
        return $self->err_line("unreg_domain");

    return $dmid;
}

sub cmd_sleep {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    sleep($args->{duration} || 10);
    return $self->ok_line;
}

sub cmd_test {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    die "Crashed on purpose" if $args->{crash};
    return $self->ok_line;
}

sub cmd_clear_cache {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    MogileFS::Device->invalidate_cache if $args->{devices} || $args->{all};
    MogileFS::Host->invalidate_cache   if $args->{hosts}   || $args->{all};
    MogileFS::Class->invalidate_cache  if $args->{class}   || $args->{all};
    MogileFS::Domain->invalidate_cache if $args->{domain}  || $args->{all};

    return $self->ok_line;
}

sub cmd_create_open {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # has to be filled out for some plugins
    $args->{dmid} = $self->check_domain($args)
        or return $self->err_line('domain_not_found');

    # first, pass this to a hook to do any manipulations needed
    eval {MogileFS::run_global_hook('cmd_create_open', $args)};

    return $self->err_line("plugin_aborted", "$@")
        if $@;

    # validate parameters
    my $dmid = $args->{dmid};
    my $key = $args->{key} || "";
    my $multi = $args->{multi_dest} ? 1 : 0;

    # optional profiling of stages, if $args->{debug_profile}
    my @profpoints;  # array of [point,hires-starttime]
    my $profstart = sub {
        my $pt = shift;
        push @profpoints, [$pt, Time::HiRes::time()];
    };
    $profstart = sub {} unless $args->{debug_profile};
    $profstart->("begin");

    # we want it to be undef if not explicit, else force to numeric
    my $exp_fidid = $args->{fid} ? int($args->{fid}) : undef;

    # get DB handle
    my $sto = Mgd::get_store();

    # figure out what classid this file is for
    my $class = $args->{class} || "";
    my $classid = 0;
    if (length($class)) {
        $classid = MogileFS::Class->class_id($dmid, $class)
            or return $self->err_line("unreg_class");
    }

    # if we haven't heard from the monitoring job yet, we need to chill a bit
    # to prevent a race where we tell a user that we can't create a file when
    # in fact we've just not heard from the monitor
    $profstart->("wait_monitor");
    $self->wait_for_monitor;

    $profstart->("find_deviceid");

    my @devices;

    unless (MogileFS::run_global_hook('cmd_create_open_order_devices', [MogileFS::Device->devices], \@devices)) {
        @devices = sort_devs_by_freespace(MogileFS::Device->devices);
    }

    # find suitable device(s) to put this file on.
    my @dests; # MogileFS::Device objects which are suitable

    while (scalar(@dests) < ($multi ? 3 : 1)) {
        my $ddev = shift @devices;

        last unless $ddev;
        next unless $ddev->not_on_hosts(map { $_->host } @dests);

        push @dests, $ddev;
    }
    return $self->err_line("no_devices") unless @dests;

    my $fidid = eval {
        $sto->register_tempfile(
                                fid     => $exp_fidid, # may be undef/NULL to mean auto-increment
                                dmid    => $dmid,
                                key     => $key,
                                classid => $classid,
                                devids  => join(',', map { $_->id } @dests),
                                );
    };
    unless ($fidid) {
        my $errc = error_code($@);
        return $self->err_line("fid_in_use") if $errc eq "dup";
        warn "Error registering tempfile: $@\n";
        return $self->err_line("db");
    }

    # make sure directories exist for client to be able to PUT into
    foreach my $dev (@dests) {
        $profstart->("vivify_dir_on_dev" . $dev->id);
        my $dfid = MogileFS::DevFID->new($dev, $fidid);
        $dfid->vivify_directories;
    }

    $profstart->("end");

    # common reply variables
    my $res = {
        fid => $fidid,
    };

    # add profiling data
    if (@profpoints) {
        $res->{profpoints} = 0;
        for (my $i=0; $i<$#profpoints; $i++) {
            my $ptnum = ++$res->{profpoints};
            $res->{"prof_${ptnum}_name"} = $profpoints[$i]->[0];
            $res->{"prof_${ptnum}_time"} =
                sprintf("%0.03f",
                        $profpoints[$i+1]->[1] - $profpoints[$i]->[1]);
        }
    }

    # add path info
    if ($multi) {
        my $ct = 0;
        foreach my $dev (@dests) {
            $ct++;
            $res->{"devid_$ct"} = $dev->id;
            $res->{"path_$ct"} = MogileFS::DevFID->new($dev, $fidid)->url;
        }
        $res->{dev_count} = $ct;
    } else {
        $res->{devid} = $dests[0]->id;
        $res->{path}  = MogileFS::DevFID->new($dests[0], $fidid)->url;
    }

    return $self->ok_line($res);
}

sub sort_devs_by_freespace {
    my @devices_with_weights = map {
        [$_, 100 * $_->percent_free]
    } sort {
        $b->percent_free <=> $a->percent_free;
    } grep {
        $_->exists &&
        $_->should_get_new_files;
    } @_;

    my @list =
        MogileFS::Util::weighted_list(splice(@devices_with_weights, 0, 20));

    return @list;
}

sub cmd_create_close {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # has to be filled out for some plugins
    $args->{dmid} = $self->check_domain($args)
        or return $self->err_line('domain_not_found');

    # call out to a hook that might modify the arguments for us
    MogileFS::run_global_hook('cmd_create_close', $args);

    # late validation of parameters
    my $dmid  = $args->{dmid};
    my $key   = $args->{key};
    my $fidid = $args->{fid}    or return $self->err_line("no_fid");
    my $devid = $args->{devid}  or return $self->err_line("no_devid");
    my $path  = $args->{path}   or return $self->err_line("no_path");

    my $fid  = MogileFS::FID->new($fidid);
    my $dfid = MogileFS::DevFID->new($devid, $fid);

    # is the provided path what we'd expect for this fid/devid?
    return $self->err_line("bogus_args")
        unless $path eq $dfid->url;

    my $sto = Mgd::get_store();

    # find the temp file we're closing and making real.  If another worker
    # already has it, bail out---the client closed it twice.
    my $trow = $sto->delete_and_return_tempfile_row($fidid) or
        return $self->err_line("no_temp_file");

    # if a temp file is closed without a provided-key, that means to
    # delete it.
    unless (defined $key && length($key)) {
        $dfid->add_to_db;
        $fid->delete;
        return $self->ok_line;
    }

    # see if we have a fid for this key already
    my $old_fid = MogileFS::FID->new_from_dmid_and_key($dmid, $key);
    if ($old_fid) {
        # Fail if a file already exists for this fid.  Should never
        # happen, as it should not be possible to close a file twice.
        return $self->err_line("fid_exists")
            unless $old_fid->{fidid} != $fidid;

        $old_fid->delete;
    }

    # get size of file and verify that it matches what we were given, if anything
    my $size = MogileFS::HTTPFile->at($path)->size;

    # size check is optional? Needs to support zero byte files.
    $args->{size} = -1 unless $args->{size};
    if (!defined($size) || $size == MogileFS::HTTPFile::FILE_MISSING) {
        # storage node is unreachable or the file is missing
        my $type    = defined $size ? "missing" : "cantreach";
        my $lasterr = MogileFS::Util::last_error();
        return $self->err_line("size_verify_error", "Expected: $args->{size}; actual: 0 ($type); path: $path; error: $lasterr")
    }

    return $self->err_line("size_mismatch", "Expected: $args->{size}; actual: $size; path: $path")
        if $args->{size} > -1 && ($args->{size} != $size);

    # TODO: check for EIO?

    # insert file_on row
    $dfid->add_to_db;

    $sto->replace_into_file(
                            fidid   => $fidid,
                            dmid    => $dmid,
                            key     => $key,
                            length  => $size,
                            classid => $trow->{classid},
                            );

    # mark it as needing replicating:
    $fid->enqueue_for_replication(from_device => $devid);

    if ($fid->update_devcount) {
        # call the hook - if this fails, we need to back the file out
        my $rv = MogileFS::run_global_hook('file_stored', $args);
        if (defined $rv && ! $rv) { # undef = no hooks, 1 = success, 0 = failure
            $fid->delete;
            return $self->err_line("plugin_aborted");
        }

        # all went well
        return $self->ok_line;
    } else {
        # FIXME: handle this better
        return $self->err_line("db_error");
    }
}

sub cmd_updateclass {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    $args->{dmid} = $self->check_domain($args)
        or return $self->err_line('domain_not_found');

    my $dmid  = $args->{dmid};
    my $key   = $args->{key}        or return $self->err_line("no_key");
    my $class = $args->{class}      or return $self->err_line("no_class");

    my $classid = MogileFS::Class->class_id($dmid, $class)
        or return $self->err_line('class_not_found');

    my $fid = MogileFS::FID->new_from_dmid_and_key($dmid, $key)
        or return $self->err_line('invalid_key');

    my @devids = $fid->devids;
    return $self->err_line("no_devices") unless @devids;

    if ($fid->classid != $classid) {
        $fid->update_class(classid => $classid);
        $fid->enqueue_for_replication();
    }

    return $self->ok_line;
}

sub cmd_delete {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # validate domain for plugins
    $args->{dmid} = $self->check_domain($args)
        or return $self->err_line('domain_not_found');

    # now invoke the plugin, abort if it tells us to
    my $rv = MogileFS::run_global_hook('cmd_delete', $args);
    return $self->err_line('plugin_aborted')
        if defined $rv && ! $rv;

    # validate parameters
    my $dmid = $args->{dmid};
    my $key = $args->{key} or return $self->err_line("no_key");

    # is this fid still owned by this key?
    my $fid = MogileFS::FID->new_from_dmid_and_key($dmid, $key)
        or return $self->err_line("unknown_key");

    $fid->delete;

    return $self->ok_line;
}

sub cmd_list_fids {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # validate parameters
    my $fromfid = ($args->{from} || 0)+0;
    my $tofid = ($args->{to} || 0)+0;
    $tofid ||= ($fromfid + 100);
    $tofid = ($fromfid + 100)
        if $tofid > $fromfid + 100 ||
           $tofid < $fromfid;

    my $rows = Mgd::get_store()->file_row_from_fidid_range($fromfid, $tofid);
    return $self->err_line('failure') unless $rows;
    return $self->ok_line({ fid_count => 0 }) unless @$rows;

    # setup temporary storage of class/host
    my (%domains, %classes);

    # now iterate over our data rows and construct result
    my $ct = 0;
    my $ret = {};
    foreach my $r (@$rows) {
        $ct++;
        my $fid = $r->{fid};
        $ret->{"fid_${ct}_fid"} = $fid;
        $ret->{"fid_${ct}_domain"} = ($domains{$r->{dmid}} ||= MogileFS::Domain->name_of_id($r->{dmid}));
        $ret->{"fid_${ct}_class"} = ($classes{$r->{dmid}}{$r->{classid}} ||= MogileFS::Class->class_name($r->{dmid}, $r->{classid}));
        $ret->{"fid_${ct}_key"} = $r->{dkey};
        $ret->{"fid_${ct}_length"} = $r->{length};
        $ret->{"fid_${ct}_devcount"} = $r->{devcount};
    }
    $ret->{fid_count} = $ct;
    return $self->ok_line($ret);
}

sub cmd_list_keys {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # validate parameters
    my $dmid = $self->check_domain($args)
        or return $self->err_line('domain_not_found');
    my ($prefix, $after, $limit) = ($args->{prefix}, $args->{after}, $args->{limit});

    if (defined $prefix and $prefix ne '') {
        # now validate that after matches prefix
        return $self->err_line('after_mismatch')
            if $after && $after !~ /^$prefix/;

        # verify there are no % or \ characters
        return $self->err_line('invalid_chars')
            if $prefix =~ /[%\\]/;

        # escape underscores
        $prefix =~ s/_/\\_/g;
    }

    $limit ||= 1000;
    $limit += 0;
    $limit = 1000 if $limit > 1000;

    my $keys = Mgd::get_store()->get_keys_like($dmid, $prefix, $after, $limit);

    # if we got nothing, say so
    return $self->err_line('none_match') unless $keys && @$keys;

    # construct the output and send
    my $ret = { key_count => 0, next_after => '' };
    foreach my $key (@$keys) {
        $ret->{key_count}++;
        $ret->{next_after} = $key
            if $key gt $ret->{next_after};
        $ret->{"key_$ret->{key_count}"} = $key;
    }
    return $self->ok_line($ret);
}

sub cmd_rename {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # validate parameters
    my $dmid = $self->check_domain($args)
        or return $self->err_line('domain_not_found');
    my ($fkey, $tkey) = ($args->{from_key}, $args->{to_key});
    return $self->err_line("no_key") unless $fkey && $tkey;

    my $fid = MogileFS::FID->new_from_dmid_and_key($dmid, $fkey)
        or return  $self->err_line("unknown_key");

    $fid->rename($tkey) or
        $self->err_line("key_exists");

    return $self->ok_line;
}

sub cmd_get_hosts {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    MogileFS::Host->invalidate_cache;

    my $ret = { hosts => 0 };
    foreach my $host (MogileFS::Host->hosts) {
        next if defined $args->{hostid} && $host->id != $args->{hostid};
        my $n = ++$ret->{hosts};
        foreach my $key (qw(hostid status hostname hostip
                            http_port http_get_port
                            altip altmask))
        {
            # must be regular data so copy it in
            $ret->{"host${n}_$key"} = $host->field($key);
        }
    }

    return $self->ok_line($ret);
}

sub cmd_get_devices {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $ret = { devices => 0 };
    foreach my $dev (MogileFS::Device->devices) {
        next if defined $args->{devid} && $dev->id != $args->{devid};
        my $n = ++$ret->{devices};

        my $sum = $dev->overview_hashref;
        while (my ($key, $val) = each %$sum) {
            $ret->{"dev${n}_$key"} = $val;
        }
    }

    return $self->ok_line($ret);
}

sub cmd_create_device {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $status = $args->{state} || "alive";
    return $self->err_line("invalid_state") unless
        device_state($status);

    my $devid = $args->{devid};
    return $self->err_line("invalid_devid") unless $devid && $devid =~ /^\d+$/;

    my ($host, $hostid);

    MogileFS::Host->check_cache;
    if ($args->{hostid} && $args->{hostid} =~ /^\d+$/) {
        $hostid = $args->{hostid};
        $host = MogileFS::Host->of_hostid($hostid);
        return $self->err_line("unknown_hostid") unless $host && $host->exists;
    } elsif (my $hname = $args->{hostname}) {
        $host = MogileFS::Host->of_hostname($hname);
        return $self->err_line("unknown_host") unless $host;
        $hostid = $host->id;
    } else {
        return $self->err_line("bad_args", "No hostid/hostname parameter");
    }

    if (eval { MogileFS::Device->create(devid  => $devid,
                                        hostid => $hostid,
                                        status => $status) }) {
        return $self->ok_line;
    }

    my $errc = error_code($@);
    return $self->err_line("existing_devid") if $errc;
    die $@;  # rethrow;
}

sub cmd_create_domain {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $domain = $args->{domain} or
        return $self->err_line('no_domain');

    # TODO: auth/permissions?

    my $dom = eval { MogileFS::Domain->create($domain); };
    if ($@) {
        if (error_code($@) eq "dup") {
            return $self->err_line('domain_exists');
        }
        return $self->err_line('failure', "$@");
    }

    return $self->ok_line({ domain => $domain });
}

sub cmd_delete_domain {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $domain = $args->{domain} or
        return $self->err_line('no_domain');

    my $dom = MogileFS::Domain->of_namespace($domain) or
        return $self->err_line('domain_not_found');

    if (eval { $dom->delete }) {
        return $self->ok_line({ domain => $domain });
    }

    my $err = error_code($@);
    return $self->err_line('domain_has_files') if $err eq "has_files";
    return $self->err_line("failure");
}

sub cmd_create_class {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $domain = $args->{domain};
    return $self->err_line('no_domain') unless length $domain;

    my $class = $args->{class};
    return $self->err_line('no_class') unless length $class;

    my $mindevcount = $args->{mindevcount}+0;
    return $self->err_line('invalid_mindevcount') unless $mindevcount > 0;

    my $replpolicy = $args->{replpolicy} || '';
    if ($replpolicy) {
        eval {
            MogileFS::ReplicationPolicy->new_from_policy_string($replpolicy);
        };
        return $self->err_line('invalid_replpolicy', $@) if $@;
    }

    my $dom  = MogileFS::Domain->of_namespace($domain) or
        return $self->err_line('domain_not_found');

    my $cls = $dom->class($class);
    if ($args->{update}) {
        return $self->err_line('class_not_found') if ! $cls;
        $cls->set_name($class);
    } else {
        return $self->err_line('class_exists') if $cls;
        $cls = $dom->create_class($class);
    }
    $cls->set_mindevcount($mindevcount);
    # don't erase an existing replpolicy if we're not setting a new one.
    $cls->set_replpolicy($replpolicy) if $replpolicy;

    # return success
    return $self->ok_line({ class => $class, mindevcount => $mindevcount, domain => $domain });
}

sub cmd_update_class {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # simply passes through to create_class with update set
    $self->cmd_create_class({ %$args, update => 1 });
}

sub cmd_delete_class {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $domain = $args->{domain};
    return $self->err_line('no_domain') unless length $domain;
    my $class = $args->{class};
    return $self->err_line('no_class') unless length $domain;

    my $dom  = MogileFS::Domain->of_namespace($domain) or
        return $self->err_line('domain_not_found');
    my $cls = $dom->class($class) or
        return $self->err_line('class_not_found');

    if (eval { $cls->delete }) {
        return $self->ok_line({ domain => $domain, class => $class });
    }

    my $errc = error_code($@);
    return $self->err_line('class_has_files') if $errc eq "has_files";
    return $self->err_line('failure');
}

sub cmd_create_host {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $hostname = $args->{host} or
        return $self->err_line('no_host');

    my $host = MogileFS::Host->of_hostname($hostname);

    # if we're creating a new host, require ip/port, and default to
    # host being down if client didn't specify
    if ($args->{update}) {
        return $self->err_line('host_not_found') unless $host;
    } else {
        return $self->err_line('host_exists') if $host;
        return $self->err_line('no_ip') unless $args->{ip};
        return $self->err_line('no_port') unless $args->{port};
        $args->{status} ||= 'down';
    }

    if ($args->{status}) {
        return $self->err_line('unknown_state')
            unless MogileFS::Host->valid_initial_state($args->{status});
    }

    # arguments all good, let's do it.

    $host ||= MogileFS::Host->create($hostname, $args->{ip});
    my %setter = (
                  status  => "set_status",
                  ip      => "set_ip",
                  port    => "set_http_port",
                  getport => "set_http_get_port",
                  altip   => "set_alt_ip",
                  altmask => "set_alt_mask",
                  );
    while (my ($f, $meth) = each %setter) {
        $host->$meth($args->{$f}) if exists $args->{$f};
    }

    # return success
    return $self->ok_line($host->overview_hashref);
}

sub cmd_update_host {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # simply passes through to create_host with update set
    $self->cmd_create_host({ %$args, update => 1 });
}

sub cmd_delete_host {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $host   = MogileFS::Host->of_hostname($args->{host})
        or return $self->err_line('unknown_host');

    my $hostid = $host->id;

    foreach my $dev (MogileFS::Device->devices) {
        return $self->err_line('host_not_empty')
            if $dev->hostid == $hostid;
    }

    $host->delete;

    return $self->ok_line;
}

sub cmd_get_domains {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    MogileFS::Domain->invalidate_cache;

    my $ret = {};
    my $dm_n = 0;
    foreach my $dom (MogileFS::Domain->domains) {
        $dm_n++;
        $ret->{"domain${dm_n}"} = $dom->name;
        my $cl_n = 0;
        foreach my $cl ($dom->classes) {
            $cl_n++;
            $ret->{"domain${dm_n}class${cl_n}name"}        = $cl->name;
            $ret->{"domain${dm_n}class${cl_n}mindevcount"} = $cl->mindevcount;
            $ret->{"domain${dm_n}class${cl_n}replpolicy"}  =
                $cl->repl_policy_string;
        }
        $ret->{"domain${dm_n}classes"} = $cl_n;
    }
    $ret->{"domains"} = $dm_n;

    return $self->ok_line($ret);
}

sub cmd_get_paths {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # memcache mappings are as follows:
    #  mogfid:<dmid>:<dkey> -> fidid     (and TODO: invalidate this when key is replaced)
    #  mogdevids:<fidid>    -> \@devids  (and TODO: invalidate when the replication or deletion is run!)

    # if you specify 'noverify', that means a correct answer isn't needed and memcache can
    # be used.
    my $use_memc = $args->{noverify};
    my $memc     = $use_memc ? MogileFS::Config->memcache_client : undef;

    # validate domain for plugins
    $args->{dmid} = $self->check_domain($args)
        or return $self->err_line('domain_not_found');

    # now invoke the plugin, abort if it tells us to
    my $rv = MogileFS::run_global_hook('cmd_get_paths', $args);
    return $self->err_line('plugin_aborted')
        if defined $rv && ! $rv;

    # validate parameters
    my $dmid = $args->{dmid};
    my $key = $args->{key} or return $self->err_line("no_key");

    # We default to returning two possible paths.
    # but the client may ask for more if they want.
    my $pathcount = $args->{pathcount} || 2;
    $pathcount = 2 if $pathcount < 2;

    # get DB handle
    my $fid;
    my $need_fid_in_memcache = 0;
    my $mogfid_memkey = "mogfid:$args->{dmid}:$key";
    if ($memc) {
        if (my $fidid = $memc->get($mogfid_memkey)) {
            $fid = MogileFS::FID->new($fidid);
        } else {
            $need_fid_in_memcache = 1;
        }
    }
    unless ($fid) {
        Mgd::get_store()->slaves_ok(sub {
            $fid = MogileFS::FID->new_from_dmid_and_key($dmid, $key);
        });
        $fid or return $self->err_line("unknown_key");
    }

    # add to memcache, if needed.  for an hour.
    $memc->add($mogfid_memkey, $fid->id, 3600) if $need_fid_in_memcache;

    my $dmap = MogileFS::Device->map;

    my $ret = {
        paths => 0,
    };

    # find devids that FID is on in memcache or db.
    my @fid_devids;
    my $need_devids_in_memcache = 0;
    my $devid_memkey = "mogdevids:" . $fid->id;
    if ($memc) {
        if (my $list = $memc->get($devid_memkey)) {
            @fid_devids = @$list;
        } else {
            $need_devids_in_memcache = 1;
        }
    }
    unless (@fid_devids) {
        Mgd::get_store()->slaves_ok(sub {
            @fid_devids = $fid->devids;
        });
        $memc->add($devid_memkey, \@fid_devids, 3600) if $need_devids_in_memcache;
    }

    my @devices = map { $dmap->{$_} } @fid_devids;

    my @sorted_devs;
    unless (MogileFS::run_global_hook('cmd_get_paths_order_devices', \@devices, \@sorted_devs)) {
        @sorted_devs = sort_devs_by_utilization(@devices);
    }

    # keep one partially-bogus path around just in case we have nothing else to send.
    my $backup_path;

    # construct result paths
    foreach my $dev (@sorted_devs) {
        next unless $dev && ($dev->can_read_from);

        my $host = $dev->host;
        next unless $dev && $host;
        my $dfid = MogileFS::DevFID->new($dev, $fid);
        my $path = $dfid->get_url;
        my $currently_down =
            $host->observed_unreachable || $dev->observed_unreachable;

        if ($currently_down) {
            $backup_path = $path;
            next;
        }

        # only verify size one first one, and never verify if they've asked not to
        next unless
            $ret->{paths}        ||
            $args->{noverify}    ||
            $dfid->size_matches;

        my $n = ++$ret->{paths};
        $ret->{"path$n"} = $path;
        last if $n == $pathcount;   # one verified, one likely seems enough for now.  time will tell.
    }

    # use our backup path if all else fails
    if ($backup_path && ! $ret->{paths}) {
        $ret->{paths} = 1;
        $ret->{path1} = $backup_path;
    }

    return $self->ok_line($ret);
}

sub sort_devs_by_utilization {
    my @devices_with_weights;

    # is this fid still owned by this key?
    foreach my $dev (@_) {
        my $weight;
        my $util = $dev->observed_utilization;

        if (defined($util) and $util =~ /\A\d+\Z/) {
            $weight = 102 - $util;
            $weight ||= 100;
        } else {
            $weight = $dev->weight;
            $weight ||= 100;
        }
        push @devices_with_weights, [$dev, $weight];
    }

    # randomly weight the devices
    my @list = MogileFS::Util::weighted_list(@devices_with_weights);

    return @list;
}

# ------------------------------------------------------------
#
# NOTE: cmd_edit_file is EXPERIMENTAL. Please see the documentation
# for edit_file in L<MogileFS::Client>.
# It is not recommended to use cmd_edit_file on production systems.
#
# cmd_edit_file is similar to cmd_get_paths, except we:
# - take the device of the first path we would have returned
# - get a tempfile with a new fid (pointing to nothing) on the same device
#   the tempfile has the same key, so will replace the old contents on
#   create_close
# - detach the old fid from that device (leaving the file in place)
# - attach the new fid to that device
# - returns only the first path to the old fid and a path to new fid
# (the client then DAV-renames the old path to the new path)
#
# TODO - what to do about situations where we would be reducing the
# replica count to zero?
# TODO - what to do about pending replications where we remove the source?
# TODO - the current implementation of cmd_edit_file is based on a copy
#   of cmd_get_paths. Once proven mature, consider factoring out common
#   code from the two functions.
# ------------------------------------------------------------
sub cmd_edit_file {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $memc = MogileFS::Config->memcache_client;

    # validate domain for plugins
    $args->{dmid} = $self->check_domain($args)
        or return $self->err_line('domain_not_found');

    # now invoke the plugin, abort if it tells us to
    my $rv = MogileFS::run_global_hook('cmd_get_paths', $args);
    return $self->err_line('plugin_aborted')
        if defined $rv && ! $rv;

    # validate parameters
    my $dmid = $args->{dmid};
    my $key = $args->{key} or return $self->err_line("no_key");

    # get DB handle
    my $fid;
    my $need_fid_in_memcache = 0;
    my $mogfid_memkey = "mogfid:$args->{dmid}:$key";
    if (my $fidid = $memc->get($mogfid_memkey)) {
        $fid = MogileFS::FID->new($fidid);
    } else {
        $need_fid_in_memcache = 1;
    }
    unless ($fid) {
        Mgd::get_store()->slaves_ok(sub {
            $fid = MogileFS::FID->new_from_dmid_and_key($dmid, $key);
        });
        $fid or return $self->err_line("unknown_key");
    }

    # add to memcache, if needed.  for an hour.
    $memc->add($mogfid_memkey, $fid->id, 3600) if $need_fid_in_memcache;

    my $dmap = MogileFS::Device->map;

    my @devices_with_weights;

    # find devids that FID is on in memcache or db.
    my @fid_devids;
    my $need_devids_in_memcache = 0;
    my $devid_memkey = "mogdevids:" . $fid->id;
    if (my $list = $memc->get($devid_memkey)) {
        @fid_devids = @$list;
    } else {
        $need_devids_in_memcache = 1;
    }
    unless (@fid_devids) {
        Mgd::get_store()->slaves_ok(sub {
            @fid_devids = $fid->devids;
        });
        $memc->add($devid_memkey, \@fid_devids, 3600) if $need_devids_in_memcache;
    }

    # is this fid still owned by this key?
    foreach my $devid (@fid_devids) {
        my $weight;
        my $dev = $dmap->{$devid};
        my $util = $dev->observed_utilization;

        if (defined($util) and $util =~ /\A\d+\Z/) {
            $weight = 102 - $util;
            $weight ||= 100;
        } else {
            $weight = $dev->weight;
            $weight ||= 100;
        }
        push @devices_with_weights, [$devid, $weight];
    }

    # randomly weight the devices
    # TODO - should we reverse the order, to leave the best
    # one there for get_paths?
    my @list = MogileFS::Util::weighted_list(@devices_with_weights);

    # Filter out bad devs
    @list = grep {
        my $devid = $_;
        my $dev = $dmap->{$devid};
        my $host = $dev ? $dev->host : undef;

        $dev
        && $host
        && $dev->can_read_from
        && !($host->observed_unreachable || $dev->observed_unreachable);
    } @list;

    # Take first remaining device from list
    my $devid = $list[0];

    my $class = MogileFS::Class->of_fid($fid);
    my $newfid = eval {
        Mgd::get_store()->register_tempfile(
            fid     => undef,   # undef => let the store pick a fid
            dmid    => $dmid,
            key     => $key,    # This tempfile will ultimately become this key
            classid => $class->classid,
            devids  => $devid,
        );
    };
    unless ($newfid) {
        my $errc = error_code($@);
        return $self->err_line("fid_in_use") if $errc eq "dup";
        warn "Error registering tempfile: $@\n";
        return $self->err_line("db");
    }
    unless (Mgd::get_store()->remove_fidid_from_devid($fid->id, $devid)) {
        warn "Error removing fidid from devid";
        return $self->err_line("db");
    }
    unless (Mgd::get_store()->add_fidid_to_devid($newfid, $devid)) {
        warn "Error removing fidid from devid";
        return $self->err_line("db");
    }

    my @paths = map {
        my $dfid = MogileFS::DevFID->new($devid, $_);
        my $path = $dfid->get_url;
    } ($fid, $newfid);
    my $ret;
    $ret->{oldpath} = $paths[0];
    $ret->{newpath} = $paths[1];
    $ret->{fid} = $newfid;
    $ret->{devid} = $devid;
    $ret->{class} = $class->classid;
    return $self->ok_line($ret);
}

sub cmd_set_weight {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # figure out what they want to do
    my ($hostname, $devid, $weight) = ($args->{host}, $args->{device}+0, $args->{weight}+0);
    return $self->err_line('bad_params')
        unless $hostname && $devid && $weight >= 0;

    my $dev = MogileFS::Device->from_devid_and_hostname($devid, $hostname)
        or return $self->err_line('host_mismatch');

    $dev->set_weight($weight);

    return $self->ok_line;
}

sub cmd_set_state {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # figure out what they want to do
    my ($hostname, $devid, $state) = ($args->{host}, $args->{device}+0, $args->{state});

    my $dstate = device_state($state);
    return $self->err_line('bad_params')
        unless $hostname && $devid && $dstate;

    my $dev = MogileFS::Device->from_devid_and_hostname($devid, $hostname)
        or return $self->err_line('host_mismatch');

    # make sure the destination state isn't too high
    return $self->err_line('state_too_high')
        unless $dev->can_change_to_state($state);

    $dev->set_state($state);
    return $self->ok_line;
}

# FIXME: this whole thing is gross, duplicative, dependent on $dbh, and doesn't scale.
# stats needs total overhaul to not suck.
sub cmd_stats {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    # get database handle
    my $ret = {};
    my $sto = Mgd::get_store();
    my $dbh = Mgd::get_dbh()
        or return $self->err_line('nodb');

    # get names of all domains and classes for use later
    my %classes;
    my $rows;

    $rows = $dbh->selectall_arrayref('SELECT d.dmid, d.namespace, c.classid, c.classname ' .
                                     'FROM domain d LEFT JOIN class c ON c.dmid=d.dmid');

    foreach my $row (@$rows) {
        $classes{$row->[0]}->{name} = $row->[1];
        $classes{$row->[0]}->{classes}->{$row->[2] || 0} = $row->[3] || 'default';
    }
    $classes{$_}->{classes}->{0} = 'default'
        foreach keys %classes;

    # get host and device information with device status
    my %devices;
    $rows = $dbh->selectall_arrayref('SELECT device.devid, hostname, device.status ' .
                                     'FROM device, host WHERE device.hostid = host.hostid');
    foreach my $row (@$rows) {
        $devices{$row->[0]}->{host} = $row->[1];
        $devices{$row->[0]}->{status} = $row->[2];
    }

    # if they want replication counts, or didn't specify what they wanted
    if ($args->{replication} || $args->{all}) {
        # replication stats
        # This is the old version that used devcount:
        my @stats = $sto->get_stats_files_per_devcount;

        my $count = 0;
        foreach my $stat (@stats) {
            $count++;
            $ret->{"replication${count}domain"} = $classes{$stat->{dmid}}->{name};
            $ret->{"replication${count}class"} = $classes{$stat->{dmid}}->{classes}->{$stat->{classid}};
            $ret->{"replication${count}devcount"} = $stat->{devcount};
            $ret->{"replication${count}files"} = $stat->{count};
        }
        $ret->{"replicationcount"} = $count;

        # now we want to do the "new" replication stats
        my $db_time = $dbh->selectrow_array('SELECT '.$sto->unix_timestamp);
        my $stats = $dbh->selectall_arrayref('SELECT nexttry, COUNT(*) FROM file_to_replicate GROUP BY 1');
        foreach my $stat (@$stats) {
            if ($stat->[0] < 1000) {
                # anything under 1000 is a specific state, so let's define those.  here's the list
                # of short names to describe them.
                my $name = {
                    0 => 'newfile', # new files that need to be replicated
                    1 => 'redo',    # files that need to go through replication again
                }->{$stat->[0]} || "unknown";

                # now put it in the output hashref.  note that we do += because we might
                # have more than one group of unknowns.
                $ret->{"to_replicate_$name"} += $stat->[1];

            } elsif ($stat->[0] == MogileFS::Worker::Replicate::end_of_time()) {
                $ret->{"to_replicate_manually"} = $stat->[1];

            } elsif ($stat->[0] < $db_time) {
                $ret->{"to_replicate_overdue"} += $stat->[1];

            } else {
                $ret->{"to_replicate_deferred"} += $stat->[1];
            }
        }
    }

    # file statistics (how many files there are and in what domains/classes)
    if ($args->{files} || $args->{all}) {
        my $stats = $dbh->selectall_arrayref('SELECT dmid, classid, COUNT(classid) FROM file GROUP BY 1, 2');
        my $count = 0;
        foreach my $stat (@$stats) {
            $count++;
            $ret->{"files${count}domain"} = $classes{$stat->[0]}->{name};
            $ret->{"files${count}class"} = $classes{$stat->[0]}->{classes}->{$stat->[1]};
            $ret->{"files${count}files"} = $stat->[2];
        }
        $ret->{"filescount"} = $count;
    }

    # device statistics (how many files are on each device)
    if ($args->{devices} || $args->{all}) {
        my $stats = $dbh->selectall_arrayref('SELECT devid, COUNT(devid) FROM file_on GROUP BY 1');
        my $count = 0;
        foreach my $stat (@$stats) {
            $count++;
            $ret->{"devices${count}id"} = $stat->[0];
            $ret->{"devices${count}host"} = $devices{$stat->[0]}->{host};
            $ret->{"devices${count}status"} = $devices{$stat->[0]}->{status};
            $ret->{"devices${count}files"} = $stat->[1];
        }
        $ret->{"devicescount"} = $count;
    }

    # now fid statistics
    if ($args->{fids} || $args->{all}) {
        my $max = $dbh->selectrow_array('SELECT MAX(fid) FROM file');
        $ret->{"fidmax"} = $max;
    }

    # FIXME: DO! add other stats

    return $self->ok_line($ret);
}

sub cmd_noop {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    return $self->ok_line;
}

sub cmd_replicate_now {
    my MogileFS::Worker::Query $self = shift;

    my $rv = Mgd::get_store()->replicate_now;
    return $self->ok_line({ count => int($rv) });
}

sub cmd_checker {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $new_setting;
    if ($args->{disable}) {
        $new_setting = 'off';
    } elsif ($args->{level}) {
        # they want to turn it on or change the level, so let's ensure they
        # specified a valid level
        if (MogileFS::Worker::Checker::is_valid_level($args->{level})) {
            $new_setting = $args->{level};
        } else {
            return $self->err_line('invalid_checker_level');
        }
    }

    if (defined $new_setting) {
        MogileFS::Config->set_server_setting('fsck_enable', $new_setting);
        return $self->ok_line;
    }

    $self->err_line('failure');
}

sub cmd_set_server_setting {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    my $key = $args->{key} or
        return $self->err_line("bad_params");
    my $val = $args->{value};

    my $chk  = MogileFS::Config->server_setting_is_writable($key) or
        return $self->err_line("not_writable");

    my $cleanval = eval { $chk->($val); };
    return $self->err_line("invalid_format", $@) if $@;

    MogileFS::Config->set_server_setting($key, $cleanval);
    return $self->ok_line;
}

sub cmd_server_setting {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    my $key = $args->{key};
    return $self->err_line("bad_params") unless $key;
    my $value = MogileFS::Config->server_setting($key);
    return $self->ok_line({key => $key, value => $value});
}

sub cmd_server_settings {
    my MogileFS::Worker::Query $self = shift;
    my $ss = Mgd::get_store()->server_settings;
    my $ret = {};
    my $n = 0;
    while (my ($k, $v) = each %$ss) {
        next unless MogileFS::Config->server_setting_is_readable($k);
        $ret->{"key_count"} = ++$n;
        $ret->{"key_$n"}    = $k;
        $ret->{"value_$n"}  = $v;
    }
    return $self->ok_line($ret);
}

sub cmd_do_monitor_round {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;
    $self->forget_that_monitor_has_run;
    $self->wait_for_monitor;
    return $self->ok_line;
}

sub cmd_fsck_start {
    my MogileFS::Worker::Query $self = shift;
    my $sto = Mgd::get_store();

    # reset position, if a previous fsck was already completed.
    my $intss       = sub { MogileFS::Config->server_setting($_[0]) || 0 };
    my $checked_fid = $intss->("fsck_highest_fid_checked");
    my $final_fid   = $intss->("fsck_fid_at_end");
    if ($checked_fid && $final_fid && $checked_fid >= $final_fid) {
        $self->_do_fsck_reset or return $self->err_line;
    }

    # set params for stats:
    $sto->set_server_setting("fsck_start_time", $sto->get_db_unixtime);
    $sto->set_server_setting("fsck_stop_time", undef);
    $sto->set_server_setting("fsck_fids_checked", 0);
    my $start_fid =
        MogileFS::Config->server_setting('fsck_highest_fid_checked') || 0;
    $sto->set_server_setting("fsck_start_fid", $start_fid);

    # and start it:
    $sto->set_server_setting("fsck_host", MogileFS::Config->hostname);
    MogileFS::ProcManager->wake_a("fsck");

    return $self->ok_line;
}

sub cmd_fsck_stop {
    my MogileFS::Worker::Query $self = shift;
    my $sto = Mgd::get_store();
    $sto->set_server_setting("fsck_host", undef);
    $sto->set_server_setting("fsck_stop_time", $sto->get_db_unixtime);
    return $self->ok_line;
}

sub cmd_fsck_reset {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $sto = Mgd::get_store();
    $sto->set_server_setting("fsck_opt_policy_only",
        ($args->{policy_only} ? "1" : undef));
    $sto->set_server_setting("fsck_highest_fid_checked", 
        ($args->{startpos} ? $args->{startpos} : "0"));

    $self->_do_fsck_reset or return $self->err_line;
    return $self->ok_line;
}

sub _do_fsck_reset {
    my MogileFS::Worker::Query $self = shift;
    my $sto = Mgd::get_store();
    $sto->set_server_setting("fsck_start_time",       undef);
    $sto->set_server_setting("fsck_stop_time",        undef);
    $sto->set_server_setting("fsck_fids_checked",     0);
    $sto->set_server_setting("fsck_fid_at_end",       $sto->max_fidid);

    # clear existing event counts summaries.
    my $ss = $sto->server_settings;
    foreach my $k (keys %$ss) {
        next unless $k =~ /^fsck_sum_evcount_/;
        $sto->set_server_setting($k, undef);
    }
    my $logid = $sto->max_fsck_logid;
    $sto->set_server_setting("fsck_start_maxlogid", $logid);
    $sto->set_server_setting("fsck_logid_processed", $logid);
}

sub cmd_fsck_clearlog {
    my MogileFS::Worker::Query $self = shift;
    my $sto = Mgd::get_store();
    $sto->clear_fsck_log;
    return $self->ok_line;
}

sub cmd_fsck_getlog {
    my MogileFS::Worker::Query $self = shift;
    my $args = shift;

    my $sto = Mgd::get_store();
    my @rows = $sto->fsck_log_rows($args->{after_logid}, 100);
    my $ret;
    my $n = 0;
    foreach my $row (@rows) {
        $n++;
        foreach my $k (keys %$row) {
            $ret->{"row_${n}_$k"} = $row->{$k} if defined $row->{$k};
        }
    }
    $ret->{row_count} = $n;
    return $self->ok_line($ret);
}

sub cmd_fsck_status {
    my MogileFS::Worker::Query $self = shift;

    my $sto        = Mgd::get_store();
    # Kick up the summary before we read the values
    $sto->fsck_log_summarize;
    my $fsck_host  = MogileFS::Config->server_setting('fsck_host');
    my $intss      = sub { MogileFS::Config->server_setting($_[0]) || 0 };
    my $ret = {
        running         => ($fsck_host ? 1 : 0),
        host            => $fsck_host,
        max_fid_checked => $intss->('fsck_highest_fid_checked'),
        policy_only     => $intss->('fsck_opt_policy_only'),
        end_fid         => $intss->('fsck_fid_at_end'),
        start_time      => $intss->('fsck_start_time'),
        stop_time       => $intss->('fsck_stop_time'),
        current_time    => $sto->get_db_unixtime,
        max_logid       => $sto->max_fsck_logid,
    };

    # throw some stats in.
    my $ss = $sto->server_settings;
    foreach my $k (keys %$ss) {
        next unless $k =~ /^fsck_sum_evcount_(.+)/;
        $ret->{"num_$1"} += $ss->{$k};
    }

    return $self->ok_line($ret);
}

sub ok_line {
    my MogileFS::Worker::Query $self = shift;

    my $delay = '';
    if ($self->{querystarttime}) {
        $delay = sprintf("%.4f ", Time::HiRes::tv_interval( $self->{querystarttime} ));
        $self->{querystarttime} = undef;
    }

    my $id = defined $self->{reqid} ? "$self->{reqid} " : '';

    my $args = shift || {};
    my $argline = join('&', map { eurl($_) . "=" . eurl($args->{$_}) } keys %$args);
    $self->send_to_parent("${id}${delay}OK $argline");
    return 1;
}

# first argument: error code.
# second argument: optional error text.  text will be taken from code if no text provided.
sub err_line {
    my MogileFS::Worker::Query $self = shift;

    my $err_code = shift;
    my $err_text = shift || {
        'dup' => "Duplicate name/number used.",
        'after_mismatch' => "Pattern does not match the after-value?",
        'bad_params' => "Invalid parameters to command; please see documentation",
        'class_exists' => "That class already exists in that domain",
        'class_has_files' => "Class still has files, unable to delete",
        'class_not_found' => "Class not found",
        'db' => "Database error",
        'domain_has_files' => "Domain still has files, unable to delete",
        'domain_exists' => "That domain already exists",
        'domain_not_empty' => "Domain still has classes, unable to delete",
        'domain_not_found' => "Domain not found",
        'failure' => "Operation failed",
        'host_exists' => "That host already exists",
        'host_mismatch' => "The device specified doesn't belong to the host specified",
        'host_not_empty' => "Unable to delete host; it contains devices still",
        'host_not_found' => "Host not found",
        'invalid_chars' => "Patterns must not contain backslashes (\\) or percent signs (%).",
        'invalid_checker_level' => "Checker level invalid.  Please see documentation on this command.",
        'invalid_mindevcount' => "The mindevcount must be at least 1",
        'key_exists' => "Target key name already exists; can't overwrite.",
        'no_class' => "No class provided",
        'no_devices' => "No devices found to store file",
        'no_domain' => "No domain provided",
        'no_host' => "No host provided",
        'no_ip' => "IP required to create host",
        'no_port' => "Port required to create host",
        'no_temp_file' => "No tempfile or file already closed",
        'none_match' => "No keys match that pattern and after-value (if any).",
        'plugin_aborted' => "Action aborted by plugin",
        'state_too_high' => "Status cannot go from dead to alive; must use down",
        'unknown_command' => "Unknown server command",
        'unknown_host' => "Host not found",
        'unknown_state' => "Invalid/unknown state",
        'unreg_domain' => "Domain name invalid/not found",
    }->{$err_code} || $err_code;

    my $delay = '';
    if ($self->{querystarttime}) {
        $delay = sprintf("%.4f ", Time::HiRes::tv_interval($self->{querystarttime}));
        $self->{querystarttime} = undef;
    }

    my $id = defined $self->{reqid} ? "$self->{reqid} " : '';

    $self->send_to_parent("${id}${delay}ERR $err_code " . eurl($err_text));
    return 0;
}

1;

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:

__END__

=head1 NAME

MogileFS::Worker::Query -- implements the MogileFS client protocol

=head1 SEE ALSO

L<MogileFS::Worker>


