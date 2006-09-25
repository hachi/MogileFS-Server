#!/usr/bin/perl
#
#   Perl Makefile for Danga-Socket
#   $Id$
#
#   Invoke with 'perl Makefile.PL'
#
#   See ExtUtils::MakeMaker (3) for more information on how to influence
#    the contents of the Makefile that is written
#

use ExtUtils::MakeMaker;

my $exefiles = ["mogstored", "mogilefsd", "mogdbsetup", "mogautomount"];
$exefiles = ["mogstored"] if exists $ENV{DANGABUILD_MOGSTOREDONLY};
$exefiles = ["mogilefsd"] if exists $ENV{DANGABUILD_MOGILEFSDONLY};

WriteMakefile(
    NAME                    => 'mogilefs-server',
    VERSION                 => '2.00_03',
    AUTHOR                  => 'Brad Fitzpatrick <brad@danga.com>',
    ABSTRACT                => 'MogileFS server components',
    EXE_FILES               => $exefiles,
    PREREQ_PM               => {
        'Danga::Socket'    => '1.22',
        'Perlbal'          => 0,    # FIXME: add version to Perlbal (added as of Perlbal 1.43)
                                    # but really we're going to make Perlbal.pm optional, for running mogstored
                                    # in only-monitor/usage-writeout mode.  so maybe not.
        'Sys::Syslog'      => 0,
        'Getopt::Long'     => 0,
        'Symbol'           => 0,
        'Net::Netmask'     => 0,
        fields            => 0,
    },
    dist                    => {
        CI                      => "cvs commit",
        RCS_LABEL               => 'cvs tag RELEASE_$(VERSION_SYM)',
        SUFFIX                  => ".gz",
        DIST_DEFAULT            => 'all tardist',
        COMPRESS                => "gzip",
    },

);

