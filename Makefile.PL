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

my $exefiles = ["mogstored", "mogstored-iostat", "mogstored-diskusage", "mogilefsd", "mogdbsetup", "mogautomount"];
$exefiles = ["mogstored", "mogstored-iostat", "mogstored-diskusage"] if exists $ENV{DANGABUILD_MOGSTOREDONLY};
$exefiles = ["mogilefsd"] if exists $ENV{DANGABUILD_MOGILEFSDONLY};

WriteMakefile(
    NAME                    => 'mogilefs-server',
    VERSION_FROM            => 'lib/MogileFS/Server.pm',
    AUTHOR                  => 'Brad Fitzpatrick <brad@danga.com>',
    ABSTRACT_FROM           => 'lib/MogileFS/Server.pm',
    EXE_FILES               => $exefiles,
    PREREQ_PM               => {
        'Danga::Socket'    => '1.56',
        'Perlbal'          => '1.53',  # Future: make Perlbal optional, for running mogstored
                                       # in only-monitor/usage-writeout mode.
        'Sys::Syslog'      => 0,,
        'Sys::Syscall'     => '0.22',
        'Getopt::Long'     => 0,
        'Symbol'           => 0,
        'Net::Netmask'     => 0,
        'Gearman::Server'  => 1.06,
        'Gearman::Client::Async' => 0.93,
        fields            => 0,
    },
);

