#!/usr/bin/perl -w
# Package for simple syslog logging.
# Version 1
package Zmon::Slog;

use vars;
use strict;
use warnings;
use Carp qw(croak);
use Sys::Syslog qw(:DEFAULT);
use Exporter qw(import);
our @EXPORT = qw(slog sfatal);

my $curr_facility = "";

sub slog {
    my %params = @_;
    $params{'level'} ||= 'warning';
    $params{'msg'} ||= 'No message provided';

    if ($params{'facility'} && $curr_facility && $params{'facility'} != $curr_facility)
    {
        closelog();
        $curr_facility ="";
    }
    if (!$curr_facility)
    {
        $curr_facility = ($params{'facility'}) ? $params{'facility'} : 'daemon';
        openlog("", "pid", $curr_facility);
    }

    syslog($params{'level'}, $params{'msg'});
}

sub sfatal {
    my %params = @_;
    slog(%params);
    croak($params{'msg'});
}

1;
