#!/usr/bin/perl -w
# Class to lock file to make sure that only one instance of script is currently running
# Version 2
package Zmon::Flocker;
use vars;
use strict;
use warnings;
#DEBUG use Data::Dumper;
use Zmon::Slog qw(slog sfatal);
use Fcntl qw(:flock);
use FindBin qw($Script);

use Exporter qw(import);
our @EXPORT = qw(new stay_single_or_die get_owner_or_lock release);


# Method: new
# Purpose: create new object with defined (or default) pidfile (and appropriate lockfile)
# Arguments: hash with keys:
# pidfile (pathname to pidfile). Default pidfile is /tmp/<script_basename>.pid
# Return value: Flocker object
sub new {
    my ($class, $params) = @_;
    my $self = {};

    $self->{'FILENAME'} = '/tmp/' . $Script . '.pid' if not defined $params->{'pidfile'};
    $self->{'LOCKNAME'} = $self->{'FILENAME'} . '.lock';

    $self->{'OWNED'} = 0;
    $self->{'LOCKFH'} = undef;

    bless($self, $class);
    return $self;
}

# Method: stay_single_or_die
# Purpose: Check that no any other instance of this script currently running on the system or exit script.
# Arguments: nothing
# Return value: nothing
sub stay_single_or_die {
    my $self = shift;
    my $ret = $self->get_owner_or_lock();
    if ($ret)
    {
        my $emsg = "Another instance of this script currently running. Check pid $ret";
        slog('msg' => $emsg);
        exit 0;
    }
}

# Method: get_owner_or_lock
# Purpose: return pid of file owner (based on pidfile) or lock file and white pid to the file
# Arguments: nothing
# Return value:
# any positive value is a pid of owner
# zero means that file was locked by current process and pid was written to the file
sub get_owner_or_lock {
    my $self = shift;
    if ($self->{'OWNED'}) {return 0};
    if ($self->_lock())
    {
        $self->_save_pid();
        $self->{'OWNED'} = 1;
	return 0;
    }
    else
    {
        return $self->_read_pid();
    }
}

# Method: release
# Purpose: remove pid from pidfile and unlock lockfile
# Return value: 0
sub release {
    my $self = shift;
    if ($self->{'OWNED'})
    {
        $self->_trunc();
        $self->_unlock();
    }
    return 0;
}

sub _lock {
    my $self = shift;

    open ($self->{'LOCKFH'}, '+>>', $self->{'LOCKNAME'}) or sfatal('msg' => "Couldn't open $self->{'LOCKNAME'} : $!");

    if (flock($self->{'LOCKFH'}, LOCK_EX | LOCK_NB)){
        return 1;
    }

    #could not lock file
    #closing file and returning zero
    close $self->{'LOCKFH'};
    return 0;
}

sub _save_pid {
    my $self = shift;
    open (my $fh, '>', $self->{'FILENAME'}) or sfatal('msg' => "Couldn't open $self->{'FILENAME'} : $!");
    print $fh $$;
    close($fh);
}

sub _trunc {
    my $self = shift;
    open (my $fh, '>', $self->{'FILENAME'}) or sfatal('msg' => "Couldn't open $self->{'FILENAME'} : $!");
    close($fh);
}

sub _unlock {
    my $self = shift;

    if (close($self->{'LOCKFH'}))
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

sub _read_pid {
    my $self = shift;
    open (my $fh, '<', $self->{'FILENAME'}) or sfatal('msg' => "Couldn't open $self->{'FILENAME'} : $!");
    local $/ = undef;
    my $cont = <$fh>;
    close $fh;
    return $cont;
}

1;
