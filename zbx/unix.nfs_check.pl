#!/usr/bin/perl -w
# Script trys to run df for every NFS mountpoint and alert if it fail or hang.
# Script should run ok on Solaris, AIX, HPUX and Linux with perl installed
# Version 2

use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/../lib";

use Getopt::Std;
use Zmon::Slog;
use Zmon::Flocker;
use Zmon::Zsb;
use Proc::Background;
use Time::HiRes qw(sleep time);

use Data::Dumper;

# Parse options
our ($opt_t, $opt_c, $opt_h, $opt_k);
getopts('t:chk:');
if ($opt_h) { usage() };
my $mode = ($opt_c) ? "collect" : "discover" ;
my $timeout = ($opt_t) ? $opt_t : 60 ;
my $metric_key = ($opt_k) ? $opt_k : 'nfsmntcheck' ;

# Get NFS mountpoints
my $nfsmounts = obtain_nfs_mounts();

# Discover part is done. Print json and exit.
if ($mode eq "discover")
{
    my @templatearr = map { {'{#NFS_NAME}' => $_->[0], '{#NFS_PATH}' => $_->[1]} } @{$nfsmounts};
    zbx_jsend('key' => $metric_key, 'value' => \@templatearr, 'addrunok' => 1, 'wrapdata' => 1);
    exit 0;
}

# Metrics collection part

# Check that no any other instance of this script currently running on the system.
my $locker = Zmon::Flocker->new();
$locker->stay_single_or_die();

my $nfs_health = check_nfs_mountpoints($nfsmounts);
my @results = map { {'nfsname' => $_, 'nfstime' => $nfs_health->{$_}} } keys %{$nfs_health};
zbx_jsend('key' => $metric_key, 'value' => \@results, 'addrunok' => 1, 'wrapdata' => 1);

# Release lock
$locker->release();

sub check_nfs_mountpoints
{
    my $nfs_mounts = shift;
    # Run df for every NFS mountpoint in background
    my $DF_CMD = "/usr/bin/df";
    my %checkers = map { $_->[0] => Proc::Background->new("$DF_CMD $_->[0]") } @{$nfs_mounts};
    
    # Wait for completion or timeout
    my %result;
    my $checkinterval = 0.5;
    my $waitstart = time;
    while (1)
    {
        my $timeisover = ((time - $waitstart) > ($timeout + $checkinterval));
        my $any_alive = 0;
        for my $nfsmnt (keys %checkers)
        {
            if ($checkers{$nfsmnt}->alive())
            {
                if($timeisover)
                {
                    # process timed out
                    $result{$nfsmnt} = -1;
                    $checkers{$nfsmnt}->die;
                }
                else 
                {
                    $any_alive = 1; 
                }
            }
            elsif (! defined $result{$nfsmnt})
            {
                # process completed without exeptions
                if ($checkers{$nfsmnt}->exit_signal == 0 && $checkers{$nfsmnt}->exit_code == 0)
                {
                    $result{$nfsmnt} = $checkers{$nfsmnt}->end_time - $checkers{$nfsmnt}->start_time;
                }
                # error occured
                else
                {
                    $result{$nfsmnt} = -2;
                }
            }
        }
        if (! $any_alive) {last};
        sleep $checkinterval;
    }
    return \%result;
}

sub usage {
print "$0 designed to monitor mounted NFS availability on client using zabbix
$0 [-c [-t timeout]] | -h
       
        Options:
        -c            - will run metrics collection [default=discover]
        -k metric_key - key of metric [default=nfsmntcheck]
        -t timeout    - timeout is seconds [default=60 sec]
        -h            - usage
";
exit;
}

sub filter_nfs_mntpts
{
    my $mntpts_shares = shift;
    my @mntpts_shares_ret;
    #exclude shares mounted in child zones
    if ($^O eq "solaris")
    {
        my @zoneinfo = `/usr/sbin/zoneadm list -p`;
        my @zroots;

        for my $line (@zoneinfo)
        {
            if (my ($zoneroot) = ($line =~ /^\d+:[\d\w\-\._]+:[\d\w\-\._]+:([\d\w\-\.\/_]+):.*/))
            {
                if ($zoneroot ne '/'){push @zroots, $zoneroot}
            }
        }
        #print Dumper(\@zroots);

        for my $mntpt_share (@{$mntpts_shares})
        {
            if (! grep { index($mntpt_share->[0], $_) == 0 } @zroots) {push @mntpts_shares_ret, $mntpt_share};
            #print Dumper(\@shareinzroot);
        }
        return \@mntpts_shares_ret
    }
    else
    {
       return $mntpts_shares;
    }
}

# Function: obtain_nfs_mounts
# Purpose: return list of nfs mounts
# Arguments: nothing
# Return value: Array of arrays. Each inner array contain two elements, where zero element is mountpoint, first element - nfs server and share.
sub obtain_nfs_mounts
{
    my $NFSSTAT_CMD = get_first_exist_file(['/usr/bin/nfsstat', '/usr/sbin/nfsstat']);
    my @nfs_mntpts_shares;
    my @nfsstatout = `$NFSSTAT_CMD -m`;
    for my $line (@nfsstatout)
    {
        if (my ($mntpt, $share) = ($line =~ /^(\/[\d\w\-\.\/_:]+)\s+from\s+([\d\w\-\.\/_:]+)/))
        {
            #print "$mntpt $share\n";
            push @nfs_mntpts_shares, [$mntpt, $share];
        }
    }
    #print Dumper(\@nfs_mntpts_shares);

    #Filter out unneeded mountpoints
    my $nfs_mntpts_shares_filtered = filter_nfs_mntpts(\@nfs_mntpts_shares);
    #print Dumper($nfs_mntpts_shares_filtered);
    return $nfs_mntpts_shares_filtered
}


sub get_first_exist_file
{
    my $filevariants = shift;
    grep { -e $_ && return $_ } @{$filevariants};
    die "There is no existing file in the list: " . join(', ', @{$filevariants});
}

