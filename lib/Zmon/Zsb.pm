#!/usr/bin/perl -w
# Wrapper package for zabbix_sender binary. Implemented as singletone.
# Version 1
package Zmon::Zsb;

use vars;
use strict;
use warnings;
use Sys::Hostname;

use JSON::PP qw(encode_json);
use IPC::Run qw( run timeout );
#DEBUG use Data::Dumper;
use Zmon::Slog qw(slog sfatal);

use Exporter qw(import);
our @EXPORT = qw(zbx_send zbx_jsend);

my @senders = ('/opt/zabbix-agent/bin/zabbix_sender', '/usr/bin/zabbix_sender', '/opt/freeware/bin/zabbix_sender');
my @configs = ('/etc/opt/zabbix-agent/zabbix_agentd.conf', '/etc/zabbix/zabbix_agentd.conf');

my $ZBXSENDER_CMD = _get_first_exist_file(\@senders);
my $ZBX_CFG = _get_first_exist_file(\@configs);
my $HNAME = _get_zbx_hostname($ZBX_CFG);

# Usage: zbx_send('key' => "KEY", 'value' => "VALUE")
sub zbx_send
{
    my %params = @_;
    if (! $params{'key'} || ! $params{'value'})
    {
        sfatal('msg' => "No key or value");
    }

    my @metric = ($HNAME, $params{'key'});
    my $Toption = '';
    if ($params{'timestamp'})
    {
        push @metric, $params{'timestamp'};
        $Toption = '-T';
    };
    push @metric, $params{'value'};
    my $send_string = join(' ', @metric);

    my @cmd = ($ZBXSENDER_CMD, '-c', $ZBX_CFG, $Toption, '-i', '-');
    my ($out, $err);
    run \@cmd, \$send_string, \$out, \$err, timeout(30) or sfatal('msg' => "$ZBXSENDER_CMD : $?");    
}

sub zbx_jsend
{
    my %params = @_;

    if (! $params{'key'} || ref $params{'value'} ne 'ARRAY')
    {   
        sfatal('msg' => "No key or value or value is not a array ref");
    }

    if ($params{'addrunok'})
    {
        push @{$params{'value'}}, {'RUN_OK' => 1};
    }

    my $data2send;
    if ($params{'wrapdata'})
    {
        $data2send = {'data' => $params{'value'}};
    }
    else
    {
        $data2send = $params{'value'};
    }

    my $ts = ($params{'timestamp'}) ? $params{'timestamp'} : "";
    zbx_send('key' => $params{'key'}, 'value' => encode_json($data2send), 'timestamp' => $ts);
}

sub _get_first_exist_file
{
    my $filevariants = shift;
    grep { -e $_ && return $_ } @{$filevariants};
    sfatal('msg' => "There is no existing file in the list: " . join(', ', @{$filevariants}));
}
 
sub _get_zbx_hostname
{
    my $file = shift;
    my $ret;
    open my $fh, '<:encoding(UTF-8)', $file or die "Could not open file $file";
    while (my $line = <$fh>)
    {
      if (($ret) = ($line =~ /^Hostname=([\w\-]+)/))
      {
         last;
      }
    }
    close $fh;
    
    return ($ret) ? $ret : hostname;
}

1;
