#!/usr/bin/perl -w
# Wrapper package for zabbix_sender binary. Implemented as singletone.
# Version 4
package Zmon::Zsb;

use vars;
use strict;
use warnings;
use Sys::Hostname;
use FindBin qw($Script);

use JSON::PP qw(encode_json);
use IPC::Run qw( run timeout harness );
#DEBUG use Data::Dumper;
use Zmon::Slog qw(slog sfatal);

use Exporter qw(import);
our @EXPORT = qw(zbx_send zbx_jsend set_custom_zcfg);

my @senders = ('/opt/zabbix-agent/bin/zabbix_sender', '/usr/bin/zabbix_sender', '/opt/freeware/bin/zabbix_sender');
my @configs = ('/etc/opt/zabbix-agent/zabbix_agentd.conf', '/etc/zabbix/zabbix_agentd.conf');

my $ZBXSENDER_CMD = _get_first_exist_file(\@senders);
my $ZBX_CFG = _get_first_exist_file(\@configs);
my $HNAME = _get_zbx_hostname($ZBX_CFG);
my $sender_input = '/tmp/' . $Script . '.' . $$ . '.zsend';

# Usage: zbx_send('key' => "KEY", 'value' => "VALUE")
sub zbx_send
{
    my %params = @_;
    if (! $params{'key'} || ! $params{'value'})
    {
        sfatal('msg' => "No key or value");
    }

    my @cmd = ($ZBXSENDER_CMD, '-c', $ZBX_CFG, '-i', $sender_input);
    my @metric = ($HNAME, $params{'key'});
    if ($params{'timestamp'})
    {
        push @metric, $params{'timestamp'};
        push @cmd, '-T';
    };
    push @metric, $params{'value'};
    my $send_string = join(' ', @metric);

    open (my $fh, '>', $sender_input) or sfatal('msg' => "Couldn't open $sender_input : $!");
    print $fh $send_string."\n" ;
    close($fh);

    my ($out, $err, $run_ok, $x);
    my $h = harness \@cmd, \undef, \$out, \$err, timeout(30);
    eval {
        $run_ok = run $h;
        1;
    };
    if ($@)
    {
        $x = $@;
        slog(msg => "zabbix_sender exception: $x");
    }
    if (! $run_ok)
    {
        slog(msg => "zabbix_sender rc: " . $h->result);
        slog(msg => "zabbix_sender $sender_input content: $send_string");
        slog(msg => "zabbix_sender stderr: $err");
        slog(msg => "zabbix_sender stdout: $out");
        slog(msg => "zabbix_sender cmd: " . join(' ', @cmd));
    }
    close $fh;
    unlink $sender_input;
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
        push @{$params{'value'}}, {'RUN_OK' => "1"};
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

sub set_custom_zcfg
{
    my $new_cfg = shift;
    if ( -e $new_cfg )
    {
        $ZBX_CFG = $new_cfg;
        $HNAME = _get_zbx_hostname($ZBX_CFG);
    }
    else
    {
        sfatal(msg => "File $new_cfg does not exist");
    }
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
