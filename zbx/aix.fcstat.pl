#!/usr/bin/perl -w
# Script for posting fcstat data to Zabbix
# Script should run ok on AIX with perl installed
# Version 1

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$Bin/../lib";

use Getopt::Std;
use Zmon::Slog;
use Zmon::Flocker;
use Zmon::Zsb;
use Time::HiRes qw(sleep time);
use IPC::Run qw(run harness timeout);
use JSON::PP qw(encode_json decode_json);
use Scalar::Util qw(looks_like_number);

use Data::Dumper;

# Parse options
our ($opt_t, $opt_c, $opt_h, $opt_k);
getopts('t:k:c:h');
if ($opt_h) { usage() };
if ($opt_c) { set_custom_zcfg($opt_c) };
my $timeout = ($opt_t) ? $opt_t : 60 ;
my $metric_key = ($opt_k) ? $opt_k : 'fcstat';

# Check that no any other instance of this script currently running on the system.
my $locker = Zmon::Flocker->new();
$locker->stay_single_or_die();

# Define file to store statistics between runs
my $datapath = '/tmp/' . $Script . '.zdata';

# Get fcs instances
my $fcses = obtain_fcs();
# Obtain current staistics
my $fcstat_curr = get_current_fcstat($fcses, $timeout);
# Read prev fcs staistics from file
my $fcstat_prev = read_json_from_file($datapath);
# Save current staistics to file
save_data($fcstat_curr, $datapath);

# Create diff between curr and prev stats
my $fcstat_results = prepare_data($fcstat_curr, $fcstat_prev); 
# Format data for Zabbix
my @data_to_send = map { $fcstat_results->{$_}->{'fcs_name'} = $_; $fcstat_results->{$_} } keys %{$fcstat_results};
# Send data to Zabbix
zbx_jsend('key' => $metric_key, 'value' => \@data_to_send, 'addrunok' => 1, 'wrapdata' => 1);

# Release lock
$locker->release();

#Functions
sub prepare_data
{
    my $fcstat_curr = shift;
    my $fcstat_prev = shift;
    my %result_data;
    for my $fcs (keys %{$fcstat_curr})
    {
        if (defined $fcstat_prev->{$fcs})
        {
            $result_data{$fcs} = make_diff($fcstat_curr->{$fcs}, $fcstat_prev->{$fcs});    
        }
    }
    return \%result_data;
}

sub make_diff
{
    my $curr = shift;
    my $prev = shift;
    my $defval = -2;

    my @key_type_name = (
        ['running_speed',     'raw',  'running_speed_Gbps' ],
        ['secs_since_reset',  'diff', 'interval_secs'      ],
        ['link_fail_cnt',     'diff', 'link_fail_pi'       ],
        ['loss_of_sync_cnt',  'diff', 'loss_of_sync_pi'    ],
        ['loss_of_signal',    'diff', 'loss_of_signal_pi'  ],
        ['inv_tx_word_cnt',   'diff', 'inv_tx_word_pi'     ],
        ['inv_crc_cnt',       'diff', 'inv_crc_pi'         ],
        ['no_dma_res_cnt',    'diff', 'no_dma_res_pi'      ],
        ['no_adapt_elem_cnt', 'diff', 'no_adapt_elem_pi'   ],
        ['no_cmd_res_cnt',    'diff', 'no_cmd_res_pi'      ],
        ['in_req',            'rate', 'in_req_ps'          ],
        ['out_req',           'rate', 'out_req_ps'         ],
        ['in_bytes',          'rate', 'in_bytes_ps'        ],
        ['out_bytes',         'rate', 'out_bytes_ps'       ]
    );

    my %result;    
    for my $line (@key_type_name)
    {
        my $key = $line->[0];
        my $type = $line->[1];
        my $name = $line->[2];
        $result{$name} = $defval;
        
        if($type eq 'raw')
        {
            if (check_val_nnn($curr, $key))
            {
                $result{$name} = $curr->{$key};
            }
            next;
        }
        
        if(check_val_nnn($prev, $key) && check_val_nnn($curr, $key) && $curr->{$key} >= $prev->{$key})
        {
            my $diff = $curr->{$key} - $prev->{$key};
            if ($type eq 'diff')
            {
                $result{$name} = $diff;
            }
            elsif ($result{'interval_secs'} > 0)
            {
                $result{$name} = sprintf("%d", $diff / $result{'interval_secs'});
            }
        }
    }

    add_bw_util_perc(\%result);
    return \%result;
}

sub add_bw_util_perc
{
    my $data = shift;
    $data->{'in_bw_util_perc'} = 0;
    $data->{'out_bw_util_perc'} = 0;
    #1342177.28 = 1024^3 / 8 / 100

    if ($data->{'running_speed_Gbps'} > 0)
    {
        ($data->{'in_bytes_ps'} > 0) && $data->{'in_bw_util_perc'} = $data->{'in_bytes_ps'} / $result{'running_speed_Gbps'} / 1342177;
        ($data->{'out_bytes_ps'} > 0) && $data->{'out_bw_util_perc'} = $data->{'out_bytes_ps'} / $result{'running_speed_Gbps'} / 1342177;
    }
}

#check that value of a key is defined and it is a non-negative number
sub check_val_nnn
{
    my $href = shift;
    my $k = shift;
    
    return (defined $href->{$k} && looks_like_number($href->{$k}) && $href->{$k} >= 0);
}

sub save_data
{
    my $data = shift;
    my $savefilepath = shift;
    open(my $fh, '>', $savefilepath) or sfatal('msg' => "Couldn't open $savefilepath : $!");
    my $json_text = encode_json($data);
    print $fh $json_text ;
    close($fh);
}

sub slurp {
    my $file = shift;
    open my $fh, '<', $file or sfatal('msg' => "Couldn't open $file : $!");
    local $/ = undef;
    my $cont = <$fh>;
    close $fh;
    return $cont;
}

sub read_json_from_file {
    my $filepathname = shift;
    my $json_struct = {};
    if (-r $filepathname)
    {
        my $json_text = slurp($filepathname);
        eval
        {
            $json_struct = decode_json $json_text;
            1;
        };
        if ($@)
        {
            my $x = $@;
            slog('msg' => "Ignoting content of $filepathname because of decode error: $x");
        }
    }
    return $json_struct;
}


sub get_current_fcstat
{
    my $fcs_instatnces = shift;
    my $tmout = shift;
   
    my $fcstat_raw_data = get_fcstat_raw_data($fcs_instatnces, $tmout);
    my %fcs_to_statlines = map { my @a = split(/\n/, $fcstat_raw_data->{$_}); $_ => \@a } keys %{$fcstat_raw_data};
    my %fcs_to_statattrs = map { $_ => parse_fcs_stat($fcs_to_statlines{$_}) } keys %fcs_to_statlines;
    return \%fcs_to_statattrs;
} 

sub parse_fcs_stat
{
    my @template_key_stanza = (
        ['Port Speed \(running\):\s+([0-9]+)\s+GBIT',   'running_speed',                    ''                      ],
        ['Seconds Since Last Reset:\s+([0-9]+)',        'secs_since_reset',                 ''                      ],
        ['Link Failure Count:\s+([0-9]+)',              'link_fail_cnt',                    ''                      ],
        ['Loss of Sync Count:\s+([0-9]+)',              'loss_of_sync_cnt',                 ''                      ],
        ['Loss of Signal:\s+([0-9]+)',                  'loss_of_signal',                   ''                      ],
        ['Invalid Tx Word Count:\s+([0-9]+)',           'inv_tx_word_cnt',                  ''                      ],
        ['Invalid CRC Count:\s+([0-9]+)',               'inv_crc_cnt',                      ''                      ],
        ['No DMA Resource Count:\s+([0-9]+)',           'no_dma_res_cnt',                   'fc_scsi_drv_info'      ],
        ['No Adapter Elements Count:\s+([0-9]+)',       'no_adapt_elem_cnt',                'fc_scsi_drv_info'      ],
        ['No Command Resource Count:\s+([0-9]+)',       'no_cmd_res_cnt',                   'fc_scsi_drv_info'      ],
        ['Input Requests:\s+([0-9]+)',                  'in_req',                           'fc_scsi_traffic_stats' ],
        ['Output Requests:\s+([0-9]+)',                 'out_req',                          'fc_scsi_traffic_stats' ],
        ['Input Bytes:\s+([0-9]+)',                     'in_bytes',                         'fc_scsi_traffic_stats' ],
        ['Output Bytes:\s+([0-9]+)',                    'out_bytes',                        'fc_scsi_traffic_stats' ]
    );
    my %stanza_to_template = ('fc_scsi_drv_info' => 'FC SCSI Adapter Driver Information', 'fc_scsi_traffic_stats' => 'FC SCSI Traffic Statistics');

    my $fcs_statlines = shift;
    my %fcs_attrs;

    my $curr_stanza = '';

    for my $line (@{$fcs_statlines})
    {
        #end of stamza check
        if ($line =~ /^\s*$/)
        {
            my $curr_stanza = '';
            next;
        }
        else
        {
            #start of stanza check
            for my $stanza_test (keys %stanza_to_template)
            {
                if ($line =~ /$stanza_to_template{$stanza_test}/) 
                {
                    $curr_stanza = $stanza_test;
                    next;
                }
            } 
            #attr check
            for my $attr_def_array (@template_key_stanza)
            {
                #print Dumper($attr_def_array);
                if ($attr_def_array->[2] eq $curr_stanza && $line =~ /$attr_def_array->[0]/)
                {
                    $fcs_attrs{$attr_def_array->[1]} = $1;
                    next;
                }
            }
        }
    }

    return \%fcs_attrs;
}

sub get_fcstat_raw_data
{
    my $fcs_instatnces = shift;
    my $tmout = shift;
    my %fcs_to_fcstat_proc = map { $_ => construct_and_run_proc($_, $tmout) } @{$fcs_instatnces};
    my %fcs_to_fcstat_out = map { $fcs_to_fcstat_proc{$_}->{'success'} = $fcs_to_fcstat_proc{$_}->{'harness'}->finish ; $_ => $fcs_to_fcstat_proc{$_}->{'out'}} @{$fcs_instatnces};

    return \%fcs_to_fcstat_out;
}

sub construct_and_run_proc
{
    my $fcs = shift;
    my $tmout = shift;
    my $FCSTAT_CMD = '/usr/bin/fcstat';
    my %procdata = (
        'in' => undef,
        'out' => undef,
        'err' => undef,
        'success' => undef
    );
    my @cmd = ($FCSTAT_CMD, $fcs);
    $procdata{'cmd'} = \@cmd;
    my $h = harness $procdata{'cmd'}, \$procdata{'in'}, \$procdata{'out'}, \$procdata{'err'}, timeout($tmout);
    $procdata{'harness'} = $h;
    $h->start;
    return \%procdata;
}


sub obtain_fcs
{
    my $LSDEV_CMD = '/usr/sbin/lsdev';
    my @cmd = ($LSDEV_CMD, '-l', 'fscsi*', '-S', 'a', '-F', 'name');
    my ($out, $err);
    run \@cmd, \undef, \$out, \$err, timeout(30) or sfatal(msg => "Error running " . join(' ', @cmd));
    chomp $out;
    my @fscsi_instances = split /\n/, $out;
    my @fcs_instances;
    for my $fscsi (@fscsi_instances)
    {
        @cmd = ($LSDEV_CMD, '-l', $fscsi, '-F', 'parent');
        $out = undef;
        $err = undef;
        run \@cmd, \undef, \$out, \$err, timeout(30) or sfatal(msg => "Error running " . join(' ', @cmd));
        chomp $out;
        push @fcs_instances, $out;
    }
    return \@fcs_instances;
}

sub usage {
print "$0 designed to monitor mounted NFS availability on client using zabbix
$0 [-t timeout] [-k metric_name] [-c zcfg_file] | -h
       
        Options:
        -k metric_key - key of metric [default: nfsmntcheck]
        -t timeout    - timeout is seconds [default=60 sec]
        -c zcfg_file  - zabbix config to use with zabbix_sender [default is to use first exist file of the list /etc/opt/zabbix-agent/zabbix_agentd.conf, /etc/zabbix/zabbix_agentd.conf]
        -h            - usage
";
exit;
}

