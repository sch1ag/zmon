#!/usr/bin/env python2
#version 1
from __future__ import division
import subprocess
import re
import pprint
import os
import sys
import time
import argparse
import json

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'lib'))
from pyzmon.zmoncfg import getcfg
from pyzmon.slog import (slog, sfatal)

class Phoenix:
    save_attrs = []
    def save_to_dict(self):
        data = {}
        for attr_name in self.save_attrs:
            data[attr_name] = getattr(self, attr_name)
        return data

    def _load_from_dict(self, data):
        #check than all needed keys are in data
        if all(an in data for an in self.save_attrs):
            for attr_name in data:
                setattr(self, attr_name, data[attr_name])
            return True
        return False
    
class BlkDevGroups(Phoenix):
    re_scsi_host_id = re.compile('^(\d+):\d+:\d+:\d')
    re_sd_dev = re.compile('^[hsv]d[a-z]+$')
    save_attrs = ['last_update_time', 'kname2groups']
    
    def __init__(self, data2load={}):
        self.kname2groups = {}
        self.last_update_time = 0

        #load global script config
        cfgobj = getcfg()
        self.cfg = dict(cfgobj.cfg)
        #compile re from cfg
        for k in self.cfg:
            if k.startswith('re_'):
                self.cfg[k] = re.compile(self.cfg[k])
        if not self._load_from_dict(data2load) or time.time() - self.last_update_time > 10800:
            self.update_dev_grps()
    
    @staticmethod
    def _get_lsblk_out():
        lsblk_base =  ['lsblk', '-P', '-s', '-o']
        lsblk_o_fields = ['NAME,KNAME,TYPE,TRAN,HCTL', 'NAME,KNAME,TYPE']
        
        out = ''
        for fields in lsblk_o_fields:
            cmd = lsblk_base[:]
            cmd.append(fields)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            out, err = p.communicate()
            if not err:
                break
        return out
    
    def update_dev_grps(self):   
        lsblk_out = BlkDevGroups._get_lsblk_out()
        re_pair_template = re.compile(r'([A-Z]+)="(\S*)"(?: |$)')
        for line in lsblk_out.split('\n'):
            if line:
                pairs = re_pair_template.findall(line)
                disk_info = dict(pairs)
                BlkDevGroups._normalize_fields(disk_info)
                #pprint.pprint(disk_info)
                self._add_dev_to_grp_mapping(disk_info)
        self.last_update_time = time.time()

    def _add_dev_to_grp_mapping(self, disk_info):
        if self.cfg['re_skip_kname'].search(disk_info['KNAME']) or self.cfg['re_skip_type'].search(disk_info['TYPE']):
            return
        else:
            groups = []
            #add device KNAME to NAME group if needed
            for attrkey in self.cfg['per_dev_stat']:
                for attrval in self.cfg['per_dev_stat'][attrkey]:
                    if attrkey in disk_info and disk_info[attrkey] ==  attrval:
                        groups.append('dev:' + disk_info['NAME'])
            #add device to groups by attributes
            for attrkey in self.cfg['group_by_attr']:
                if attrkey in disk_info and disk_info[attrkey]:
                    groups.append(attrkey.lower() + ':' + disk_info[attrkey])            
            self.kname2groups[disk_info['KNAME']] = groups

    @staticmethod
    def _normalize_fields(disk_info):
        hctl = ''
        scsi_host = ''
        tran = ''
        fc_host_listing = None
        #We will try to enrich disk_info only for sdX devices
        if BlkDevGroups.re_sd_dev.match(disk_info['KNAME']):
            if 'HCTL' in disk_info:
                hctl = disk_info['HCTL']
            else:
                devslink = '/sys/class/block/' + disk_info['KNAME']
                if os.path.islink(devslink):
                    devpath = os.readlink(devslink)
                    hctl = devpath.split('/')[-3]
            if hctl:
                scsi_host_id_m = BlkDevGroups.re_scsi_host_id.match(hctl)
                if scsi_host_id_m:
                    scsi_host = 'host' + scsi_host_id_m.group(1)
            #if TRAN does not exist we'll try to check is it FC device 
            if not 'TRAN' in disk_info and scsi_host:
                if fc_host_listing == None:
                    fc_host_listing = BlkDevGroups._list_fc_hosts()
                if disk_info in fc_host_listing:
                    tran = 'fc'

        if not 'HCTL' in disk_info:
            disk_info['HCTL'] = hctl

        disk_info['SCSI_HOST'] = scsi_host

        if not 'TRAN' in disk_info:
            disk_info['TRAN'] = tran

    @staticmethod
    def _list_fc_hosts():
        if os.path.exists('/sys/class/fc_host/'):
            return os.listdir('/sys/class/fc_host/')
        else:
            return []
        
class IOFields:
    #fields as discribed in https://www.kernel.org/doc/Documentation/admin-guide/iostats.rst
    #fieldnames = ['r_io', 'r_merge', 'r_sect', 'r_ms', 'w_io', 'w_merge', 'w_sect', 'w_ms', 'io_flight', 'actv_ms']

    #define /proc/diskstats fields which will be used as input data
    diskstats_kname = 2
    diskstats_num_first = 3
    diskstats_num_last = 14
    
    #fields from diskstats_num_first to diskstats_num_last
    r_io = 0
    r_merge = 1
    r_sect = 2
    r_ms = 3
    w_io = 4
    w_merge = 5
    w_sect = 6
    w_ms = 7
    io_flight = 8
    actv_ms = 9
    weighted_ms = 10
    #additional calculated fields
    top_r_ms = 11
    top_w_ms = 12
    
class GroupStats:
    def __init__(self):
        self.stat_by_groups = {}

    def do_dev_accounting(self, kname, dev_stat_diff, groupobj):
        if kname in groupobj.kname2groups:
            knamegroups = groupobj.kname2groups[kname]
            for statgroup in knamegroups:
                self._add_devstat_to_grp(statgroup, dev_stat_diff)

    def _add_devstat_to_grp(self, statgroup, dev_stat_diff):
        if statgroup in self.stat_by_groups:
            statgroup_data = self.stat_by_groups[statgroup]
            for i, v in enumerate(dev_stat_diff):
                if i == IOFields.actv_ms or i == IOFields.top_r_ms or i == IOFields.top_w_ms:
                    statgroup_data[i] = max(v, statgroup_data[i])
                else:
                    statgroup_data[i] += v
        else:
            self.stat_by_groups[statgroup] = dev_stat_diff
            
    def calclate_stats(self, time_diff):
        result_stat_by_group = {}
        time_diff_ms = round(time_diff * 1000)
        if time_diff:
            for statgroup in self.stat_by_groups:
                result_group_stat = {}
                group_data = self.stat_by_groups[statgroup]
                #read
                result_group_stat['r_io/s']    = round(group_data[IOFields.r_io] / time_diff, 1)
                result_group_stat['r_merge/s'] = round(group_data[IOFields.r_merge] / time_diff, 1)
                result_group_stat['r_bytes/s'] = round(group_data[IOFields.r_sect] / time_diff * 512)
                if group_data[IOFields.r_io] > 0:
                    result_group_stat['r_ms']  = round(group_data[IOFields.r_ms] / group_data[IOFields.r_io], 2)
                    result_group_stat['r_bytes/io'] = round(group_data[IOFields.r_sect] / group_data[IOFields.r_io] * 512)
                else:
                    result_group_stat['r_ms']  = 0.0
                    result_group_stat['r_bytes/io'] = 0.0
                result_group_stat['top_r_ms'] = group_data[IOFields.top_r_ms]
                
                #write
                result_group_stat['w_io/s']    = round(group_data[IOFields.w_io] / time_diff, 1)
                result_group_stat['w_merge/s'] = round(group_data[IOFields.w_merge] / time_diff, 1)
                result_group_stat['w_bytes/s'] = round(group_data[IOFields.w_sect] / time_diff * 512)
                if group_data[IOFields.w_io] > 0:
                    result_group_stat['w_ms']  = round(group_data[IOFields.w_ms] / group_data[IOFields.w_io], 2)
                    result_group_stat['w_bytes/io'] = round(group_data[IOFields.w_sect] / group_data[IOFields.w_io] * 512)
                else:
                    result_group_stat['w_ms']  = 0.0
                    result_group_stat['w_bytes/io'] = 0.0
                result_group_stat['top_w_ms'] = group_data[IOFields.top_w_ms]

                #other
                result_group_stat['sum_aq_length'] = round(group_data[IOFields.weighted_ms] / time_diff_ms, 2)
                result_group_stat['top_actv_perc'] = round(group_data[IOFields.actv_ms] / time_diff_ms * 100, 2)

                result_stat_by_group[statgroup] = result_group_stat
        self.stat_by_groups = {}
        return result_stat_by_group

class BlkStatsReader(Phoenix):
    
    save_attrs = ['time_curr', 'curr', 'nrows']
    
    def __init__(self, data2load={}):
        cfgobj = getcfg()
        self.re_skip_kname = re.compile(cfgobj.cfg['re_skip_kname'])
        self.statfile = '/proc/diskstats'
        self.nrows = 0
        self.curr = {}
        self.prev = {}
        self.time_curr = 0
        self.time_diff = 0
        self._load_from_dict(data2load)

    def read_stats(self, blk_dev_grps, grp_stats):
        self.prev = self.curr
        self.curr = {}
        time_prev = self.time_curr
        self.time_curr = time.time()
        
        self.time_diff = 0
        if time_prev:
            self.time_diff = self.time_curr - time_prev
        
        cnt = 0 
        with open(self.statfile, 'r') as fp:
            line = fp.readline()
            cnt = 1
            while line:
                line = fp.readline().strip()
                cnt += 1
                kname = self._set_dev_stat(line)
                if kname:
                    dev_interval_data = self._prepare_interval_data(kname)
                    if dev_interval_data:
                        grp_stats.do_dev_accounting(kname, dev_interval_data, blk_dev_grps)
                #file reading done
           
        #update groups if number of rows in file has changed
        if self.nrows == 0:
            self.nrows = cnt
        elif cnt != self.nrows:
            blk_dev_grps.update_dev_grps()
            self.nrows = cnt
    
    def _set_dev_stat(self, devstatline):
        kname = ''
        statlist = devstatline.split()
        if len(statlist) >= IOFields.diskstats_num_last:
            kname = statlist[IOFields.diskstats_kname]
            if self.re_skip_kname.match(kname):
                kname = ''
            else:
                self.curr[kname] = list(map(int, statlist[IOFields.diskstats_num_first 
                                                          : 
                                                          IOFields.diskstats_num_last]))
        return kname

    def _prepare_interval_data(self, kname):
        diff = []
        #assumpt that kname is in self.curr when we call _mk_diff
        if kname in self.prev:
            prev_dev_stat = self.prev[kname]
            curr_dev_stat = self.curr[kname]
            #pprint.pprint(curr_dev_stat)
            for (fieldnum, curr_val) in enumerate(curr_dev_stat):
                #field 8 - io_flight 
                if fieldnum == IOFields.io_flight:
                    diff.append(curr_val)
                else:
                    diff.append(curr_val - prev_dev_stat[fieldnum])
            #pprint.pprint(diff)
            #add per device response time to find group max
            ms = 0.0
            if diff[IOFields.r_io] > 0:
                ms = round(diff[IOFields.r_ms] / diff[IOFields.r_io], 2)
            diff.append(ms)
            ms = 0.0
            if diff[IOFields.w_io] > 0:
                ms = round(diff[IOFields.w_ms] / diff[IOFields.w_io], 2)
            diff.append(ms)
        #print(kname)
        #pprint.pprint(diff)
        return diff

def check_metric_name(metric_name):
    if re.match('^[a-z0-9_-]+$', metric_name):
        return metric_name
    else:
        sfatal("Metric name " + metric_name + ". Only alphanumeric characters, underscore and dash are allowed.")
        return ""

def load_saved_data(data_path, datakeys):
    dict_to_load = {}
    objdata = dict(map(lambda x: (x, {}), datakeys))
    #if intermediate file exists and it is younger than program file    
    if os.path.exists(data_path) and os.path.getmtime(data_path) > os.path.getmtime(__file__):
        try:
            with open(data_path, 'r') as infile:
                dict_to_load = json.load(infile)
        except Exception as e:
            slog("Something wrong with " + data_path + " json file: " + str(e.args))
        else:
            if type(dict_to_load) is dict:
                for k in objdata:
                    if k in dict_to_load:
                        objdata[k] = dict_to_load[k]
            else:
                slog("File " + data_path + " contains wrong data. Ignoring it.")
    return objdata
                
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Script to gather and aggregate block device statistics for Zabbix')
    parser.add_argument('-k', '--key', dest='metric_name', nargs='?', action='store', default='blkstat', required=False, help='')
    args = parser.parse_args()
    metric_name = check_metric_name(args.metric_name)

    #make global script config object
    cfgobj = getcfg(script_pathname=__file__,
                  cfg={'re_skip_kname': r'^VxVM|^VxDMP|^zram|^loop|^sd[a-z]+\d+$',
                       're_skip_type': r'^part$|^rom$',
                       'per_dev_stat': {'TYPE': ['mpath', 'lvm']},
                       'group_by_attr': ['TYPE', 'SCSI_HOST', 'TRAN']
                      }
                 # cfg={'re_skip_kname': r'^VxVM|^VxDMP|^zram|^loop|^sd[a-z]+\d+$',
                 #      're_skip_type': r'^part$|^rom$',
                 #      'per_dev_stat': {'TYPE': ['disk']},
                 #      'group_by_attr': []
                 #     }
                 )
    
    #pprint.pprint(cfgobj.cfg)

    #define file to store data between executions
    data_path = os.path.join('/dev/shm', cfgobj.scriptname + '.' + metric_name + '.zdata')
    objdata = load_saved_data(data_path, ['blk_dev_grps', 'blk_stats'])

    #pprint.pprint(objdata)

    blk_dev_grps = BlkDevGroups(objdata['blk_dev_grps'])
    grp_stats = GroupStats()
    blk_stats = BlkStatsReader(objdata['blk_stats'])

    blk_stats.read_stats(blk_dev_grps, grp_stats)
    stats_by_group = grp_stats.calclate_stats(blk_stats.time_diff)

    #pprint.pprint(blk_dev_grps.kname2groups)
        
    result = {'RUNOK': 1}
    data = []
    for dev_grp in stats_by_group:
        stats_by_group[dev_grp]['name'] = dev_grp
        data.append(stats_by_group[dev_grp])
    if data:
        result['data'] = data
    print(json.dumps(result))

    #pprint.pprint(result)    
    objdata['blk_stats'] = blk_stats.save_to_dict()
    objdata['blk_dev_grps'] = blk_dev_grps.save_to_dict()
        
    with open(data_path, 'w') as outfile:
        json.dump(objdata, outfile)
    
    