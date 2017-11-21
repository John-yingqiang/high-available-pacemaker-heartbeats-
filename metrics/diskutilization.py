# coding=utf-8

"""
Collect IO Stats

Note: You may need to artifically generate some IO load on a disk/partition
before graphite will generate the metrics.

 * http://www.kernel.org/doc/Documentation/iostats.txt

#### Dependencies

 * /proc/diskstats

"""

import diamond.service_collector
import diamond.convertor
import time
import os, sys
import re
import json
import string

sys.path.insert(0, "/opt/milio/libs/atlas")
from cmd import *

try:
    import psutil
    psutil  # workaround for pyflakes issue #13
except ImportError:
    psutil = None


class DiskUtilizationCollector(diamond.service_collector.ServiceCollector):

    # Global variables
    atlas_conf = '/etc/ilio/atlas.json'
    ilio_uuid = ''
    myrole = ''
    isVolume = False

    MAX_VALUES = {
        'reads':                    4294967295,
        'reads_merged':             4294967295,
        'reads_milliseconds':       4294967295,
        'writes':                   4294967295,
        'writes_merged':            4294967295,
        'writes_milliseconds':      4294967295,
        'io_milliseconds':          4294967295,
        'io_milliseconds_weighted': 4294967295
    }

    LastCollectTime = None

    def get_default_config_help(self):
        config_help = super(DiskUsageCollector, self).get_default_config_help()
        config_help.update({
            'devices': "A regex of which devices to gather metrics for."
                       + " Defaults to md, sd, xvd, disk, and dm devices",
            'sector_size': 'The size to use to calculate sector usage',
            'send_zero': 'Send io data even when there is no io',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        volume = 'VOLUME'

        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a virtual volume
				ilio = jsondata.get("usx")
				self.ilio_uuid  = ilio.get("uuid")
				self.myrole = ilio.get("roles")[0]
            else:
				self.ilio_uuid = jsondata.get("uuid")
				self.myrole = roles[0]
            fp.close()

        if re.search(volume,self.myrole,re.IGNORECASE):
            self.isVolume = True

        config = super(DiskUtilizationCollector, self).get_default_config()
        config.update({
            'enabled':  'True',
            #'path':     'iostat',
            'devices':  ('PhysicalDrive[0-9]+$'
                         + '|md[0-9]+$'
                         #+ '|sd[a-z]+[0-9]*$'
                         + '|sd[a-z]+[1-9]*$' # only report
                         + '|x?vd[a-z]+[0-9]*$'
                         + '|disk[0-9]+$'
                         + '|dm\-[0-9]+$'
                         + '|ibd[0-9]*$'),
                         #+ '|(nbd[0-9]*(p[0-9])*)*$'), # report all nbd devices, including partitions
                         #+ '|(nbd[0-9]*(p[0-9])*$'), # report nbd partitions
            'sector_size': 512,
            'send_zero': 'False',
        })
        return config

    def get_disk_statistics(self):
        """
        Create a map of disks in the machine.

        http://www.kernel.org/doc/Documentation/iostats.txt

        Returns:
          (major, minor) -> DiskStatistics(device, ...)
        """
        result = {}

        if os.access('/proc/diskstats', os.R_OK):
            fp = open('/proc/diskstats')

            try:
                for line in fp:
                    try:
                        columns = line.split()
                        # On early linux v2.6 versions, partitions have only 4
                        # output fields not 11. From linux 2.6.25 partitions
                        # have the full stats set.
                        if len(columns) < 14:
                            continue
                        major = int(columns[0])
                        minor = int(columns[1])
                        device = columns[2]

                        if (device.startswith('ram')
                                or device.startswith('loop')):
                            continue

                        result[(major, minor)] = {
                            'device': device,
                            'reads': float(columns[3]),
                            'reads_merged': float(columns[4]),
                            'reads_sectors': float(columns[5]),
                            'reads_milliseconds': float(columns[6]),
                            'writes': float(columns[7]),
                            'writes_merged': float(columns[8]),
                            'writes_sectors': float(columns[9]),
                            'writes_milliseconds': float(columns[10]),
                            'io_in_progress': float(columns[11]),
                            'io_milliseconds': float(columns[12]),
                            'io_milliseconds_weighted': float(columns[13])
                        }
                    except ValueError:
                        continue
            finally:
                fp.close()
        else:
            if not psutil:
                self.log.error('Unable to import psutil')
                return None

            disks = psutil.disk_io_counters(True)
            for disk in disks:
                    result[(0, len(result))] = {
                        'device': disk,
                        'reads': disks[disk].read_count,
                        'reads_merged': 0,
                        'reads_sectors': (disks[disk].read_bytes
                                          / int(self.config['sector_size'])),
                        'reads_milliseconds': disks[disk].read_time,
                        'writes': disks[disk].write_count,
                        'writes_merged': 0,
                        'writes_sectors': (disks[disk].write_bytes
                                           / int(self.config['sector_size'])),
                        'writes_milliseconds': disks[disk].write_time,
                        'io_in_progress': 0,
                        'io_milliseconds':
                        disks[disk].read_time + disks[disk].write_time,
                        'io_milliseconds_weighted':
                        disks[disk].read_time + disks[disk].write_time
                    }

        return result

    def get_device_mapped_name(self, map, dev_name):
        """
        Return mapped device name based on 'ls -l /dev/mapper'
        return value : parsed name if cmd returns result
                       dev_name if cmd returns nothing
        """
        dev_mapped_name = None
        if self.isVolume: # get device name mapping only for volumes
            if map:
                for line in map.split('\n'):
                    if '->' in line and '_log' not in line: # ignore log device
                        line_item = line.split('->')
                        this_device = line_item[-1].split('/')[-1].split('p')[0] # get the md device names: /dev/md3p2 -- md3
                        if 'indisk' in line_item[0] and this_device == dev_name:
                            dev_mapped_name = 'capacitypool'
                            break
                        elif 'inmemory' in line_item[0] and this_device == dev_name:
                            dev_mapped_name = 'performancepool'
                            break

        return dev_mapped_name

    def get_md_to_ibd_mapping(self, isUtil=0):
        """
        Check /proc/mdstat to get md to ibd mapping for capacity pool and performance pool
         construct, returns a dictionary
         isUtil = 1: return dict with mapped device name. DEPRECATED as vScaler cache is replaced
                     by compound device
         return value: dict of md to ibd mapping
                       key: md devices for capacity pool and performance pool
                       value: list of ibd devices used by the md device
        """
        dev_dict = {}
        temp_dict = {}
        temp = []
        dev_list = []

        if os.access('/proc/mdstat', os.R_OK): # Get md/ibd mapping from mdstat
            cmd = 'cat /proc/mdstat'
            for line in os.popen(cmd).readlines()[1:]: # skip the first line
                if 'active raid' in line:
                    line_item = line.split(':')
                    mykey = line_item[0].strip()
                    if 'active raid5' in line_item[1]:
                        dev_items = line_item[1].split('active raid5')
                        temp = dev_items[-1].strip().split(' ')
                        for device in temp:
                            the_device = device.split('p')[0]
                            dev_list.append(the_device)
                        dev_dict[mykey] = dev_list
                        dev_list = [] # reset the placeholder
                    if 'active raid1' in line_item[1]:
                        dev_items = line_item[1].split('active raid1')
                        temp_dict[mykey] = dev_items[-1].strip().split('p')[0]
        for key, value in dev_dict.iteritems():
            for item in value:
                if temp_dict.has_key(item):
                    dev_list.append(temp_dict[item])
            dev_dict[key] = dev_list
            dev_list = []

        mapped_dev_dict = {}
        cmd = 'ls -l /dev/ibd*'
        for line in os.popen(cmd).readlines():
            if '->' in line and '_log' not in line: # ignore log device
                line_item = line.split('->')
                this_device = line_item[-1].split('/')[-1].split('p')[0] # get the md device names: /dev/md3p2 -- md3 
                if 'indisk' in line_item[0] and this_device in dev_dict:
                    dev_mapped_name = 'capacitypool'
                    mapped_dev_dict[dev_mapped_name] = dev_dict[this_device]
                elif 'inmemory' in line_item[0] and this_device in dev_dict:
                    dev_mapped_name = 'performancepool'
                    mapped_dev_dict[dev_mapped_name] = dev_dict[this_device]

        if isUtil:
            return mapped_dev_dict
        else:
            return dev_dict

    def collect(self):

        # Handle collection time intervals correctly
        CollectTime = time.time()
        time_delta = float(self.config['interval'])
        if self.LastCollectTime:
            time_delta = CollectTime - self.LastCollectTime
        if not time_delta:
            time_delta = float(self.config['interval'])
        self.LastCollectTime = CollectTime

        exp = self.config['devices']
        reg = re.compile(exp)

        # Get volume resource uuid
        resource_id = self.get_resource_id()
 
        # Get device name mapping
        dev_mapping = None
        #cmd = 'ls -l /dev/mapper'
        if self.isVolume:
            cmd = 'ls -l /dev/ibd*'
            dev_mapping = os.popen(cmd).read()
            if not dev_mapping:
                self.log.debug("Get device mapped name command returns nothing: %s" % cmd)

        # Get md to ibd mapping, these dict are used to calculate ibd devices average statistics
        adjusted_util={}
        adjusted_servtime={}
        serv_time_dict={}
        disk_util_dict={}
        if self.isVolume:
            serv_time_dict = self.get_md_to_ibd_mapping()
            disk_util_dict = self.get_md_to_ibd_mapping()
        # Get disk statistics
        results = self.get_disk_statistics()
        if not results:
            self.log.error('No diskspace metrics retrieved')
            return None
        for key, info in results.iteritems():
            metrics = {}
            name = info['device']
            if not reg.match(name):
                continue
            capacity_size = 0 
            if "dm" in info['device']:
                lv_cmd = "ls -l /dev/mapper/ | awk '$11~/%s$/{print $9;}' | grep -o -P '(?<=[^-]\-)[^-].*$' | sed -e 's/--/-/g'" % info['device']
                lv_name = os.popen(lv_cmd).read()
                if lv_name != "":
                    lv_name = lv_name.strip("\n")
                    lv_detail_cmd = "lvs | grep '%s'" % lv_name
                    lv_detail = os.popen(lv_detail_cmd).read()
                    if lv_detail != "":
                        lvs_list = lv_detail.split("\n")
                        for lv in lvs_list:
                            if lv == "":
                                continue
                            lv_list = lv.split()
                            if lv_list[0] == lv_name:
                                m = re.search("(\d+\.\d+)(\w)", lv_list[3])
                                if m is not None:
                                    if 'g' == m.group(2).lower():
                                        capacity_size = long(float(m.group(1))) * 1024 * 1024 * 1024
                                    if 'm' == m.group(2).lower():
                                        capacity_size = long(float(m.group(1))) * 1024 * 1024
            elif "sd" in info['device'] or "vd" in info['device']:
                cmd = "pvs | grep '%s'" % info['device']
                pv_detail = os.popen(cmd).read()
                pvs_list = pv_detail.split("\n")
                for pv in pvs_list:
                    if pv == "": 
                       continue
                    pv_list = pv.split() 
                    m = re.search("(\d+\.\d+)(\w)", pv_list[4])
                    if m is not None:
                        if 'g' == m.group(2).lower():
                            capacity_s = long(float(m.group(1))) * 1024 * 1024 * 1024
                        if 'm' == m.group(2).lower():
                            capacity_s = long(float(m.group(1))) * 1024 * 1024
                        capacity_size = capacity_size + capacity_s
            if capacity_size != 0:
                info['capacity'] = capacity_size

            mapped_name = self.get_device_mapped_name(dev_mapping, info['device'])
            for key, value in info.iteritems():
                if key == 'device':
                    continue
                oldkey = key

                for unit in self.config['byte_unit']:
                    key = oldkey
                    if key.endswith('sectors'):
                        key = key.replace('sectors', unit)
                        value /= (1024 / int(self.config['sector_size']))
                        value = diamond.convertor.binary.convert(value=value,
                                                                 oldUnit='kB',
                                                                 newUnit=unit)
                        self.MAX_VALUES[key] = diamond.convertor.binary.convert(
                            value=diamond.collector.MAX_COUNTER,
                            oldUnit='byte',
                            newUnit=unit)
 
                    metric_name = '.'.join([info['device'], key])
                    # io_in_progress is a point in time counter, !derivative
                    if key != 'io_in_progress' and key != 'capacity':
                        metric_value = self.derivative(
                            metric_name,
                            value,
                            self.MAX_VALUES[key],
                            time_delta=False)
                    else:
                        metric_value = value
                    metrics[key] = metric_value

            metrics['read_requests_merged_per_second'] = (
                metrics['reads_merged'] / time_delta)
            metrics['write_requests_merged_per_second'] = (
                metrics['writes_merged'] / time_delta)
            metrics['reads_per_second'] = metrics['reads'] / time_delta
            metrics['writes_per_second'] = metrics['writes'] / time_delta

            for unit in self.config['byte_unit']:
                metric_name = 'read_%s_per_second' % unit
                key = 'reads_%s' % unit
                metrics[metric_name] = metrics[key] / time_delta

                metric_name = 'write_%s_per_second' % unit
                key = 'writes_%s' % unit
                metrics[metric_name] = metrics[key] / time_delta

                # Set to zero so the nodes are valid even if we have 0 io for
                # the metric duration
                metric_name = 'average_request_size_%s' % unit
                metrics[metric_name] = 0

            metrics['io'] = metrics['reads'] + metrics['writes']

            metrics['average_queue_length'] = (
                metrics['io_milliseconds_weighted']
                / time_delta
                / 1000.0)

            metrics['util_percentage'] = (metrics['io_milliseconds']
                                          / time_delta
                                          / 1000.0) # utilization = percentage of time that device spent in serving IO requests
            metrics['iops'] = 0
            metrics['service_time'] = 0
            metrics['await'] = 0
            metrics['concurrent_io'] = 0

            if metrics['reads'] > 0:
                metrics['read_await'] = (
                    metrics['reads_milliseconds'] / metrics['reads'])
            else:
                metrics['read_await'] = 0

            if metrics['writes'] > 0:
                metrics['write_await'] = (
                    metrics['writes_milliseconds'] / metrics['writes'])
            else:
                metrics['write_await'] = 0

            for unit in self.config['byte_unit']:
                rkey = 'reads_%s' % unit
                wkey = 'writes_%s' % unit
                metric_name = 'average_request_size_%s' % unit
                if (metrics['io'] > 0):
                    metrics[metric_name] = (
                        metrics[rkey] + metrics[wkey]) / metrics['io']
                else:
                    metrics[metric_name] = 0

            metrics['iops'] = metrics['io'] / time_delta

            if (metrics['io'] > 0):
                metrics['service_time'] = (
                    metrics['io_milliseconds'] / metrics['io'])
                metrics['await'] = (
                    metrics['reads_milliseconds']
                    + metrics['writes_milliseconds']) / metrics['io']
            else:
                metrics['service_time'] = 0
                metrics['await'] = 0

            # http://www.scribd.com/doc/15013525
            # Page 28
            metrics['concurrent_io'] = (metrics['reads_per_second']
                                        + metrics['writes_per_second']
                                        ) * (metrics['service_time']
                                             / 1000.0)

            # Only publish when we have io figures
            if (metrics['io'] > 0 or self.config['send_zero']): 
                for metkey in metrics:
                    metric_name = None
                    if mapped_name is not None:
                        if self.isVolume:
                            metric_name = '.'.join([mapped_name, mapped_name, metkey]).replace('/', '_') # Fix TISILIO-3463: change ADS iostat metrics key mapping to mapped_name.mapped_name.metrics; avoiding pools mapped to different nbd/md devices after failover, creating duplicated mapped names
                        else:
                            metric_name = '.'.join([info['device'], mapped_name, metkey]).replace('/', '-') # all other nodes: device_name.mapped_name.metrics
                    else:
                        metric_name = '.'.join([info['device'], info['device'], metkey]).replace('/', '_') # if no mapped name for this device, append its name again in metrics key, dot separated
                    # For 'sda', since it's the disk VM resides on, report the metrics with ILIO UUID instead of resource ID
                    disk_dev = 'sda'
                    if os.path.exists('/dev/xvda'):
                        disk_dev = 'xvda'
                    if metkey == 'reads':
                        metrics_name = '.'.join([metric_name, 'reads_per_second']).replace('/', '_')
                        value = long(metrics[metkey]) / float(self.config['interval']) # reports reads per second
                        if disk_dev == 'xvda': metrics_name = metrics_name.replace('xvd', 'sd')
                        #if disk_dev in metric_name:
                        self.publish(metrics_name, value, self.get_metric_path(metrics_name), None, 2, imprecise_metric=True)
                        #else:
                        #    mypath = string.replace(self.get_metric_path(metrics_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                        #    self.publish(metrics_name, value, mypath, None, 2, imprecise_metric=True)
                    if metkey == 'capacity':
                        metrics_name = '.'.join([metric_name]).replace('/', '_')
                        value = long(metrics[metkey])
                        if disk_dev == 'xvda': metrics_name = metrics_name.replace('xvd', 'sd')
                        #if disk_dev in metric_name:
                        self.publish(metrics_name, value, self.get_metric_path(metrics_name), None, 2, imprecise_metric=True)
                        #else:
                        #    mypath = string.replace(self.get_metric_path(metrics_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                        #    self.publish(metrics_name, value, mypath, None, 2, imprecise_metric=True)

                    if metkey == 'writes':
                        metrics_name = '.'.join([metric_name, 'writes_per_second']).replace('/', '_')
                        value = long(metrics[metkey]) / float(self.config['interval']) # reports writes per second
                        if disk_dev == 'xvda': metrics_name = metrics_name.replace('xvd', 'sd')
                        #if disk_dev in metric_name:
                        self.publish(metrics_name, value, self.get_metric_path(metrics_name), None, 2, imprecise_metric=True)
                        #else:
                        #    mypath = string.replace(self.get_metric_path(metrics_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                        #    self.publish(metrics_name, value, mypath, None, 2, imprecise_metric=True)
                    if metkey == 'average_request_size_byte':
                        if disk_dev == 'xvda': metric_name = metric_name.replace('xvd', 'sd')
                        #if disk_dev in metric_name:
                        self.publish(metric_name, metrics[metkey], self.get_metric_path(metric_name), None, 2, imprecise_metric=True)
                        #else:
                        #    mypath = string.replace(self.get_metric_path(metric_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                        #    self.publish(metric_name, metrics[metkey], mypath, None, 2, imprecise_metric=True)
                    if metkey == 'service_time':
                        if disk_dev == 'xvda': metric_name = metric_name.replace('xvd', 'sd')
                        if disk_dev in metric_name:
                            self.publish(metric_name, metrics[metkey], self.get_metric_path(metric_name), None, 2, imprecise_metric=True)
                        else:
                            mypath = self.get_metric_path(metric_name)
                            #mypath = string.replace(self.get_metric_path(metric_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                            self.publish(metric_name, metrics[metkey], mypath, None, 2, imprecise_metric=True)
                            if 'dm-0' in metric_name: # for Hybrid volume, report usxdedup service time
                                myname = string.replace(metric_name, 'dm-0', 'usxdedup', 2)
                                mypath = string.replace(self.get_metric_path(metric_name), 'dm-0', 'usxdedup', 2) # replace metrics path key word for latency dashboard display
                                #mypath = string.replace(mypath, self.ilio_uuid, resource_id, 1) # repalce with the correct resource uuid
                                self.publish(myname, metrics[metkey], mypath, None, 2, imprecise_metric=True)
                            else:
                                if len(serv_time_dict) == 1: # only one capacity or mem device, filters hybrid volume
                                    #self.log.debug(serv_time_dict)
                                    for mk, mv in serv_time_dict.iteritems():
                                        if mk not in metric_name:
                                            for item in mv:
                                                if item in metric_name:
                                                    #self.log.debug("*****%s ***** %s" % (metric_name, metrics[metkey]))
                                                    for li, lv in enumerate(mv):
                                                        if lv == item:
                                                            mv.pop(li)
                                                            mv.insert(li, str(metrics[metkey]))
                                                            break
                                                    break
                                        else: # capacity pool or performance pool
                                            #self.log.debug(mk)
                                            #self.log.debug(metric_name)
                                            mypath = string.replace(self.get_metric_path(metric_name), mk, 'usxdedup', 2)
                                            #mypath = string.replace(mypath, self.ilio_uuid, resource_id, 1)
                                            adjusted_servtime[mk] = mypath
                    if metkey == 'await':
                        if disk_dev == 'xvda': metric_name = metric_name.replace('xvd', 'sd')
                        #if disk_dev in metric_name:
                        self.publish(metric_name, metrics[metkey], self.get_metric_path(metric_name), None, 2, imprecise_metric=True)
                        #else:
                        #    mypath = string.replace(self.get_metric_path(metric_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                        #    self.publish(metric_name, metrics[metkey], mypath, None, 2, imprecise_metric=True)
                    if metkey == 'read_byte_per_second' or metkey == 'write_byte_per_second':
                        if disk_dev == 'xvda': metric_name = metric_name.replace('xvd', 'sd')
                       # if disk_dev in metric_name:
                        self.publish(metric_name, metrics[metkey], self.get_metric_path(metric_name), None, 2, imprecise_metric=True)
                        #else:
                        #    mypath = string.replace(self.get_metric_path(metric_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                        #    self.publish(metric_name, metrics[metkey], mypath, None, 2, imprecise_metric=True)
                    if metkey == 'util_percentage':
                        if disk_dev == 'xvda': metric_name = metric_name.replace('xvd', 'sd')
                        if disk_dev in metric_name:
                            self.publish(metric_name, metrics[metkey] * 100, self.get_metric_path(metric_name), None, 2, imprecise_metric=True) # disk utilization in percentage
                        else: 
                            #mypath = string.replace(self.get_metric_path(metric_name), self.ilio_uuid, resource_id, 1) # replace ILIO UUID with resource ID
                            mypath = self.get_metric_path(metric_name)
                            self.publish(metric_name, metrics[metkey] * 100, mypath, None, 2, imprecise_metric=True)
                            self.log.debug(disk_util_dict)
                            # collect all ibd disk utilitzation belong to this md devices, store them in two dict to be processed at the end
                            for mk, mv in disk_util_dict.iteritems():
                                if mk not in metric_name:
                                    for item in mv:
                                        if item in metric_name:
                                            #self.log.debug("=====%s ===== %s" % (metric_name, metrics[metkey] * 100))
                                            for li, lv in enumerate(mv):
                                                if lv == item:
                                                    mv.pop(li)
                                                    mv.insert(li, str(metrics[metkey] * 100))
                                                    break
                                            break
                                else: # capacitypool or performance pool
                                    adjusted_util[mk] = mypath
                                    #self.log.debug("~~~~~~ %s" % metric_name)
 
        # Sending adjusted utilization
        #self.log.debug(adjusted_servtime)
        #self.log.debug(serv_time_dict)
        #self.log.debug(adjusted_util) 
        #self.log.debug(disk_util_dict)
 
        if adjusted_servtime: # non-Hybrid volume, gather service time average from all ibds
            for rkey, rvalue in serv_time_dict.iteritems():
                rvalue = [float(i) for i in rvalue] # convert string to float
                adjusted_service_time = float(sum(rvalue))/len(rvalue)
                self.publish("service_time", adjusted_service_time, adjusted_servtime[rkey], None, 2, imprecise_metric=True)
 
        for lkey, lvalue in disk_util_dict.iteritems():
            lvalue = [float(i) for i in lvalue] # convert string to float
            adjusted_disk_util_percentage = float(sum(lvalue))/len(lvalue)
            self.publish("util_percentage", adjusted_disk_util_percentage, adjusted_util[lkey], None, 2, imprecise_metric=True)
