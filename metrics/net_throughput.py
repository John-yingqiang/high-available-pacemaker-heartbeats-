# coding=utf-8
"""
Collect network related statuses
#### Dependencies
* /proc/net/dev
"""

import diamond.collector
import time
import os, sys
import re
import json

class NetworkThroughputCollector(diamond.collector.Collector):
    
    proc_stat = '/proc/net/dev'
    net_prop = '/opt/amc/agent/networkstatus.prop'
	
	# Configuration file locations
    atlas_conf = '/etc/ilio/atlas.json'
    
    # Dictionary for ILIO configuration parameters
    ilio_dict = {}
    
    def get_default_config_help(self):
        config_help = super(NetworkThroughputCollector, self).get_default_config_help()
        config_help.update({})
        return config_help
    
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        if os.access(self.atlas_conf, os.R_OK): # get ilio uuid from atlas_json
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a volume
                ilio = jsondata.get("usx")
                self.ilio_dict['role'] = ilio.get("roles")[0]
                self.ilio_dict['uuid'] = ilio.get("uuid")
                self.ilio_dict['nics'] = ilio.get("nics")
                nic1 = self.ilio_dict['nics'][0]
            else:
                self.ilio_dict['role'] = jsondata.get("roles")[0]
                self.ilio_dict['uuid'] = jsondata.get("uuid")
            fp.close()
		
        config = super(NetworkThroughputCollector, self).get_default_config()
        config.update({
            'enabled' : 'True',
        })
        rc = self.record_net_stats()
        return config
    
    def record_net_stats(self):
        """
        Record the current net stats in a persistent file
        """
        if os.access(self.proc_stat, os.R_OK):
            fp = open(self.net_prop, 'w+')
            cmd = 'cat ' + self.proc_stat
            for iface in os.popen(cmd).readlines()[2:]:
                if 'eth' in iface:
                    if 'role' in self.ilio_dict and self.ilio_dict['role'] == 'VOLUME':
                        for nic in self.ilio_dict['nics']:
                            devicename = nic.get("devicename")
                            ifacename = iface.split(':')[0].strip() # get the device name from iface
                            if nic.get("devicename") == ifacename and nic.get("storagenetwork") == True:
                                fp.write(iface) # write to persistent file
                    else:
                        fp.write(iface) # write to persistent file
            fp.close()
            return True
        else:
            self.log.error("Cannot access system network status file: " + self.proc_stat)
            return False
    
    def build_stats_dict(self, ifstat):
        """
        Build a dict with {interface:{metric_key:data}} format
        """
        stat_dict = AutoVivification() # initiate a nested dict 
        if ifstat: # status data exists
            info = ifstat.split()
            stat_dict[info[0][:-1]]['receive']['bytes'] = info[1]
            #stat_dict[info[0][:-1]]['receive']['packets'] = info[2]
            #stat_dict[info[0][:-1]]['receive']['errs'] = info[3]
            stat_dict[info[0][:-1]]['receive']['drop'] = info[4]
            #stat_dict[info[0][:-1]]['receive']['fifo'] = info[5]
            #stat_dict[info[0][:-1]]['receive']['frame'] = info[6]
            #stat_dict[info[0][:-1]]['receive']['compressed'] = info[7]
            #stat_dict[info[0][:-1]]['receive']['multicast'] = info[8]
            stat_dict[info[0][:-1]]['transmit']['bytes'] = info[9]
            #stat_dict[info[0][:-1]]['transmit']['packets'] = info[10]
            #stat_dict[info[0][:-1]]['transmit']['errs'] = info[11]
            stat_dict[info[0][:-1]]['transmit']['drop'] = info[12]
            #stat_dict[info[0][:-1]]['transmit']['fifo'] = info[13]
            #stat_dict[info[0][:-1]]['transmit']['colls'] = info[14]
            #stat_dict[info[0][:-1]]['transmit']['carrier'] = info[15]
            #stat_dict[info[0][:-1]]['transmit']['compressed'] = info[16]
                        
        return stat_dict
            
    def calculate_net_stats(self):
        """
        Get network related status from system
        """
        stats= {}
        net_stat_old = {}
        net_stat_current = {}
        rx_throughput = 0
        tx_throughput = 0
        rx_drop = 0
        tx_drop = 0
        if os.access(self.net_prop, os.R_OK):
            fp = open(self.net_prop, 'r')
            for line in fp:
                net_stat_old.update(self.build_stats_dict(line)) # build a dict with previous interval net_statsf
            fp.close()
        else:
            self.log.error("networkstatus.prop not found")
            self.record_net_stats()
        
        if os.access(self.proc_stat, os.R_OK):
            fp = open(self.net_prop, 'w+')
            cmd = 'cat ' + self.proc_stat
            for iface in os.popen(cmd).readlines()[2:]:
                if 'eth' in iface:
                    if 'role' in self.ilio_dict and self.ilio_dict['role'] == 'VOLUME':
                        for nic in self.ilio_dict['nics']:
					        devicename = nic.get("devicename")
					        ifacename = iface.split(':')[0].strip() # get the device name from iface
					        if nic.get("devicename") == ifacename and nic.get("storagenetwork") == True:
					            net_stat_current.update(self.build_stats_dict(iface)) # build a dict with current interval net_stats
					            fp.write(iface) # write to persistent file
                    else:
                        net_stat_current.update(self.build_stats_dict(iface)) # build a dict with current interval net_stats
                        fp.write(iface) # write to persistent file
            fp.close()            
        else:
            self.log.error("Cannot access system network status file: " + self.proc_stat)
        
        if net_stat_old: # prop file for previous interval stats exists
            for key, value in net_stat_old.iteritems():
                if key in net_stat_current.keys():
                    # need implement check for /proc/net/dev stats rollover
                    metric_name = key + '.rx_bytes'
                    rx_throughput = (long(net_stat_current[key]['receive']['bytes']) - long(value['receive']['bytes'])) / long(self.config['interval'])
                    stats[metric_name] = rx_throughput
                    metric_name = key + '.tx_bytes'
                    tx_throughput = (long(net_stat_current[key]['transmit']['bytes']) - long(value['transmit']['bytes'])) / long(self.config['interval'])
                    stats[metric_name] = tx_throughput
                    metric_name = key + '.rx_drop'
                    rx_drop = (long(net_stat_current[key]['receive']['drop']) - long(value['receive']['drop'])) / long(self.config['interval'])
                    stats[metric_name] = rx_drop
                    metric_name = key + '.tx_drop'
                    tx_drop = (long(net_stat_current[key]['transmit']['drop']) - long(value['transmit']['drop'])) / long(self.config['interval'])
                    stats[metric_name] = tx_drop
        else: # the networkstatus.prop file is missing
            self.record_net_stats()        
        return stats            
                    
    def collect(self):
        """
        Collect stats
        """
        stats = self.calculate_net_stats()
        if not stats:
            self.log.error("No network status metrics retrieved. Will not publish network throughput and packets dropped data")
            return None
        for key, value in stats.iteritems():
            self.publish(key, value, imprecise_metric=True)

class AutoVivification(dict):
    """
    Implementation of perl's autovivification feature
    """
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value
