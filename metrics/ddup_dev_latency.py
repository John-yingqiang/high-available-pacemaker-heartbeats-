# coding=utf-8

"""
Collect Offload Stats
#### Dependencies
* /proc/diskstats
"""

import diamond.service_collector
import time
import os
import re
import json
import string
import sys
sys.path.insert(0,'/opt/milio/libs/atlas')
from atl_constants import *

class IOLatencyCollector(diamond.service_collector.ServiceCollector):
    
    # Global variables
    ilio_uuid = ''
    
    def get_default_config_help(self):
        config_help = super(IOLatencyCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help
    
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        atlas_conf = '/etc/ilio/atlas.json'
        volume = 'VOLUME'
        global ilio_uuid
        config = super(IOLatencyCollector, self).get_default_config()
        
        #Check if the machine is an ADS Volume
        isVolume= False
        if os.access(atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a virtual volume
				ilio = jsondata.get("usx")
				ilio_uuid  = ilio.get("uuid")
				role = ilio.get("roles")[0]
            else:
				ilio_uuid = jsondata.get("uuid")
				role = roles[0]
            fp.close()
			
            if re.search(volume,role,re.IGNORECASE):
				isVolume = True
        #Enable reporting if it is an Virtual Volume
        if isVolume:
            propfile = "/opt/amc/agent/diskio.prop"
            dev = self.get_dedup_devname()
            if not os.access(propfile, os.R_OK):
                prop = open(propfile,"w")
                if not dev:
                    prop.write("0 0 failover_dev 0 0 0 0 0 0 0 0 0 0 0")
                else:
                    prop.write("0 0 " + dev + " 0 0 0 0 0 0 0 0 0 0 0")
                prop.close()
            config.update({
                'enabled':  'True',
            })
        else:
            self.log.info("Not an ADS Volume. Disabling the collector.")
            config.update({
                'enabled':  'False',
            })
        return config

    def get_io_latency(self,dev_name):
        """
        Gets IO Latency for the dedup device
        """
        diskstats = '/proc/diskstats'
        propfile = "/opt/amc/agent/diskio.prop"
        cmd = 'cat ' + propfile
        data_dict = {}
        if os.access(diskstats, os.R_OK) and os.access(propfile, os.F_OK):
            fp2 = open(diskstats)
            try:
                olddiskstats = os.popen(cmd).read() 
                if not olddiskstats:
                    self.log.error("Not able to read /opt/amc/agent/diskio.prop")
                    return data_dict
                for newdiskstats in fp2:
                    newdiskdata = newdiskstats.split()
                    if dev_name == newdiskdata[2]:
                        fp1 = open(propfile,"w+")
                        olddiskdata = olddiskstats.split()
                        cur_reads_comp = long(newdiskdata[3])
                        cur_reads_merg = long(newdiskdata[4])
                        old_reads_comp = long(olddiskdata[3])
                        old_reads_merg = long(olddiskdata[4])
                        data_dict["num_new_reads"]=(cur_reads_comp-cur_reads_merg)-(old_reads_comp-old_reads_merg)
                        if data_dict["num_new_reads"] < 0:
                            data_dict["num_new_reads"]=(cur_reads_comp-cur_reads_merg)
                        cur_writes_comp = long(newdiskdata[7])
                        cur_writes_merg = long(newdiskdata[8])
                        old_writes_comp = long(olddiskdata[7])
                        old_writes_merg = long(olddiskdata[8])
                        data_dict["num_new_writes"]=(cur_writes_comp-cur_writes_merg)-(old_writes_comp-old_writes_merg)
                        if data_dict["num_new_writes"] < 0:
                            data_dict["num_new_writes"]=(cur_writes_comp-cur_writes_merg)
                        cur_read_time = long(newdiskdata[6])
                        old_read_time = long(olddiskdata[6])
                        data_dict["read_time"]= cur_read_time-old_read_time
                        if data_dict["read_time"] < 0:
                            data_dict["read_time"]= cur_read_time
                        cur_write_time = long(newdiskdata[10])
                        old_write_time = long(olddiskdata[10])
                        data_dict["write_time"]= cur_write_time-old_write_time
                        if data_dict["write_time"] < 0:
                            data_dict["write_time"]= cur_write_time
                        fp1.write(newdiskstats)
                        fp1.close()
                        break
            except ValueError:
                pass
            finally:
                fp2.close()
        return data_dict
 
    def collect(self):
        """
        Collect stats
        """
        stats = {}
        metrics = {}
        dev_name = self.get_dedup_devname()
        if not dev_name: # device name is empty, no dedup FS mounted; It is an HA ADS 
            self.log.debug("This is an HA ADS node, and is not hosting Dedup FS right now. Latency data will not be published.")
            return
        stats = self.get_io_latency(dev_name)
        if not stats:
            self.log.error("Failed reading Disk IO Latency stats")
            return

        if int(self.config["interval"]) > 0 and stats["num_new_reads"] > 0:
            metrics["dedup_dev_new_reads_per_sec"] = (stats["num_new_reads"] / int(self.config["interval"]))
        else:
            self.log.info("Poll Interval for IOLatencyCollector is set to 0. "
                           + "It should be set to a value > 0. "
                           + "Change the value at /etc/diamond/collectors/IOLatencyCollector.conf, "
                           + "untill then new_reads_per_sec will be reported 0.")
            metrics["dedup_dev_new_reads_per_sec"] = long("0")

        if int(self.config["interval"]) > 0 and stats["num_new_writes"] > 0:
            metrics["dedup_dev_new_writes_per_sec"] = (stats["num_new_writes"] / int(self.config["interval"]))
        else:
            self.log.info("Poll Interval for IOLatencyCollector is set to 0. "
                           + "It should be set to a value > 0. "
                           + "Change the value at /etc/diamond/collectors/IOLatencyCollector.conf, "
                           + "untill then new_writes_per_sec will be reported 0.")
            metrics["dedup_dev_new_writes_per_sec"] = long("0")

        if stats["num_new_reads"] > 0:
            metrics["dedup_dev_read_latency_ms"] = (stats["read_time"] / stats["num_new_reads"])
        else:
            metrics["dedup_dev_read_latency_ms"] = long("0")

        if stats["num_new_writes"] > 0:
            metrics["dedup_dev_write_latency_ms"] = (stats["write_time"] / stats["num_new_writes"])
        else:
            metrics["dedup_dev_write_latency_ms"] = long("0")

        resource_id = self.get_resource_id() 
        for key in metrics:
            mypath = string.replace(self.get_metric_path(key), ilio_uuid, resource_id, 1) # replace ILIO uuid with resource id
            self.publish(key, metrics[key], mypath)
