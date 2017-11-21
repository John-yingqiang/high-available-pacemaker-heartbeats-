# coding=utf-8

"""
Collect Offload Stats
#### Dependencies
* /proc/net/rpc/nfsd
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

class VolumeIOPSOffloadCollector(diamond.service_collector.ServiceCollector):

    # Global variables
    ilio_uuid = ''
    
    def get_default_config_help(self):
        config_help = super(VolumeIOPSOffloadCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        volume = 'VOLUME'
        atlas_conf = '/etc/ilio/atlas.json'
        global ilio_uuid
        config = super(VolumeIOPSOffloadCollector, self).get_default_config()

        #Check if the machine is a Volume
        isVolume= False
        if os.access(atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a volume
				ilio = jsondata.get("usx")
				ilio_uuid  = ilio.get("uuid")
				role = ilio.get("roles")[0]
            else:
				ilio_uuid = jsondata.get("uuid")
				role = roles[0]
            fp.close()
			
            if re.search(volume,role,re.IGNORECASE):
				isVolume = True
        #Enable reporting if it is a Volume
        if isVolume:
            config.update({
                'enabled':  'True',
            })
        else:
            config.update({
                'enabled':  'False',
            })
        return config

    def get_iops_offload_numbers(self):
        """
        Collect IOPS Offload numbers
        Returns: IOPS Offload numbers
        """
        result = {}
        dev_r_ops = 0
        dev_w_ops = 0
        nfs_r_ops = 0
        nfs_w_ops = 0
        cmd = 'vmstat -d'
        dm_dev = self.get_dedup_devname()
        atlas_conf = '/etc/ilio/atlas.json'
        if (os.access('/proc/net/rpc/nfsd', os.R_OK) and (dm_dev)):
            for x in range(0, 2):
                #TODO use config.ads_type key to check if this is a all memory
                #ADS. In that case, just set total_dev_read/write_ops = 0
                #Get the disk read / write IOs
                for vmstat_data in os.popen(cmd).readlines():
                    if dm_dev in vmstat_data:
                        data = vmstat_data.split()
                        if ( long(data[1]) >= dev_r_ops ):
                            dev_r_ops = long(data[1]) - dev_r_ops
                        else:
                            #TODO Need to build logic to calculate ceiling of dev counter
                            dev_r_ops = long(data[1])
                        if ( long(data[5]) >= dev_w_ops ):
                            dev_w_ops = long(data[5]) - dev_w_ops
                        else:
                            #TODO Need to build logic to calculate ceiling of dev counter
                            dev_w_ops = long(data[5])
                        break;
                #Get the nfs read/write IOs
                fp = open('/proc/net/rpc/nfsd')
                try:
                    for line in fp:
                        if line.startswith('proc3'):
                            data = line.split()
                            if ( long(data[8]) >= nfs_r_ops ):
                                nfs_r_ops = long(data[8]) - nfs_r_ops
                            else:
                                #TODO Need to build logic to calculate ceiling of nfs counter
                                nfs_r_ops = long(data[8])
                            if ( long(data[9]) >= nfs_w_ops ):
                                nfs_w_ops = long(data[9]) - nfs_w_ops
                            else:
                                #TODO Need to build logic to calculate ceiling of nfs counter
                                nfs_w_ops = long(data[9])
                            break;
                except ValueError:
                    continue
                finally:
                    fp.close()
                time.sleep(1)
            result['nfs_r_ops']=nfs_r_ops
            result['nfs_w_ops']=nfs_w_ops
            result['dev_r_ops']=dev_r_ops
            result['dev_w_ops']=dev_w_ops
        else:
            self.log.error('Unable to read /proc/net/rpc/nfsd, OR there is no Dedup FS on this machine.')
            return None
        return result

    def collect(self):
        results = self.get_iops_offload_numbers()
        if not results:
            self.log.error('No offload metrics retrieved')
            return None
        #Adding metrics to dictionary
        metrics = {}
        if(results['nfs_r_ops'] > 0 and (results['nfs_r_ops'] > results['dev_r_ops'])):
            metrics['IOPS_Offload_Read_percent']=100-((float(results['dev_r_ops'])/results['nfs_r_ops'])*100)
            metrics['IOPS_Offload_Read']=results['nfs_r_ops']-results['dev_r_ops']
        else:
            metrics['IOPS_Offload_Read_percent']=0
            metrics['IOPS_Offload_Read']=0
    
        if(results['nfs_w_ops'] > 0 and (results['nfs_w_ops'] > results['dev_w_ops'])):
            metrics['IOPS_Offload_Write_percent']=100-((float(results['dev_w_ops'])/results['nfs_w_ops'])*100)
            metrics['IOPS_Offload_Write']=results['nfs_w_ops']-results['dev_w_ops']
        else:
            metrics['IOPS_Offload_Write_percent']=0
            metrics['IOPS_Offload_Write']=0
        metrics["DEV_Read_Ops"] = results["dev_r_ops"]
        metrics["DEV_Write_Ops"] = results["dev_w_ops"]
        metrics["NFS_Read_Ops"] = results["nfs_r_ops"]
        metrics["NFS_Write_Ops"] = results["nfs_w_ops"]
        resource_id = self.get_resource_id()
        for key in metrics:
            mypath = string.replace(self.get_metric_path(key), ilio_uuid, resource_id, 1) # replace ILIO uuid with resource id
            self.publish(key, metrics[key], mypath)
