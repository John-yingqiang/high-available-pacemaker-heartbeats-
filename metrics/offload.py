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
class OffloadCollector(diamond.service_collector.ServiceCollector):

    # Global variables
    vm_uuid = ''
    
    def get_default_config_help(self):
        config_help = super(OffloadCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        volume = 'VOLUME'
        atlas_conf = '/etc/ilio/atlas.json'
        global vm_uuid
        config = super(OffloadCollector, self).get_default_config()

        #Check if the machine is an ADS Volume
        isVolume= False
        if os.access(atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a virtual volume
				ilio = jsondata.get("usx")
				vm_uuid  = ilio.get("uuid")
				role = ilio.get("roles")[0]
            else:
				vm_uuid = jsondata.get("uuid")
				role = roles[0]
            fp.close()
			
            if re.search(volume,role,re.IGNORECASE):
				isVolume = True
        #Enable reporting if it is an Volume
        if isVolume:
            config.update({
                'enabled':  'True',
            })
        else:
            config.update({
                'enabled':  'False',
            })
        return config

    def get_byte_offload_numbers(self):
        """
        Collect Byte Offload numbers
        Returns: Byte Offload numbers
        """
        result = {}
        dev_read_sectors=0
        dev_write_sectors=0
        nfs_r_bytes = 0
        nfs_w_bytes = 0
        cmd = 'vmstat -d'
        dm_dev = self.get_dedup_devname()
        #Read /proc/net/rpc/nfsd to get nfs read / write data
        if os.access('/proc/net/rpc/nfsd', os.R_OK) and dm_dev :
            for x in range(0, 2):
                #TODO: Read the /etc/ilio/atlas.json file to check if the ADS Volume is
                #all memory.In that case, device stats will be zero.
                #Get the disk read / write bytes                
                for vmstat_data in os.popen(cmd).readlines():
                    if dm_dev in vmstat_data:
                        data = vmstat_data.split()
                        if long(data[3]) >= dev_read_sectors:
                            dev_read_sectors = long(data[3]) - dev_read_sectors
                        else:
                            #TODO: rollover adjustment
                            dev_read_sectors = long(data[3])
                        if long(data[7]) >= dev_write_sectors:
                            dev_write_sectors = long(data[7]) - dev_write_sectors
                        else:
                            #TODO: rollover adjustment
                            dev_write_sectors = long(data[7])
                        break
                #Get nfs read / write per second
                fp = open('/proc/net/rpc/nfsd')
                try:
                    for line in fp:
                        if line.startswith('io'):
                            data = line.split()
                            if long(data[1]) >= nfs_r_bytes:
                                nfs_r_bytes = long(data[1]) - nfs_r_bytes
                            else:
                                #TODO: rollover adjustment
                                nfs_r_bytes = long(data[1])
                            if long(data[2]) >= nfs_w_bytes:
                                nfs_w_bytes = long(data[2]) - nfs_w_bytes
                            else:
                                #TODO: rollover adjustment
                                nfs_w_bytes = long(data[2])
                            break
                except ValueError:
                    continue
                finally:
                    fp.close()
                time.sleep(1)
            result['avg_nfs_r_bytes_mb'] = nfs_r_bytes
            result['avg_nfs_w_bytes_mb'] = nfs_w_bytes
            result['dev_read_mb'] = dev_read_sectors * 512
            result['dev_write_mb'] = dev_write_sectors * 512
        else:
            self.log.error("Unable to read /proc/net/rpc/nfsd, OR there is no Dedup FS on this machine")
            return None
        return result

    def collect(self):
        results = self.get_byte_offload_numbers()
        if not results:
            self.log.error('No offload metrics retrieved')
            return None
        #Adding metrics to dictionary
        self.log.debug(results)
        metrics = {}
        if(results['avg_nfs_r_bytes_mb'] > 0 and (results['avg_nfs_r_bytes_mb'] > results['dev_read_mb'])):
            metrics['Byte_Offload_Read_percent']=100-((float(results['dev_read_mb'])/results['avg_nfs_r_bytes_mb'])*100)
            metrics['Byte_Offload_Read']=results['avg_nfs_r_bytes_mb']-results['dev_read_mb']
        else:
            metrics['Byte_Offload_Read_percent']=0
            metrics['Byte_Offload_Read']=0
        if(results['avg_nfs_w_bytes_mb'] > 0 and (results['avg_nfs_w_bytes_mb'] > results['dev_write_mb'])):
            metrics['Byte_Offload_Write_percent']=100-((float(results['dev_write_mb'])/results['avg_nfs_w_bytes_mb'])*100)
            metrics['Byte_Offload_Write']=results['avg_nfs_w_bytes_mb']-results['dev_write_mb']
        else:
            metrics['Byte_Offload_Write_percent']=0
            metrics['Byte_Offload_Write']=0
            
        resource_id = self.get_resource_id()
        for key in metrics:
            mypath = string.replace(self.get_metric_path(key), vm_uuid, resource_id, 1) # replace ILIO uuid with resource id
            self.publish(key, metrics[key], mypath)
