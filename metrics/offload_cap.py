# coding=utf-8

"""
Collect capacity offload for ADS Volume
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
from atl_util import *

class VolumeCapOffloadCollector(diamond.service_collector.ServiceCollector):

    # Global variables
    ilio_uuid = ''
        
    def get_default_config_help(self):
        config_help = super(VolumeCapOffloadCollector, self).get_default_config_help()
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
        config = super(VolumeCapOffloadCollector, self).get_default_config()

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
		
		
    def get_capacity_stat(self):
        """
        Process capacity stat info to publish
        """
        ret = {}
        actualusedspace = 0
        apparentusedspace = 0
        #Getting the actual used space value
        cmd = 'df -T | grep -i -E \'%s\'' % EXPORT_REG_EXPRESSION
        result = os.popen(cmd).read()
        if result == '':
            self.log.info("This machine does not seem to be an ADS VM")
            return ret
        data = result.split()
        actualusedspace =  long(data[3])
        #Getting the apparent used space value
        cmd2 = 'du -s -B 1000 --apparent '+ data[6]
        result2 = os.popen(cmd2).read()
        if result2 == '':
            self.log.info("This machine does not seem to be an ADS VM")
            return ret
        else:
            apparentusedspace = long(result2.split()[0])
            ret['actualusedspace']=actualusedspace
            ret['apparentusedspace']=apparentusedspace
        cmd = 'mount | grep \'%s\'' % ZPOOL_REG_EXPRESSION
        result = os.popen(cmd).read()
        if result and ZPOOL_REG_EXPRESSION in result:
            cmd = '%s | grep -i -E \'%s\'' % (CMD_ZPOOL_LIST, ZPOOL)
            result = os.popen(cmd).read()
            if result == '' and len(ret.keys()) == 0:
                self.log.info('This machine does not seem to be an ILIO VM')
                return ret
            data = result.split()
            self.log.info('Disk capacity get from zpool:%s' % data)
            expression = re.compile(r'[a-zA-Z]')
            used = long(disksize_convert(expression.sub('',data[2]), get_disksize_measurement_type(data[2]), DISK_SIZE_KB))
            ret['actualusedspace'] = used
            # Used (form df -h EXT4 export) will be used for apparentusedspace on zpool framework
            ret['apparentusedspace'] = actualusedspace
            self.log.info('Disk capacity get from zpool:%s' % ret)
        return ret

    def collect(self):
        results = self.get_capacity_stat()
        if not results:
            self.log.error('No capacity offload metrics retrieved. Will not publish capacity offload numbers for this ADS')
            return None
        for key in results:
            self.log.debug("RV : " + key + " , " + str(results[key]))
        #Adding metrics to dictionary
        
        # 09-17-2014: Added utilization, it is inverted of dedup ratio, if dedup ratio is not zero:
        #  ex. Capacity_Offload_Percent = 90
        #      utilization = 10
        #   
        #      Capacity_Offload_Percent = 0
        #      utilization = 0
        #
        # This is for alert reporting purpose; server checks utilization threshold of 40%, 60%, 80%, etc.
        # corresponding to cap offload percent: 60%, 40%, 20%
        
        metrics = {}
        metrics['Apparent_Used_Space']=(results['apparentusedspace']) * 1024
        metrics['Actual_Used_Space']=(results['actualusedspace']) * 1024
        if results['apparentusedspace']==0 or (results['apparentusedspace'] < results['actualusedspace']):
            metrics['Capacity_Offload_Percent']=0
            metrics['Capacity_Offload']=0
            metrics['utilization'] = 0
        else:
            metrics['Capacity_Offload_Percent']=((float(results['apparentusedspace']) - float(results['actualusedspace']))/float(results['apparentusedspace'])*100)
            metrics['Capacity_Offload']=(results['apparentusedspace']-results['actualusedspace']) * 1024
            metrics['utilization'] = (float(results['actualusedspace'])/results['apparentusedspace'])*100
            
        resource_id = self.get_resource_id()
        for key in metrics:
            mypath = string.replace(self.get_metric_path(key), ilio_uuid, resource_id, 1) # replace ILIO uuid with resource id
            self.publish(key, metrics[key], mypath)
