# coding=utf-8

"""
Collect capacity stats for ADS Volume
### Dependencies
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
from atl_util import get_disksize_measurement_type, disksize_convert, check_simple_memory

class VolumeCapacityCollector(diamond.service_collector.ServiceCollector):
    
    # Global variables
    ilio_uuid = ''
    
    def get_default_config_help(self):
        config_help = super(VolumeCapacityCollector, self).get_default_config_help()
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
        config = super(VolumeCapacityCollector, self).get_default_config()

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
            config.update({
                'byte_unit':  'byte',
                'enabled':  'True',
            })
        else:
            config.update({
                'enabled':  'False',
            })
        return config
    
    def get_capacity_stat(self):
        """
        Returns the ADS volume capacity stats
        Returns the ADS metadata stats, calculated as:
            5% of used capacity = current metadata size
            5% of full file system size = maximum metadata size
        """
        ret = {}
        info = []
        cmd = 'df -T | grep -i -E \'%s\'' % EXPORT_REG_EXPRESSION
        result = os.popen(cmd).read()
        if result == '':
            self.log.info('This machine does not seem to be an ILIO VM')
            return ret

        data = result.split()
        ret['Capacity_Free'] = long(data[4]) * 1024
        ret['Capacity_Used'] = long(data[3]) * 1024
        ret['Capacity_Total'] = long(data[2]) * 1024
        self.log.info('Disk capacity get from df:%s' % ret)
        #following code is used for calculate capacity of zfs if volume has zfs
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
            free = disksize_convert(expression.sub('',data[3]), get_disksize_measurement_type(data[3]), DISK_SIZE_BYTE)
            used = disksize_convert(expression.sub('',data[2]), get_disksize_measurement_type(data[2]), DISK_SIZE_BYTE)
            total = disksize_convert(expression.sub('',data[1]), get_disksize_measurement_type(data[1]), DISK_SIZE_BYTE)
            if free and used and total:
                #ret['Capacity_Free'] = long(free)
                ret['Capacity_Used'] = long(used)
                ret['Capacity_Total'] = long(total)
                ret['Capacity_Free'] = long(total) - long(used)
            self.log.info('Disk capacity get from zpool:%s' % ret)
        ret['utilization'] = ret['Capacity_Used'] / float(ret['Capacity_Total']) * 100
        ret['Metadata.Current_Size'] = ret['Capacity_Used'] * 0.05 # naive method to calculate metadata size
        ret['Metadata.Maximum_Size'] = ret['Capacity_Total'] * 0.05
        return ret 

    def get_compression_ratio(self):
        simple_memory = check_simple_memory()
        if simple_memory:
            ret = {}
            if os.path.exists("/sys/block/zram0/compr_data_size") \
            and os.path.exists("/sys/block/zram0/orig_data_size"):
                compr_data_size = os.popen("cat /sys/block/zram0/compr_data_size").read()
                orig_data_size = os.popen("cat /sys/block/zram0/orig_data_size").read()
                self.log.info("compr_data_size %s" % compr_data_size )
                self.log.info("orig_data_size %s" % orig_data_size)
                if compr_data_size and orig_data_size:
                    compr_data_size = float(compr_data_size)
                    orig_data_size = float(orig_data_size)
                    if orig_data_size > 0:
                        ret["Compression_Ratio"] = (compr_data_size / orig_data_size ) * 100
                    else:
                        self.log.info('Original data size should not be zero. Will not publish compression ratio for current volume')
                return ret
            else:
                self.log.error('Cannot find compression data size file. Will not publish compression ratio for current volume')
        else:
            self.log.info('Not a Simple Memory Volume. Will not publish compression ratio for current volume')


    def collect(self):
        """
        Collect stats
        """
        stats = self.get_capacity_stat()
        if not stats:
            self.log.error('No ADS Volume capacity metrics retrieved. Will not publish capacity info for this ADS')
            return None
        resource_id = self.get_resource_id()
        for key, value in stats.iteritems():
            mypath = string.replace(self.get_metric_path(key), ilio_uuid, resource_id, 1) # replace ILIO uuid with resource id
            self.publish(key, value, mypath)

        compression_ratio = self.get_compression_ratio()
        if compression_ratio:
            mypath = string.replace(self.get_metric_path(compression_ratio.keys()[0]), ilio_uuid, resource_id, 1) # replace ILIO uuid with resource id
            self.publish(compression_ratio.keys()[0], compression_ratio.values()[0], mypath)

