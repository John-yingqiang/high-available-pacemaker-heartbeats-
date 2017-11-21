# coding=utf-8

"""
Collect capacity stats for ADS Volume, publish stats to data grid
"""

import diamond.collector
import time
import datetime
import os, sys
import re
import json
import subprocess

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_constants import *

class ADSCapacityGridCollector(diamond.collector.Collector):

    ilio_uuid = ''
	
    url= 'http://localhost:8080/usxmanager'
    atlas_conf = '/etc/ilio/atlas.json'
    # if count == MAX_COUNT, force to send data
    MAX_COUNT = 5
    count = 0

    def get_default_config_help(self):
        config_help = super(ADSCapacityGridCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help
    
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        volume = 'VOLUME'
        config = super(ADSCapacityGridCollector, self).get_default_config()
        #Check if the machine is an ADS volume
        isVolume= False
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a virtual volume
                ilio = jsondata.get("usx")
                role = ilio.get("roles")[0]
                self.ilio_uuid=ilio.get("uuid")
            else:
                role = roles[0]
                self.ilio_uuid=jsondata.get("uuid")
            fp.close()
			
            if role is not None:
                if re.search(volume,role,re.IGNORECASE):
                    isVolume = True
        #Enable reporting if it is an ADS Volume
        if isVolume:
            rc = self.init_capacity_prop_file()
            config.update({
                'byte_unit': 'byte',
                'enabled':  'True',
            })
        else:
            config.update({
                'enabled':  'False', # bypass ADS test, change to False after debug
            })
        return config

    def init_capacity_prop_file(self):
        """
        Write capacity stats into a persistent file
        fp = open('/opt/amc/agent/adscapacity.prop', 'w+')
        wline = 'init 0' + '\n'
        fp.write(wline)
        fp.close()
        """ 
 
    def prep_stats_to_send(self, stats):
        """
        Set a flag to send to grid when there is a change in stats
        """
        send_data = False
        statsFile = {}
        if os.access('/opt/amc/agent/adscapacity.prop', os.R_OK):
            fp = open('/opt/amc/agent/adscapacity.prop', 'r')
            for line in fp:
                fline = line.split()
                statsFile[fline[0]] = long(fline[1])
            fp.close()
            for key in set(stats):
                if key not in statsFile or stats[key] != statsFile[key]:
                    self.log.debug("************** ADS Cap Diff ***************")
                    self.log.debug(stats)
                    self.log.debug(statsFile)
                    send_data = True
                    break
            if send_data:
                fp = open('/opt/amc/agent/adscapacity.prop', 'w+')
                for key, value in stats.iteritems():
                    wline = key + ' ' + str(value) + '\n'
                    fp.write(wline)
                fp.close()
        else: # persistent file does not exist, write current stats to the file 
            fp = open('/opt/amc/agent/adscapacity.prop', 'w+')
            for key, value in stats.iteritems():
                wline = key + ' ' + str(value) + '\n'
                fp.write(wline)
            fp.close()
            send_data = True
        return send_data
    
    def get_capacity_stat(self):
        """
        Process capacity stat info to publish
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
        return ret 

    def get_volume_resource_uuid(self):
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('volumeresources') and len(jsondata['volumeresources']) > 0:
                return jsondata['volumeresources'][0]['uuid']
        return ""

    def publish_to_grid(self, url, stats):
        """
        Publishes the metrics data to grid
        """
        out = ['']
        json_str = ''
        


        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %I:%M:%S%p')

        amcpost_str = '/usx/inventory/volume/resources/metrics' # API url suffix
        apiurl_str = url + amcpost_str # actual API url
        volres_uuid = self.get_volume_resource_uuid()

        json_str += '{\\"usxmetrics\\":[{\\"uuid\\":\\"%s\\",\\"metrics\\":{\\"totalcapacity\\":{\\"name\\":\\"totalcapacity\\",\\"value\\":%d,\\"timestamp\\":\\"%s\\",\\"unit\\":\\"bytes\\"},\\"availablecapacity\\":{\\"name\\":\\"availablecapacity\\",\\"value\\":%d,\\"timestamp\\":\\"%s\\",\\"unit\\":\\"bytes\\"},\\"usedcapacity\\":{\\"name\\":\\"usedcapacity\\",\\"value\\":%d,\\"timestamp\\":\\"%s\\",\\"unit\\":\\"bytes\\"}}}]}' % (volres_uuid, stats['Capacity_Total'], timestamp, stats['Capacity_Free'], timestamp, stats['Capacity_Used'], timestamp) # fix TISILIO-2532, send usedcapacity to grid to be displayed in "Overview" chart
        cmd = r'curl -s -k -X PUT -H "Content-Type:application/json" -d "%s" %s' % (json_str, apiurl_str) # actual curl command to send a JSON formatted body
        rc = do_system(cmd, out, log=False)
        if rc != 0: # curl system call failed, return error
            do_system("rm -rf /opt/amc/agent/adscapacity.prop", log=False)
            return False

        #self.log.debug(stats)
        if out[0] == 'true': # API call return success
            return True
        else:
            do_system("rm -rf /opt/amc/agent/adscapacity.prop", log=False)
            return False 

    def send_now(self):
        if self.count == self.MAX_COUNT:
            self.count = 0
            return True
        return False

    def collect(self):
        """
        Collect stats
        """
        self.count = self.count + 1
        stats = self.get_capacity_stat()
        if not stats:
            self.log.error('No ADS Volume capacity metrics retrieved. Will not publish capacity info for this ADS')
            return None
        else:
            send_data = self.prep_stats_to_send(stats)
            if self.send_now() or send_data:
                rc = self.publish_to_grid(self.url, stats)
