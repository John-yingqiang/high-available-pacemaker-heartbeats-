# coding=utf-8

"""
Collect capacity offload for ADS Volume, publish stats to data grid
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

class ADSCapOffloadGridCollector(diamond.service_collector.ServiceCollector):
    amcurl_str = ''
    vm_uuid = ''

    def get_default_config_help(self):
        config_help = super(ADSCapOffloadGridCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help
    
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        global amcurl_str
        volume = 'VOLUME'
        atlas_conf = '/etc/ilio/atlas.json'
        config = super(ADSCapOffloadGridCollector, self).get_default_config()
        amcurl_str = 'http://localhost:8080/usxmanager'
        #Check if the machine is a volume
        isVolume= False
        if os.access(atlas_conf, os.R_OK):
            fp = open(atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a virtual volume
                ilio = jsondata.get("usx")
                role = ilio.get("roles")[0]
                self.vm_uuid=ilio.get("uuid")
            else:
                role = roles[0]
                self.vm_uuid=jsondata.get("uuid")
            fp.close()
			
            if role is not None:
                if re.search(volume,role,re.IGNORECASE):
                    isVolume = True
					
        #Enable reporting if it is a Volume
        if isVolume:
            rc = self.init_capacity_prop_file()
            config.update({
                'enabled':  'True',
            })
        else:
            config.update({
                'enabled':  'False',
            })
        return config
    
    def init_capacity_prop_file(self):
        """
        Write offload capacity into a persistent file
        """
        fp = open('/opt/amc/agent/offloadcapacity.prop', 'w+')
        wline = 'init 0' + '\n'
        fp.write(wline)
        fp.close()

    def prep_stats_to_send(self, stats):
        """
        Set a flag to send to grid when there is a change in stats; return True or False to determine whether to send the data
        """
        send_data = False
        statsFile = {}
        
        if os.access('/opt/amc/agent/offloadcapacity.prop', os.R_OK): 
            fp = open('/opt/amc/agent/offloadcapacity.prop', 'r')
            for line in fp:
                fline = line.split()
                statsFile[fline[0]] = long(fline[1])
            fp.close()
            if 'init' in statsFile: # check for 'init 0' in prop file. in this case. no previous metrics has written to prop file. always send metrics to grid
                if statsFile['init'] == 0:
                    send_data = True
            else: # metrics data from previous interval exists, check for changes
                for key in set(stats) & set(statsFile):
                    if stats[key] != statsFile[key]:
                        self.log.debug("******************** Offload Cap Diff ********************")
                        self.log.debug(stats)
                        self.log.debug(statsFile)
                        send_data = True
                        break
            if send_data:
                fp = open('/opt/amc/agent/offloadcapacity.prop', 'w+')
                for key, value in stats.iteritems():
                    wline = key + ' ' + str(value) + '\n'
                    fp.write(wline)
                fp.close()
        else: # persistent file does not exist, write the current stats to the file
            fp = open('/opt/amc/agent/offloadcapacity.prop', 'w+')
            for key, value in stats.iteritems():
                wline = key + ' ' + str(value) + '\n'
                fp.write(wline)
            fp.close()
        return send_data

    def get_capacity_stat(self):
        """
        Process capacity stat info to publish; return metrics stats
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

    def get_volume_resource_uuid(self):
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('volumeresources') and len(jsondata['volumeresources']) > 0:
                return jsondata['volumeresources'][0]['uuid']
        return ""

    def publish_to_grid(self, amcurl_str, metrics):
        """
        Publishes the metrics data to the grid; Return True if publish to grid successfully
        """
        out = ['']
        json_str = ''

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %I:%M:%S%p')

        amcpost_str = '/usx/inventory/volume/resources/metrics' # API url suffix
        apiurl_str = amcurl_str + amcpost_str # actual API url
        volres_uuid = self.get_volume_resource_uuid()

        json_str += '{\\"usxmetrics\\":[{\\"uuid\\":\\"%s\\",\\"metrics\\":{\\"virtualcapacity\\":{\\"name\\":\\"virtualcapacity\\",\\"value\\":%d,\\"timestamp\\":\\"%s\\",\\"unit\\":\\"bytes\\"},\\"dedupratio\\":{\\"name\\":\\"dedupratio\\",\\"value\\":%.2f,\\"timestamp\\":\\"%s\\",\\"unit\\":\\"bytes\\"}}}]}' % (volres_uuid, metrics['apparentusedspace'], timestamp, metrics['Capacity_Offload_Percent'], timestamp)
        cmd = r'curl -s -k -X PUT -H "Content-Type:application/json" -d "%s" %s' % (json_str, apiurl_str) # actual curl command to send a JSON formatted body
        #self.log.debug(metrics)

        rc = do_system(cmd, out, log=False)
        if rc != 0: # curl system call failed, return error
            do_system("rm -rf /opt/amc/agent/offloadcapacity.prop", log=False)
            return False

        if out[0] == 'true': # API call return success
            return True
        else:
            do_system("rm -rf /opt/amc/agent/offloadcapacity.prop", log=False)
            return False

    def collect(self):
        """
        Collect stats
        """
        results = self.get_capacity_stat()
        if not results:
            self.log.error('No capacity offload metrics retrieved. Will not publish capacity offload numbers for this ADS')
            return None
        
        if self.prep_stats_to_send(results):
            #Adding metrics to dictionary
            #metrics = {}
            if results['apparentusedspace']==0 or (results['apparentusedspace'] < results['actualusedspace']):
                results['Capacity_Offload_Percent']=0
                results['Capacity_Offload']=0
            else:
                results['Capacity_Offload_Percent']=((float(results['apparentusedspace']) - float(results['actualusedspace']))/float(results['apparentusedspace'])*100)
                results['Capacity_Offload']=(results['apparentusedspace']-results['actualusedspace']) * 1024
                
            #metrics.update(results)
            rc = self.publish_to_grid(amcurl_str, results)
