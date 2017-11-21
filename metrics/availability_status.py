# coding=utf-8
"""
Collect Availability statuses for HA service, IBD service and AMC Agent service
### Dependencies
## HA service
/usr/sbin/corosync
/usr/sbin/pacemakerd

## NBD service
/usr/local/bin/nbd-server
/usr/local/sbin/nbd-client

## Agent service
/usr/bin/java
/opt/amc/agent/lib/amc-agent.jar

## IBD service
ibdmanager -r s (IBD server status)
ibdmanager -r a (IBD client status)

# Modified April 15, 2014: Fix TISILIO-2969: for capacity pool node, check "vgs" instead of "nbd-client" (corresponding "Storage Network" status in UI)
                                             if vgs returns size info, check if ibds.conf exists:
                                                 if ibds.conf exists, check ibds process
# Modified August 8, 2014: Fix TISILIO-3905: changes for availability status for USX 2.0. collecting IBD server/client status 
                                             using ibdmanager utility.
                                             Change USX roles, remove "pool" role
# Modified August 14, 2014: Fix TISILIO-4811: for IBD status, check if it is an HA standby node first; if so, ignore IBD status check
                                              this is to fix the issue that in HA standby node ibd process occassionally starts and stops
# Modified August 22, 2014: Fix TISILIO-5424: For All flash volume using only shared storage, don't check ibd client status

# Modified September 9, 2014: Fix TISILIO-5578: Skip reporting IBD status for VDI volumes since no ibd client is running on VDI volumes 

# Modified September 17, 2014: Fix TISILIO-5730: This is a regression introduced by 5578 fix, it checks volume type in atlas json for VDI volumes and
                                                 skip reporting; but in an HA standby node volume resources is not presented, causing an exception.
                                                 This is fixed by checking crm_mon to make sure the volume is not in HA cluster and then check volume type
                                                 (per current design, VDI volume does not support HA)
# Modified September 19, 2014: Fix TISILIO-5806: Altas JSON format change causes condition to determine ALL_FLASH /ALL_FLASH with sharedstorage only throwing
                                                 python exceptions. 
                                                 The correct check is: check whether raidbricks object is empty to determine whether IBD agent is up or down, 
                                                 regardless what volume type is. (hybrid and memory can also use shared storage only configuration, using SSD
                                                 as memory
# Modified September 25, 2014: Fix TISILIO-5920/5578: condition check for simple volume was checked by "elif" instead of "if", should perform this condition 
                                                      check regardless whatever previous condition check result
# Modified October 9, 2014: Availability status script is only to report HA status now
"""

import diamond.collector
import time
import datetime
import os, sys
import re
import json
import subprocess
import socket
from calendar import calendar

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_constants import *

class ILIOAvailabilityStatusCollector(diamond.collector.Collector):
    
    # Global variables for file location
    atlas_conf = '/etc/ilio/atlas.json'
    nbd_server_conf = '/etc/ilio/ibdserver.conf'
    status_prop = '/opt/amc/agent/availabilitystatus.prop'
    
    # Status dictionary
    status_dict = {0:'OK',1:'WARN',2:'CRITICAL',3:'FATAL',4:'UNKNOWN'}
    
    # Status name
    HA_STATUS = 'HA_STATUS'
    IBD_STATUS = 'IBD_STATUS'
    AGENT_STATUS = 'AGENT_STATUS'
    
    # resource count
    NUM_RESOURCE = 4
    
    # ilio dictionary
    ilio_dict = {}
    
    # vdi types
    VDI_DISKBACKED = 'SIMPLE_HYBRID'
    VDI_FLASH = 'SIMPLE_FLASH'
    VDI_DISKLESS = 'SIMPLE_MEMORY'
    
    # Log file
    LOG_FILENAME = '/var/log/diamond/diamond.log'
	
    api_url = 'http://localhost:8080/usxmanager'
    
    def get_default_config_help(self):
        config_help = super(ILIOAvailabilityStatusCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help
    
    def get_default_config(self):
        """
        Returns default configuration settings
        """
        config = super(ILIOAvailabilityStatusCollector, self).get_default_config()
        # Initialize persistent file for checking status change
        self.init_stats_prop_file()
        # Initialize a global dict for ilio related information
        self.init_global_variables()
        config.update({
            'enabled' : 'False',
        })
        return config
    
    """
    Helper methods for prepare initialize status, and get ILIO related information
    """
    def init_stats_prop_file(self):
        """
        Initialize the availability status persistent file 
        """
        if self.ilio_dict: # make sure global ilio dict is initialized
            fp = open(self.status_prop, 'w+')
            wline = 'init 0' + '\n'
            fp.write(wline)
            fp.close()

    def init_global_variables(self):
        """
        Generate ilio info dictionary from atlas.json
        """
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('usx'): # this is a volume
                self.ilio_dict['role'] = jsondata['usx']['roles'][0]
                self.ilio_dict['uuid'] = jsondata['usx']['uuid']
                self.ilio_dict['amcurl'] = jsondata['usx']['usxmanagerurl']
                self.ilio_dict['resources'] = jsondata['volumeresources']
            else: # this is a service vm
                self.ilio_dict['role'] = jsondata['roles'][0]
                self.ilio_dict['uuid'] = jsondata['uuid']
                self.ilio_dict['amcurl'] = jsondata['usxmanagerurl']
            self.ilio_dict['amcurl'] = get_master_amc_api_url()
        else:
            self.log.error("Error: cannot open " + self.atlas_conf + " for read") 
            
    def ilio_type_check(self):
        """
        Returns the type of ILIO: aggregate, pool or ads
        """
        volume = 'VOLUME'
        service_vm = 'SERVICE_VM'

        # check ILIO type
        iliotype = ''
        if self.ilio_dict: 
            role = self.ilio_dict['role']
            if role is not None:
                if re.search(service_vm,role,re.IGNORECASE):
                    iliotype = 'service_vm'
                elif re.search(volume,role,re.IGNORECASE):
                    iliotype = 'volume'

        return iliotype
    
    def get_param_from_conf(self, para_name):
        """
        Returns parameter value from /etc/ilio/atlas.json, given the parameter key
        """
        result = ''
        if os.access(self.atlas_conf, os.R_OK):
            fp=open(self.atlas_conf)
            jsondata = json.load(fp)
            result = jsondata.get(para_name) # get the value of the key in JSON
            fp.close()
        return result
    
    def build_json_str(self, stats, uuid):
        """
        Construct JSON string for ILIO availability status
        """
        #utc_tuple = time.gmtime(None) # get current time in UTC time tuple, None arg implies current time is used
        #timestamp = time.strftime('%Y-%m-%d %I:%M:%S%p', utc_tuple) # convert back to timestamp format with UTC time; Fix TISILIO-2309

        epoch_time = int(time.time())
        #self.log.debug('================ epoch time:%s' % epoch_time) 
        
        status_content = ''
        for key, value in stats.iteritems():
            if value != -1: # value = -1 means this status is not enabled for reporting
                #status_content += '\\"' + key + '\\":{\\"name\\":\\"' + key + '\\",\\"value\\":\\"' + self.status_dict[value] + '\\",\\"timestamp\\":\\"' + epoch_time + '\\"},'
                status_content += '{\\"name\\":\\"' + key + '\\",\\"value\\":\\"' + self.status_dict[value] + '\\"},'
        content = status_content.rstrip(',') # remove the trailing comma
        result = ('{\\"usxstatuslist\\":[' + content + '], \\"usxuuid\\":\\"' + self.ilio_dict['uuid'] + 
                  '\\", \\"usxtype\\":\\"VOLUME_CONTAINER\\", \\"usxcontaineruuid\\":\\"' + self.ilio_dict['uuid'] + '\\"}') # final JSON string
        return result
    
    def publish_to_grid(self, stats):
        """
        Publishes the availability status data to the grid
        """
        ilio_type = self.ilio_type_check()
        ilio_uuid = self.ilio_dict['uuid']

        out = ['']
        json_str = ''
        restapi_url = self.api_url
        
        result = False
        restapi_url += '/usx/status/update'
#         if ilio_type.lower() == 'volume':
#             restapi_url += '/usx/inventory/volume/containers/' + ilio_uuid + '/availabilitystatus' # concatenate amc_url with the REST API Path 
#         elif ilio_type.lower() == 'service_vm':
#             restapi_url += '/usx/inventory/servicevm/containers/' + ilio_uuid + '/availabilitystatus'
        
        json_str = self.build_json_str(stats, ilio_uuid)
        cmd = r'curl -s -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, restapi_url)
        
        retry_count = 2
        while retry_count > 0:
            rc = do_system(cmd, out, log=False) # remove False to turn on log for debugging
            if out[0] == 'true': # API call returns successfully, break retry loop
                result = True
                break
            retry_count -= 1
        
        return result
                 
    """
    Availability statuses collection methods
    """
    def get_ha_status(self):
        """
        Invoke commands to check HA service status
        """
        ilio_type = self.ilio_type_check()
        ilio_uuid = self.ilio_dict['uuid']
        retVal = {}
        #cmd1 = 'ps -ef | grep corosync | grep -v grep'
        #cmd2 = 'ps -ef | grep pacemakerd | grep -v grep'
        
        failed_start_resource = False
        resource_count = 0
        
        if ilio_type.lower() == 'service_vm': # no HA related service running on service vm; if HA not configured, don't report ha_status
            pass # noop
        elif ilio_type.lower() == 'volume':
            if os.access(self.atlas_conf, os.R_OK): # check the atlas.json; enable HA will overwrite it.
                fp = open(self.atlas_conf)
                jsondata = json.load(fp)
                fp.close()
                if jsondata.has_key('usx'): # this is a volume
                    ha_enabled = jsondata['usx']['ha']
                    if ha_enabled: # check ha enabled, set by enable HA process/pacemaker config
                        cmd = 'crm_mon -1 > /dev/null; echo $?' # check to make sure the cluster is up
                        result = os.popen(cmd).read()
                        if long(result.rstrip()) == 0: # crm_mon -1 succeeded, HA is enabled
                            # check to see if node is online and not in maintenance mode
                            cmd1 = 'crm_mon -1'                            
                            for line in os.popen(cmd1).readlines():
                                self.log.debug("@@@ line: %s" % line)
                                if 'maintenance' in line:
                                    self.log.debug(line.split(' '))
                                    if line.split(' ')[1] in socket.gethostname(): # node is in maintenance mode, HA is disabled
                                        retVal[self.HA_STATUS] = long(4) # status: UNKNOWN; UI will interpret it as disabled HA
                                        #retVal.clear()
                                        return retVal
                                if 'Stopped' in line:
                                    retVal[self.HA_STATUS] = long(3)
                                    return retVal
                                if 'FAILED' in line:
                                    myhostname = line.split('FAILED') # if one of the resource is failed on the node, HA status is fatal
                                    if myhostname[-1].strip() in socket.gethostname():
                                        retVal[self.HA_STATUS] = long(3)
                                        return retVal
                                if 'Failed actions' in line:
                                    failed_start_resource = True
                                #self.log.debug(line.split(' '))
                                if failed_start_resource and 'on' in line.split(' '): # check if resource failed to start on this node
                                    if line.split(' ')[6] in socket.gethostname(): # node name after "on", indicating resource failed to start on it
                                        retVal[self.HA_STATUS] = long(3)
                                        return retVal
                                    
                            cmd2 = 'crm resource status' # check resource status to see if enable HA has completed successfully
                            for line in os.popen(cmd2).readlines():
                                # check if all resources are started 
                                if '_ds' in line and 'Started' in line and ilio_uuid in line:
                                    resource_count += 1
                                    #self.log.debug(resource_count)
                                if '_atl_dedup' in line and 'Started' in line and ilio_uuid in line:
                                    resource_count += 1
                                    #self.log.debug(resource_count)
                                if ('_atl_nfs' in line or '_atl_iscsi' in line) and 'Started' in line and ilio_uuid in line:
                                    resource_count += 1
                                    #self.log.debug(resource_count)
                                if '_ip' in line and 'Started' in line and ilio_uuid in line:
                                    resource_count += 1
                                    #self.log.debug(resource_count)
                            if resource_count == self.NUM_RESOURCE:
                                #self.log.debug("All resources have started.")
                                retVal[self.HA_STATUS] = long(0)
                            else:
                                #self.log.debug("num of resource is less than defined; but no failure detected, assuming is still in process of enabling HA.")
                                retVal[self.HA_STATUS] = long(1) 
                            #self.log.debug(retVal)
                        else: # ha is true in atlas.json but HA cluster is not formed
                            self.log.debug("crm_mon not running yet, still in the process of enable HA")
                            retVal[self.HA_STATUS] = long(1) # status WARN; it may be still in the process of enable HA
                    else: # ha is disabled
                        retVal[self.HA_STATUS] = long(4) # status: UNKNOWN; UI will interpret it as disabled HA
            else:
                self.log.error("ERROR: get_ha_status: Cannot read atlas.json")
        return retVal
        
    def get_agent_status(self):        
        """
        Invoke commands to check AMC agent service status, applies to all types of ILIO
        """
        ilio_uuid = self.ilio_dict['uuid']
        retVal = {}
        cmd = 'ps -ef | grep amc-agent | grep -v grep'
        result = os.popen(cmd).read()
        if not result:
            self.log.info("AMC agent service is not running on " + ilio_uuid)
            retVal['agent_status'] = long(3)
        else:
            retVal['agent_status'] = long(0)
        return retVal
    
    def get_nbd_status(self):
        """
        Invoke commands to check ibd server/cient status for service vm and volume
         For volume:
           if it is an HA node (check if volumeresource is empty). Don't send ibd status at all. 
           This is to manage the occasional ibd process start and then stop on HA node, causing
           collector to send an IBD fail status and not overwritten because IBD stop will not
           send IBD status
           
           else. Check the ibd client status for the volume
            
        """
        ilio_type = self.ilio_type_check()
        ilio_uuid = self.ilio_dict['uuid']
        
        retVal = {}
        cmd_server = 'ibdmanager -r s > /dev/null; echo $?' # return 0 : server OK; return non-zero : server down
        cmd_client = 'ibdmanager -r a > /dev/null; echo $?' # return 0 : agent OK; return non-zero : agent down 
        cmd_vgs = 'vgs' # TISILIO-2969, check vgs instead of ibd-client, for shared storage only capacity pool does not have ibd-client running
        if ilio_type.lower() == 'service_vm':
            result_server = os.popen(cmd_server).read()
            if long(result_server.rstrip()) != 0:
                self.log.info("ibdserver is CRITICAL on " + ilio_uuid)
                retVal['nbd_status'] = long(3)
            else:
                retVal['nbd_status'] = long(0)
        elif ilio_type.lower() == 'volume': 
            vol_resources = self.ilio_dict['resources']
            # check wheter the volume is an HA standby node
            #  Don't use Atlas JSON alone to determine HA standby node; use crm_mon to determine whether this node is standby nor not
            cmd = 'crm_mon -1 > /dev/null; echo $?'
            result = os.popen(cmd).read()
            if long(result.rstrip()) != 0: # crm_mon -1 failed, HA not enabled or join HA cluster failed
                if vol_resources:
                    if vol_resources[0].has_key('raidplans'):
                        AMC_raidplan = vol_resources[0]['raidplans']
                        for plan in AMC_raidplan:
                            if plan.has_key('raidbricks'):
                                if not plan['raidbricks']: # Fix TISILIO-5806: raidbricks is empty; no svm exports, no IBD agent process
                                    return retVal
                    if vol_resources[0]['volumetype'].upper() in [self.VDI_DISKBACKED, self.VDI_FLASH, self.VDI_DISKLESS]: # Fix TISILIO-5578, VDI volume does not have IBD
                        return retVal
                        
                    result_client = os.popen(cmd_client).read()
                    if long(result_client.rstrip()) != 0:
                        self.log.info("ibdclient is CRITICAL on " + ilio_uuid)
                        retVal['nbd_status'] = long(3)
                    else:
                        retVal['nbd_status'] = long(0)
            else: # HA enabled
                cmd2 = 'crm_mon -1'
                for info in os.popen(cmd2).readlines():
                    if '_dedup' in info: # check one resource out of 4 to see if it is started on this volume
                        myhostname = info.split('Started') # sample line: vCenter1201_knAF51014_atl_dedup    (ocf::heartbeat:dedup-filesystem):    Started knAF51014 
                        if myhostname[-1].strip() == socket.gethostname(): # resource has started on this node; IT IS NOT A STANDBY; report IBD client status
                            result_client = os.popen(cmd_client).read()
                            if long(result_client.rstrip()) != 0:
                                self.log.info("ibdclient is CRITICAL on " + ilio_uuid)
                                retVal['nbd_status'] = long(3)
                            else:
                                retVal['nbd_status'] = long(0)
                        elif myhostname[-1].strip().split()[0] == socket.gethostname() and myhostname[-1].strip().split()[-1] == '(unmanaged)': # HA is disabled
                            result_client = os.popen(cmd_client).read()
                            if long(result_client.rstrip()) != 0:
                                self.log.info("ibdclient is CRITICAL on " + ilio_uuid)
                                retVal['nbd_status'] = long(3)
                            else:
                                retVal['nbd_status'] = long(0)

        else:
            self.log.error("ERROR: Unknown role type: " + ilio_type)        

        return retVal
        
    def prep_availability_status(self):
        """
        Store all status in a dictionary
        """
        ha_status = self.get_ha_status()
        #agent_status = self.get_agent_status()
        #nbd_status = self.get_nbd_status()
        
        statuses = {}
        statuses = ha_status
        #statuses = agent_status
        #statuses.update(nbd_status)
        #statuses.update(ha_status)

        return statuses

    """
    Collect method: default collect method; send on change method publishes when there is a status change
    """
    
    def send_on_change(self, stats):
        """
        Detect whether a change has occurred in the availability status 
        """
        publish_ready = False
        stats_from_file = {}
        if os.access(self.status_prop, os.R_OK):        
            fp = open(self.status_prop, 'r')
            for line in fp:
                fline = line.split()
                stats_from_file[fline[0]] = long(fline[1]) # convert status indicator from str to long
            fp.close()
            if 'init' in stats_from_file: # check for 'init 0' in prop file, in this case, no previous metrics has written to prop file, always send the metrics to gird 
                if stats_from_file['init'] == 0:
                    self.log.debug("************* Initial availstat change *************")
                    publish_ready = True
            elif len(stats) != len(stats_from_file): # persistent file size changed, additional info written to the file, set send to true
                self.log.debug("************* Additional availstatu found *************")
                publish_ready = True
            else:
                for key in set(stats) & set(stats_from_file):
                    if long(stats[key]) != long(stats_from_file[key]):
                        self.log.debug("************* availstat has changed *************")
                        self.log.debug(stats)
                        self.log.debug(stats_from_file)
                        publish_ready = True
                        break # will set publish to true as long as one status has changed
            if publish_ready:
                fp = open(self.status_prop, 'w+') # overwrite the persistent file with new statuses
                for key, value in stats.iteritems():
                    wline = key + ' ' + str(value) + '\n' # convert status indicator from long to str
                    fp.write(wline)
                fp.close()
        else: # availability_status.prop file not found, write current health status to the prop file
            fp = open(self.status_prop, 'w+') # overwrite the persistent file with new statuses
            for key, value in stats.iteritems():
                wline = key + ' ' + str(value) + '\n' # convert status indicator from long to str
                fp.write(wline)
            fp.close()
            
        return publish_ready
            
    def collect(self):
        """
        Collect stats
        """        
        ilio_type = self.ilio_type_check()
        if ilio_type.lower() == 'volume': # Modified Oct. 9, 2014: Availability status script is only to report HA status now
            stats = self.prep_availability_status()
            if not stats:
                self.log.error(os.path.basename(__file__) + " error: No USX availability status retrieved!")
                return None
            ready_send = self.send_on_change(stats)
            if ready_send:
                # publish to graphite
                for key, value in stats.iteritems():
                    status_key = 'availabilitystatus.' + key # corresponding to the JSON format in design spec
                    self.publish(status_key, value)
                # publish to grid    
                rc = self.publish_to_grid(stats)
