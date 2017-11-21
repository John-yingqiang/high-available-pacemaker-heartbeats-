# coding=utf-8

"""
Collect Reachable status
Using diamond framework to send reachable status update to grid; act as a heartbeat mechanism to send a timestamp update to grid every 60 seconds
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

class ReachableStatusCollector(diamond.collector.Collector):
    
    # Configuration file locations
    atlas_conf = '/etc/ilio/atlas.json'
    
    # Status dictionary
    status_dict = {0:'OK',1:'WARNING',2:'CRITICAL',3:'FATAL'}
    
    # Dictionary for ILIO configuration parameters
    ilio_dict = {}
    
    # Log file
    LOG_FILENAME = '/var/log/diamond/diamond.log'
    
    def get_default_config(self):
        config_help = super(ReachableStatusCollector, self).get_defualt_config_help()
        config_help.update({
        })
        return config_help
    
    def get_default_config(self):
        """
        Returns default configuration settings
        """
        config = super(ReachableStatusCollector, self).get_default_config()
        config.update({
            'enabled' : 'False',
        })
        # Initialize a global dict for ilio related information
        self.init_global_variables()
        return config

    def init_global_variables(self):
        """
        Generate ilio info dictionary from atlas.json, store in global variable ilio_dict
        """
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            roles = jsondata.get("roles")
            if roles is None:  #if its a volume
                ilio = jsondata.get("usx")
                self.ilio_dict['role'] = ilio.get("roles")[0]
                self.ilio_dict['uuid'] = ilio.get("uuid")
            else:
                self.ilio_dict['role'] = jsondata.get("roles")[0]
                self.ilio_dict['uuid'] = jsondata.get("uuid")
                myrole = roles[0]
            fp.close()
        else:
            self.log.error("Error: cannot open " + self.atlas_conf + " for read") 
            
    def vm_type_check(self):
        """
        Returns the type of ILIO: volume or servicevm
        """
        volume = 'VOLUME'
        service_vm = 'SERVICE_VM'

        # check ILIO type
        vmtype = ''
        if self.ilio_dict: 
            role = self.ilio_dict['role']
            if role is not None:
                if re.search(service_vm,role,re.IGNORECASE):
                    vmtype = 'service_vm'
                elif re.search(volume,role,re.IGNORECASE):
                    vmtype = 'volume'
        return vmtype
    
    def build_json_str(self, uuid):
        """
        Construct JSON string for ILIO reachable status
        """        
        time_tuple = time.localtime(None) # Fix TISILIO-3410: get current time in local time zone, None implies current time is used
        timestamp = time.strftime('%Y-%m-%d %I:%M:%S%p', time_tuple)
        
        status_content = '\\"reachable_status\\":{\\"name\\":\\"reachable_status\\",\\"status\\":\\"' + self.status_dict[0] + '\\",\\"timestamp\\":\\"' + timestamp + '\\"}'
        result = '{\\"usxavailabilitystatus\\":[{\\"uuid\\":\\"' + uuid + '\\",\\"availabilitystatus\\":{' + status_content + '}}]}' # final JSON string
        return result
 
    def publish_to_grid(self):
        """
        Publishes the reachable status data to the grid
        """
        if self.ilio_dict: # only send when global variables are initialized
            vm_type = self.vm_type_check()
            vm_uuid = self.ilio_dict['uuid']
                
            out = ['']
            json_str = ''
            restapi_url = 'http://127.0.0.1:8080/usxmanager' # using amc agent API to update grid
            result = False
            if vm_type.lower() == 'service_vm':
                restapi_url += '/usx/inventory/servicevm/' + vm_uuid + '/availabilitystatus' # concatenate amc_url with the REST API Path
            elif vm_type.lower() == 'volume':
                restapi_url += '/usx/inventory/volume/' + vm_uuid + '/availabilitystatus'
            else: # vm_type is empty or unknown ilio type, don't call curl command and return False
                return result
            json_str = self.build_json_str(vm_uuid)
            cmd = r'curl -s -k -X PUT -H "Content-Type:application/json" -d "%s" %s' % (json_str, restapi_url)
            rc = do_system(cmd, out, False)
            if out[0] == 'true': # API call returns successfully, break retry loop
                result = True

            return result
    
    def collect(self):
        """
        Collect stats
        """
        rc = self.publish_to_grid()
         
