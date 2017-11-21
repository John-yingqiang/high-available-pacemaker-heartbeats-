# coding=utf-8
"""
Check status (read/write or read only) of filesystem and dedup file system and send an alert if the status change

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

DEDUP_VOLUME_EXTEND_LOCKFILE = "/etc/ilio/dedup_volume_extend_lockfile"

class FileSystemStatusCollector(diamond.service_collector.ServiceCollector):
    
    # Global variables for file location
    atlas_conf = '/etc/ilio/atlas.json'
    dedupfs_read_status_file = '/opt/amc/agent/dedupfs_read_status.prop'
    dedupfs_availability_status_file = '/opt/amc/agent/dedupfs_availability_status.prop'
    volume_export_availability_status_file = '/opt/amc/agent/volume_export_availability_status.prop'


    # Status dictionary
    status_dict = {0:'OK',1:'WARN',2:'CRITICAL',3:'FATAL',4:'UNKNOWN'}
    read_write_status= 'RW'
    read_only_status= 'R'
    DEDUP_FILESYSTEM_STATUS='DEDUP_FILESYSTEM_STATUS'
    VOLUME_EXPORT_AVAILABILITY='VOLUME_EXPORT_AVAILABILITY'
    unmounted_status='UNMOUNTED'
    mounted_status='MOUNTED'
    no_status='NO'
    alert_error = 'ERROR'
    alert_warning = 'WARN'
    alert_ok = 'OK'
	
    # Alerts Message
    dedup_mounted_description = 'Deduplication filesystem is available'
    dedup_unmounted_description = 'Deduplication filesystem is unavailable'
    dedup_unmounted_extend_description = 'Extend volume: Dedup fs will be temporarily unmounted and then remounted during the extend volume operation'
    dedup_read_only_description = 'Deduplication filesystem is in read only mode'
    dedup_read_write_description = 'Deduplication filesystem is in read write mode'
	
	# Target name
    read_write_target_name = 'FileSystemStatusCollector.read_write_status'
    availability_target_name = 'FileSystemStatusCollector.mount_status'

	# check id
    dedup_read_checkid = 'dedup-read-status'
    dedup_availability_checkid = 'dedup-read-availability'
    
    # ilio dictionary
    ilio_dict = {}
    
    # Log file
    LOG_FILENAME = '/var/log/diamond/diamond.log'

    api_url = 'http://localhost:8080/usxmanager'
    amcpost_url = api_url+'/alerts' # API url suffix	

    isVolume = False
    '''
    * Every 20 seconds self.collect() is called.
    * Every 3 minutes we want to update current status regardlessly
    '''
    STATUS_UPDATE_NOW = 9
    status_update_counter = 0

    def get_default_config_help(self):
        config_help = super(FileSystemStatusCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help
    
    def get_default_config(self):
        """
        Returns default configuration settings
        """
        config = super(FileSystemStatusCollector, self).get_default_config()
 
        self.status_update_counter = 0
        self.init_stats_prop_file()

		# Initialize a global dict for ilio related information
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('usx'): # this is a volume
                self.ilio_dict['role'] = jsondata['usx']['roles'][0]
                self.ilio_dict['uuid'] = jsondata['usx']['uuid']
                self.ilio_dict['displayname'] = jsondata['usx']['displayname']
                self.ilio_dict['amcurl'] = jsondata['usx']['usxmanagerurl']
		self.ilio_dict['serviceip'] = ""
		self.ilio_dict['volumeresourceuuid'] = ""
		if len(jsondata['volumeresources']) > 0:
		    if 'serviceip' in jsondata['volumeresources'][0]:
		        self.ilio_dict['serviceip'] = jsondata['volumeresources'][0]['serviceip']
		    self.ilio_dict['volumeresourceuuid'] = jsondata['volumeresources'][0]['uuid']
                self.isVolume = True
            else: # this is a service vm
                self.ilio_dict['role'] = jsondata['roles'][0]
                self.ilio_dict['uuid'] = jsondata['uuid']
                self.ilio_dict['displayname'] = jsondata['uuid']
                self.ilio_dict['amcurl'] = jsondata['usxmanagerurl']            
        else:
            self.log.error("Error: cannot open " + self.atlas_conf + " for read")
        self.ilio_dict['amcurl'] = get_master_amc_api_url()
        #Enable reporting if it is an Virtual Volume
        if self.isVolume:
            config.update({
                'enabled':  'True',
            })
        else:
            config.update({
                'enabled':  'False',
            })
        return config 

    def init_stats_prop_file(self):
        '''
        Initialize the availability status persistent file 
        '''
        export_status = self.get_previous_volume_export_status()
        dedupfs_status = self.previous_mount_dedup_fs_status()
        read_status = self.previous_dedup_fs_status()
        with open(self.volume_export_availability_status_file, 'w') as fd:
            fd.write(export_status)
            #fd.write('UNKNOWN')
        with open(self.dedupfs_availability_status_file, 'w') as fd:
            fd.write(dedupfs_status)
            #fd.write(self.unmounted_status)
        with open(self.dedupfs_read_status_file, 'w') as fd:
            if os.access(self.dedupfs_read_status_file, os.R_OK):
                fd.write(read_status)
            else:
                fd.write('UNKNOWN')

    def get_volume_resource_uuid(self):
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('volumeresources') and len(jsondata['volumeresources']) > 0:
                return jsondata['volumeresources'][0]['uuid']
        return ""

    def previous_dedup_fs_status(self):
        """
        Return the previous status of the dedup fs "R" or "RW" stored in a file 
        """
        publish_ready = False
        stats_from_file = {}
        if os.access(self.dedupfs_read_status_file, os.R_OK):        
            fp = open(self.dedupfs_read_status_file, 'r')
            for line in fp:
                if line.strip() == self.read_only_status:
                    return self.read_only_status
                elif line.strip() == self.read_write_status:
                    return self.read_write_status
                else:
                    return self.no_status
            fp.close()
            return self.no_status
        else:
            return self.no_status

    def previous_mount_dedup_fs_status(self):
        """
        Return the previous mount status of the  dedup fs "MOUNTED" or "UNMOUNTED"
        """
	try:
	    with open(self.dedupfs_availability_status_file, 'r') as fd:
                if fd.read().strip() != self.unmounted_status:
                    return self.mounted_status
                return self.unmounted_status
	except:
            return self.unmounted_status

    def current_mount_dedup_fs_status(self):
        """
        Return the current mount status of the  dedup fs "MOUNTED" or "UNMOUNTED"
        """
        cmd = 'mount | grep -i -E \'%s\'' % STORAGE_REG_EXPRESSION
        result = os.popen(cmd).read()
        if len(result.strip()) > 0:
            return self.mounted_status
        return self.unmounted_status

    def get_dedup_fs_status(self):
        """
        Return the status of the dedup file system "R" or "RW" 
        """

        device_name = self.get_dedup_devname()
        if not device_name:
            return self.unmounted_status

        cmd = 'mount | grep -i -E \'%s\' | grep rw' % STORAGE_REG_EXPRESSION
        result = os.popen(cmd).read()
        if result.strip() != '':
            return self.read_write_status
        else:
            return self.read_only_status

    def get_service_ip_status(self):
        if os.access(self.atlas_conf, os.R_OK):
            fp = open(self.atlas_conf)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('volumeresources') and len(jsondata['volumeresources']) > 0:
                if 'serviceip' in jsondata['volumeresources'][0]:
                    serviceip = jsondata['volumeresources'][0]['serviceip']
                    cmd = 'ip addr show | grep "scope global secondary" | grep -v grep'
                    result = os.popen(cmd).read().strip()
                    if serviceip + '/' not in result:
                        return 'ERROR'
            return 'OK'
	return 'ERROR'

    def get_previous_volume_export_status(self):
	try:
	    with open(self.volume_export_availability_status_file, 'r') as fd:
               return fd.read().strip()
	except:
	    return 'UNKNOWN'

    def get_volume_export_status(self):
        cmd = 'service nfs-kernel-server status'
        result = os.popen(cmd).read().strip()
        if 'nfsd running' in result:
	    with open(self.volume_export_availability_status_file, 'w') as fd:
	        fd.write('NFS')
	    return 'NFS'
        cmd = 'service scst status'
        result = os.popen(cmd).read().strip()
        if 'SCST status: OK' in result:
	    with open(self.volume_export_availability_status_file, 'w') as fd:
	        fd.write('iSCSI')
	    return 'iSCSI'
	with open(self.volume_export_availability_status_file, 'w') as fd:
	    fd.write('FATAL')
        return 'FATAL'

    def write_dedup_read_status(self, status):
        """
        Write the dedup fs read status in the file /opt/amc/agent/dedupfs_read_status.prop
        """
        fp = open(self.dedupfs_read_status_file, 'w+')
        fp.write(status)
        fp.close()
		
    def write_dedup_availability_status(self, status):
        """
        Write the dedup fs availability status in the file /opt/amc/agent/dedupfs_availability_status.prop
        """
        fp = open(self.dedupfs_availability_status_file, 'w+')
        fp.write(status)
        fp.close()

    def build_json_str(self, stats, uuid):
        """
        Construct JSON string for ILIO availability status
        """

        epoch_time = int(time.time())
        #self.log.debug('================ epoch time:%s' % epoch_time) 
        
        status_content = ''
        for key, value in stats.iteritems():
            if value != -1: # value = -1 means this status is not enabled for reporting
                status_content += '{\\"name\\":\\"' + key + '\\",\\"value\\":\\"' + self.status_dict[value] + '\\"},'
        content = status_content.rstrip(',') # remove the trailing comma
        volumeresourceuuid = self.get_volume_resource_uuid()
        self.log.debug('volume resource uuid: %s' % volumeresourceuuid) 
        result = ('{\\"usxstatuslist\\":[' + content + '], \\"usxuuid\\":\\"' + volumeresourceuuid + '\\", \\"usxtype\\":\\"VOLUME_RESOURCE\\", \\"usxcontaineruuid\\":\\"' + self.ilio_dict['uuid'] + '\\"}') # final JSON string
        return result
    
    def publish_availability_status_to_grid(self, stats):
        """
        Publishes the availability status data to the grid
        """
        ilio_uuid = self.ilio_dict['uuid']

        out = ['']
        json_str = ''
        restapi_url = self.api_url
        
        result = False
        restapi_url += '/usx/status/update'
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

    def send_dedup_fs_status(self, current):
	stats = {}
	if current == self.mounted_status:
	    stats[self.DEDUP_FILESYSTEM_STATUS] = long(0)
	elif current == self.unmounted_status:
	    stats[self.DEDUP_FILESYSTEM_STATUS] = long(3)
	else:
	    stats[self.DEDUP_FILESYSTEM_STATUS] = long(4)
        # self.publish_availability_status_to_grid(stats)

    def send_volume_export_status(self, current):
	stats = {}
	if current == 'NFS' or current == 'iSCSI':
	    stats[self.VOLUME_EXPORT_AVAILABILITY] = long(0)
	elif current == 'FATAL':
	    stats[self.VOLUME_EXPORT_AVAILABILITY] = long(3)
	else:
	    stats[self.VOLUME_EXPORT_AVAILABILITY] = long(4)
        # self.publish_availability_status_to_grid(stats)
	
    def send_availability_status_alert(self, current):
        """
        Send availability alert 
        """	
        targetname = self.availability_target_name
        target = "servers."+self.ilio_dict['uuid']+"."+self.availability_target_name
        if current == self.mounted_status:
            description = self.dedup_mounted_description
            value = '1'
            oldStatus = self.alert_error
            status = self.alert_ok
        elif  current == self.unmounted_status:
            description = self.dedup_unmounted_description
            value = '0'
            oldStatus = self.alert_ok
            status = self.alert_error
            # Fix for 24163. We won't share error info during extending volume.
            fd = node_trylock(DEDUP_VOLUME_EXTEND_LOCKFILE)
            if fd == None:
                description = self.dedup_unmounted_extend_description
                status = self.alert_warning
            else:
                node_unlock(fd)
        self.publish_to_grid(description,targetname, value, target, oldStatus, status )
	
    def send_read_status_alert(self, current):
        """
        Send read alert 
        """	
        targetname = self.read_write_target_name
        target = "servers."+self.ilio_dict['uuid']+"."+self.read_write_target_name
        if current == self.read_only_status:
            description = self.dedup_read_only_description
            value = '0'
            oldStatus = self.alert_ok
            status = self.alert_error
        elif current == self.read_write_status:
            description = self.dedup_read_write_description
            value = '1'
            oldStatus = self.alert_error
            status = self.alert_ok
        self.publish_to_grid(description,targetname,value, target, oldStatus, status )

    def publish_to_grid(self,description,targetname,value, target, oldStatus, status):
        """
        Send alert to the grid; Return True if publish to grid successfully
        """		
        usxuuid = self.ilio_dict['uuid']
        warn = '0'
        error = '0'
        service = 'MONITORING'
        ts = time.time()
        alertTimestamp = datetime.datetime.fromtimestamp(ts).strftime('%s')
        iliotype = 'VOLUME'
        checkid=usxuuid+"-"+targetname
        uuid=usxuuid+"-"+targetname+"_"+alertTimestamp
		
        return self.publish_alert(uuid,checkid,usxuuid,value,target,warn,error,oldStatus,status,description,service,alertTimestamp,iliotype,self.amcpost_url)

    def publish_alert(self,uuid,checkid,usxuuid,value,target,warn,error,oldStatus,status,description,service,alertTimestamp,iliotype, amcpost_alert_url):
        """
        Send alert to the grid; Return True if publish to grid successfully
        """
        self.log.info('FileSystemStatusCollector:  Generate Alert: \"'+description+'\"')		
        out = ['']
        json_str = ''
        json_str += '{ \\"uuid\\":\\"%s\\",\\"checkId\\":\\"%s\\",\\"usxuuid\\":\\"%s\\",\\"value\\":%s,\\"target\\":\\"%s\\",\\"warn\\":%s,\\"error\\":%s,\\"oldStatus\\":\\"%s\\",\\"status\\":\\"%s\\",\\"description\\":\\"%s\\",\\"service\\":\\"%s\\",\\"alertTimestamp\\":%s,\\"iliotype\\":\\"%s\\",\\"displayname\\":\\"%s\\"}' % (uuid,checkid,usxuuid,value,target,warn,error,oldStatus,status,description,service,alertTimestamp,iliotype,self.ilio_dict['displayname'])
        cmd = r'curl -s -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, amcpost_alert_url) # actual curl command to send a JSON formatted body
        rc = do_system(cmd, out)
        if rc != 0: # curl system call failed, return error
            self.log.info("Error during posting alert with command:   " + cmd)
            return False

        if out[0] == 'true': # API call return success
            return True
        else:
            return False

    # This method is unnecessary
    def isDedupFsInitialized(self):
        """
        Check if the dedup fs has been initialized
        """
	"""
        cmd = 'mount | grep -i -E \'%s\'' % STORAGE_REG_EXPRESSION
        result = os.popen(cmd).read()
        if len(result.strip()) > 0:
            return True
        else:
            return False
	"""

	return True
	
    def collect(self):
        """
        Collect stats
        """
        if self.isDedupFsInitialized() :
            previous = self.previous_dedup_fs_status()
            self.log.info('FileSystemStatusCollector:  Previous Read Write Status:\"'+previous+'\"')
            current = self.get_dedup_fs_status()
            self.log.info('FileSystemStatusCollector: Current Write Status: \"'+current+'\"')
            previous_mount = self.previous_mount_dedup_fs_status()
            self.log.info('FileSystemStatusCollector: Previous Availability Status: \"'+previous_mount+'\"')
            current_mount = self.current_mount_dedup_fs_status()
            self.log.info('FileSystemStatusCollector: Current Availability Status: \"'+current_mount+'\"')

            #check if the the dedup fs is mounted
            if current_mount != self.unmounted_status :#DEDUP MOUNTED

                #check if the read status has changed
                if current != previous:
                    #write the current status in the prop file
                    self.write_dedup_read_status(current)
                    if (current == self.read_only_status and previous == self.no_status) or (previous != self.no_status):
                        #send alert for read status
                        self.send_read_status_alert(current)
					
                #if the availability status has changed send alert
                if previous_mount == self.unmounted_status:
                    self.log.info('Sending dedup fs available alert!')
                    self.send_availability_status_alert(self.mounted_status)
			    #write the status in the file 
                if previous_mount != self.mounted_status:
                    self.write_dedup_availability_status(self.mounted_status)
					
            else: #DEDUP UNMOUNTED
                #if the availability status has changed send alert
                if previous_mount != self.unmounted_status:
            	    self.log.info('Sending dedup fs unavailable alert!')
                    self.send_availability_status_alert(self.unmounted_status)
		    	    #write the staus in the file 
                    self.write_dedup_availability_status(self.unmounted_status)

            self.log.info('FileSystemStatusCollector:  Previous Dedup FS Status:\"'+previous_mount+'\" Current Dedup FS Status: \"'+current_mount+'\"')
	    previous = self.get_previous_volume_export_status()
            self.log.info('FileSystemStatusCollector: Previous Volume Export Status:\"'+previous+'\"')
	    current = self.get_volume_export_status()
            self.log.info('FileSystemStatusCollector: Current Volume Export Status: \"'+current+'\"')

            self.status_update_counter = self.status_update_counter + 1
            self.log.info('FileSystemStatusCollector: counter: \"'+str(self.status_update_counter)+'\"')
            self.log.info('FileSystemStatusCollector: MAX: \"'+str(self.STATUS_UPDATE_NOW)+'\"')

            if self.status_update_counter >= self.STATUS_UPDATE_NOW:
                self.status_update_counter = 0
                self.send_dedup_fs_status(current_mount)
	        with open(self.dedupfs_availability_status_file, 'w') as fd:
                    fd.write(current_mount)
	        self.send_volume_export_status(current)
	        with open(self.volume_export_availability_status_file, 'w') as fd:
                    fd.write(current)
                return

	    if previous_mount != current_mount:
                self.send_dedup_fs_status(current_mount)

	    if previous != current:
	        if 'OK' == self.get_service_ip_status():
	            self.send_volume_export_status(current)
	        else:
	            self.send_volume_export_status('FATAL')
	            with open(self.volume_export_availability_status_file, 'w') as fd:
                        fd.write('FATAL')
        else:
            self.log.info('Dedup fs not initialized')
            prev = self.previous_mount_dedup_fs_status()
            self.log.info('previous dedup FS status: %s' % prev)
	    if prev != self.unmounted_status:
                self.write_dedup_availability_status(self.unmounted_status)
                self.send_dedup_fs_status(self.unmounted_status)

	    prev = self.get_previous_volume_export_status()
            self.log.info('previous volume export status: %s' % prev)
	    if prev != 'FATAL':
	        self.send_volume_export_status('FATAL')
	        with open(self.volume_export_availability_status_file, 'w') as fd:
                    fd.write('FATAL')
