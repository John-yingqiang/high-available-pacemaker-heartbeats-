#!/usr/bin/python
import os,sys
#import logging
import socket
sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
from ha_util import *
sys.path.insert(0, "/opt/milio/libs/atlas/")
from log import *
from atl_alerts import *

LOG_FILENAME = '/var/log/atlas-backup.log'
#LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'

#
# Author: Steve Wei
# Description: 
#   This script is used to send backup status alert to AMC server. It is invoked by usx_simplememory_sync.sh 
# 

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
	logging.debug("".join([str(x) for x in args]))
	print("".join([str(x) for x in args]))
'''

def usage():
	debug('Wrong command: %s' % str(sys.argv))
	print('Usage: python /opt/milio/atlas/backup/ilio_backup_alert.pyc status(OK|ERROR)')

'''
def _send_alert_backup(ilio_id, iliotype, status):
	cmd = 'date +%s'
	(ret, epoch_time) = runcmd(cmd, print_ret=True)
	epoch_time = epoch_time.rstrip('\n')
	cfgfile = open("/etc/ilio/atlas.json", 'r')
	s = cfgfile.read()
	cfgfile.close()
	node_dict = json.loads(s)
	usx = node_dict.get('usx')
	usx_displayname = usx.get('displayname')

	ad = {
		"uuid"			:"",
		"checkId"		:"",
		"usxuuid"		:"",
		"value"			:0.0,
		"target"		:"",
		"warn"			:0.0,
		"error"			:0.0,
		"oldStatus"		:"OK",
		"status"		:"OK",
		"description"		:"BACKUP",
		"service"		:"BACKUP",
		"alertTimestamp"	:"",
		"iliotype"		:"VOLUME"
	}

	ad["uuid"] = ilio_id + '-backup-alert-' + str(epoch_time)
	ad["checkId"] = ilio_id + '-backup'
	ad["usxuuid"] = ilio_id
	ad["displayname"] = usx_displayname
	ad["target"] = "servers." + ilio_id + ".backup"
	ad["alertTimestamp"] = epoch_time
	ad["iliotype"] = iliotype
        ad["status"] = status
        if status.lower() == "ok":
           ad["description"] = "Backup successfully"
        else:
           ad["description"] = "Backup failed" 
        data = json.dumps(ad)
	cmd = 'curl -X POST -H "Content-type:application/json" ' + LOCAL_AGENT + 'alerts/ -d \'' + data + '\''
	(ret, out) = runcmd(cmd, print_ret=True, block=False)
'''

def main():
    	if len(sys.argv) != 2:
		usage()
        	return 1
	(ret, ha_enabled, role, node_dict) = readHAJsonFile()
	if ret != 0:
		debug('Fail to get the configure from local json file in this node. rc=%d' %ret )
		return ret

	usx_dict = node_dict.get('usx')
	if not usx_dict:
		debug('Could not get the USX info.')
		uuid = node_dict.get('uuid')
		if not uuid:
			debug('Could not get UUID.')
			return 1
		role = node_dict.get('roles')[0]
	else:
		uuid = usx_dict.get('uuid')
		role = usx_dict.get('roles')[0]
	send_alert_backup(uuid, role, sys.argv[1])
	return 0

if __name__ == '__main__':
	sys.exit(main())
