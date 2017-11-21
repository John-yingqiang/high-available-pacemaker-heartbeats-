#!/usr/bin/python
import os,sys
import logging
import socket
import httplib
from ha_util import *

LOG_FILENAME = '/var/log/usx-atlas-ha.log'
LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'
SV_CONTAINER_URL ='/usxmanager/usx/inventory/servicevm/containers/'

# This script is used to monitor the resources in the cluster, configure it
# with ClusterMon. It can resport the resource status to data grid.
# Currently, we only report the failover event.

# The external agent is fed with environment variables allowing us to know
# what transition happened and to react accordingly:
#  http://clusterlabs.org/doc/en-US/Pacemaker/1.1-crmsh/html/Pacemaker_Explained/s-notification-external.html
'''
def call_rest_api(usxmanagerurl, apistr):
    """
    Get information from grid
     Input: REST API URL, REST API query string
     Return: response data
    """
    try:
        protocol = usxmanagerurl.split(':')[0]
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        apiaddr = usxmanagerurl.split('/')[2]  # like: 10.15.107.2:8443
        #debug(usxmanagerurl)
        #debug(apiaddr)
        if use_https == True:
            conn = httplib.HTTPSConnection(apiaddr)
        else:
            conn = httplib.HTTPConnection(apiaddr)
        conn.request("GET", apistr)
        debug(usxmanagerurl)
        debug('apiaddr: '+apiaddr+' | apistr: ' + apistr)
        response = conn.getresponse()
        debug(response.status, response.reason)
        if response.status != 200 and response.reason != 'OK':
            return None
        else:
            data = response.read()
            debug('Response:')
            debug(data)
    except:
        debug("ERROR : Cannot connect to USX Manager to query")
        return None

    return data
'''
'''
def get_sv_displayname(uuid):
    retVal = ''

    api_str = '%s%s?composite=false' % (SV_CONTAINER_URL,uuid)
    url = LOCAL_AGENT.split('/')[0:-2]
    url = ('/').join(url)

    code, ret = call_rest_api(url+api_str, 'GET')
    if ret:
        data = json.loads(ret)
        if data.get('data'): # no service vm uuid retrieved
            retVal = data.get('data')['displayname']
    return retVal

def _send_alert_shutdown(ilio_id, iliotype):
	cmd = 'date +%s'
	(ret, epoch_time) = runcmd(cmd, print_ret=True)
	epoch_time = epoch_time.rstrip('\n')
	cfgfile = open("/etc/ilio/atlas.json", 'r')
	s = cfgfile.read()
	cfgfile.close()
	node_dict = json.loads(s)
	usx = node_dict.get('usx')

	if role == 'VOLUME':
		usx_displayname = usx.get('displayname')
	elif role == 'SERVICE_VM':
		#
		# TODO:
		#
		# note servicevm's atlas.json doesn't
		# has 'displayname' field, we will have
		# to call rest api via usxuuid to get it
		#
		usx_displayname = get_sv_displayname(uuid)

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
		"description"		:"USX VOLUME SHUTDOWN",
		"service"		:"MONITORING",
		"alertTimestamp"	:"",
		"usxtype"		:"VOLUME"
	}

	ad["uuid"] = ilio_id + '-shutdown-alert-' + str(epoch_time)
	ad["checkId"] = 'SHUTDOWN'
	ad["usxuuid"] = ilio_id
	ad["displayname"] = usx_displayname
	ad["target"] = "servers." + ilio_id + ".shutdown"
	ad["alertTimestamp"] = epoch_time
	ad["usxtype"] = iliotype

	data = json.dumps(ad)
	cmd = 'curl -X POST -H "Content-type:application/json" ' + LOCAL_AGENT + 'alerts/ -d \'' + data + '\''
	(ret, out) = runcmd(cmd, print_ret=True, block=False)
'''

set_log_file(LOG_FILENAME)

(ret, ha_enabled, role, node_dict) = readHAJsonFile()
if ret != CLUS_SUCCESS:
	debug('Fail to get the configure from local json file in this node. rc=%d' %ret )
	sys.exit(JSON_ROLE_NOT_DEFINED)

usx_dict = node_dict.get('usx')
if usx_dict is None:
	debug('Could not get the USX info.')
	uuid = node_dict.get('uuid')
	if uuid is None:
		debug('Could not get UUID.')
		sys.exit(JSON_VCENTER_NOT_DEFINED)
	role = node_dict.get('roles')[0]
else:
	uuid = usx_dict.get('uuid')
	role = usx_dict.get('roles')[0]

send_alert_shutdown(uuid, role)

sys.exit(CLUS_SUCCESS)
