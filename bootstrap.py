#!/usr/bin/python

import xml.etree.ElementTree as ET
import os
import sys, tempfile, re
import logging
import pickle
from subprocess import *
import json
from pprint import pprint
import time
import urllib2
import fcntl
#import requests # TODO: Please use requests for all future API/HTTP stuff

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from log import *
from atl_util import do_system_timeout
from atl_alerts import send_alert_boot

sys.path.insert(0, "/opt/milio/atlas/roles/ha")
from ha_util import is_hyperconverged, change_crash_file_location

ilio_hostname=""
ilio_version=""
ilio_ramdisk_size=""
eth0_mode=""
eth0_ip_address=""
eth0_netmask=""
eth0_gateway=""
eth1_mode=""
eth1_ip_address=""
eth1_netmask=""
eth1_gateway=""
eth2_mode=""
eth2_ip_address=""
eth2_netmask=""
eth2_gateway=""
eth3_mode=""
eth3_ip_address=""
eth3_netmask=""
eth3_gateway=""
datastore_type=""
data_disk="/dev/sdb"
tear_down=""
ilio_function_type=""
ilio_poweruser_p=""
ilio_timezone=""
usxmanagerurl=""
iliouuid=""
iliorole=""
myjson=""
myrole = None
myha_enabled = None
myfullrole = None
amc_ip = ""
JSON_FILE_LOCATION="/etc/ilio/atlas.json" # This is also defined in milio/scripts/cfgilioenv.py so if you change it here, then change it there.
RSYSLOG_CONF_FILE="/etc/rsyslog.conf"
LOCAL_AGENT = "http://127.0.0.1:8080"
GRID_MEMBERS = "/opt/amc/agent/config/grid_members.json"

##### KARTIK : TISILIO-3967 : Set multicast route
# This list contains the eth devices which are on the storage network
storage_network_list = []
# This dictionary contains the multicast_IP/subnet-mask pair. The key is the multicast IP
multicast_network_dict = {
	# Multicast IP : Subnet mask for that multicast IP
	'224.0.0.0' : '255.0.0.0'
}
STORAGE_NETWORK_LIST_FILE = "/etc/ilio/atlas-ha-multicast-storage-networks.lst"

# TISILIO-3016 : Track if we have a valid vApp properties config XML element tree root
et_root_exists = False


# The command to start the Atlas agent server
AGENT_JAR_START_COMMAND="/opt/amc/agent/bin/amc_agent_start.sh"

# USX SSH keygen utility
USX_SSHKEY_GEN_COMMAND_INIT = "/usr/bin/python /opt/milio/atlas/system/usx_gen_sshkeys.pyc -t"
USX_SSHKEY_GEN_COMMAND_START = "/usr/bin/python /opt/milio/atlas/system/usx_gen_sshkeys.pyc"

#Path to main role directory
ROLE_DIR="/opt/milio/atlas/roles"

#ROLE - Aggregator node
AGGNODE_ROLE_DIR="aggregate"
AGGNODE_CONFIG_SCRIPT="agstart config"
AGGNODE_START_SCRIPT="agstart start"

#ROLE - witness node
WITNESS_ROLE_DIR="witness"
WITNESS_CONFIG_SCRIPT="witnessstart config"
WITNESS_START_SCRIPT="witnessstart start"
#ROLE - Pool Node :: TODO: Jin / Tony, please change this as applicable
POOLNODE_ROLE_DIR="pool"
POOLNODE_CONFIG_SCRIPT="cp-load init"
POOLNODE_START_SCRIPT="cp-load start"

#ROLE - ADS Node :: TODO: Jin / Tony, please change this as applicable
ADSNODE_ROLE_DIR="ads"
ADSNODE_CONFIG_SCRIPT="ads-load init"
ADSNODE_START_SCRIPT="ads-load start"

#ROLE - Virtual Volume :: Replace ADS Volume for Atlas 2.0
VVNODE_ROLE_DIR='virtvol'
VVNODE_CONFIG_SCRIPT="vv-load init"
VVNODE_START_SCRIPT="vv-load start"

#ROLE - VDI node, for VDI_DISKLESS(_FREE)/VDI_DISKBASED(_FREE)
VDINODE_ROLE_DIR="vdi"
VDINODE_CONFIG_SCRIPT="vdistart config"
VDINODE_START_SCRIPT="vdistart start"

#ROLE - Atlas Management Center (AMC)
AMCNODE_ROLE_DIR="amc"
AMCNODE_CONFIG_SCRIPT="bin/run_amc_server.sh"
AMCNODE_START_SCRIPT="bin/run_amc_server.sh"

# HA scripts
HA_ROLE_DIR="ha"
HA_PACEMAKER_CONFIG_SCRIPT="pacemaker_config"
HA_COROSYNC_CONFIG_SCRIPT="corosync_config"
HA_PACEMAKER_START_SCRIPT="pacemaker_start"
HA_COROSYNC_START_SCRIPT="corosync_start"

#ATLANTIS2923/ATLANTIS-2926
#Keys for vscaler kernel parameter tunables
VSCALER_DIRTY_THRESH_PCT="dirty_thresh_pct"
VSCALER_MAX_CLEAN_IOS_SET="max_clean_ios_set"
VSCALER_MAX_CLEAN_IOS_TOTAL="max_clean_ios_total"
VSCALER_RECLAIM_POLICY="reclaim_policy"

#ATLANTIS2923/ATLANTIS-2926
#Values for vscaler kernel parameter tunables
VSCALER_DIRTY_THRESH_PCT_VALUE="80"
VSCALER_MAX_CLEAN_IOS_SET_VALUE="65536"
VSCALER_MAX_CLEAN_IOS_TOTAL_VALUE="65536"
VSCALER_RECLAIM_POLICY_VALUE="1"

# For sending status updates to AMC/Grid
sys.path.insert(0, "/opt/milio/")
from libs.atlas.status_update import *
from libs.atlas.set_multicast_route import set_multicast_routes_for_ha
from libs.atlas.log import *
from libs.atlas.cmd import *

LOG_FILENAME = '/var/log/usx-atlas-bootstrap.log'
set_log_file(LOG_FILENAME)

"""
is_hybrid_volume
    checks if the appliance is a Hybrid USX Volume
    JIRA Ticket: ATLANTIS2923/ATLANTIS-2926

Parameters:
    myjsons :   /etc/ilio/atlas.json file

Return:
    True    :   if the appliance is Hybrid USX Volume
    False   :   otherwise
"""
def is_hybrid_volume():
        debug("BEGIN: is_hybrid_volume")
        global myjson
        is_hybrid = False
        try:
                debug('Checking if this is a Hybrid USX Volume...')
                fulljson = json.loads(myjson)
                #Hybrid USX Volume is identified if json file has the
                #following key value pair
                #["volumeresources"][0]["volumetype"] == "HYBRID"
                if fulljson.has_key('volumeresources'):
                    if fulljson['volumeresources']:
                        if fulljson['volumeresources'][0]['volumetype'] == "HYBRID":
                            is_hybrid = True

                if is_hybrid:
                    debug('This is a Hybrid USX Volume')
                else:
                    debug('This is NOT a Hybrid USX Volume')

                debug("END: is_hybrid_volume")
                return is_hybrid
        except:
                debug('ERROR : Exception while checking if this appliance is a Hybrid USX Volume. Assuming it is NOT. Exception was: %s' % sys.exc_info()[0])
                return False


"""
is_simplified_volume
    checks if the appliance is a simplified USX Volume

Parameters:
    myjsons :   /etc/ilio/atlas.json file

Return:
    True    :   if the appliance is simplified Volume
    False   :   otherwise
"""
def is_simplified_volume():
        debug("BEGIN: is_simplified_volume")
        global myjson
        is_simplified = False
        try:
                debug('Checking if this is a simplified USX Volume...')
                fulljson = json.loads(myjson)
                if fulljson.has_key('volumeresources'):
                    if fulljson['volumeresources']:
                        volume_type = fulljson['volumeresources'][0]['volumetype']
                        if volume_type.upper() in ['SIMPLE_HYBRID', 'SIMPLE_MEMORY', 'SIMPLE_FLASH']:
                            is_simplified = True

                if is_simplified:
                    debug('This is a simplified USX Volume')
                else:
                    debug('This is NOT a simplified USX Volume')

                debug("END: is_simplified_volume")
                return is_simplified
        except:
                debug('ERROR : Exception while checking if this appliance is a simplified USX Volume. Assuming it is NOT. Exception was: %s' % sys.exc_info()[0])
                return False


"""
get_vscaler_dev_name
    gets the device name for vscaler.
    JIRA Ticket: ATLANTIS2923/ATLANTIS-2926

Parameters:
    None    : No input parameter is required

Returns:
    list  :   vscaler device name. list is empty in case of errors, or if
              there is no vscaler device. caller should check for the len
              of list to be greater than zero.
"""
def get_vsaler_dev_name():
        debug("BEGIN: get_vscaler_dev_name")

        vscaler_dev = []
        # Check the /proc/sys/dev/vscaler/ directory to see if any vscaler device
        # is present on this machine
        path = "/proc/sys/dev/vscaler/"
        debug("ls " + path)
        try:
                vscaler_dev = os.listdir(path)
        except:
                debug("'ls " + path + " 'resulted in an exception.")
                debug(sys.exc_info()[1])
                debug("NOTE: If -> 'No such file or directory', it can be a vaild exception, after resource failover.")

        debug("END: get_vscaler_dev_name")
        return vscaler_dev

"""
tune_vscaler_kernel_parameter
    tunes  of the specified kernel configuration parameter for vscaler
    device

    JIRA Ticket: ATLANTIS2923/ATLANTIS-2926

Parameters:
    vscaler_dev_name    :   name of the vscaler device to be tuned
    key                 :   name of the parameter to be tuned
    value               :   value to be set for this parameter

Returns:
    0                   :   success
    -1                  :   failure
"""
def tune_vscaler_kernel_parameter(vscaler_devname, key, value):
        debug("BEGIN: tune_vscaler_kernel_parameter")

        debug("ABORT: tune_vscaler_kernel_parameter")
	return 0

        retValue = 0
        # Set the required kernel parameter(key) to the value provided
        debug("setting " + vscaler_devname + "." + key + " to " + value)
        cmd = "sysctl -e dev.vscaler." + vscaler_devname + "." + key + "=" + value
        debug("Running command: " + cmd)
        # open a pipe to read the command output
        fo_stdout = os.popen(cmd)
        # read output at stdout
        output = fo_stdout.read()
        # return value of the command
        ret = fo_stdout.close()
        # return value from popen's file object close operation is None, for success
        # and not None otherwise
        if ret is not None:
                debug("Command:'" + cmd + "' failed. Return code: " + str(ret))
                retValue = -1

        debug(output)
        debug("retValue: " + str(retValue))
        debug("END: tune_vscaler_kernel_parameter")
        return retValue

'''
Check whether a given IPv4 extended netmask is a valid netmask.

This fix was put in for TISILIO-3738.

NOTE: This function DOES NOT check whether a given combination of IP address
and netmask is a valid combination; it only checks for a valid netmask.

For more info on what constitues a valid netmask, please read:
	http://www.gadgetwiz.com/network/netmask.html

Parameters:
	netmask : netmask to be checked, in extended (x.y.z.a) format

Returns:
	0 	: 	Given netmask is a valid netmask
	!=0	:	Given netmask is invalid, or there was an error checking given
			netmask.
'''
def validate_netmask(netmask):
	if netmask is None or not netmask:
		debug("ERROR : Check netmask : Null or empty netmask received. Cannot check netmask.")
		return 1
	try:
		# Split the given netmask into octets
		octets = netmask.split('.')
		if len(octets) != 4:
			debug("ERROR : Check netmask : Decomposing given netmask "+netmask+" into octets yielded "+str(len(octets)) + " octets, but we expect exactly 4 octets.")
			return 2

		# OK, we have the expected number of octets. Now convert the given
		# netmask into a single integer
		addr = 0
		for octet in octets:
			addr = addr * 256 + int(octet)

		# addr is now a single integer representing the given netmask.
		# We now convert addr into binary, and discard the leading "0b"
		binaddr = bin(addr)[2:]

		# This is the key: Now we check if the binary representation of addr
		# contains the string "01". A valid netmask will ONLY have 0's on the
		# right hand side; there is never a 0 followed by 1 in a valid netmask
		strpos = binaddr.find("01")

		if strpos >= 0:
			debug("ERROR : Check netmask : Netmask "+netmask+" is INVALID!")
			return 3

		# If we got here, we have a valid netmask.
		debug("INFO : Check netmask : Netmask "+netmask+" is a valid netmask, all OK.")
		return 0

	except:
		debug("ERROR : Check netmask : There was an exception validating the netmask.")
		return 4



def write_atlas_json_file(myjson):
	global usxmanagerurl # added to get usxmanager url from vapp properties
	global iliouuid
	global iliorole

	myjson_dict = ""

	debug('INFO : Write JSON file : JSON received from vApp environment = %s' % myjson)
	myjson=json.dumps(json.loads(myjson), sort_keys=True, indent=4, separators=(',', ': '))
	if myjson:
		# Get the usx manager url from vApp property Atlas-JSON first
		myjson_dict = json.loads(myjson)
		if myjson_dict.has_key("usx"):
			if myjson_dict['usx'].has_key("usxmanagerurl"):
				usxmanagerurl = myjson_dict['usx']['usxmanagerurl']
			if myjson_dict['usx'].has_key("uuid"):
				iliouuid = myjson_dict['usx']['uuid']
			if myjson_dict['usx'].has_key("roles"):
				iliorole = myjson_dict['usx']['roles'][0]

		# IN USX 2.0, the HA config is written AFTER bootstrap, so we should
		# NOT be overwriting the existing atlas.json file with the info from
		# the vApp properties, because the vApp properties may not correctly
		# reflect the HA config state.
		# NOTE: By doing this, we lose the ability to have manual changes to
		#       the vApp JSON be reflected in the atlas.json file.
		#       To make an omelette, one must break some eggs.
		if os.path.isfile(JSON_FILE_LOCATION):
			debug('WARNING : Write JSON file : File %s already exists, NOT writing JSON from vApp properties, since that would potentially overwrite existing configurations!' % (JSON_FILE_LOCATION) )
			return

		# If we got here, we don't have a local atlas.json, so we write the
		# info received from the vApp properties into the JSON.
		debug('INFO : Write JSON file : JSON received from vApp environment contains data, writing json data to %s' % JSON_FILE_LOCATION)
		try:
			with open(JSON_FILE_LOCATION, 'w') as f:
				f.write(myjson)
			debug('INFO : Write JSON file : SUCCESSFULLY wrote JSON received from vApp environment to %s and will use this local data in subsequent calls to load/get the JSON config.' % JSON_FILE_LOCATION)
		except:
			debug('WARNING : Write JSON file : EXCEPTION writing json data to %s - JSON data might not have been saved properly on the local system!' % JSON_FILE_LOCATION)
	# NO JSON data received from vApp environment
	else:
		debug('WARNING : Write JSON file : JSON received from vApp environment is empty, nothing to write')


#
### Figure out what our role is, so we can call the appropriate scripts.
# Returns role as String:
#	aggregate if this is an Aggregator node
#	pool if this is a Pool node
#	ads if this is an ADS node
#	Python 'None' if no applicable role was found
#
def get_role_from_json(myjsons):
	global myfullrole
	thisrole = None
	try:
		print('Trying to get role...')
		fulljson = json.loads(myjsons)
		pprint(fulljson)
		if fulljson.has_key('ilio'): # virtual volume Atlas JSON added 'ilio' key around 'roles'
			thisrole = fulljson['ilio']['roles'][0]
		elif fulljson.has_key('usx'): # USX 2.0 Rebranding - 'ilio' key changed to USX
			thisrole = fulljson['usx']['roles'][0]
		else:
			thisrole = fulljson["roles"][0]
	except:
		debug('ERROR : Exception getting role from JSON! Exception was %s' % sys.exc_info()[0])
		thisrole = None
	if thisrole is None:
		debug('ERROR : FAILED getting role from JSON!')
		return None
	try:
		debug('ThisRole from JSON = %s' % thisrole)
		myfullrole = thisrole
		debug('myfullrole from JSON = %s' % myfullrole)
		thisrole = thisrole.lower()
		if 'service_vm' in thisrole:
			debug('returning role=service vm')
			return 'service vm'
		elif 'pool' in thisrole:
			debug('returning role=pool')
			return 'pool'
		elif 'ads' in thisrole:
			debug('returning role=ads')
			return 'ads'
		elif 'volume' in thisrole:
			debug('returning role=volume')
			return 'volume'
		elif 'vdi' in thisrole: # for VDI_DISKLESS(_FREE)/VDI_DISKBASED(_FREE)
			debug('returning role=vdi')
			return 'vdi'
		elif 'usx_witness_vm' in thisrole: # for witness node
			debug('returning role=witness')
			return 'witness'
		elif thisrole.startswith('amc'):
			debug('returning role=amc')
			return 'amc'
		else:
			debug('ERROR : Role %s is not a valid role' % thisrole)
			return None
	except:
		debug('ERROR : Exception processing role from JSON! Exception was %s' % sys.exc_info()[0])
		return None


#
# From the JSON, get whether HA is enabled or not
# Returns:
#	True : if ha=true in JSON
#	False : if ha=false in JSON or if it is not defined
#
# Change : 22-JULY-2014 : The HA Enabled setting in JSON
# is now a boolean instead of a string. Revamped this
# function to check the boolean value in the JSON instead
# of checking for a string value.
#
def is_ha_enabled(myjsons):
	ha_bool = False
	try:
		debug('Trying to get if HA is enabled in JSON...')
		fulljson = json.loads(myjsons)
		if fulljson.has_key("ilio"): # New USX 2.0 puts this in the "ilio" key
			ha_bool = fulljson["ilio"]["ha"]
		elif fulljson.has_key('usx'): # USX 2.0 Rebranding - 'ilio' key changed to USX
			ha_bool = fulljson["usx"]["ha"]
		else:
			ha_bool = fulljson["ha"]

		if ha_bool is None:
			debug('WARNING: Could not get HA enabled setting from JSON, assuming HA Enabled = False!')
			return False

		debug('HA setting from JSON: %s' % str(ha_bool))

		if ha_bool:
			debug('HA Setting from JSON = true, setting HA Enabled to True.')
			return True
		else:
			debug('HA Setting from JSON is NOT "true", setting HA Enabled to False.')
			return False
	except:
		debug('ERROR : Exception getting HA enabled status from JSON. Assuming HA is NOT enabled. Exception was: %s' % sys.exc_info()[0])
		return False


'''
Get the AMC IP address from the AMC URL in the JSON.

TODO: Get the AMC IP from the grid

Parameters:
	myjson : JSON String read from JSON file

Returns:
	AMC IP on success
	Empty string ("") on errors
'''
def get_amc_ip_from_json(myjsons):
	aip = ""
	try:
		debug("Trying to get USX Manager IP from JSON...")
		fulljson = json.loads(myjsons)
		amci = ""

		# New USX 2.0 JSON format has put the amcurl key inside an "ilio" key
		if fulljson.has_key('ilio'):
			if fulljson["ilio"]["amcurl"]:
				amci = fulljson["ilio"]["amcurl"]
		elif fulljson.has_key('usx'): # USX 2.0 Rebranding - 'ilio' key changed to USX
			if fulljson["usx"].has_key("amcurl"):
				amci = fulljson["usx"]["amcurl"]
			elif fulljson["usx"].has_key("usxmanagerurl"):
				amci = fulljson["usx"]["usxmanagerurl"]
		elif fulljson["usxmanagerurl"]: # USX 2.0 Rebranding - Service VM (Formerly known as Agg Node) AMC URL field changed from "amcurl" to "usxmanagerurl"
				amci = fulljson["usxmanagerurl"]
		elif fulljson["amcurl"]:
			amci = fulljson["amcurl"]

		if amci is None or not amci:
			debug("ERROR : Failed getting AMC URL from JSON - it is null or empty")
			return aip
		'''
		Logic to get AMC IP from AMC URL (eg. URL is 'https://10.15.103.50:8443/amc'):
		1. Split the URL by the ':' character.
		   This will give us ['http', '//10.15.103.50', '8443/amc']
		2. Get the second element in the array above (array[1]):
		   This will give us '//10.15.103.50'
		3. Replace "//" in this element with nothing
		   This will give us '10.15.103.50'
		'''
		amcii = amci.split(':')[1].replace("//", "")
		if amcii is None or not amcii:
			debug("ERROR : Failed getting USX Manager IP from AMC URL")
			return aip

		aip = amcii
		debug("USX Manager IP parsed from JSON: "+aip)
		return aip
	except:
		debug("ERROR : Exception getting USX Manager IP from JSON. Cannot set USX Manager IP!")
		return ""


"""
Invoke PUT/POST REST API to update entries in USX grid.

Parameters:
	usxmanagerurl : url of usx manager/agent
	apistr : the REST API url portion
	data : JSON object/String to be saved into USX grid

Returns:
	REST API execution status
"""
'''
def _publish_to_usx_grid(usxmanagerurl, apistr, data, putFlag=0):
    """
    Call REST API to update availability status to grid
    """
    retVal = 0
    conn = urllib2.Request(usxmanagerurl + apistr)
    debug(usxmanagerurl+apistr)
    conn.add_header('Content-type','application/json')
    if putFlag == 1:
        conn.get_method = lambda: 'PUT'
    debug('**** data to be uploaded to AMC: ', data)
    res = urllib2.urlopen(conn, json.dumps(data))
    debug('Returned response code: ' + str(res.code))
    if res.code != 200:
        retVal = 1
    res.close()
    return retVal
'''

def send_bootstrap_status(ha_flag=False, status=False):
	"""
	Send bootstrap status via REST API
	"""
	global myjson

	retVal = 0
	myrole = ''
	myuuid = ''

	try:
		fulljson = json.loads(myjson)
		if fulljson.has_key('usx'):
			if fulljson['usx'].has_key('roles'):
				myrole = fulljson['usx']['roles'][0]
			if fulljson['usx'].has_key('uuid'):
				myuuid = fulljson['usx']['uuid']
		if fulljson.has_key('roles'):
			myrole = fulljson['roles'][0]
		if fulljson.has_key('uuid'):
			myuuid = fulljson['uuid']
	except:
		debug("ERROR : Exception getting USX type and UUID from JSON. Cannot send bootstrap status!")
		return 1

	data = {}
	bootstrap_status = {}
	ha_status = {}
	data['usxstatuslist'] = []
	if myrole.lower() == 'volume':
		data['usxtype'] = 'VOLUME_CONTAINER'
	elif myrole.lower() == 'service_vm':
		data['usxtype'] = 'SERVICE_CONTAINER'
	data['usxuuid'] = myuuid

	bootstrap_status['name'] = 'BOOTSTRAP_STATUS'
	if status == True:
		bootstrap_status['value'] = 'OK'
	else:
		bootstrap_status['value'] = 'FATAL'
	if myrole.lower() == 'volume' and ha_flag == True and status == True:
		ha_status['name'] = 'HA_STATUS'
		ha_status['value'] = 'OK'
		data['usxstatuslist'].append(ha_status)
	#epoch_time = int(time.time())
	#bootstrap_status['timestamp'] = epoch_time

	data['usxstatuslist'].append(bootstrap_status)
	post_apistr = '/usxmanager/usx/status/update'
	rc = publish_to_usx_grid(LOCAL_AGENT, post_apistr, data)
	if rc != 0:
		debug("ERROR : REST API call to publish status to USX grid failed!")
		retVal = rc
	if status is True:
		os.system('/usr/bin/python /opt/milio/atlas/system/availability_status_update.pyc all')
	return retVal


def send_bootstrap_alert():
	"""
	Send bootstrap alert via REST API
	"""
	global myjson

	retVal = 0
	myrole = ''
	myuuid = ''

	try:
		fulljson = json.loads(myjson)
		if fulljson.has_key('usx'):
			if fulljson['usx'].has_key('roles'):
				myrole = fulljson['usx']['roles'][0]
			if fulljson['usx'].has_key('uuid'):
				myuuid = fulljson['usx']['uuid']
		if fulljson.has_key('roles'):
			myrole = fulljson['roles'][0]
		if fulljson.has_key('uuid'):
			myuuid = fulljson['uuid']
	except:
		debug("ERROR : Exception getting USX type and UUID from JSON. Cannot send bootstrap status!")
		return 1

	send_alert_boot(myuuid, myrole)
#
### Based on the role, and whether the USX Node has already been configured
# or not, figure out the correct script which we need to run.
# Returns: path to script to execute based on role and config status
#	   Python 'None' if we could not determine which script to run
#
def build_path_to_atlas_script(role, needs_configuring=True):
	scriptpath = None
	debug("Role = %s, needs_configuring=%s" % (role, needs_configuring))
	try:
		if role == 'service vm':
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, AGGNODE_ROLE_DIR, AGGNODE_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, AGGNODE_ROLE_DIR, AGGNODE_START_SCRIPT)
		elif role == 'pool':
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, POOLNODE_ROLE_DIR, POOLNODE_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, POOLNODE_ROLE_DIR, POOLNODE_START_SCRIPT)
		elif role == 'ads':
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, ADSNODE_ROLE_DIR, ADSNODE_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, ADSNODE_ROLE_DIR, ADSNODE_START_SCRIPT)
		elif role == 'volume': # added virtvol config script path, to replace ADS in Atlas 2.0. June 9, 2014
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, VVNODE_ROLE_DIR, VVNODE_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, VVNODE_ROLE_DIR, VVNODE_START_SCRIPT)
		elif role == 'vdi': # added vdi config script path April 8, 2014
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, VDINODE_ROLE_DIR, VDINODE_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, VDINODE_ROLE_DIR, VDINODE_START_SCRIPT)
		elif role == 'amc':
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, AMCNODE_ROLE_DIR, AMCNODE_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, AMCNODE_ROLE_DIR, AMCNODE_START_SCRIPT)
                elif role == 'witness':
			if needs_configuring:
				scriptpath = os.path.join(ROLE_DIR, WITNESS_ROLE_DIR, WITNESS_CONFIG_SCRIPT)
			else:
				scriptpath = os.path.join(ROLE_DIR, WITNESS_ROLE_DIR, WITNESS_START_SCRIPT)
		else:
			scriptpath = None
	except:
		debug('ERROR : Exception building path to script to run. Exception was: %s' % sys.exc_info()[0])
		scriptpath = None
	if scriptpath is None:
		debug('ERROR : Could not determine the location of the configuration script for role %s, NOT CONFIGURING' % role)
		return None

	return scriptpath



#
# Appends py or pyc to the given script path, then tries to find either
# the py or the pyc file on the system. It prefers to run pyc's over pys
# If either is found on the system, it returns scriptname+'py[c]', and
# if neither a py nor a pyc file is found on the system, it returns
# the python null object None.
#
def find_runnable_pyfile(scriptname):
	try:
		if (scriptname is None) or (not scriptname):
			debug('ERROR : Got invalid path when trying to find runnable script. Returning None.')
			return None

		pyfile = scriptname + '.py'
		pycfile = scriptname + '.pyc'

		#debug('pycfile = %s, pyfile = %s' % (pycfile, pyfile))

		# Find out if we need to run the pyc or the py script
		# We prefer to run the pyc over the py
		if os.path.isfile(pycfile):
			debug('Found compiled version %s' % pycfile)
			return pycfile
		elif os.path.isfile(pyfile):
			debug('Found non-compiled %s' % pyfile)
			return pyfile
		else:
			debug('ERROR : Could not find which script to run, cannot run anything')
			return None
	except:
		debug('ERROR : exception trying to find runnable file for %s :: Exception was %s' %(scriptname, sys.exc_info()[0]))
		return None





#
# From a given script path, check whether we have the corresponding pyc or py
# file, and run it. Priority is given to pyc files over py files.
#
# This function will also handle script arguments passed in.
# It returns the system return code (usually 0) of the script
# to be run if it ran successfully, non-zero otherwise.
def runscript(role,scriptpath):
	if scriptpath is None:
		debug('ERROR : Script path to execute is Null, nothing to execute! Error!')
		return(120)

	scriptpath = scriptpath.strip()
	debug('stripped script path received: %s' % scriptpath)

	if not scriptpath:
		debug('ERROR : Script path after trimming is empty, nothing to execute! Error!')
		return(120)

	scriptname = ''
	scriptparams = ''
	cmd = ''

	# If we have been passed a path with spaces, we assume that the first
	# part of the string before the first whitespace is the script name,
	# and the rest of the string following the first space character are
	# command line arguments to the script.
	if ' ' in scriptpath:
		scriptname, scriptparams = scriptpath.split(None, 1)
	else:
		scriptname = scriptpath

	if role == 'amc':
		# Don't append py or pyc to the script name
		cmd = scriptname
	else:
		# We're not an AMC, so we'll be running py[c] files
		pyfile = find_runnable_pyfile(scriptname)

		if (pyfile is None) or (not pyfile):
			debug('ERROR : Cannot determine proper script name to execute! Not executing anything')
			return 121

		#debug('pycfile = %s, pyfile = %s' % (pycfile, pyfile))
		cmd = 'python ' + pyfile


	cmd += (' ' + scriptparams)

	debug('cmd = %s' % cmd)
	ret = os.system(cmd)
	return(ret)


#
# Run the HA framework scripts. This will only be done if we are a pool or ADS node
#
# We first run the corosync script, then the pacemaker script
#
# Returns 0 if all went well, non-zero otherwise
#
def run_ha_framework_scripts(needs_configuring=True):
	debug('Trying to run HA framework scripts, needs_configuring=%s' % needs_configuring)

	ha_cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_set_start_up_preparation &'
	os.system(ha_cmd)
	debug('Set start up preparation flag for ha enabled node')

	if needs_configuring:
		corosync_scriptpath = os.path.join(ROLE_DIR, HA_ROLE_DIR, HA_COROSYNC_CONFIG_SCRIPT)
		pacemaker_scriptpath = os.path.join(ROLE_DIR, HA_ROLE_DIR, HA_PACEMAKER_CONFIG_SCRIPT)
	else:
		corosync_scriptpath = os.path.join(ROLE_DIR, HA_ROLE_DIR, HA_COROSYNC_START_SCRIPT)
		pacemaker_scriptpath = os.path.join(ROLE_DIR, HA_ROLE_DIR, HA_PACEMAKER_START_SCRIPT)

	corosync_to_execute = find_runnable_pyfile(corosync_scriptpath)
	pacemaker_to_execute = find_runnable_pyfile(pacemaker_scriptpath)

	if (corosync_to_execute is None) or (not corosync_to_execute) or (pacemaker_to_execute is None) or (not pacemaker_to_execute):
		debug('ERROR : One or more invalid paths encountered when trying to find runnable HA framework scripts. NOT RUNNING HA FRAMEWORK!')
		return 122

	try:
		retval = 0
		corosync_cmd = 'python '+corosync_to_execute
		pacemaker_cmd = 'python '+pacemaker_to_execute
		debug('Running HA Framework Corosync script...')
		ret = os.system(corosync_cmd)
		if ret != 0:
			debug('ERROR : Trying to run HA framework corosync script returned nonzero value %s, this may indicate an ERROR.' % str(ret))
			retval = 123
			debug("ERROR : Exiting function due to failure to start corosync script.")
			# Exit function with error. No need to call pacemaker start, since corosync start failed.
			return retval
		debug('Running HA Framework Pacemaker script...')
		ret = os.system(pacemaker_cmd)
		if ret != 0:
			debug('ERROR : Trying to run HA framework pacemaker script returned nonzero value %s, this may indicate an ERROR.' % str(ret))
			retval = 124

		if retval == 0:
			debug('Successfully started HA framework scripts')
		else:
			debug('ERROR : Could not start one or more HA framework scripts')

		return retval
	except:
		debug('ERROR : Exception when trying to start HA framework scripts. HA might not be properly configured on this node! Exception was: %s' % sys.exc_info()[0])
		return 125


'''
Checks if a default gateway is set for this USX Node. If a default gateway is NOT
set, it queries the first four network interfaces in order from eth0 to eth4,
and sets the default gateway to the gateway specified for the first NIC in the
list which has a gateway specified for it.

E.g. If the NIC setup is the following:
	eth0 -> no gateway
	eth1 -> HAS gateway
	eth2 -> no gateway
	eth3 -> HAS gateway
then it will set the default gateway to the one specified for eth1.

This function is written as a workaround for TISILIO-1681

TODO: IMPROVEMENT : For each nic that has a default gateway specified,
use "route add default gw <NIC_GW> <NIC>" to add default routes for
all NICS which have a valid default gateway specified for them in the
vApp properties for this USX Node. Not doing this right now
because it would require extensive testing, and there's no time left
to do this before the GA release. So we have this quick and dirty fix.
I'm sorry :(

Returns:
	True : Successfully set default gateway, or setting default gateway is
			not required because a default gateway already exists on the
			USX Node.

	False: Needed to explicitly set a default gateway, but we were unable
			to do so.
'''
def set_default_gateway_if_required():
	global eth0_gateway
	global eth1_gateway
	global eth2_gateway
	global eth3_gateway
	try:
		ret = os.system('route -n | grep UG')
		if ret == 0:
			debug("This USX Node already has a default gateway set, nothing to configure :)")
			return True

		# If we got here, we need to set a default gateway
		# Find the first NIC that has a gateway specified.
		debug("Default gateway is NOT set, attempting to set default gateway to first available and valid gateway entry")
		gw_ip = "0.0.0.0"
		if eth0_gateway and eth0_gateway != '0.0.0.0':
			gw_ip = eth0_gateway
		elif eth1_gateway and eth1_gateway != '0.0.0.0':
			gw_ip = eth1_gateway
		elif eth2_gateway and eth2_gateway != '0.0.0.0':
			gw_ip = eth2_gateway
		elif eth3_gateway and eth3_gateway != '0.0.0.0':
			gw_ip = eth3_gateway

		if not gw_ip or gw_ip == "0.0.0.0":
			debug(" WARNING : Needed to set default gateway, but unable to find a valid entry for default gateway. This USX Node may not have proper connectivity!")
			return False

		# If we got here, we have a valid entry for gateway. Let's set it!
		debug("Attempting to set "+gw_ip+" as the default gateway for this USX Node...")
		cmd = "route add default gw "+gw_ip
		ret = os.system(cmd)
		if ret != 0:
			debug(" WARNING : Needed to set default gateway to "+gw_ip+", but gateway set command FAILED. This USX Node may not have proper connectivity!")
			return False

		debug("Default gateway SUCCESSFULLY set to "+gw_ip)
		return True
	except:
		debug(" WARNING : Exception determining/setting default gateway. If no default gateway is set for this USX Node, it may not have proper connectivity!")
		return False



'''
TISILIO-3967 : UNIT TEST

WARNING: DO NOT CALL THIS CODE IN PRODUCTION!!!
'''
def unit_test_ha_multicast_route():
	global myha_enabled
	oldha = myha_enabled
	myha_enabled = True
	set_multicast_routes_for_ha()
	myha_enabled = oldha
	set_multicast_routes_for_ha()



###### TODO (from Kartik) : Refactor this entire file and cfgilioenv.py to remove duplicated code (e.g. NIC settings)
# Yes, please!  << MarkN

'''
Get the OVF vApp settings from the VM OVF environment. After getting the vApp
properties, set all the globals pertaining to the OVF vApp settings


REFACTORED FOR TISILIO-3016:
Moved the code for getting vApp settings and setting NIC info etc from the
main code segment into this function, to fix TISILIO-3016 due to a
customer issue where the USX Nodes lost their vApp settings after a host
reboot.

So when this function is called, if the USX Node has already been configured
then we just get the data from the local files (/etc/ilio/atlas.json etc)
but if the the USX Node has NOT been configured, we cannot continue.

Parameters: None

Returns:
    True : Got vApp settings and made initial settings
    False : Failed getting vApp properties and making settings
'''
def get_ovf_vapp_settings():
	# Define our globals
	global ilio_hostname
	global ilio_version
	global ilio_ramdisk_size
	global eth0_mode
	global eth0_ip_address
	global eth0_netmask
	global eth0_gateway
	global eth1_mode
	global eth1_ip_address
	global eth1_netmask
	global eth1_gateway
	global eth2_mode
	global eth2_ip_address
	global eth2_netmask
	global eth2_gateway
	global eth3_mode
	global eth3_ip_address
	global eth3_netmask
	global eth3_gateway
	global datastore_type
	global data_disk
	global tear_down
	global ilio_function_type
	global ilio_poweruser_p
	global ilio_timezone
	global myjson
	global myrole
	global myha_enabled
	global myfullrole
	global amc_ip
	global et_root_exists

	# TODO :: support multiple virtualization technologies
	vmtools_cmd='vmtoolsd --cmd "info-get guestinfo.ovfEnv"'

	try:
		ovf_env=check_output(vmtools_cmd, shell=True)
	except:
		debug('ERROR : Exception getting ovfEnv: %s' % sys.exc_info()[0])
		return False


	root = ET.fromstring(ovf_env)
	if not root:
		debug("ERROR : Failed parsing OvfEnv data, no root element!")
		return False

	et_root_exists = True
	myjson_list = []

	for property in root.findall('./{http://schemas.dmtf.org/ovf/environment/1}PropertySection/{http://schemas.dmtf.org/ovf/environment/1}Property'):
		key_name=property.attrib['{http://schemas.dmtf.org/ovf/environment/1}key']
		key_value=property.attrib['{http://schemas.dmtf.org/ovf/environment/1}value']
		if key_name == 'guestinfo.ilio.hostname':
			ilio_hostname=key_value
		if key_name == 'guestinfo.ilio.ilio_version':
			ilio_version=key_value
		if key_name == 'guestinfo.ilio.ramdisk_size':
			ilio_ramdisk_size=key_value
		if key_name == 'guestinfo.ilio.eth0_mode':
			eth0_mode=key_value
		if key_name == 'guestinfo.ilio.eth0_ipaddress':
			eth0_ip_address=key_value
		if key_name == 'guestinfo.ilio.eth0_netmask':
			eth0_netmask=key_value
		if key_name == 'guestinfo.ilio.eth0_gateway':
			eth0_gateway=key_value
		# KARTIK : TISILIO-3967 : Check if interface eth0 is defined as a
		# storage network interface, and add to list if it is
		stornet = ''
		if key_name == 'guestinfo.ilio.eth0_storagenetwork':
			stornet=key_value
			if str(stornet) == '1':
				debug("INFO : Bootstrap : eth0 is defined to be on the Storage Network, adding it to storage network interface list")
				storage_network_list.append('eth0')
		if key_name == 'guestinfo.ilio.eth1_mode':
			eth1_mode=key_value
		if key_name == 'guestinfo.ilio.eth1_ipaddress':
			eth1_ip_address=key_value
		if key_name == 'guestinfo.ilio.eth1_netmask':
			eth1_netmask=key_value
		if key_name == 'guestinfo.ilio.eth1_gateway':
			eth1_gateway=key_value
		# KARTIK : TISILIO-3967 : Check if interface eth1 is defined as a
		# storage network interface, and add to list if it is
		stornet = ''
		if key_name == 'guestinfo.ilio.eth1_storagenetwork':
			stornet=key_value
			if str(stornet) == '1':
				debug("INFO : Bootstrap : eth1 is defined to be on the Storage Network, adding it to storage network interface list")
				storage_network_list.append('eth1')
		if key_name == 'guestinfo.ilio.eth2_mode':
			eth2_mode=key_value
		if key_name == 'guestinfo.ilio.eth2_ipaddress':
			eth2_ip_address=key_value
		if key_name == 'guestinfo.ilio.eth2_netmask':
			eth2_netmask=key_value
		if key_name == 'guestinfo.ilio.eth2_gateway':
			eth2_gateway=key_value
		# KARTIK : TISILIO-3967 : Check if interface eth2 is defined as a
		# storage network interface, and add to list if it is
		stornet = ''
		if key_name == 'guestinfo.ilio.eth2_storagenetwork':
			stornet=key_value
			if str(stornet) == '1':
				debug("INFO : Bootstrap : eth2 is defined to be on the Storage Network, adding it to storage network interface list")
				storage_network_list.append('eth2')
		if key_name == 'guestinfo.ilio.eth3_mode':
			eth3_mode=key_value
		if key_name == 'guestinfo.ilio.eth3_ipaddress':
			eth3_ip_address=key_value
		if key_name == 'guestinfo.ilio.eth3_netmask':
			eth3_netmask=key_value
		if key_name == 'guestinfo.ilio.eth3_gateway':
			eth3_gateway=key_value
		# KARTIK : TISILIO-3967 : Check if interface eth3 is defined as a
		# storage network interface, and add to list if it is
		stornet = ''
		if key_name == 'guestinfo.ilio.eth3_storagenetwork':
			stornet=key_value
			if str(stornet) == '1':
				debug("INFO : Bootstrap : eth3 is defined to be on the Storage Network, adding it to storage network interface list")
				storage_network_list.append('eth3')
		if key_name == 'guestinfo.ilio.datastore_type':
			datastore_type=key_value
		if key_name == 'guestinfo.ilio.data_disk':
			data_disk=key_value
		if key_name == 'guestinfo.ilio.function_type':
			ilio_function_type=key_value
		if key_name == 'guestinfo.ilio.timezone':
			ilio_timezone=key_value
		if key_name == 'guestinfo.ilio.poweruserp':
			ilio_poweruser_p=key_value
		if key_name == 'Atlas-JSON':
			myjson=key_value
		if key_name.startswith('Atlas-JSON-'):
			x = key_name.split('-')[2]
			myjson_list.append((int(x), key_value))

# TODO : KARTIK : Refactor this to load JSON data from local file if it
# exists, and refactor the JSON write and load functions accordingly
	if len(myjson_list) > 0:
		myjson_list.sort(key = lambda tup : tup[0])
		for i, (x, key_value) in enumerate(myjson_list):
			myjson += key_value

	# Write any received JSON to file
	if myjson is None or not myjson:
		debug("ERROR : Get VM settings data : Failed to get USX config data from VM environment, not writing local USX config file!")
		return False

	debug("INFO : Get VM settings data : Writing USX config data received from VM environment to local USX config file...")
	write_atlas_json_file(myjson)
	return True



'''
Parameters: None

Returns:
    True : Got vApp settings and made initial settings
    False : Failed getting vApp properties and making settings
'''

def get_ovf_vapp_settings_hyperv():
	# Define our globals
	global ilio_hostname
	global ilio_version
	global ilio_ramdisk_size
	global eth0_mode
	global eth0_ip_address
	global eth0_netmask
	global eth0_gateway
	global eth1_mode
	global eth1_ip_address
	global eth1_netmask
	global eth1_gateway
	global eth2_mode
	global eth2_ip_address
	global eth2_netmask
	global eth2_gateway
	global eth3_mode
	global eth3_ip_address
	global eth3_netmask
	global eth3_gateway
	global datastore_type
	global data_disk
	global tear_down
	global ilio_function_type
	global ilio_poweruser_p
	global ilio_timezone
	global myjson
	global myrole
	global myha_enabled
	global myfullrole
	global amc_ip
	global et_root_exists

	KVP_FILE_LOCATION="/var/lib/hyperv/.kvp_pool_0"
	VAPP_FILE_LOCATION="/etc/ilio/vapp.json"

	values=""
	key=""
	value=""

	#with open(".kvp_pool_0") as file:	# Use file to refer to the file object
	kvp_file = open(KVP_FILE_LOCATION, "r")
	while True:
		key = kvp_file.read(512)
		os.system('echo $key | sed "s/\\x0//g" | sed "s/Test//g"  ')
		if key == '':
			break
		value = kvp_file.read(2048)
		for char in value:
			if char == '\0':
				continue
			values += char

	kvp_file.close()

	hyper_file = open(VAPP_FILE_LOCATION, "w")
	hyper_file.write(values)
	hyper_file.close()


	try:
		myfile = open(VAPP_FILE_LOCATION, 'r')
		data1 = myfile.read()
		myfile.close()
	except:
		debug('ERROR : Failed opening JSON file to read config data, cannot continue!')
		sys.exit(2)

	if data1 is None or not data1:
		debug( 'ERROR : No data available in Atlas json file, exiting')
		sys.exit(91)

	vapp_dict = json.loads(data1)
	if vapp_dict is None or not vapp_dict:
		debug( 'ERROR : No JSON data read from vapp json file, exiting')

	et_root_exists = True
        #debug( 'data1 is %s' % data1)
	#print json.dumps(vapp_dict['atlas-Json'])


	if vapp_dict.has_key("guestinfo.ilio.hostname"):
		ilio_hostname= vapp_dict["guestinfo.ilio.hostname"]
	if vapp_dict.has_key("guestinfo.ilio.ilio_version"):
		ilio_version=vapp_dict["guestinfo.ilio.ilio_version"]
	if vapp_dict.has_key("guestinfo.ilio.ramdisk_size"):
		ilio_ramdisk_size=vapp_dict["guestinfo.ilio.ramdisk_size"]

	if vapp_dict.has_key("guestinfo.ilio.eth0_mode"):
		eth0_mode=vapp_dict["guestinfo.ilio.eth0_mode"]
	if vapp_dict.has_key("guestinfo.ilio.eth0_ipaddress"):
		eth0_ip_address=vapp_dict["guestinfo.ilio.eth0_ipaddress"]
	if vapp_dict.has_key("guestinfo.ilio.eth0_netmask"):
		eth0_netmask=vapp_dict["guestinfo.ilio.eth0_netmask"]
	if vapp_dict.has_key("guestinfo.ilio.eth0_gateway"):
		eth0_gateway=vapp_dict["guestinfo.ilio.eth0_gateway"]
	stornet = ''
	if vapp_dict.has_key("guestinfo.ilio.eth0_storagenetwork"):
		stornet=vapp_dict["guestinfo.ilio.eth0_storagenetwork"]
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth0 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth0')

	if vapp_dict.has_key("guestinfo.ilio.eth1_mode"):
		eth1_mode=vapp_dict["guestinfo.ilio.eth1_mode"]
	if vapp_dict.has_key("guestinfo.ilio.eth1_ipaddress"):
		eth1_ip_address=vapp_dict["guestinfo.ilio.eth1_ipaddress"]
	if vapp_dict.has_key("guestinfo.ilio.eth1_netmask"):
		eth1_netmask=vapp_dict["guestinfo.ilio.eth1_netmask"]
	if vapp_dict.has_key("guestinfo.ilio.eth1_gateway"):
		eth1_gateway=vapp_dict["guestinfo.ilio.eth1_gateway"]
	stornet = ''
	if vapp_dict.has_key("guestinfo.ilio.eth1_storagenetwork"):
		stornet=vapp_dict["guestinfo.ilio.eth1_storagenetwork"]
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth1 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth1')

	if vapp_dict.has_key("guestinfo.ilio.eth2_mode"):
		eth2_mode=vapp_dict["guestinfo.ilio.eth2_mode"]
	if vapp_dict.has_key("guestinfo.ilio.eth2_ipaddress"):
		eth2_ip_address=vapp_dict["guestinfo.ilio.eth2_ipaddress"]
	if vapp_dict.has_key("guestinfo.ilio.eth2_netmask"):
		eth2_netmask=vapp_dict["guestinfo.ilio.eth2_netmask"]
	if vapp_dict.has_key("guestinfo.ilio.eth2_gateway"):
		eth2_gateway=vapp_dict["guestinfo.ilio.eth2_gateway"]
	stornet = ''
	if vapp_dict.has_key("guestinfo.ilio.eth2_storagenetwork"):
		stornet=vapp_dict["guestinfo.ilio.eth2_storagenetwork"]
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth2 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth2')

	if vapp_dict.has_key("guestinfo.ilio.eth3_mode"):
		eth3_mode=vapp_dict["guestinfo.ilio.eth3_mode"]
	if vapp_dict.has_key("guestinfo.ilio.eth3_ipaddress"):
		eth3_ip_address=vapp_dict["guestinfo.ilio.eth3_ipaddress"]
	if vapp_dict.has_key("guestinfo.ilio.eth3_netmask"):
		eth3_netmask=vapp_dict["guestinfo.ilio.eth3_netmask"]
	if vapp_dict.has_key("guestinfo.ilio.eth3_gateway"):
		eth3_gateway=vapp_dict["guestinfo.ilio.eth3_gateway"]
	stornet = ''
	if vapp_dict.has_key("guestinfo.ilio.eth3_storagenetwork"):
		stornet=vapp_dict["guestinfo.ilio.eth3_storagenetwork"]
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth3 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth3')

	if vapp_dict.has_key("guestinfo.ilio.datastore_type"):
		datastore_type=vapp_dict["guestinfo.ilio.datastore_type"]
	if vapp_dict.has_key("guestinfo.ilio.data_disk"):
		data_disk=vapp_dict["guestinfo.ilio.data_disk"]
	if vapp_dict.has_key("guestinfo.ilio.function_type"):
		ilio_function_type=vapp_dict["guestinfo.ilio.function_type"]
	if vapp_dict.has_key("guestinfo.ilio.timezone"):
		ilio_timezone=vapp_dict["guestinfo.ilio.timezone"]
	if vapp_dict.has_key("guestinfo.ilio.poweruserp"):
		ilio_poweruser_p=vapp_dict["guestinfo.ilio.poweruserp"]
	if vapp_dict.has_key("Atlas-JSON"):
		tmpjson=json.loads(vapp_dict['Atlas-JSON'])
		myjson=json.dumps(tmpjson)

	# Write any received JSON to file
	if myjson is None or not myjson:
		debug("ERROR : Get VM settings data : Failed to get USX config data from VM environment, not writing local USX config file!")
		return False

	debug("INFO : Get VM settings data : Writing USX config data received from VM environment to local USX config file...")
	write_atlas_json_file(myjson)
	os.system('rm /etc/ilio/vapp.json')
	return True



def get_ovf_vapp_settings_xen():
	global ilio_hostname
	global ilio_version
	global ilio_ramdisk_size
	global eth0_mode
	global eth0_ip_address
	global eth0_netmask
	global eth0_gateway
	global eth1_mode
	global eth1_ip_address
	global eth1_netmask
	global eth1_gateway
	global eth2_mode
	global eth2_ip_address
	global eth2_netmask
	global eth2_gateway
	global eth3_mode
	global eth3_ip_address
	global eth3_netmask
	global eth3_gateway
	global datastore_type
	global data_disk
	global tear_down
	global ilio_function_type
	global ilio_poweruser_p
	global ilio_timezone
	global myjson
	global myrole
	global myha_enabled
	global myfullrole
	global amc_ip
	global et_root_exists
	atlas_json = ""
	tmpjson = ""

	debug('Running Xen Bootstrap')

	# Required XenStore entries
	xenstore_entries = check_output(['/usr/bin/xenstore-list', 'vm-data']).rstrip('\n').split('\n')

	if "hostname" in xenstore_entries:
		ilio_hostname= check_output(['/usr/bin/xenstore-read', 'vm-data/hostname']).rstrip('\n')
	if "eth0Mode" in xenstore_entries:
		eth0_mode= check_output(['/usr/bin/xenstore-read', 'vm-data/eth0Mode']).rstrip('\n')
	if "eth0Ipaddress" in xenstore_entries:
		eth0_ip_address= check_output(['/usr/bin/xenstore-read', 'vm-data/eth0Ipaddress']).rstrip('\n')
	if "eth0Netmask" in xenstore_entries:
		eth0_netmask= check_output(['/usr/bin/xenstore-read', 'vm-data/eth0Netmask']).rstrip('\n')
	if "eth0Gateway" in xenstore_entries:
		eth0_gateway= check_output(['/usr/bin/xenstore-read', 'vm-data/eth0Gateway']).rstrip('\n')
	if "eth1Mode" in xenstore_entries:
		eth1_mode= check_output(['/usr/bin/xenstore-read', 'vm-data/eth1Mode']).rstrip('\n')
	if "eth1Ipaddress" in xenstore_entries:
		eth1_ip_address= check_output(['/usr/bin/xenstore-read', 'vm-data/eth1Ipaddress']).rstrip('\n')
	if "eth1Netmask" in xenstore_entries:
		eth1_netmask= check_output(['/usr/bin/xenstore-read', 'vm-data/eth1Netmask']).rstrip('\n')
	if "eth1Gateway" in xenstore_entries:
		eth1_gateway= check_output(['/usr/bin/xenstore-read', 'vm-data/eth1Gateway']).rstrip('\n')
	if "eth2Mode" in xenstore_entries:
		eth2_mode= check_output(['/usr/bin/xenstore-read', 'vm-data/eth2Mode']).rstrip('\n')
	if "eth2Ipaddress" in xenstore_entries:
		eth2_ip_address= check_output(['/usr/bin/xenstore-read', 'vm-data/eth2Ipaddress']).rstrip('\n')
	if "eth2Netmask" in xenstore_entries:
		eth2_netmask= check_output(['/usr/bin/xenstore-read', 'vm-data/eth2Netmask']).rstrip('\n')
	if "eth2Gateway" in xenstore_entries:
		eth2_gateway= check_output(['/usr/bin/xenstore-read', 'vm-data/eth2Gateway']).rstrip('\n')
	if "eth3Mode" in xenstore_entries:
		eth3_mode= check_output(['/usr/bin/xenstore-read', 'vm-data/eth3Mode']).rstrip('\n')
	if "eth3Ipaddress" in xenstore_entries:
		eth3_ip_address= check_output(['/usr/bin/xenstore-read', 'vm-data/eth3Ipaddress']).rstrip('\n')
	if "eth3Netmask" in xenstore_entries:
		eth3_netmask= check_output(['/usr/bin/xenstore-read', 'vm-data/eth3Netmask']).rstrip('\n')
	if "eth3Gateway" in xenstore_entries:
		eth3_gateway= check_output(['/usr/bin/xenstore-read', 'vm-data/eth3Gateway']).rstrip('\n')
	if "timezone" in xenstore_entries:
		ilio_timezone= check_output(['/usr/bin/xenstore-read', 'vm-data/timezone']).rstrip('\n')
	if "poweruserp" in xenstore_entries:
		ilio_poweruser_p= check_output(['/usr/bin/xenstore-read', 'vm-data/poweruserp']).rstrip('\n')

	if "eth0Storagenework" in xenstore_entries:
		stornet= check_output(['/usr/bin/xenstore-read', 'vm-data/eth0Storagenework']).rstrip('\n')
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth0 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth0')
	if "eth1Storagenework" in xenstore_entries:
		stornet= check_output(['/usr/bin/xenstore-read', 'vm-data/eth1Storagenework']).rstrip('\n')
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth1 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth1')
	if "eth2Storagenework" in xenstore_entries:
		stornet= check_output(['/usr/bin/xenstore-read', 'vm-data/eth2Storagenework']).rstrip('\n')
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth2 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth2')
	if "eth3Storagenework" in xenstore_entries:
		stornet= check_output(['/usr/bin/xenstore-read', 'vm-data/eth3Storagenework']).rstrip('\n')
		if str(stornet) == '1':
			debug("INFO : Bootstrap : eth3 is defined to be on the Storage Network, adding it to storage network interface list")
			storage_network_list.append('eth3')

	jsonSplit = False

	try:
		datastore_type= check_output(['/usr/bin/xenstore-read', 'vm-data/AtlasJSONisSplit']).rstrip('\n')
		debug("AtlasJSON is stored in fragments. Going to reconstruct")
		jsonSplit = True
	except:
		pass

	if jsonSplit == False:
		tmpjson= check_output(['/usr/bin/xenstore-read', 'vm-data/AtlasJSON']).rstrip('\n')
	else:
		i = 0
		bContinue = True
		print "First one is: " + check_output(['/usr/bin/xenstore-read', 'vm-data/AtlasJSON-'+str(i)]).rstrip('\n')
		while (bContinue):
			try:
				tmpjson += check_output(['/usr/bin/xenstore-read', 'vm-data/AtlasJSON-'+str(i)]).rstrip('\n').replace('\\\\','\\')
				i = i + 1
			except:
				bContinue = False

		print "Reconstructed tmpjson = " + tmpjson

	# Optional entries
	#Do we do this or do you place the JSON in /etc/ilio?
	stornet = '1'
	try:
		datastore_type= check_output(['/usr/bin/xenstore-read', 'vm-data/datastoreType']).rstrip('\n')
	except:
		debug("xenstore did not contain optional field datastoreType")

	try:
		data_disk= check_output(['/usr/bin/xenstore-read', 'vm-data/dataDisk']).rstrip('\n')
	except:
		debug("xenstore did not contain optional field dataDisk")

	try:
		ilio_function_type= check_output(['/usr/bin/xenstore-read', 'vm-data/functionType']).rstrip('\n')
	except:
		debug("xenstore did not contain optional field fucntionType")

	try:
		ilio_version= check_output(['/usr/bin/xenstore-read', 'vm-data/version']).rstrip('\n')
	except:
		debug("xenstore did not contain optional field version")

	try:
		ilio_ramdisk_size= check_output(['/usr/bin/xenstore-read', 'vm-data/ramdiskSize']).rstrip('\n')
	except:
		debug("xenstore did not contain optional field ramdiskSize")

	myjson = tmpjson

	et_root_exists = True

	debug("INFO : Get VM settings data : Writing USX config data received from VM environment to local USX config file...")
	write_atlas_json_file(myjson)
	#os.system('rm /etc/ilio/vapp.json')
	return True



'''
Get the USX-specific config data from the config JSON. If our global "myjson"
is empty due to no vApp properties, attempt to read it from the local JSON
file defined in the global "JSON_FILE_LOCATION"

TISILIO-3016

KARTIK : 17-Jul-2014 : Reworked JSON load for new HA config in USX 2.0
In USX 2.0, the HA config is written to /etc/ilio/atlas.json AFTER the
initial load of the JSON data from the vApp properties. Thus, in this
function, we only use the JSON data from the vApp properties if the
local JSON data does NOT exist.

Parameters : None

Returns:
	True : Able to get JSON data from vApp properties or Local JSON file
	False : Failed to get JSON from vApp properties or Local JSON file
'''
def get_bootstrap_json_data():
	global myjson
	ljsonstr = ""
	myjsonlocal = ""
	loaded_locally = False

	if myjson is None or not myjson:
		debug("WARNING : Get bootstrap JSON data : No USX JSON data found in VM environment configuration.")
	else:
		debug("INFO : Get bootstrap JSON data : USX JSON data found in environment.")


	debug("INFO : Get bootstrap JSON data : Checking if local USX JSON data exists. If it does, the local data will take precedence over the data received from the VM environment.")

	if os.path.isfile(JSON_FILE_LOCATION):
		debug("INFO : Get bootstrap JSON data : Local JSON data exists, attempting to load local USX JSON config file...")
		try:
			debug("    Attempting to open local USX JSON config file and load data from it...")
			ljsonstr = ""
			with open(JSON_FILE_LOCATION) as jfile:
				ljsonstr = jfile.read()
			if ljsonstr is None or not ljsonstr:
				debug("ERROR : Get bootstrap JSON data : Local JSON data file exists, but failed to load the data from it. We will use the data from the VM environment!")
				loaded_locally = False
			else:
				myjsonlocal = ljsonstr.strip()

			if myjsonlocal:
				debug("INFO : Get bootstrap JSON data : Found local JSON config data.")
				loaded_locally = True
			else:
				debug("ERROR : Get bootstrap JSON data : Found local JSON config file, but could not load data from it.")
		except:
			debug("ERROR : Get bootstrap JSON data : EXCEPTION getting local JSON data if any!")
			loaded_locally = False
			myjsonlocal = False
	else:
		# No Local JSON data exists.
		debug("INFO : Get bootstrap JSON data : NO Local JSON data exists.")

	### If we got here, we ought to have the JSON data, if it exists either locally or in the vApp environment

	# Check if we need to overwrite the vApp JSON with the data from the local JSON
	if loaded_locally and myjsonlocal:
		debug("INFO : Get bootstrap JSON data : Local JSON data exists, using this local data in preference to any existing JSON data from the VM environment.")
		myjson = myjsonlocal
	else:
		debug("INFO : Get bootstrap JSON data : NO Local JSON data exists or was loaded.")

	# Sanity check
	debug("INFO : Get bootstrap JSON data : Sanity checking JSON data...")
	if myjson is None or not myjson:
		debug("ERROR : Get bootstrap JSON data : FAILED Sanity checking JSON data. This indicates that we failed to load it from the VM environment AND locally. This is a problem!")
		return False

	debug("INFO : Get bootstrap JSON data : Sanity checking JSON data SUCCEEDED.")
	return True


"""
Get composite volume container vo (including volume resource) from USX Manager
  and also overwrite the existing /etc/ilio/atlas.json since it probabally contains
  incorrect information; retrieved it from grid due to failover may trigger resource
  to move, cannot rely on the static Atlas-JSON from vApp properties

Note:
  Write the retrieved JSON to local atlas.json file so that any scripts relies on
  /etc/ilio/atlas.json can function properly (such as /opt/milio/libs/atlas/status_update.py)

Return myjson format: similar to the ATLAS-JSON set in vApp properties
"""
def get_json_from_amc_and_write_local_file():
	global myjson
	global usxmanagerurl
	global iliouuid
	global iliorole

# 	if os.path.isfile(GRID_MEMBERS):
# 		debug("INFO : Get USX Manager URLs : attempting to load local grid members json file...")
# 		try:
# 			ljsonstr = ""
# 			with open(GRID_MEMBERS) as jfile:
# 				ljsonstr = jfile.read()
# 				if ljsonstr is None or not ljsonstr:
# 					debug("ERROR : Get USX Manager URLs : Local grid members json file exists, but failed to load the data from it. We will use the data from the VM environment!")
# 				else:
# 					gridmemberjson = ljsonstr.strip()
# 		except:
# 			debug("ERROR : Get USX Manager URLs : EXCEPTION getting local JSON data if any!")
#
# 	if gridmemberjson:
# 		gridmemberjson_dict = json.loads(gridmemberjson)
# 		pprint(gridmemberjson_dict['ipaddress'])

	if iliorole.upper() == 'SERVICE_VM':
		apistr = "/usx/inventory/servicevm/containers/" + iliouuid + "?composite=true&api_key=" + iliouuid
	if iliorole.upper() == 'VOLUME':
		apistr = "/usx/inventory/volume/containers/" + iliouuid + "?composite=true&api_key=" + iliouuid
  	debug(usxmanagerurl + apistr)

	retry_num = 5
	retry_interval_time = 10
	cnt = 0
	while cnt < retry_num:
		conn = urllib2.Request(usxmanagerurl + apistr)
		try:
			res = urllib2.urlopen(conn, timeout=10)
		except Exception as e: # API invocation exception, retry
			cnt += 1
			debug("Exception caught: retry count: %d" % cnt)
			time.sleep(retry_interval_time)
			continue

		if res.code != 200: # API invocation did not return correctly, retry
			cnt += 1
			debug("ERROR : REST API invocation failed, retry count: %d" % cnt)
			time.sleep(retry_interval_time)
			continue
		else:
			retJson = json.load(res)
			if retJson.has_key('data'):
				tmpjson = retJson['data']
				#myjson=json.dumps(tmpjson, indent=4, separators=(',', ': '))
				myjson = json.dumps(tmpjson)
				pprint(myjson)

				# overwrite the local atlas json since it has incorrect data that got us here
				try:
					with open(JSON_FILE_LOCATION, 'w') as f:
						f.write(myjson)
					debug('INFO : Write JSON file : SUCCESSFULLY wrote JSON received from USX Manager to %s and will use this local data in subsequent calls to load/get the JSON config.' % JSON_FILE_LOCATION)
				except:
					debug('WARNING : Write JSON file : EXCEPTION writing json data to %s - JSON data might not have been saved properly on the local system!' % JSON_FILE_LOCATION)

			break

	return myjson
'''
Set the initial USX data from the JSON config.

Refactored due to TISILIO-3016

Parameters:
	None

Returns:
	Nothing
'''
def set_initial_atlas_bootstrap_data():
	global myjson
	global myha_enabled
	global amc_ip
	global myrole

	# From the JSON, get the role for this USX Node
	myrole = get_role_from_json(myjson)
	debug("ROLE from JSON: %s" % myrole)
	if myrole is None or myrole == 'None':
		debug("WARNING: Unable to obtain role from local file, retrieve the configuration data from grid")
		myjson = get_json_from_amc_and_write_local_file()
		if myjson:
			debug("Configuration data retrieved from the grid, proceed to parse the bootstrap data")
		else:
			debug("ERROR: No configuration data retrieved from the grid! Bootstrap will fail!")
		myrole = get_role_from_json(myjson)
		debug("ROLE from JSON (grid): %s" % myrole)

	myha_enabled = is_ha_enabled(myjson)
	debug('HA Enabled from JSON: %s' % myha_enabled)
	amc_ip = get_amc_ip_from_json(myjson)
	debug("Using USX Manager IP parsed from JSON: "+amc_ip)

	debug('Performing USX bootstrap...')
	if myrole == 'vdi':
		if 'free' in myfullrole:
			os.system('splash.sh "Performing USX Free Trial bootstrap..."')
		else:
			os.system('splash.sh "Performing USX VDI bootstrap"')
	else:
		os.system('splash.sh "Performing USX bootstrap..."')
	#debug("Removing any existing Atlas job ID file...")
	#delete_jobid_file()

def check_ip_conflict(interface, ip):
	rc = 0
	if not os.path.isfile("/usr/sbin/arping"):
		debug("arping not found, skip ip conflict check.")
		return 0

	ipup_cmd_str = "ifconfig  %s up" % (interface)
	rc, msg = runcmd(ipup_cmd_str, print_ret=True)
	if rc == 0:
		debug("ifconfig %s up is successful!" % ( interface))
		debug(msg)
	else:
		debug("error on ifconfig %s up!" % ( interface))
		debug(msg)
		return rc

	# arping wait time less than 1 second doesn't work, bug in arping?
	# TODO: arping 2.15 fixed this issue, should upgrade arping.
	cmd_str = "/usr/sbin/arping -r -0 -w 1000000 -c 2 -i %s %s" % (interface, ip)
	rc, msg = runcmd(cmd_str, print_ret=True)
	if rc == 0:
		debug("ip conflict detected: %s already exist on interface: %s!" % (ip, interface))
		debug(msg)
		rc = 1
	else:
		debug("ip: %s not found on interface: %s. rc: %d." % (ip, interface, rc))
		debug(msg)
		rc = 0


	return rc

def exit_on_ip_conflict(interface, ip):
	rc = check_ip_conflict(interface, ip)
	if rc != 0:
		msgstr = "ERROR : IP conflict detected for interface: %s ip: %s, BOOTSTRAP ABORTED!" % (interface, ip)
		debug(msgstr)
		send_status("CONFIGURE", 3, 1, "Bootstrap", ' Error IP conflict detected when Configuring %s in static mode with IP Address %s' % (interface, ip))
		os.system('splash.sh "'+msgstr+'"')
		sys.exit(64)
	return 0


def check_hypervisor_type():
        hypervisor_type=""
        ret, output = runcmd('dmidecode -s system-manufacturer', print_ret = True)

        if (ret != 0) or (output is None) or (not output) or (len(output) <= 0):
                debug('WARNING could not get hypervisor_type from dmidecode. Checking for Xen...')
                if os.path.exists('/dev/xvda') == True:
                        hypervisor_type='Xen'
                elif os.path.exists('/dev/sda') == True:
                        hypervisor_type='VMware'
        else:
                output=output.strip()
                if 'Microsoft' in output:
                        hypervisor_type='hyper-v'
                elif 'VMware' in output:
                        hypervisor_type='VMware'
                elif 'Xen' in output:
                        hypervisor_type='Xen'
                else:
                        debug('WARNING do not support hypervisor_type %s' % output)

        return hypervisor_type

"""
Purpose:  Checks if this VM is a Volume and is VVOL.  If so, then calls the
          API to set VVOL.  Otherwise, if this is not a Volume or is not VVOL,
          then do nothing.

Parameters: (none)

Output:  Returns 0 if successful. (i.e. VVOL turned on, or VVOL was not
         needed.)
         Returns 1 otherwise. (i.e. API call fails.)
"""
def set_vvol():

    # variables
    global myjson
    retJson = ''
    fulljson = ''
    volresjson = ''
    myrole = ''
    voluuid = ''
    amcurl = ''

    debug('Checking VVOL Status')

    # first determine if this is a Volume
    myrole = get_role_from_json(myjson)
    if myrole.lower() == 'volume':
        debug('This VM is a Volume.  Checking vvol value.')
        # determine if this Volume has VVOL
        fulljson = json.loads(myjson)
        debug(str(fulljson))
        # fix for TISILIO-8553 - HA Standby Nodes have no volumeresources
        # therefore, only make this assignment if there's something here
        # otherwise, leave it blank
        if fulljson['volumeresources']:
            volresjson = fulljson['volumeresources'][0]

        if 'vvol' in volresjson:
            if volresjson['vvol'] == True:
                debug('Setting VVOL for Volume')
                voluuid = volresjson['uuid']

                amcurl = fulljson['usx']['usxmanagerurl']

                # updated URL for TISILIO-8545
                apistr = '/usx/inventory/volume/resources/' + voluuid +'/vvol?isvvol=true&cleanup=false&api_key=' + voluuid

                debug('Calling url: ', amcurl, apistr)

                try:
                    # Reverting back to urllib2 for now
                    # response = requests.put(amcurl + apistr, verify=False, timeout=60)
                    # NO 10 SECOND TIMEOUT!
                    # NO RETRIES - retries will just cause errors
                    conn = urllib2.Request(amcurl+apistr)
                    conn.get_method = lambda: 'PUT'
                    response = urllib2.urlopen(conn)

                except Exception as e:
                    debug('ERROR - Exception caught: ', e)
                    return 1

                if response.code == 200:
                    # VVOL successfully set
                    debug('VVOL successfully set for this Volume.')
                    #debug(str(res))
                    return 0

                else:
                    debug('ERROR - API error: ', str(response.code))
                    debug('Response Body: ', str(response.read()))
                    return 1

            else:
                debug('Volume is not VVOL - Nothing to do.')
                return 0

        else:
            debug('Volume is not VVOL - Nothing to do.')
            return 0

    else:
        debug('Not a Volume - Nothing to do.')
        return 0

"""
Purpose: Calls the License API to check if there is adequate capacity available
         according to the license(s) on the USX Manager to allow this volume to
         be mounted or not.

         [TISILIO-8282]
         By default, this method will contact the USX DB Master.  However if
         that machine is unreachable (e.g. powered off) then the method will
         try contacting the remaining USX Managers via the agent on the Volume
         itself to find out if there is unused licensed space to allow the
         Volume to boot up.

         If the agent is unreachable, or returns an error, or the space
         check fails, then this function returns False (Fail)


Parameters: (none)

Output:  Returns True if the license check passes (adequate capacity exists)
         Returns False if the license check fails (not enough capacity left)
"""
def check_capacity_license():
    # variables
    global myjson
    retVal = False
    retJson = ''
    needToTryAgent = False
    agenturl = ''

    myrole = get_role_from_json(myjson)
    debug("Checking license.")
    if myrole.lower() == "service vm": # skip capacity check for service vms
        debug("This is a service vm, skip license capacity check...")
        retVal = True
    else: # volume, check licensed capacity
        fulljson = json.loads(myjson)
        if fulljson.has_key('usx'):
            if fulljson['usx']:
                containeruuid = fulljson['usx']['uuid']
            amcurl = fulljson['usx']['usxmanagerurl']

        apistr = '/usx/license/capacityinfo/volume/container/' + containeruuid +'?api_key=' + containeruuid
        debug(amcurl + apistr)

        retry_num = 5
        retry_interval_time = 10
        cnt = 0
        exception_time = 0
        while cnt < retry_num:
            conn = urllib2.Request(amcurl + apistr)
            try:
                res = urllib2.urlopen(conn, timeout=10)
            except Exception as e:
                cnt += 1
                exception_time += 1
                debug("Exception caught: retry count: %d" % cnt)
                time.sleep(retry_interval_time)
                # Timeout means the Agent was unreachable - need to check the Agent
                needToTryAgent = True
                continue

            if res.code != 200:
                cnt += 1
                debug("ERROR : REST API invocation failed, retry count: %d" % cnt)
                time.sleep(retry_interval_time)
                # Since the USX Manager DB Host responded, there is no need to check with the Agent.
                needToTryAgent = False
                continue
            else:
                # The USX Manager with the DB Host has responded properly, so there's
                # no need to also check the Agent
                needToTryAgent = False

                retJson = json.load(res)
                if retJson.has_key("totalLicensedCapacity"):
                    # When volume boots, it notifies the AMC, which updates the remaining
                    # capacity.
                    # Therefore, when this check is made, there must be 0 or more capacity
                    # remaining to be allowed to boot.
                    if retJson["totalLicensedCapacity"] <= 0:
                        debug('ERROR : Insufficient licensed Capcity: ' + str(retJson["totalLicensedCapacity"]))
                    else:
                        debug("Total licensed capacity: " + str(retJson['totalLicensedCapacity']))
                        debug("Proceed with bootstrap...")
                        retVal = True
                    break # break the retry while loop
                else:
                    # Got API success but no JSON so something bad happened
                    debug('Return JSON object missing capacity information.')

                res.close()

        if needToTryAgent :
            # try the above the call, but use the agent API instead
            agenturl = 'http://127.0.0.1:8080'

            #apistr = '/usxmanager/license/capacityinfo/volume/container/' + containeruuid +'?api_key=' + containeruuid
            apistr = '/usxmanager/license/capacityinfo/volume/container/' + containeruuid

            debug("Trying Agent API instead")
            debug(agenturl + apistr)

            retry_num = 5
            retry_interval_time = 10
            cnt = 0
            while cnt < retry_num:
                conn = urllib2.Request(agenturl + apistr)
                try:
                    res = urllib2.urlopen(conn, timeout=10)
                except Exception as e:
                    cnt += 1
                    exception_time += 1
                    debug("Exception caught: retry count: %d" % cnt)
                    time.sleep(retry_interval_time)
                    continue

                if res.code != 200:
                    cnt += 1
                    debug("ERROR : AGENT REST API invocation failed, retry count: %d" % cnt)
                    time.sleep(retry_interval_time)
                    continue
                else:
                    retJson = json.load(res)
                    if retJson.has_key("totalLicensedCapacity"):
                        # When volume boots, it notifies the AMC, which updates the remaining
                        # capacity.
                        # Therefore, when this check is made, there must be 0 or more capacity
                        # remaining to be allowed to boot.
                        if retJson["totalLicensedCapacity"] <= 0:
                            debug('ERROR : Insufficient licensed capacity: ' + str(retJson["totalLicensedCapacity"]))
                        else:
                            debug("Total licensed capacity: " + str(retJson['totalLicensedCapacity']))
                            debug("Proceed with bootstrap...")
                            retVal = True
                            break # break the retry while loop
                    else:
                        # Got API success but no JSON so something bad happened
                        debug('Return JSON object missing capacity information.')

                    res.close()

            # USX-58716, skip check license if USX Manager is disconnected
            if exception_time == 10:
                debug('Catch exception when checking license for 10 times, USX Manager should be down, skip check license')
                retVal = True
    return retVal

def get_deployment_jobid():
# From the JSON, get deployment jobid
# Returns:deployment job id
# Fix bug USX-72162: Combine deployment job with bootstrap job to be a single job
    cmd = 'cat %s | grep deploymentjobid| sed \'/deploymentjobid/ s/.* "\(.*\)".*/\\1/\'' % JSON_FILE_LOCATION
    rc, msg = runcmd(cmd, print_ret=True)
    if rc == 0:
        jobid = msg.replace('\n', '')
    else:
        jobid = None
    return jobid

def update_reboot_status():
    debug("***START: update reboot status")
    rtn = True
    cmd = ""
    try:
        fulljson = json.loads(myjson)
        if fulljson.has_key('usx'): # this is a volume
            if len(fulljson['volumeresources']) > 0:
                resourceuuid = fulljson['volumeresources'][0]['uuid']
            if resourceuuid != "":
                cmd = 'curl -k -X PUT -H "Content-Type:text/plain" %s/usxmanager/usx/inventory/volume/resources/%s/reboot?reboot=true' % (LOCAL_AGENT, resourceuuid)
        else: #this is svm
            contain_uuid = fulljson['uuid']
            if contain_uuid != "":
                cmd = 'curl -k -X PUT -H "Content-Type:text/plain" %s/usxmanager/usx/inventory/servicevm/containers/%s/reboot?reboot=true' % (LOCAL_AGENT,contain_uuid)
        if cmd != "":
            ret, msg = runcmd(cmd, print_ret=True)
            if ret != 0:
                rtn = False
    except Exception as err:
        debug("exception %s" % err)
        rtn = False
    debug("***END: update reboot status: %s" % rtn)
    return rtn

# Main logic starts here
debug('======= STARTING USX Bootstrap =======')
os.system('splash.sh "Initializing bootstrap..."')
hypervisor_type=check_hypervisor_type()
debug('hypervisor_type is %s' % hypervisor_type)

# set HA flag for sending status update
ha_flag = False

# get the OVF vApp settings
if hypervisor_type == 'VMware':
	vsets = get_ovf_vapp_settings()
elif hypervisor_type == 'hyper-v':
	debug('Running setup for Hyper-V VM')
	vsets = get_ovf_vapp_settings_hyperv()
elif hypervisor_type == 'Xen':
	debug('Running setup for XenServer VM')
	vsets = get_ovf_vapp_settings_xen()
else:
        sys.exit(100)

# stop corosync service now then start it later at appropriate time
ret = os.system("service corosync stop")

# Create forcefsck flag
if not os.path.isfile('/forcefsck'):
    os.mknod('/forcefsck')

# Check /usr/local/bin/fsck, if yes, remove it.
fsck_file = '/usr/local/bin/fsck'
if os.path.isfile(fsck_file):
    debug('Remove %s file' % fsck_file)
    os.remove(fsck_file)

# Check FSCKFIX flag on /etc/default/rcS
file_rcS = '/etc/default/rcS'
file_fd1 = open(file_rcS, 'r')
file_r = file_fd1.read()
file_str = str(file_r).split('\n')
file_fd1.close

rt = 0
for line in file_str:
    rt_type1 = re.search('FSCKFIX', line)
    if rt_type1:
        rt += 1
        rt_type2 = re.search('#|no',line)
        if rt_type2:
            os.system('sed -i s/"%s"/FSCKFIX=yes/ /etc/default/rcS' %line)

if 0 == rt:
    file_fd2 = open(file_rcS, 'a')
    file_fd2.write('FSCKFIX=yes\n')
    file_fd2.close

if os.path.exists('/usr/share/ilio/configured') == False:
	# This USX Node has not been configured. Configure it.
	debug("Removing any existing Atlas job ID file...")
	delete_jobid_file()
        # Fix bug USX-72162: Combine deployment job with bootstrap job to be a single job
        deployment_jobid = get_deployment_jobid()
        if deployment_jobid:
            write_jobid_to_file(deployment_jobid)
        #write_jobid_to_file(deployment_jobid, True)

	##### TISILIO-3016
	# Failed to find the required config data. If the USX Node is unconfigured, we ABSOLUTELY need the JSON from the vApp properties
	if not vsets and not et_root_exists:
		debug("ERROR : Bootstrap init : USX node not configured, and no config details found in VM environment. CANNOT CONFIGURE!")
		send_status("CONFIGURE", 51, 1, "Bootstrap", ' Error initializing bootstrap for unconfigured USX node.')
		os.system('splash.sh "ERROR : Failed to initialize bootstrap for unconfigured USX node, BOOTSTRAP ABORTED!"')
		rc = send_bootstrap_status()
		if rc != 0:
			debug("ERROR : Sending bootstrap status to usx manager failed!")
		sys.exit(58)

	if myjson is None or not myjson:
		# If we don't have the JSON data from the vApp properties, and this USX Node is NOT configured, it is an error.
		# In this case, WE DO NOT read any existing local JSON file EVEN IF IT EXISTS.
		debug("ERROR : Bootstrap init : USX node not configured, and no USX role details found in VM environment. CANNOT CONFIGURE!")
		send_status("CONFIGURE", 52, 1, "Bootstrap", ' Error getting bootstrap JSON data for unconfigured USX node.')
		os.system('splash.sh "ERROR : Failed to get bootstrap JSON data for unconfigured USX node, BOOTSTRAP ABORTED!"')
		rc = send_bootstrap_status()
		if rc != 0:
			debug("ERROR : Sending bootstrap status to usx manager failed!")
		sys.exit(59)

	set_initial_atlas_bootstrap_data()

	# USX-71137 Change hyperconverged volume crash location.
	if is_hyperconverged():
		change_crash_file_location()

	#configure the network interfaces first
	# TODO : Refactor the network interface config code to get rid of copypasta.
	debug('Performing autoconfigure for initial USX Node configuration (network, hostname etc)...')
	send_status("CONFIGURE", 51, 0, "Bootstrap", "Starting bootstrap for CONFIGURE task...")
	if eth0_mode == 'static':
		# Validate the netmask
		debug("INFO : Validating eth0 netmask "+eth0_netmask)
		eth0_nm_ret = validate_netmask(eth0_netmask)
		if eth0_nm_ret != 0:
			msgstr = "ERROR : Failed to validate eth0 netmask "+eth0_netmask+", BOOTSTRAP ABORTED!"
			debug(msgstr)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error validating eth0 netmask '+eth0_netmask+' when Configuring eth0 in static mode with IP Address ' + eth0_ip_address)
			os.system('splash.sh "'+msgstr+'"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)

		exit_on_ip_conflict("eth0", eth0_ip_address)

		cmd='ilio net add_static --interface=eth0 --address=%s --netmask=%s' % (eth0_ip_address, eth0_netmask)
                if eth0_gateway != '' and eth0_gateway != '0.0.0.0':
                    if 'eth0' not in storage_network_list or (eth1_mode == '' and eth2_mode == '' and eth3_mode == ''):
                        cmd += ' --gateway=%s --gwtype=default' % eth0_gateway
                    else:
                        cmd += ' --gateway=%s --gwtype=network' % eth0_gateway
		debug('Configuring eth0 in static mode with IP Address ' + eth0_ip_address)
		send_status("CONFIGURE", 53, 0, "Bootstrap", 'Configuring eth0 with IP Address ' + eth0_ip_address)
		ret, msg = runcmd('ilio net remove --interface=eth0', print_ret=True)
		ret, msg = runcmd(cmd, print_ret = True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth0 in static mode: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth0 in static mode with IP Address ' + eth0_ip_address)
			os.system('splash.sh "ERROR : Failed to configure eth0 in static mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth0_mode == 'dhcp':
		ret, msg = runcmd('ilio net remove --interface=eth0', print_ret=True)
		ret, msg = runcmd('ilio net add_dhcp --interface=eth0', print_ret=True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth0 in DHCP mode: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth0 in DHCP mode')
			os.system('splash.sh "ERROR : Failed to configure eth0 in DHCP mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth1_mode == 'static':
		# Validate the netmask
		debug("INFO : Validating eth1 netmask "+eth1_netmask)
		eth1_nm_ret = validate_netmask(eth1_netmask)
		if eth1_nm_ret != 0:
			msgstr = "ERROR : Failed to validate eth1 netmask "+eth1_netmask+", BOOTSTRAP ABORTED!"
			debug(msgstr)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error validating eth1 netmask '+eth1_netmask+' when Configuring eth1 with IP Address ' + eth1_ip_address)
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			os.system('splash.sh "'+msgstr+'"')
			sys.exit(64)

		exit_on_ip_conflict("eth1", eth1_ip_address)

		cmd='ilio net add_static --interface=eth1 --address=%s --netmask=%s' % (eth1_ip_address, eth1_netmask)
                if eth1_gateway != '' and eth1_gateway != '0.0.0.0':
                    if 'eth1' not in storage_network_list or (eth0_mode == '' and eth2_mode == '' and eth3_mode == ''):
                        cmd += ' --gateway=%s --gwtype=default' % eth1_gateway
                    else:
                        cmd += ' --gateway=%s --gwtype=network' % eth1_gateway
		debug( 'Configuring eth1 in static mode with IP Address ' + eth1_ip_address)
		ret, msg = runcmd('ilio net remove --interface=eth1', print_ret=True)
		ret, msg = runcmd(cmd, print_ret = True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth1: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth1 in static mode with IP Address ' + eth1_ip_address)
			os.system('splash.sh "ERROR : Failed to configure eth1 in static mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth1_mode == 'dhcp':
		ret, msg = runcmd('ilio net remove --interface=eth1', print_ret=True)
		ret, msg = runcmd('ilio net add_dhcp --interface=eth1', print_ret=True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth1 in DHCP mode: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth1 in DHCP mode')
			os.system('splash.sh "ERROR : Failed to configure eth1 in DHCP mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth2_mode == 'static':
		# Validate the netmask
		debug("INFO : Validating eth2 netmask "+eth2_netmask)
		eth2_nm_ret = validate_netmask(eth2_netmask)
		if eth2_nm_ret != 0:
			msgstr = "ERROR : Failed to validate eth2 netmask "+eth2_netmask+", BOOTSTRAP ABORTED!"
			debug(msgstr)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error validating eth2 netmask '+eth2_netmask+' when Configuring eth2 with IP Address ' + eth2_ip_address)
			os.system('splash.sh "'+msgstr+'"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)

		exit_on_ip_conflict("eth2", eth2_ip_address)

		cmd='ilio net add_static --interface=eth2 --address=%s --netmask=%s' % (eth2_ip_address, eth2_netmask)
                if eth2_gateway != '' and eth2_gateway != '0.0.0.0':
                    if 'eth2' not in storage_network_list or (eth1_mode == '' and eth0_mode == '' and eth3_mode == ''):
                        cmd += ' --gateway=%s --gwtype=default' % eth2_gateway
                    else:
                        cmd += ' --gateway=%s --gwtype=network' % eth2_gateway
		debug( 'Configuring eth2 with IP Address ' + eth2_ip_address)
		ret, msg = runcmd('ilio net remove --interface=eth2', print_ret=True)
		ret, msg = runcmd(cmd, print_ret = True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth2: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth2 in static mode with IP Address ' + eth2_ip_address)
			os.system('splash.sh "ERROR : Failed to configure eth2 in static mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth2_mode == 'dhcp':
		ret, msg = runcmd('ilio net remove --interface=eth2', print_ret=True)
		ret, msg = runcmd('ilio net add_dhcp --interface=eth2', print_ret=True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth2: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth2 in DHCP mode')
			os.system('splash.sh "ERROR : Failed to configure eth2 in DHCP mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth3_mode == 'static':
		# Validate the netmask
		debug("INFO : Validating eth3 netmask "+eth3_netmask)
		eth3_nm_ret = validate_netmask(eth3_netmask)
		if eth3_nm_ret != 0:
			msgstr = "ERROR : Failed to validate eth0 netmask "+eth3_netmask+", BOOTSTRAP ABORTED!"
			debug(msgstr)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error validating eth3 netmask '+eth3_netmask+' when Configuring eth3 with IP Address ' + eth3_ip_address)
			os.system('splash.sh "'+msgstr+'"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)

		exit_on_ip_conflict("eth3", eth3_ip_address)

		cmd='ilio net add_static --interface=eth3 --address=%s --netmask=%s' % (eth3_ip_address, eth3_netmask)
                if eth3_gateway != '' and eth3_gateway != '0.0.0.0':
                    if 'eth3' not in storage_network_list or (eth1_mode == '' and eth2_mode == '' and eth0_mode == ''):
                        cmd += ' --gateway=%s --gwtype=default' % eth3_gateway
                    else:
                        cmd += ' --gateway=%s --gwtype=network' % eth3_gateway
		debug( 'Configuring eth3 with IP Address ' + eth3_ip_address)
		ret, msg = runcmd('ilio net remove --interface=eth3', print_ret=True)
		ret, msg = runcmd(cmd, print_ret = True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth3: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth3 in static mode with IP Address ' + eth3_ip_address)
			os.system('splash.sh "ERROR : Failed to configure eth3 in static mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)
	if eth3_mode == 'dhcp':
		ret, msg = runcmd('ilio net remove --interface=eth3', print_ret=True)
		ret, msg = runcmd('ilio net add_dhcp --interface=eth3', print_ret=True)
		if (ret != 0):
			debug( 'ERROR : Error configuring eth3: ' + msg)
			send_status("CONFIGURE", 53, 1, "Bootstrap", ' Error Configuring eth3 in DHCP mode')
			os.system('splash.sh "ERROR : Failed to configure eth3 in DHCP mode, BOOTSTRAP ABORTED!"')
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(64)


	send_status("CONFIGURE", 54, 0, "Bootstrap", 'Done configuring network interfaces, setting other USX Node details' )

	if ilio_version != '':
		debug('Setting ilio version to %s' % (ilio_version))
		f = open('/etc/ilio/release-version', 'w')
		f.write('%s' % ilio_version)
		f.flush()
		f.close()

	if ilio_hostname != '':
		debug('Setting hostname to %s' % (ilio_hostname))
		f = open('/etc/hostname', 'w')
		f.write('%s' % ilio_hostname)
		f.flush()
		f.close()
		runcmd('/etc/init.d/hostname.sh start')

		f = open('/etc/hosts', 'r')
		lines = f.readlines()
		f.close()
		f = open('/etc/hosts', 'w')
		for line in lines:
			fields = line.split()
			if len(fields) == 0:
				continue
			if fields[0] == '127.0.1.1':
				f.write('127.0.1.1 ' + ilio_hostname + '\n')
				continue
			f.write(line)

		f.close()

	if data_disk != '':
		if 'ssd:' in data_disk:
			debug('Setting session host disk type and data disk to %s' % (data_disk))
			f=open('/etc/ilio/shdisktype','w')
			f.write('%s' % data_disk)
			f.flush()
			f.close()
			disks = data_disk.split(':')
			disk = disks[1]
			data_disk=disk

	if datastore_type != '':
		debug('Setting ilio datastore type to %s' % (datastore_type))
		f=open('/etc/ilio/iliodatastoretype', 'w')
		f.write('%s' % datastore_type)
		f.flush()
		f.close()
		if datastore_type == 'diskless' or datastore_type == 'session_host':
			if ilio_ramdisk_size != '':
				debug('Setting up compressed diskless ilio')
				cmd='ilio cache auto_setup --vram=%s --name=USX Node_VirtualDesktops --storage-type= --device=  --export-type=nfs --compression=True' % (ilio_ramdisk_size)
			else:
				debug('ERROR : Ramdisk size missing !!!')
				cmd='false'
		if datastore_type == 'diskbased' or datastore_type == 'replication_host':
			debug('Setting up disk based ilio')
			cmd='ilio cache auto_setup --name=USX Node_VirtualDesktops --device=%s --storage-type=local --export-type=nfs' % (data_disk)
			ret, msg = runcmd(cmd, print_ret=True)
			if ret != 0:
				debug('ERROR : Error creating filesystem. ' + msg)

	if datastore_type != '' and datastore_type == 'session_host':
		debug('Tear down requested')
		runcmd('umount /exports/USX Node_VirtualDesktops')

	if ilio_function_type != '':
		debug('Setting ilio function type to %s' % (ilio_function_type))
		f=open('/etc/ilio/iliofunctiontype', 'w')
		f.write('%s' % ilio_function_type)
		f.flush()
		f.close()

	if ilio_poweruser_p != '' and 'vdi' not in myrole:
		debug('Changing poweruser password')
		cmd='/bin/echo -e "%s\\n%s"|passwd poweruser' % (ilio_poweruser_p,ilio_poweruser_p)
		runcmd(cmd)
	else:
		debug('poweruser password change not requested')

	if ilio_timezone != '':
		runcmd('rm -f /etc/localtime')
		runcmd('ln -s /usr/share/zoneinfo/%s /etc/localtime'%(ilio_timezone))
		f=open('/etc/timezone', 'w')
		f.write('%s' % ilio_timezone)
		f.flush()
		f.close()
		runcmd('service cron restart')

	# Check if default gateway is set, and try to set it if not.
	set_default_gateway_if_required()

	# Perform USX SSH keygen AND sync keys with USXM
	ret = os.system(USX_SSHKEY_GEN_COMMAND_INIT)
	if ret != 0:
		debug("===WARN=== USX SSH keygen/sync failure, please refer to the log file for details")
	runcmd('/usr/bin/python /opt/milio/atlas/scripts/usx_trigger_plugins.pyc')
	send_status("CONFIGURE", 57, 0, "Bootstrap", 'Checking USX Node Role and performing appropriate setup steps' )
	if myrole is None:
		debug('WARNING : Could not determine an Atlas Role for this USX Node, skipping calling configure scripts')
		os.system('splash.sh "===ERROR=== This appears to be a USX Node but there seems to be no role assigned to it"')
		debug('ERROR : ===NO ROLE ASSIGNED=== This appears to be a USX Node but there seems to be no role assigned to it')
		send_status("CONFIGURE", 57, 1, "Bootstrap", 'Error Checking USX Node Role, cannot perform appropriate setup steps', True )
		rc = send_bootstrap_status()
		if rc != 0:
			debug("ERROR : Sending bootstrap status to usx manager failed!")
		sys.exit(60)
	else:
		if myrole != 'amc' and myrole != 'witness':
			# We're not an AMC so run the agent
			debug('Not a Management Center Role, so starting USX Agent...')
			if myrole == 'vdi':
				if 'free' in myfullrole:
					os.system('splash.sh "Starting USX Free Trial agent..."')
				else: # vdi_diskless or vdi_diskbased
					os.system('splash.sh "Starting USX VDI agent..."')
			else:
				os.system('splash.sh "Starting USX agent..."')
				send_status("CONFIGURE", 60, 0, "Bootstrap", 'Running node agent ...' )
				ret = os.system(AGENT_JAR_START_COMMAND)
				if ret != 0:
					debug("ERROR : Agent start command returned nonzero, agent might not have started! Exiting bootstrap with error!")
					send_status("CONFIGURE", 60, 1, "Bootstrap", 'Error - Failed Running USX Node Agent!', True )
					rc = send_bootstrap_status()
					if rc != 0:
						debug("ERROR : Sending bootstrap status to usx manager failed!")
					sys.exit(61)
				#if backup target, start agent-db
				#if not backup target, disable agent-db and delete all files from /opt/amc_db/*
				if os.path.exists('/opt/amc_db/replication.lock') == True:
					ret = os.system("service agent-db start")
					if ret != 0:
						debug("ERROR : Agent DB start command returned nonzero.")
				else:
					ret = os.system("service agent-db stop")
					if ret != 0:
						debug("ERROR : Agent DB stop command returned nonzero.")
					os.system("rm -rf /opt/amc_db/*")

		if myrole == 'witness':
                        #Just start ibdserver if role is witness
			ret = runscript( myrole, build_path_to_atlas_script(myrole))
			debug('Calling configure script for role=%s returned exit code %s' % (myrole, str(ret)))
			if(ret != 0):
				debug('ERROR : Configure script for role=%s failed with return code %s' % (myrole, str(ret)))
                                send_status("CONFIGURE", 100, 1, "Bootstrap", 'ERROR : Configure script for role=%s failed with return code %s' % (myrole, str(ret)), True)
				sys.exit(61)
			else:
                                #touch configured flag file
				f = open('/usr/share/ilio/configured', 'w')
				f.write(' ')
				f.flush()
				f.close()
                                send_status("CONFIGURE", 100, 0, "Bootstrap", 'Successfully configured bootstrap for USX node with role '+myfullrole, True)
				sys.exit(0)

		debug('Since we have not been configured yet, calling configure script for role = %s' % myrole)
		if myrole == 'vdi':
			if 'free' in myfullrole:
				os.system('splash.sh "Configuring USX Free Trial..."')
			else: # vdi_diskless or vdi_diskbased
				os.system('splash.sh "Configuring USX VDI in %s role..."' % myfullrole)
		else:
			os.system('splash.sh "Configuring USX Node in %s role..."' % myrole)
		send_status("CONFIGURE", 62, 0, "Bootstrap", 'Calling configuration script for role: '+myfullrole)
		ret = runscript( myrole, build_path_to_atlas_script(myrole))
		debug('Calling configure script for role=%s returned exit code %s' % (myrole, str(ret)))
		if(ret != 0):
			debug('ERROR : Configure script for role=%s failed with return code %s' % (myrole, str(ret)))
			send_status("CONFIGURE", 62, 1, "Bootstrap", 'Error - Aborted Bootstrap due to failed run of configure script for role = '+myfullrole, True)
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(61)
		else:
			send_status("CONFIGURE", 92, 0, "Bootstrap", 'Successfully called configure script for role: '+myfullrole)
			debug(' Configure script for role=%s SUCCEEDED. ' % myfullrole)

			#### Set the AMC IP in rsyslog before we go further
			debug("Setting USX Manager IP in "+RSYSLOG_CONF_FILE+" to send syslog to AMC...")
			send_status("CONFIGURE", 93, 0, "Bootstrap", "Setting USX Manager IP in "+RSYSLOG_CONF_FILE+" to send syslog to AMC...")
			if amc_ip:
				cmd = 'perl -p -i -e "s/local3\.\*.*$/local3\.\* \@\@'+amc_ip+':10514/" '+RSYSLOG_CONF_FILE
				ret = os.system(cmd)
				if ret != 0:
					debug("ERROR : Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")
					send_status("CONFIGURE", 93, 1, "Bootstrap", "Error - Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")
				else:
					debug("Set USX Manager IP to "+amc_ip+" in rsyslog conf file.")
					send_status("CONFIGURE", 93, 0, "Bootstrap", "Set USX Manager IP to "+amc_ip+" in rsyslog conf file.")
					# Now restart rsyslog
					debug("Restarting rsyslog after setting USX Manager IP to "+amc_ip)
					send_status("CONFIGURE", 93, 0, "Bootstrap", "Restarting rsyslog after setting USX Manager IP to "+amc_ip)
					ret = os.system('service rsyslog restart')
					if ret != 0:
						debug("ERROR : Failed Restarting rsyslog after setting USX Manager IP to "+amc_ip+" in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap.")
						send_status("CONFIGURE", 93, 0, "Bootstrap", "Error - Failed Restarting rsyslog after setting USX Manager IP to "+amc_ip+" in rsyslog conf file. USX Manager will not contain logs from this USX Node. Continuing with bootstrap")
					else:
						debug("Successfully restarted rsyslog after setting USX Manager IP to "+amc_ip)
						send_status("CONFIGURE", 93, 0, "Bootstrap", "Successfully restarted rsyslog after setting USX Manager IP to "+amc_ip)
			else:
				debug("ERROR : USX Manager IP from JSON is blank! Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")
				send_status("CONFIGURE", 93, 1, "Bootstrap", "Error - USX Manager IP from JSON is blank! Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")

			#### Call run_ha_framework_scripts()
			if (myrole == 'ads' or myrole == 'pool' or myrole == 'virtvol' or myrole == 'volume' ) and (myha_enabled is True):
				debug('Role: %s, HA ENABLED. Setting multicast routes required for HA.' % myfullrole)
				ret3 = set_multicast_routes_for_ha()
				if ret3 != 0:
					debug('WARNING : Failed to set HA multicast routes for Role=%s but continuing with bootstrap.' % myfullrole)
					send_status("CONFIGURE", 94, 0, "Bootstrap", 'Warning - Failed setting multicast routes required for HA. Continuing with bootstrap.')
				else:
					debug('Successfully set HA multicast routes (required when HA is enabled) for Role=%s' % myfullrole)
					send_status("CONFIGURE", 94, 0, "Bootstrap", 'Successfully set multicast routes required for HA.')

				debug('Role: %s, HA is Enabled, starting HA config scripts...' % myfullrole)
				send_status("CONFIGURE", 94, 0, "Bootstrap", 'HA is enabled, calling HA framework scripts in configure mode...')

				ret2 = run_ha_framework_scripts()
				if(ret2 != 0):
					# 14-Jan-2014 : Failure to start HA config script should be considered an error, according to Su Chen and Huan Trinh
					debug('ERROR : Failed to start HA config script for Role=%s so aborting bootstrap. Reboot this USX Node to try to re-configure the HA.' % myfullrole)
					send_status("CONFIGURE", 94, 1, "Bootstrap", 'Error - HA is enabled, but calling HA framework scripts resulted in error. Not Continuing with bootstrap; this USX Node will be unconfigured. Please reboot this USX Node to re-run configuration for HA.', True)
					os.system('splash.sh "USX Bootstrap: ERROR : HA is enabled, but starting HA framework FAILED. This USX Node will be unconfigured. Please reboot this USX Node to re-run configuration for HA"')
					time.sleep(2)
					rc = send_bootstrap_status()
					if rc != 0:
						debug("ERROR : Sending bootstrap status to usx manager failed!")
					sys.exit(62)
				else:
					ha_flag = True
					debug('Successfully started HA config script for Role=%s' % myfullrole)
					send_status("CONFIGURE", 94, 0, "Bootstrap", 'HA is enabled. Successfully called HA framework scripts.')
			else:
				debug('Either this is not a node for which HA needs to be run, or HA is not enabled. Did not run HA Configure script.')
				send_status("CONFIGURE", 94, 0, "Bootstrap", 'HA is NOT enabled, no need to call HA framework scripts...')

                        # Tune the kernel parameters for Hybrid USX Volumes
                        # JIRA Ticket: ATLANTIS2923/ATLANTIS-2926
                        if is_hybrid_volume():
                                vscaler_dev = get_vsaler_dev_name()
                                if len(vscaler_dev) > 0:
                                        debug('Tuning Hybrid volume kernel parameters for vScaler')
                                        send_status("CONFIGURE", 96, 0, "Bootstrap", 'Tuning Hybrid volume kernel parameters for vScaler')
                                        #Tune vscaler kernel parameters
                                        debug("Tuning kernel parameters for the vscaler device: " + vscaler_dev[0])
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_DIRTY_THRESH_PCT, VSCALER_DIRTY_THRESH_PCT_VALUE)
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_MAX_CLEAN_IOS_SET, VSCALER_MAX_CLEAN_IOS_SET_VALUE)
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_MAX_CLEAN_IOS_TOTAL, VSCALER_MAX_CLEAN_IOS_TOTAL_VALUE)
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_RECLAIM_POLICY, VSCALER_RECLAIM_POLICY_VALUE)

			# Touch the "configured" script
			send_status("CONFIGURE", 99, 0, "Bootstrap", 'Setting configured property for USX Node with role='+myfullrole)
			debug('Setting "Configured" property for this node with role=%s' % myfullrole)
			f = open('/usr/share/ilio/configured', 'w')
			f.write(' ')
			f.flush()
			f.close()
			#support multiple virtualization technologies, set ovfEnv
			if hypervisor_type == 'VMware':
				runcmd('vmtoolsd --cmd "info-set guestinfo.ilio.configured True"');

                        # Set VVOL if this Volume was deployed with it
                        set_vvol()

			if myrole == 'volume':
				ha_cmd = 'python /opt/milio/atlas/roles/ha/usx_daemon.pyc start '
				ret = os.system(ha_cmd)

			debug('======= END USX bootstrap : Configured for role=%s  =======' % myfullrole)
			send_status("CONFIGURE", 100, 0, "Bootstrap", 'Successfully configured bootstrap for USX node with role '+myfullrole, True)
			rc = send_bootstrap_status(ha_flag, True)
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			# enable teardown when shutdown machine"
			runcmd('update-rc.d teardownatlantisdatastore stop 01 0 1 6 .');
			#runcmd('bash /opt/support/Upgrade-ExptalFeatures-Deps-USX3.x.run');
			# Check and backup ovf environment.
			debug("Check and backup ovf environment.")
			rc, msg = runcmd('/usr/bin/python /opt/milio/atlas/scripts/modify_network_cfg.pyc -ovf_check', print_ret=True)
			sys.exit(0)
        # Fix bug USX-72162: Combine deployment job with bootstrap job to be a single job
        # Delete deploymentjob id file
        if deployment_jobid:
            delete_jobid_file()
else:
	debug('USX Node already configured, skipping autoconfigure')

	# Check whether crash file exists, delete if it used a lot of space.
	crash_location = '/var/crash'
	crash_partition = '/'
	high_water_mark = 90

	if is_hyperconverged():
		crash_location = '/var/log/crash'
		crash_partition = '/var/log/'

	# Check disk space usage.
	try:
		debug('Check used space for %s.' % crash_partition)
		check_space_cmd = "df -BM %s | grep -v Filesystem | awk {'print $5'}" % crash_partition
		ret, msg = runcmd(check_space_cmd, print_ret=True)
		used_space_percentage = int(float(msg.strip().replace('%','')))
		# If more than 90% sapce used, delete crash if it exists.
		debug('Space usage on %s is: %s.' % (crash_partition, str(used_space_percentage) + '%'))
		if used_space_percentage > high_water_mark:
			debug('Delete crash file if it exists.')
			del_cmd = 'find %s -type f | grep -v dmesg | xargs rm' % crash_location
			ret, msg = runcmd(del_cmd, print_ret=True)
	except Exception as e:
		debug('Check disk space exception: %s' % str(e))

	# Check and backup ovf environment.
	debug("Check and backup ovf environment.")
	rc, msg = runcmd('/usr/bin/python /opt/milio/atlas/scripts/modify_network_cfg.pyc -ovf_check', print_ret=True)
	# Check if network config was changed in ovf environment
	debug("Check if network config was changed in ovf environment.")
	rc, msg = runcmd('/usr/bin/python /opt/milio/atlas/scripts/modify_network_cfg.pyc -update_nfg', print_ret=True)
	# Check if default gateway is set, and try to set it if not.
	set_default_gateway_if_required()

	# TISILIO-3016 : Try to get the JSON data from the local file, if it does not already exist in the vApp properties
	if myjson is None or not myjson:
		debug("No JSON data retrieved from vApp properties. Attempting to get JSON data from local config if not found in vApp properties...")
	bsd = get_bootstrap_json_data()

	# TISILIO-3016 : Sanity check it again! (Safe) = (!sorry)
	if not bsd or myjson is None or not myjson:
		debug("ERROR : Bootstrap init : USX node ALREADY configured, but no USX role details found in VM environment. CANNOT Continue Bootstrap. BOOTSTRAP ABORTED!")
		send_status("CONFIGURE", 2, 1, "Bootstrap", ' Error getting bootstrap JSON data for already-configured USX node. Bootstrap aborted.')
		os.system('splash.sh "ERROR : Failed to get bootstrap JSON data for already-configured USX node, BOOTSTRAP ABORTED!"')
		rc = send_bootstrap_status()
		if rc != 0:
			debug("ERROR : Sending bootstrap status to usx manager failed!")
		sys.exit(59)

	# If we got here, we have the atlas JSON data, either from the vApp properties or from local file.
	set_initial_atlas_bootstrap_data()

	# USX-75700 Delete the latest metric
	os.system("rm -rf /opt/amc/agent/adscapacity.prop")
	os.system("rm -rf /opt/amc/agent/offloadcapacity.prop")

	# Perform USX SSH key sync with USXM
	ret = os.system(USX_SSHKEY_GEN_COMMAND_START)
	if ret != 0:
		debug("===WARN=== USX SSH key distribution failure, please refer to the log file for details")
	runcmd('/usr/bin/python /opt/milio/atlas/scripts/usx_trigger_plugins.pyc')
	send_status("START", 15, 0, "Bootstrap", 'USX Node has already been configured. Checking USX Node Role and performing appropriate setup steps' )
	if myrole is None:
		debug('WARNING : Could not determine an Atlas Role for this USX Node, skipping calling Atlas start scripts')
		os.system('splash.sh "===ERROR=== This appears to be a USX Node but there seems to be no role assigned to it"')
		debug('===ERROR=== This appears to be a USX Node but there seems to be no role assigned to it')
		send_status("START", 15, 1, "Bootstrap", 'Error Checking USX Node Role, cannot perform appropriate setup steps', True )
		rc = send_bootstrap_status()
		if rc != 0:
			debug("ERROR : Sending bootstrap status to usx manager failed!")
		sys.exit(62)
	else:
		if myrole != 'amc' and myrole != 'witness':
			# We're not an AMC so run the agent
			debug('Not a Management Center Role, so starting USX Agent...')
			if myrole == 'vdi':
				if 'free' in myfullrole:
					os.system('splash.sh "Starting USX Free Trial agent..."')
				else: # vdi_diskless or vdi_diskbased
					os.system('splash.sh "Starting USX VDI agent..."')
			else:
				os.system('splash.sh "Starting USX agent..."')
				send_status("START", 20, 0, "Bootstrap", 'Running node agent ...' )
				ret = os.system(AGENT_JAR_START_COMMAND)
				if ret != 0:
					debug("ERROR : Agent start command returned nonzero, agent might not have started! Exiting bootstrap with error!")
					send_status("START", 20, 1, "Bootstrap", 'Error - Failed Running USX Node Agent!', True )
					rc = send_bootstrap_status()
					if rc != 0:
						debug("ERROR : Sending bootstrap status to usx manager failed!")
					sys.exit(61)
				#if backup target, start agent-db
				#if not backup target, disable agent-db and delete all files from /opt/amc_db/*
				if os.path.exists('/opt/amc_db/replication.lock') == True:
					ret = os.system("service agent-db start")
					if ret != 0:
						debug("ERROR : Agent DB start command returned nonzero.")
				else:
					ret = os.system("service agent-db stop")
					if ret != 0:
						debug("ERROR : Agent DB stop command returned nonzero.")
					os.system("rm -rf /opt/amc_db/*")

				#update reboot status
				update_reboot_status()

			if myha_enabled is True:
				debug("INFO: skip license capacity checking for HA node.")
			else:
				# dougj - insert license check
				if not check_capacity_license():
					# not enough unused capacity
					debug("ERROR : License expired.")
					send_status("CONFIGURE", 23, 1, "Bootstrap", 'Error - License expired! Abort bootstrap.', True )
					rc = send_bootstrap_status()
					if rc != 0:
						debug("ERROR : Sending bootstrap status to usx manager failed!")
					sys.exit(61)

		if myrole == 'witness':
                        #Just start ibdserver if role is witness
			ret = runscript(myrole, build_path_to_atlas_script(myrole, False))
			if(ret != 0):
				debug('ERROR : Start script for role=%s failed with return code %s' % (myrole, ret))
                                send_status("START", 100, 1, "Bootstrap", 'ERROR : Start script for role=%s failed with return code %s' % (myrole, ret), True)
				sys.exit(63)
			else:
                                send_status("START", 100, 0, "Bootstrap", 'Successfully started bootstrap for USX node with role '+myfullrole, True)
				sys.exit(0)

		debug('Since we have already been configured as an Atlas %s node, calling start script for role = %s' % (myrole, myrole))
		if myrole == 'vdi':
			if 'free' in myfullrole:
				os.system('splash.sh "Starting USX Free Trial..."')
			else: # vdi_diskless or vdi_diskbased
				os.system('splash.sh "Starting USX VDI..."')
		else:
			os.system('splash.sh "Starting USX Node in %s role..."' % myrole)
		send_status("START", 25, 0, "Bootstrap", 'Calling start script for role '+myfullrole)
		ret = runscript(myrole, build_path_to_atlas_script(myrole, False))
		if(ret != 0):
			debug('ERROR : Start script for role=%s failed with return code %s' % (myrole, ret))
			send_status("START", 25, 1, "Bootstrap", 'Error - Aborted Bootstrap due to failed run of start script for role = '+myfullrole, True)
			rc = send_bootstrap_status()
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(63)
		else:
			send_status("START", 95, 0, "Bootstrap", 'Successfully called start script for role '+myrole)
			debug(' Start  script for role=%s SUCCEEDED. ' % myfullrole)

			#### Set the AMC IP in rsyslog before we go further. We do this even in start because the AMC IP might have changed.
			debug("Setting USX Manager IP in "+RSYSLOG_CONF_FILE+" to send syslog to AMC...")
			send_status("START", 96, 0, "Bootstrap", "Setting USX Manager IP in "+RSYSLOG_CONF_FILE+" to send syslog to AMC...")
			if amc_ip:
				cmd = 'perl -p -i -e "s/local3\.\*.*$/local3\.\* \@\@'+amc_ip+':10514/" '+RSYSLOG_CONF_FILE
				ret = os.system(cmd)
				if ret != 0:
					debug("ERROR : Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")
					send_status("START", 96, 1, "Bootstrap", "Error - Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")
				else:
					debug("Set USX Manager IP to "+amc_ip+" in rsyslog conf file.")
					send_status("START", 96, 0, "Bootstrap", "Set USX Manager IP to "+amc_ip+" in rsyslog conf file.")
					# Now restart rsyslog
					debug("Restarting rsyslog after setting USX Manager IP to "+amc_ip)
					send_status("START", 97, 0, "Bootstrap", "Restarting rsyslog after setting USX Manager IP to "+amc_ip)
					ret = os.system('service rsyslog restart')
					if ret != 0:
						debug("ERROR : Failed Restarting rsyslog after setting USX Manager IP to "+amc_ip+" in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap.")
						send_status("START", 97, 0, "Bootstrap", "Error - Failed Restarting rsyslog after setting USX Manager IP to "+amc_ip+" in rsyslog conf file. USX Manager will not contain logs from this USX Node. Continuing with bootstrap")
					else:
						debug("Successfully restarted rsyslog after setting USX Manager IP to "+amc_ip)
						send_status("START", 97, 0, "Bootstrap", "Successfully restarted rsyslog after setting USX Manager IP to "+amc_ip)
			else:
				debug("ERROR : USX Manager IP from JSON is blank! Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")
				send_status("", 96, 1, "Bootstrap", "Error - USX Manager IP from JSON is blank! Unable to set USX Manager IP in rsyslog conf file. USX Manager will not contain logs from this USX Node! Continuing with bootstrap")

			#### Call run_ha_framework_scripts(False)
			if (myrole == 'ads' or myrole == 'pool' or myrole == 'virtvol' or myrole == 'volume') and (myha_enabled is True):
				debug('Role: %s, HA ENABLED. Setting multicast routes required for HA.' % myfullrole)
				ret3 = set_multicast_routes_for_ha()
				if ret3 != 0:
					debug('WARNING : Failed to set HA multicast routes for Role=%s but continuing with bootstrap.' % myfullrole)
					send_status("CONFIGURE", 97, 0, "Bootstrap", 'Warning - Failed setting multicast routes required for HA. Continuing with bootstrap.')
				else:
					debug('Successfully set HA multicast routes (required when HA is enabled) for Role=%s' % myfullrole)
					send_status("CONFIGURE", 97, 0, "Bootstrap", 'Successfully set multicast routes required for HA.')

				debug('Role: %s, HA is Enabled, starting HA start scripts...' % myfullrole)
				send_status("START", 97, 0, "Bootstrap", 'HA is enabled. Calling HA framework scripts in start mode.')
				ret2 = run_ha_framework_scripts(False)
				if(ret2 != 0):
					debug('ERROR : Failed to start HA config script for Role=%s' % myfullrole)
					send_status("START", 97, 0, "Bootstrap", 'Warning - HA is enabled, but calling HA framework scripts resulted in error. Continuing with bootstrap.')
				else:
					ha_flag = True
					debug('Successfully started HA config script for Role=%s' % myfullrole)
					send_status("START", 98, 0, "Bootstrap", 'HA is enabled. Successfully called HA framework scripts.')
			else:
				debug('Either this is not a node for which HA needs to be run, or HA is not enabled. Did not run HA Configure script.')
				send_status("START", 98, 0, "Bootstrap", 'HA is NOT enabled, no need to call HA framework scripts...')

                        # Tune the kernel parameters for Hybrid USX Volumes
                        # JIRA Ticket: ATLANTIS2923/ATLANTIS-2926
                        if is_hybrid_volume():
                                vscaler_dev = get_vsaler_dev_name()
                                if len(vscaler_dev) > 0:
                                        debug('Tuning Hybrid volume kernel parameters for vScaler')
                                        send_status("START", 99, 0, "Bootstrap", 'Tuning Hybrid volume kernel parameters for vScaler')
                                        #Tune vscaler kernel parameters
                                        debug("Tuning kernel parameters for the vscaler device: " + vscaler_dev[0])
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_DIRTY_THRESH_PCT, VSCALER_DIRTY_THRESH_PCT_VALUE)
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_MAX_CLEAN_IOS_SET, VSCALER_MAX_CLEAN_IOS_SET_VALUE)
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_MAX_CLEAN_IOS_TOTAL, VSCALER_MAX_CLEAN_IOS_TOTAL_VALUE)
                                        tune_vscaler_kernel_parameter(vscaler_dev[0], VSCALER_RECLAIM_POLICY, VSCALER_RECLAIM_POLICY_VALUE)

			# set the IP address to speed up bootstrap
			if hypervisor_type == "Xen":
				ret, msg = runcmd('/usr/sbin/xe-update-guest-attrs', print_ret=True)

			if myrole == 'volume':
				ha_cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc cleanup_vol_ha_status &'
				ret = os.system(ha_cmd)
				ha_cmd = 'python /opt/milio/atlas/roles/ha/usx_daemon.pyc start '
				ret = os.system(ha_cmd)

			#if myrole == 'ads' or myrole == 'pool' or myrole == 'virtvol' or myrole == 'volume':
			#	cp_cmd = 'cp /bin/echo /run/shm '
			#	ret = os.system(cp_cmd)
			#	cp_cmd = 'cp /usr/local/bin/ioping /run/shm '
			#	ret = os.system(cp_cmd)
			#	daemond_cmd = 'python /opt/milio/atlas/roles/ha/ha_daemon.pyc start'
			#	ret = os.system(daemond_cmd)

			if myrole == 'service vm' or myrole == 'service_vm' or myrole == 'volume' or is_simplified_volume():
				stop_cmd = '/usr/lib/heartbeat/ha_logd -k '
				ret = os.system(stop_cmd)

			debug('======= END USX bootstrap : Start script for role=%s  =======' % myfullrole)
			send_status("START", 100, 0, "Bootstrap", 'Successfully started bootstrap for USX node with role '+myfullrole, True)
			send_bootstrap_alert()

			# Set sync timeout as 60s.
			cmd_str = 'sync'
			do_system_timeout(cmd_str, 60)
			rc = send_bootstrap_status(ha_flag, True)
			if rc != 0:
				debug("ERROR : Sending bootstrap status to usx manager failed!")
			sys.exit(0)


# Should never get here. If it gets here, it's a problem.
debug("Uh Oh, bootstrap reached a point which should not have been reached. This is an error. Bootstrap ends here. This USX Node might not have been configured/started")
sys.exit(1)

