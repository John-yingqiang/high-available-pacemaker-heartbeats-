#!/usr/bin/python

import os, sys
import json
#import logging
import tempfile
from time import sleep
from subprocess import *
from ha_util import *
import httplib

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_storage import *
from log import *

PK_NOT_RUNNING = 12

ATLAS_CONF = '/etc/ilio/atlas.json'
CONFIGURED_FILE='/usr/share/ilio/configured'
STARTED_FILE='/run/pacemaker_started'
STORAGE_NETWORK_DOWN='/var/log/storagenetwork_down'
STORAGE_NETWORK_DOWN_RESET='storagenetwork_down'
COROSYNC_DOWN_RESET = '/var/log/corosync_down_reset'
SET_HA_FLAG='/var/log/set_ha_flag'
LOG_FILENAME='/var/log/usx-atlas-ha.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename='/var/log/usx-atlas-ha.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug(''.join([str(x) for x in args]))
    print ''.join([str(x) for x in args])
'''

def runcmd(
    cmd,
    print_ret=False,
    lines=False,
    input_string=None,
    ):
    if print_ret:
        debug('Running: %s' % cmd)
    try:
        tmpfile = tempfile.TemporaryFile()
        p = Popen(
            [cmd],
            stdin=PIPE,
            stdout=tmpfile,
            stderr=STDOUT,
            shell=True,
            close_fds=True,
            )
        (out, err) = p.communicate(input_string)
        status = p.returncode
        tmpfile.flush()
        tmpfile.seek(0)
        out = tmpfile.read()
        tmpfile.close()

        if lines and out:
            out = [line for line in out.split('\n') if line != '']
        # if print_ret and out: debug(' -> %s: %s: %s' % (status, err, out))

        if print_ret:
            debug(' -> %s: %s: %s' % (status, err, out))
        return (status, out)
    except OSError:
        return (127, 'OSError')

def get_nics_data(uuid):
	# Storage network can be changed by users and is not updated to local atlas json.
	amcfile = "/usxmanager/usx/inventory/volume/containers/" + uuid + '?composite=true'
	conn = httplib.HTTPConnection("127.0.0.1:8080")
	conn.request("GET", amcfile)
	res = conn.getresponse()
	data = res.read()
	node_dict = json.loads(data)
	if node_dict is None:
		debug('Error getting Node dictionary information from AMC')
		return None
    	#debug('corosync_config node dict JSON: ', json.dumps(node_dict, sort_keys=True, indent=4, separators=(',', ': ')))
	node_dict = node_dict.get('data')
	if node_dict is None:
		debug('Error getting Ilio data from AMC')
		return None
	ilio_dict = node_dict.get('usx')
	if ilio_dict is None:
		debug('Error getting Ilio information from AMC')
		return None
	return ilio_dict.get('nics')

def get_storage_network_status(ilio_dict):
	uuid = ilio_dict.get('uuid')
	nics = get_nics_data(uuid)
	if nics is None:
		debug('nics is None')
		nics = ilio_dict.get('nics')
		if nics is None:
			debug('nics is None again')
	ipaddr = None
	devname = None
	for nic in nics:
		if nic.get("storagenetwork") is True:
			ipaddr = nic.get("ipaddress")
			devname = nic.get("devicename")
			break
	if devname is None:
		debug('Error getting storage network information.')
		return 1

	(ret, msg) = runcmd('cat /sys/class/net/' + devname + '/operstate', print_ret=False,lines=True)
	if ret != 0:
		return ret
	for line in msg:
		if line == "up":
			return 0
	return 1

def get_node_dict_from_AMC(uuid):
	amcfile = "/usxmanager/usx/inventory/volume/containers/" + uuid + '?composite=true'
	conn = httplib.HTTPConnection("127.0.0.1:8080")
	conn.request("GET", amcfile)
	res = conn.getresponse()
	data = res.read()
	node_dict = json.loads(data)
	if node_dict is None:
		debug('Error getting Node dictionary information from AMC')
		return None
    	#debug('corosync_config node dict JSON: ', json.dumps(node_dict, sort_keys=True, indent=4, separators=(',', ': ')))
	node_dict = node_dict.get('data')
	if node_dict is None:
		debug('Error getting Ilio data from AMC')
		return None
	return node_dict

def stop_corosync_with_condition():
	debug('Enter stop_corosync_with_condition')
	if not os.path.isfile(STARTED_FILE):
		(ret, msg) = runcmd('killall -9 corosync', print_ret=True)
		(ret, msg) = runcmd('killall -9 pacemakerd', print_ret=True)
		sys.exit(0)


#########################################################
#                    START HERE                         #
#########################################################

cfgfile = open(ATLAS_CONF, 'r')
s = cfgfile.read()
cfgfile.close()
node_dict = json.loads(s)
if node_dict is None:
	sys.exit(0)
ilio_dict = node_dict.get('usx')
if ilio_dict is None:
	sys.exit(0)
if ilio_dict.get('ha') is None or ilio_dict.get('ha') is False:
	sys.exit(0)				# stop if this is not a ha node

if not os.path.isfile(CONFIGURED_FILE):
	sys.exit(0)

if not os.path.isfile(STARTED_FILE):
	sys.exit(0)

if check_usxmanager_alive_flag():
    uuid = ilio_dict.get('uuid')
    node_dict = get_node_dict_from_AMC(uuid)
    if node_dict is None:
        sys.exit(0)
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        sys.exit(0)

    resuuid = None
    volres = node_dict.get('volumeresources')
    if volres != None and len(volres) > 0:
        resuuid = volres[0].get('uuid')
    amcurl = ilio_dict.get('usxmanagerurl')
    if amcurl is None:
        debug('Error getting USX AMC url.')
        sys.exit(0)

    if os.path.isfile(SET_HA_FLAG):
        (ret, out) = update_ha_flag(amcurl, uuid, resuuid, "true", "false")
        if (ret == 0):
            os.remove(SET_HA_FLAG)

    rc1 = get_storage_network_status(ilio_dict)
    rc2 = 0
    if rc1 == 0:
        rc2 = ha_check_storage_network_status()
    if rc1 != 0 or rc2 != 0:
        ha_adjust_quorum_policy(True)
        if not os.path.isfile(STORAGE_NETWORK_DOWN):
            ha_reset_node_fake(STORAGE_NETWORK_DOWN_RESET)
        else:
            sys.exit(0)
    else:
        ha_adjust_quorum_policy(False)
        if os.path.isfile(STORAGE_NETWORK_DOWN):
            (ret, msg) = runcmd('rm -f ' + STORAGE_NETWORK_DOWN, print_ret=False)

(ret, msg) = runcmd('service corosync status', print_ret=False)
if ret == 0 and msg.find('is running') >= 0:
	(ret, msg) = runcmd('service pacemaker status', print_ret=False)
	if ret == 0 and msg.find('is running') >= 0:
		ha_cleanup_failed_resource()
		ha_adjust_expected_votes()
		sys.exit(0)

fd = open(COROSYNC_DOWN_RESET, 'a')
fd.flush()
os.fsync(fd)
fd.close()

stop_corosync_with_condition()
(ret, msg) = runcmd('service corosync restart', print_ret=True)
stop_corosync_with_condition()
count = 120
cmd = 'service corosync status'
pcmkrunning = False
while count > 0:
    stop_corosync_with_condition()
    (ret, msg) = runcmd(cmd, print_ret=False)
    if ret == 0 and msg.find('is running') >= 0:
	cnt = 120
	while cnt > 0:
	    stop_corosync_with_condition()
	    sleep(3)
	    (ret, msg) = runcmd('service pacemaker start', print_ret=False)
	    sleep(1)
	    (ret, msg) = runcmd('service pacemaker status', print_ret=False)
	    if ret == 0 and msg.find('is running') >= 0:
		pcmkrunning = True
		break
	    cnt -= 1
    if pcmkrunning is True:
	break
    sleep(5)
    count -= 1

if pcmkrunning is True:
    sys.exit(0)
else:
    sys.exit(PK_NOT_RUNNING)
