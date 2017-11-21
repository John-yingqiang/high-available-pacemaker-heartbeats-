#!/usr/bin/python

import json
import os
import string
import sys
import tempfile
import socket
from subprocess import *
import logging
import time
import datetime
import random
import re
#import urllib2
import traceback
import fcntl
import glob
import base64
import zlib
from collections import defaultdict

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from log import *
from atl_storage import *
from atl_alerts import *
from cmd import runcmd
from status_update import does_jobid_file_exist
from status_update import send_status

# HA Modules Version
VERSION = '3.5.1'

##################################################################
#  Hard coded error code                                         #
##################################################################

CLUS_SUCCESS = 0
PCMK_RES_TASK_DONE = 1
PCMK_RETRY_EXCEED_LIMIT = 2
JSON_PARSE_EXCEPTION = 10
CORO_NOT_RUNNING = 11
PCMK_START_FAIL = 12
PCMK_SET_PROPERTY_FAIL = 13
PCMK_GET_HOSTLIST_FAIL = 14
PCMK_HOSTLIST_EMPTY = 15
PCMK_GET_CONFIGURE_FAIL = 16
PCMK_STONITH_TYPE_ERR = 17
PCMK_GET_RESOURCE_PARAM_FAIL = 18
PCMK_SET_RESOURCE_PARAM_FAIL = 19
PCMK_SET_VCLI_CREDENTIAL_FAIL = 20
PCMK_GET_VCLI_CREDENTIAL_FAIL = 21
PCMK_LS_VCLI_CREDENTIAL_FAIL = 22
PCMK_SET_RESOURCE_FAIL = 23
PCMK_ENABLE_STONITH_FAIL = 24
PCMK_CONF_POOLDS_FAIL = 25
PCMK_CONF_POOLIP_FAIL = 26
PCMK_CONF_POOLRES_FAIL = 27
PCMK_RESOURCE_TYPE_ERR = 28
PCMK_RESOURCE_PARAM_ERR = 29
PCMK_FETCH_AMC_FAIL = 30
PCMK_NOT_SUPPORT_ADS_YET = 31
PCMK_SET_QUORUM_FAIL = 32
PCMK_CONF_ADSDS_FAIL = 33
PCMK_CONF_ADSIP_FAIL = 34
PCMK_SETTING_UP_ADS_RES_FAIL = 35
PCMK_LIST_RESOURCE_FAIL = 36
PCMK_SET_CONFIGURE_FAIL = 37
PCMK_CONF_POOLLOC_FAIL = 38
PCMK_CREATE_NFS_SHARED_INFODIR_FAIL = 39
PCMK_READ_MNTTAB_FAIL = 40
PCMK_CONF_ADSVSCALER_FAIL = 41
PCMK_CONF_ADSDDP_FAIL = 42
PCMK_CONF_ADSNFS_FAIL = 43
PCMK_CONF_WRONG_STICKINESS_SCORE = 44
PCMK_START_RESOURCE_FAIL = 45
PCMK_CONF_ADS_ISCSI_TARGET_FAIL = 46
MK_CONF_ADS_ISCSI_LUN_FAIL = 47
PCMK_CONF_ADS_EXPORT_NOT_SUPPORTED = 48
PCMK_QUERY_RESOURCE_STATUS_FAIL = 49
PCMK_STOP_RESOURCE_FAIL = 50
PCMK_CONF_WRONG_MIGRATION_THRESHOLD = 51


JSON_HA_NOT_ENABLED = 60
JSON_HA_NOT_FOUND = 61
JSON_ROLE_NOT_DEFINED = 62
JSON_ROLE_NOT_SUPPORTED= 63
JSON_VCENTER_NOT_DEFINED = 64

PCMK_POST_AMC_FAIL = 65
PCMK_CONF_MON_FAIL = 66
JSON_NEGATIVE_INT = 67

JSON_HA_NOT_SUPPORTED= 68


LVM_FETCH_LV_FAIL = 80
ADS_STOP_RES_FAIL = 81
ADS_DESTROY_RES_FAIL = 82
POOL_STOP_RES_FAIL = 83
POOL_DESTROY_RES_FAIL = 84
NO_DEV_TO_STOP = 85

ERROR_RESOURCE_IN_ANOTHER_NODE = 2
ERROR_ANOTHER_RESOURCE_RUNNING = 3
ERROR_RESOURCE_RUNNING = 4

VOL_STATUS_OK = 0
VOL_STATUS_WARN = 1
VOL_STATUS_CRITICAL = 2
VOL_STATUS_FATAL = 3
VOL_STATUS_UNKNOWN = 4


##################################################################
#  Hard coded configure parameters                               #
##################################################################
ATLAS_CONF                     = '/etc/ilio/atlas.json'
INTERNAL_LV_NAME               = 'atlas_internal_lv'
SVM_IP_LIST_FILE               = '/tmp/svm_ip_list.txt'
SVM_INFO_LIST                  = '/etc/ilio/svm_info.list'
PACEMAKER_RSC_LIST             = '/etc/ilio/pacemaker_rsc.list'
OTHER_NODE_IP                  = '/etc/ilio/other_node_ip'
USX_DAEMON_VERSION             = '/tmp/usx_deamon_version'
HA_DISABLE_FILE                = '/tmp/ha_disable'
USXM_DEAD_FILE                 = '/tmp/usxm_dead'
COROSYNC_ROTATE_CONF           = "/etc/logrotate.d/corosync"
IBD_AGENT_CONFIG_FILE          = '/etc/ilio/ibdagent.conf'
TIEBREAKER_LIST                = '/etc/ilio/tiebreaker.list'
USXM_TIEBREAKER_LIST           = '/etc/ilio/usxm_tiebreaker.list'
COROSYNC_CONFIG_FILE           = '/etc/corosync/corosync.conf'
VMMANAGER_IP                   = '/etc/ilio/vmmip'
RUNNING_STATUS                 = 'running'
HA_RUNNING_FILE                = '/tmp/HASM_RUNNING'
HA_STARTING_FLAG               = '/tmp/HA_STARTING'
HA_FIRSTTIME_FLAG              = '/tmp/HASM_FIRSTTIME'
HA_LOCK_FILE                   = '/etc/ilio/ha_lockfile'
SKIP_MOUNT_SNAPSHOT_FLAG       = '/tmp/SKIP_MOUNT_SNAPSHOT_FLAG'
PUSH_RAID1_PRIMARY_INFO_FLAG   = '/tmp/PUSH_RAID1_PRIMARY_INFO_FLAG'
HA_FORCESTARTING_FLAG          = '/tmp/HASM_FORCESTARTING'
IPADDR2_RUNNING_FILE           = '/tmp/IPaddr2_RUNNING'
SHARED_STORAGE_NOT_FOUND       = '/tmp/shared_storage_not_found'
HA_START_UP_PREPARATION        = '/tmp/ha_start_up_preparation'
TEARDOWN_FLAG                  = "/tmp/doing_teardown"
LOCAL_AGENT                    = 'http://127.0.0.1:8080/usxmanager'
volume_status_dict             = {0:'OK',1:'WARN',2:'CRITICAL',3:'FATAL',4:'UNKNOWN'}
CMD_IBDMANAGER                 = "/bin/ibdmanager"
CMD_IBDMANAGER_STAT            = CMD_IBDMANAGER + " -r a -s get"
ADS_LOCATION_SCORE             = 20
HA_SLEEP_TIME                  = 3
HA_RETRY_TIMES                 = 5
HA_STRETCHCLUSTER_LOCK_TIMEOUT = 20
RAID1PRIMARY                   = 'raid1PrimaryInfo'
RESOURCE                       = 'resources'


##################################################################
# Functions related with json file parsing                       #
##################################################################

# Function to parse the local json file
def readHAJsonFile():

    role = None
    node_dict = None
    haconf_dict = None
    ha_enabled = None

    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()

    try:
        node_dict = json.loads(s)
        if node_dict is None:
            debug('Critical: no node configuration found in %s.' % ATLAS_CONF)
            return (JSON_PARSE_EXCEPTION, ha_enabled, role, node_dict)  # stop if this is not a ilio node
    except:
        debug('Critical: failed to check %s. Exception was: %s'
               % (ATLAS_CONF, sys.exc_info()[0]))
        return (JSON_PARSE_EXCEPTION, ha_enabled, role, node_dict)

    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        debug('Error getting Ilio info')
        ilio_dict = node_dict
        #return (JSON_PARSE_EXCEPTION, ha_enabled, role, node_dict)
    ha_enabled = ilio_dict.get('ha')
    if ha_enabled is None:
        debug('HA field does not exist in ' + ATLAS_CONF)
    elif ha_enabled is False:
        debug('HA enabled is false in ' + ATLAS_CONF)

    # check the roles
    roles = ilio_dict.get('roles')
    if roles is None or len(roles) == 0:
        debug('Error getting role information. HA will NOT be enabled for this node')
        return (JSON_ROLE_NOT_DEFINED, ha_enabled, role, node_dict)
    role = roles[0]

    return (CLUS_SUCCESS, ha_enabled, role, node_dict)


##################################################################
# Functions related with network                                 #
##################################################################

# function to calculate how many 1s in the binary format of an int
# this function is used to calculate the cidr_netmask from netmask

def dec2bin(n):
    binstr = ''
    if n < 0:
        return (JSON_NEGATIVE_INT, binstr)
    if n == 0:
        return (CLUS_SUCCESS, '0')
    while n > 0:
        binstr = str(n % 2) + binstr
        n = n >> 1
    return (CLUS_SUCCESS, binstr)

def netmask2cidrmask(netmask):
    # calculate the cidr_netmask from netmask
    # For example, netmask 255.255.252.0 will be calculated to 22
    binmask = ''
    quadsplit = netmask.split(".")
    for num in quadsplit:
        (subret, quadint) = dec2bin(int(num))
        if (subret != CLUS_SUCCESS):
            return subret
        binmask = binmask + quadint

    # TODO: we do not validate whether the netmask is valid or not
    #       and just simply count the '1' insided the mask.
    cidrmask = binmask.count('1')
    debug('netmask is %s, binmask is %s, and cidrmask is %d' % (netmask, binmask, cidrmask))
    return (CLUS_SUCCESS, cidrmask)

##################################################################
# Functions related with lvm                                     #
##################################################################

def lvm_fetch_lv_in_vg(nodeip, vgname):
    # the command looks like below for remote node, local node, local ip
    # su-pool-mem-64 ~ # curl -k -X POST -H "Content-Type:application/json" -d "{\"type\":\"os\",\"command\":\"lvs\",\"arguments\":\" -o lv_name --noheadings\"}" http://10.121.109.64:8080/amc/commands/ops
    # su-pool-mem-64 ~ # curl -k -X POST -H "Content-Type:application/json" -d "{\"type\":\"os\",\"command\":\"lvs\",\"arguments\":\" -o lv_name --noheadings\"}" http://localhost:8080/amc/commands/ops
    # su-pool-mem-64 ~ # curl -k -X POST -H "Content-Type:application/json" -d "{\"type\":\"os\",\"command\":\"lvs\",\"arguments\":\" -o lv_name --noheadings\"}" http://127.0.0.1:8080/amc/commands/ops
    # the response looks like
    # {"stdErr":"","stdOut":"  atlas_internal_lv           \n  su-ads-memp-68_importsdisk  \n  atlas_internal_lv           \n  su-ads-mem-67_inmemory      \n  su-ads-memp-68_importsmemory\n","retCode":0}

    lvlist = None
    retlist = None
    args_str = ' ' + vgname + ' -o lv_name --noheadings'
    (rc, out, err) = remote_exec(nodeip, 'lvs', args_str)
    if rc != 0:
        debug('fetch lv from %s for vg %s failed, errcode=%d, errmsg=%s' %(nodeip, vgname, rc, err))
        return (rc, lvlist)

    lvlist = out.split('\n')
    retlist = [lv.strip() for lv in lvlist]
    debug('lvlist='+str(retlist))
    return (CLUS_SUCCESS, retlist)

def is_vg_used(nodeip, vgname):
    (ret, lvlist) = lvm_fetch_lv_in_vg(nodeip, vgname)
    if ret != CLUS_SUCCESS:
        return (LVM_FETCH_LV_FAIL, True)

    is_used = False
    for lv in lvlist:
        if lv == '':
            continue
        if lv != INTERNAL_LV_NAME:
            is_used = True
            break

    return (CLUS_SUCCESS, is_used)


##################################################################
# Functions related with Pacemaker                               #
##################################################################
def ha_check_enabled():
	debug('Enter ha_check_enabled')
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	try:
		node_dict = json.loads(s)
		ilio_dict = node_dict.get('usx')
		if ilio_dict is None:
			ilio_dict = node_dict
		ha = ilio_dict.get('ha')
		if ha is None or ha is False:
			debug('HA is NOT enabled')
			return False
		else:
			debug('HA is enabled')
			return True
	except ValueError as err:
		debug('HA is NOT enabled')
		return False


def ha_disable_ha_flag():
	debug('Enter ha_disable_ha_flag')
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	try:
		node_dict = json.loads(s)
		ilio_dict = node_dict.get('usx')
		if ilio_dict is None:
			debug('wrong json file %s' %str(node_dict))
			return 1
		ha = ilio_dict.get('ha')
		if ha is None or ha is False:
			debug('HA flag has been already NOT enabled')
			return 0
		else:
			debug('HA flag was enabled, change it to be disabled')
	except ValueError as err:
		debug('ERROR: wrong HA is NOT enabled')
		return 1

	# save the new jason file
	tmp_fname = '/tmp/new_atlas_conf.json'
	cfgfile = open(tmp_fname, "w")
	node_dict['usx']['ha'] = False
	json.dump(node_dict, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
	cfgfile.close()
	os.rename(tmp_fname, ATLAS_CONF)
	debug('done with disabling HA flag')
	cmd = 'python /opt/milio/atlas/roles/ha/usx_daemon.pyc restart '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	return 0


def ha_enable_ha_flag():
	debug('Enter ha_enable_ha_flag')
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	try:
		node_dict = json.loads(s)
		ilio_dict = node_dict.get('usx')
		if ilio_dict is None:
			debug('ERROR: wrong json file %s' %str(node_dict))
			return 1
		ha = ilio_dict.get('ha')
		if ha is None or ha is False:
			debug('HA was disabled')
		else:
			debug('HA has been already enabled')
			return 0
	except ValueError as err:
		debug('ERROR: wrong atlas.json')
		return 1

	# save the new jason file
	tmp_fname = '/tmp/new_atlas_conf.json'
	cfgfile = open(tmp_fname, "w")
	node_dict['usx']['ha'] = True
	json.dump(node_dict, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
	cfgfile.close()
	os.rename(tmp_fname, ATLAS_CONF)
	debug('done with enabling HA flag')
	return 0


def ha_retrieve_config():
	debug('Enter ha_retrieve_config')
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()

	#"volumeresources": [
	#	{
	#		"containeruuid": "vc13417_AAA-111-53B-tis18-Hybrid-111",
	#		"dedupfsmountpoint": "/exports/AAA-111-53B-tis18-Hybrid-111",
	#		"exporttype": "NFS",
	#		"serviceip": "10.121.148.51"
	#		"volumetype": "HYBRID"
	#	}
	#]
	containeruuid = ''
	dedupfsmountpoint = ''
	exporttype = ''
	service_ip = ''
	volumetype = ''
	try:
		node_dict = json.loads(s)
		if node_dict.has_key('volumeresources'):
			if node_dict['volumeresources']:
				if node_dict['volumeresources'][0].has_key('containeruuid'):
					containeruuid = node_dict['volumeresources'][0]['containeruuid']

				if node_dict['volumeresources'][0].has_key('dedupfsmountpoint'):
					dedupfsmountpoint = node_dict['volumeresources'][0]['dedupfsmountpoint']

				if node_dict['volumeresources'][0].has_key('exporttype'):
					exporttype = node_dict['volumeresources'][0]['exporttype']

				if node_dict['volumeresources'][0].has_key('serviceip'):
					service_ip = node_dict['volumeresources'][0]['serviceip']

				if node_dict['volumeresources'][0].has_key('volumetype'):
					volumetype = node_dict['volumeresources'][0]['volumetype']

	except ValueError as err:
		debug('Exception caught within ha_retrieve_config')

	return (containeruuid, dedupfsmountpoint, exporttype, service_ip, volumetype)


def ha_get_local_node_name():
    try:
        hostname = socket.gethostname()
    except:
        debug('ERROR: Can not get hostname!')
        return None
    return hostname
'''
	cmd = 'corosync-quorumtool -i | grep local'
	(ret, msg) = runcmd(cmd, print_ret=False, lines=True)
	for line in msg:
		if line.find('local') >= 0:
			node_id = line.split()[0]
			cmd = 'crm_node -l'
			(ret, submsg) = runcmd(cmd, print_ret=False, lines=True)
			for line1 in submsg:
				tmp = line1.split()
				if len(tmp) < 2:
					continue
				if tmp[0] == node_id:
					return tmp[1]
	return None
'''


# Get json file from pacemaker but not USX Manager
# We store base64 encoded atlas.json and pool_infrastructure.json-
# in pacemaker when Enable HA
# type: resource or raid or raid1PrimaryInfo
def ha_get_conf_from_crm(type, resource_uuid):
    debug("Get %s conf from crm for %s" %(type, resource_uuid))
    conf_json = '{}'

    if type in ['resource', 'raid']:
        cmd = 'crm resource param ' + resource_uuid + '_ds show ' + type + 'Json'
    elif type in ['raid1PrimaryInfo']:
        cmd = 'crm resource param ' + resource_uuid + '_ip show ' + type + 'Json'
    else:
        debug("Unsupport type")
        return conf_json

    (ret, msg) = ha_retry_cmd(cmd, 1, 1)
    if ret == 0:
        if len(msg) == 1:
            conf_json = zlib.decompress(base64.decodestring(msg[0]))

    return conf_json


def ha_build_rest_api_url(feature, type, resource_uuid=None):
    url_list = []
    feature_url = {
            RESOURCE: "/usx/inventory/volume/resources"
            }
    type_url = {
            RAID1PRIMARY: "/raid1primary"
            }
    if feature in feature_url and type in type_url:
        url_list.append(feature_url[feature])
        if resource_uuid:
            url_list.append("/{}".format(resource_uuid))
        url_list.append(type_url[type])
    return ''.join(url_list)


def ha_get_conf_from_usxm(feature, type, resource_uuid):
    debug("Get {type} under {feature} conf from USX Manager for {resource_uuid}".format(type=type, feature=feature, resource_uuid=resource_uuid))
    conf_json = {}

    (ret, res_data) = ha_query_amc2(LOCAL_AGENT, ha_build_rest_api_url(feature, type, resource_uuid), 1)
    if ret == 0:
        conf_json = res_data

    return conf_json


def ha_update_conf_to_usxm(feature, type, resource_uuid, data):
    debug("Update {type} under {feature} conf to USX Manager for {resource_uuid}".format(type=type, feature=feature, resource_uuid=resource_uuid))
    return ha_post_amc(LOCAL_AGENT, ha_build_rest_api_url(feature, type, resource_uuid), data, 1)


# Update json file to pacemaker
# type: resource or raid or raid1PrimaryInfo
def ha_update_conf_to_crm(type, resource_uuid, data):
    debug("Update %s conf to crm for %s" %(type, resource_uuid))
    ret = 1

    (rc, msg) = ha_retry_cmd('crm_mon -r1', 1, 1)
    if len(msg) > 0 and "Connection to cluster failed: Transport endpoint is not connected" in msg[0]:
        debug("ERROR: Failed to connect to pacemaker cluster")
    else:
        if type in ['resource', 'raid']:
            cmd = 'crm resource param ' + resource_uuid + '_ds set ' + type + 'Json ' + \
                    base64.encodestring(zlib.compress(str(data))).replace('\n', '')
            (ret, msg) = ha_retry_cmd(cmd, 2, 5)
        elif type in ['raid1PrimaryInfo']:
            if json.dumps(json.loads(ha_get_conf_from_crm(type, resource_uuid))) == data:
                debug("Same data is already in pacemaker, no need to update")
            else:
                debug(json.dumps(json.loads(ha_get_conf_from_crm(type, resource_uuid))))
                debug(data)
                cmd = 'crm resource param ' + resource_uuid + '_ip set ' + type + 'Json ' + \
                        base64.encodestring(zlib.compress(str(data))).replace('\n', '')
                (ret, msg) = ha_retry_cmd(cmd, 2, 5)
                # Update parameters for resource will lead to pacemaker try to stop the resources, we need to cleanup the failed status for "_ip"
                time.sleep(2)
                cmd = 'crm_resource -C -r {ip_resource} --node {node_name}'.format(ip_resource=resource_uuid + "_ip", node_name = ha_get_local_node_name())
                ha_retry_cmd(cmd, 2, 5)
        else:
            debug("Unsupport type")
    return ret


def ha_check_maintenance_mode():
	debug('Enter ha_check_maintenance_mode')
	node_name = ha_get_local_node_name()
	if node_name == None:
		return False
	else:
		cmd = 'crm node status ' + node_name
		(ret, msg) = runcmd(cmd, print_ret=False, lines=True)
		for the_line in msg:
			if the_line.find('name="maintenance" value="on"') >= 0:
				return True
	return False


def ha_set_maintenance_mode():
	debug('Enter ha_set_maintenance_mode')
	cmd = 'crm node maintenance '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	return ret

def ha_set_ready_mode():
	debug('Enter ha_set_ready_mode')
	cmd = 'crm node ready '
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
	return ret

#TODO: should be removed, dead code.
def ha_set_quorum_policy():
	total_num_nodes = 0
	num_maintenance_nodes = 0
	num_active_nodes = 0

	node_name = ha_get_local_node_name()
	cmd = 'crm node status '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	for the_line in msg:
		if the_line.find('</node>') >= 0:
			total_num_nodes += 1
		elif the_line.find('name="maintenance" value="on"') >= 0:
			num_maintenance_nodes += 1

	quorum_policy = 'freeze'
	num_active_nodes = total_num_nodes - num_maintenance_nodes
	debug('total_num_nodes = %d, num_maintenance_nodes = %d, num_active_nodes = %d '
		%(total_num_nodes, num_maintenance_nodes, num_active_nodes))
	if num_active_nodes < 0:
		debug('ERROR: num_active_nodes = %d ' % num_active_nodes)
		return 1
	elif num_active_nodes <= 2:
		quorum_policy = 'ignore'
	else:
		quorum_policy = 'freeze'

	needset = False
	cmd = 'crm_attribute --type crm_config --name no-quorum-policy --query'
	(ret, msg) = runcmd(cmd, print_ret=True)
	if ret == 6:
		needset = True
	else:
		policy_str = 'name=no-quorum-policy value=' + quorum_policy
		msgindex = msg.find(policy_str)
		if msgindex >= 0:
			# the policy has been set correctly
			debug('node %s configure quorum policy %s had been done before' %(node_name, quorum_policy))
			return 0
		else:
			# the policy is different
			needset = True

	if needset:
		cmd = 'crm_attribute --type crm_config --name no-quorum-policy --update ' + quorum_policy
		(ret, msg) = runcmd(cmd, print_ret=True)
		if ret != 0:
			if msg.find('Update was older than existing configuration') >= 0:
				return 0
			debug('ERROR : fail to run %s, err=%s, msg=%s' % (cmd, str(ret),
				msg))
			return 1
	return 0


# Consider HA should be in enabled state.
def is_stretchcluster():
    debug('Enter is_stretchcluster')

    # HyperScale CX-4 is considered as Stretch Cluster
    if ( is_hyperscale() and is_robo() ):
        debug("HyperScale CX-4 is considered as Stretch Cluster")
        return True

    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    try:
        node_dict = json.loads(s)

        ilio_dict = node_dict.get('usx')
        if ilio_dict is None:
            ilio_dict = node_dict
        ha = ilio_dict.get('ha')
        if not ha:
            # HA is not enabled.
            return False

        ha_dict = node_dict.get('haconfig')
        if ha_dict is None:
            return False
        flag = False
        if ha_dict.has_key('stretchcluster'):
            flag = ha_dict.get('stretchcluster')
        if flag is None or flag is False:
            debug('It is NOT a stretch cluster')
            return False
        else:
            debug('It is a stretch cluster')
            return True
    except:
        debug('ERROR: Exception on checking is_stretchcluster')
        return False


# Consider HA should be in enabled state.
def is_robo():
	debug('Enter is_robo')
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	try:
		node_dict = json.loads(s)

		ilio_dict = node_dict.get('usx')
		if ilio_dict is None:
			ilio_dict = node_dict
		ha = ilio_dict.get('ha')
		if not ha:
			# HA is not enabled.
			return False

		ha_dict = node_dict.get('haconfig')
		if ha_dict is None:
			return False
		flag = False
		if ha_dict.has_key('raid1enabled'):
			flag = ha_dict.get('raid1enabled')
		if flag is None or flag is False:
			debug('It is NOT in a robo ha group')
			return False
		else:
			debug('It is in a robo ha group')
			return True
	except Exception as err:
		debug('ERROR: Exception on checking is_robo')
	return False


def is_stretchcluster_or_robo():
	debug('Enter is_stretch_or_robo')
	if (is_stretchcluster() or is_robo()):
		return True
	else:
		return False


def is_infrastructure():
    debug('Enter is_infrastructure')
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    try:
        node_dict = json.loads(s)

        ilio_dict = node_dict.get('usx')
        if ilio_dict is None:
            ilio_dict = node_dict
        ha = ilio_dict.get('ha')
        if not ha:
            # HA is not enabled.
            return False

        ha_dict = node_dict.get('haconfig')
        if ha_dict is None:
            return False
        flag = False
        if ha_dict.has_key('infrastructurevolume'):
            flag = ha_dict.get('infrastructurevolume')
        if flag is None or flag is False:
            debug('It is NOT in an infrastructurevolume ha group')
            return False
        else:
            debug('It is in an infrastructurevolume ha group')
            return True
    except Exception as err:
        debug('ERROR: Exception on checking is_infrastructure')
    return False


def is_hyperscale():
    debug('Enter is_hyperscale')
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    try:
        node_dict = json.loads(s)

        ilio_dict = node_dict.get('usx')
        if ilio_dict is None:
            ilio_dict = node_dict

        flag = False
        if ilio_dict.has_key("chassisuuid"):
            flag = (ilio_dict["chassisuuid"] != 'chassis1')
        if flag is False:
            debug('It is NOT a HyperScale node')
            return False
        else:
            debug('It is a HyperScale node')
            return True
    except Exception as err:
        debug('ERROR: Exception on checking is_hyperscale')
    return False


# Doesn't consider whether HA is enabled.
def is_stretchcluster_or_robo_raw():
	debug('Enter is_stretchcluster_or_robo_raw')
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	try:
		node_dict = json.loads(s)

		ha_dict = node_dict.get('haconfig')
		if ha_dict is None:
			return False
		flag_strechcluster = None
		flag_robo = None
		if ha_dict.has_key('stretchcluster'):
			flag_strechcluster = ha_dict.get('stretchcluster')
		if ha_dict.has_key('raid1enabled'):
			flag_robo = ha_dict.get('raid1enabled')

		if (flag_strechcluster is None or flag_strechcluster is False):
			debug('It is NOT a stretch cluster')
			if (flag_robo is None or flag_robo is False):
				debug('It is NOT a robo cluster')
				return False
			else:
				debug('It is a robo cluster')
				return True
		else:
			debug('It is a stretch cluster')
			return True
	except:
		debug('ERROR: Exception on checking is_stretchcluster_or_robo_raw')
		return False


def is_hyperconverged():
    debug('Enter is_hyperconverged')
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    try:
        node_dict = json.loads(s)

        ha_dict = node_dict.get('haconfig', None)
        if ha_dict != None:
            flag = ha_dict.get('hyperconverged', None)
            if flag is None or flag is False:
                debug('It is not in a hyperconverged HA group.')
                return False
            else:
                debug('It is in a hyperconverged HA group.')
                return True

        ilio_dict = node_dict.get('usx', None)
        if ilio_dict is None:
            ilio_dict = node_dict

        flag = ilio_dict.get('hyperconverged', None)
        if flag is None or flag is False:
            debug('It is not a hyperconverged volume.')
            return False
        else:
            debug('It is a hyperconverged volume.')
            return True
    except Exception as err:
        debug('ERROR: Exception on checking is_hyperconverged')
    return False

def change_crash_file_location():
    debug('Enter change_crash_file_location.')
    kdump_tools_location = '/etc/default/kdump-tools'
    cmd_str = "sed -i 's/\/var\/crash/\/var\/log\/crash/g' %s" %(kdump_tools_location)
    ha_retry_cmd(cmd_str, 2, 5)
    return 0

# Get Service VM's managementip, exportuuid, hypervisoruuid, containeruuid
def ha_get_service_vm_info():
    debug('Enter ha_get_service_vm_info')
    svm_info_list = {}

    # Using previous Service VMs if USX Manager is disconnected
    if not check_usxmanager_alive_flag():
        debug('ERROR: failed to get SVM management ip list from USX Manager by resourceuuid')
        if os.path.exists(SVM_INFO_LIST) == True:
            previous_svm_info = _json_load(SVM_INFO_LIST)
            debug('Get Service VM info from local file')
            return previous_svm_info

    ret = 1
    try:
        # Get Resource uuid in ha group
        grps = ha_get_res_group_list()
        resource_uuid_in_group = grps[0][0]

        # Get Service VM info list from USX Manager by resourceuuid
        # /usxmanager/usx/inventory/volume/resources?query=.%5Buuid%3D%27USX_10cb95ae-7df2-3157-bc40-b3e33e518942%27%5D&fields=raidplans
        svm_info_query = "/usx/inventory/volume/resources?query=.%5Buuid=\'" + resource_uuid_in_group + "\'%5D&fields=raidplans"
        (ret, res_data) = ha_query_amc2(LOCAL_AGENT, svm_info_query, 2)

        if ret == 0:
            if res_data['count'] == 1:
                fulljson = res_data['items']
                if len(fulljson) > 0:
                    for key in fulljson:
                        if key.has_key("raidplans"):
                            for rp in key['raidplans']:
                                if rp.has_key('raidbricks'):
                                    for item in rp['raidbricks']:
                                        if item.has_key('managementip') and item.has_key('hypervisoruuid') and item.has_key('exportuuid') and item.has_key('serviceip'):
                                            svm_info_list[item['managementip']] = {'managementip':item['managementip'],'hypervisoruuid':item['hypervisoruuid'],'exportuuid':item['exportuuid'], 'serviceip':item['serviceip']}
    except:
        debug('ERROR: Exception when try to get SVM management ip list from USX Manager by resourceuuid')
    finally:
        if ret != 0:
            debug('ERROR: failed to get SVM management ip list from USX Manager by resourceuuid')
            if os.path.exists(SVM_INFO_LIST) == True:
                previous_svm_info = _json_load(SVM_INFO_LIST)
                debug('Get Service VM info from local file')
                return previous_svm_info

    # Get Service VM container uuid from USX Manager by exportuuid
    # /usxmanager/usx/inventory/servicevm/containers?query=.%5Bexportuuids=\'USX_c67e87c9-4d25-343f-8fe3-42b443d4f593\'%5D
    for svm in svm_info_list:
        containeruuid_query = "/usx/inventory/servicevm/containers?query=.%5Bexportuuids=\'" + svm_info_list[svm]['exportuuid'] + "\'%5D"
        (ret, res_data) = ha_query_amc2(LOCAL_AGENT, containeruuid_query, 2)
        try:
            if ret == 0:
                if res_data['count'] == 1:
                    fulljson = res_data['items']
                    if len(fulljson) > 0:
                        for key in fulljson:
                            if key.has_key("uuid"):
                                svm_info_list[svm]['containeruuid'] = key['uuid']
        except:
            debug('ERROR: Exception when try to get SVM container uuid from USX Manager by exportuuid')
        finally:
            if ret != 0:
                debug('ERROR: failed to get SVM container uuid from USX Manager by exportuuid')
                if os.path.exists(SVM_INFO_LIST) == True:
                    previous_svm_info = _json_load(SVM_INFO_LIST)
                    debug('Get Service VM info from local file')
                    return previous_svm_info

    debug('svm_info_list is: ' + str(svm_info_list))

    # Save to SVM_INFO_LIST
    previous_svm_info = _json_load(SVM_INFO_LIST)
    if previous_svm_info == svm_info_list:
        return svm_info_list
    if svm_info_list != {}:
        fd = open(SVM_INFO_LIST, "w")
        json.dump(svm_info_list, fd)
        fd.flush()
        fd.close()

    return svm_info_list


def _json_load(file):
    output = {}
    try:
        fd = open(file, "r")
        output = json.load(fd)
        fd.close()
    except:
        pass
    return output

# This method is for ROBO with or without Stretch Cluster
# Just name the first one as local and the last one is remote
# Return local_svm_dict and remote_svm_dict
def ha_get_local_remote_Service_VM():
    debug('ha_get_local_remote_Service_VM')
    local_svm_dict = {}
    remote_svm_dict = {}
    svm_info_list = ha_get_service_vm_info()
    if svm_info_list == {}:
        debug("ERROR: Failed to get SVM info")
    else:
        local_svm_dict = svm_info_list.values()[0]
        remote_svm_dict = svm_info_list.values()[1]

    return (local_svm_dict, remote_svm_dict)


# Ger Service VM Power status from USX Manager
# Return local_svm_power_status and remote_svm_power_status
def ha_get_local_remote_Service_VM_power_status():
    list = []

    # Return (255,255) if USX Manager is disconnected
    if not check_usxmanager_alive_flag():
        return (255, 255)

    (local_svm, remote_svm) = ha_get_local_remote_Service_VM()
    if local_svm == {} or remote_svm == {}:
        debug('ERROR: Failed to get local or remote Service VM')
        return (255, 255)

    # Get Service VM Power status from USX Manager
    # http://127.0.0.1:8080/usxmanager/usx/status/SERVICE_CONTAINER/USX_79d1f2d4-4111-3427-a728-8e86a1b9c1c6/
    for svm in [local_svm, remote_svm]:
        svm_power_status_query = "/usx/status/SERVICE_CONTAINER/" + svm['containeruuid']
        (ret, res_data) = ha_query_amc2(LOCAL_AGENT, svm_power_status_query, 2)

        try:
            if ret == 0:
                if res_data.has_key('usxstatuslist'):
                    fulljson = res_data['usxstatuslist']
                    if len(fulljson) > 0:
                        for key in fulljson:
                            if key.has_key('name'):
                                if key['name'] == 'POWER_STATUS':
                                    if key.has_key('value'):
                                        if key['value'] == 'ON':
                                            list.append(1)
                                        else:
                                            list.append(0)
                                        break
        except:
            debug('ERROR: Exception on get local Service VM')

    if len(list) == 2:
        return (list[0], list[1])
    else:
        return (255, 255)


def ha_has_quorum():
	# crm_node might say we has quorum event if we actually don't: USX-48953.
	#cmd = 'crm_node -q '
	cmd = 'crm_mon -1 '
	(ret, msg) = runcmd(cmd,  print_ret=False, lines=True)
	if ret == 0:
		for the_line in msg:
			if 'partition with quorum' in the_line:
				return True
	# In the no quorum case, it will be: 'partition WITHOUT quorum'
	debug('WARN: no quorum!')
	return False


# Check USX Manager is alive or not
def is_usxmanager_alive():
    debug('Enter is_usxmanager_alive')
    amc_query = '/settings/DEPLOYMENT/key'
    (rc, res_data) = ha_query_amc2(LOCAL_AGENT, amc_query, 2)
    if rc == 0:
        debug('USX Manager is alive')
        if os.path.exists(USXM_DEAD_FILE) == True:
            try:
                os.remove(USXM_DEAD_FILE)
            except:
                debug('USXM_DEAD_FILE is already removed')
        return True
    else:
        debug('USX Manager is disconnected')
        if os.path.exists(USXM_DEAD_FILE) == False:
            cmd = 'touch ' + USXM_DEAD_FILE
            ha_retry_cmd(cmd, 2, 3)
        return False


# Check USX Manager dead flag file
def check_usxmanager_alive_flag():
#    debug('Enter check_usxmanager_alive_flag')
    return not os.path.exists(USXM_DEAD_FILE)


# Use previous tiebreaker
def ha_get_previous_tiebreaker(tiebreakerfile):
    tiebreakerip = []
    if os.path.exists(tiebreakerfile) == True:
        fd = open(tiebreakerfile, "r")
        tiebreakerip = [line.replace("\n", '') for line in fd.readlines()]
        fd.close()
        debug('Keep use previous tiebreaker')
    return tiebreakerip


# Get vmm ip
def get_vmmanager_ip():
    debug('Enter get_vmmanager_ip')
    vmm_ip = '0.0.0.0'
    if os.path.exists(VMMANAGER_IP) == True:
        fd = open(VMMANAGER_IP, "r")
        lines = [line.replace("\n", '') for line in fd.readlines()]
        fd.close()
        if len(lines) > 0:
            return lines[0]

    amc_query = '/vmm/volumeonvmmanager'
    try:
        (rc, res_data) = ha_query_amc2(LOCAL_AGENT, amc_query, 2)
        if rc == 0:
            vmm_ip = res_data['data']['ipaddress']
    except:
            vmm_ip = '0.0.0.0'

    # Save to VMMANAGER_IP
    if vmm_ip != '0.0.0.0':
        fd = open(VMMANAGER_IP, "w")
        fd.write("%s\n" % vmm_ip)
        fd.flush()
        fd.close()

    return vmm_ip


def is_vmmanager_reachable():
    debug('Enter is_vmmanager_reachable')
    ip = get_vmmanager_ip()
    return is_reachable(ip, 2, 5)


def is_reachable(ip, retry_time, time_interval):
    if ip not in ['0.0.0.0', '']:
        cmd = 'ping -c 4 -W 2 ' + ip
        (rc, msg) = ha_retry_cmd(cmd, retry_time, time_interval)
        if rc == 0 and len(msg) > 0:
            for line in msg:
                if '0 received' in line:
                    break
            else:
                return True
    return False


# Get tiebreaker from USX Manager
def ha_get_tiebreakerip_from_usxm():
    debug('Enter ha_get_tiebreakerip_from_usxm')
    tiebreakerip = []

    # User previous tiebreaker if USX Manager is disconnected
    if not check_usxmanager_alive_flag():
        return ha_get_previous_tiebreaker(USXM_TIEBREAKER_LIST)

    # curl -X GET http://127.0.0.1:8080/usxmanager/settings/DEPLOYMENT/tiebreakerip
    amc_query = '/settings/DEPLOYMENT/tiebreakerip'
    (rc, res_data) = ha_query_amc2(LOCAL_AGENT, amc_query, 2)
    if len(res_data) >= 1:
        tiebreakerip = [res_data.get('tiebreakerip')]
    else:
        return ha_get_previous_tiebreaker(USXM_TIEBREAKER_LIST)

    # Save to USXM_TIEBREAKER_LIST
    if os.path.exists(USXM_TIEBREAKER_LIST) == True:
        fd = open(USXM_TIEBREAKER_LIST, "r")
        lines = [line.replace("\n", '') for line in fd.readlines()]
        fd.close()
        if lines == tiebreakerip:
            return tiebreakerip
    if tiebreakerip != []:
        fd = open(USXM_TIEBREAKER_LIST, "w")
        for the_ip in tiebreakerip:
               fd.write("%s\n" % the_ip)
        fd.flush()
        fd.close()

    return tiebreakerip


# Get HA group uuid
def get_ha_uuid():
    debug('Enter get_ha_uuid')
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    hauuid = None
    try:
        node_dict = json.loads(s)
        ha_config = node_dict.get('haconfig')
        if ha_config is None:
            debug('ERROR: wrong json file %s' %str(node_dict))
        hauuid = node_dict['haconfig'].get('uuid')
        if hauuid is None:
            debug('ERROR: cannot find hauuid')
    except ValueError as err:
        debug('ERROR: wrong atlas.json')

    return hauuid


# Return a list, because in below situations, we will use 2 Service VM's managementip
# 1. ROBO without Stretch Cluster
# 2. ROBO Stretch cluster but the user-set tiebreaker is not isolated
# list[0] is local tiebreakerip and list[1] is remote tiebreakerip
def ha_get_tiebreakerip():
    debug('Enter ha_get_tiebreakerip')
    tiebreakerip        = []
    tiebreaker_status   = 1
    stretchcluster_flag = is_stretchcluster()
    robo_flag           = is_robo()
    previous_tb         = 'USERSET'
    current_tb          = 'USERSET'

    # If USX Manager is disconnected, use previous tiebreaker anyway
    if check_usxmanager_alive_flag():
        # Double ckeck, this step is quick if USX Manager is alive
        if not is_usxmanager_alive():
            debug('Using previous tiebreaker when USX Manager is disconnected')
            return ha_get_previous_tiebreaker(TIEBREAKER_LIST)

    # Get tiebreaker from USX Manager(to avoid using '0.0.0.0' for ROBO)
    tiebreakerip_usxm = ha_get_tiebreakerip_from_usxm()

    # For Normal StretchCluster, use user-set tiebreaker anyway
    if stretchcluster_flag and not robo_flag:
        debug('Use user-set tiebreaker for Normal Stretch Cluster')
    # For Non-StretchCluster ROBO, use Service VMs anyway
    elif robo_flag and not stretchcluster_flag:
        debug('Use Service VMs as tiebreaker for Non-StretchCluster ROBO')
        current_tb = 'SVMS'
    # For ROBO StretchCluster, we need to check the tiebreaker status from USX Manager
    elif robo_flag and stretchcluster_flag:
        # If user-set tiebreaker is 0.0.0.0, use Service VMs
        if tiebreakerip_usxm in [['0.0.0.0'], []]:
            debug('Tiebreaker from USX Manager is 0.0.0.0 or failed to get tiebreaker from both \
                    USX Manager and local JSON file, use Service VMs instead')
            current_tb = 'SVMS'
        else:
            debug('Use user-set tiebreaker for ROBO Stretch Cluster')
            current_tb = 'USERSET'
#            # Get ha group uuid
#            ha_uuid = get_ha_uuid()
#
#            # Get tiebreaker status for this HA Group from USX Manager
#            # 0 for the tiebreaker is working for all nodes, should use user-set tiebreaker
#            # 3 for the tiebreaker is not working for all nodes, should use Service VMs
#            # 1 for unknown, keep use the previous tiebreaker
#            # curl -X GET http://127.0.0.1:8080/usxmanager/usx/status/tiebreakerStatus
#            amc_query = '/usx/status/tiebreakerStatus/' + ha_uuid
#            (rc, res_data) = ha_query_amc2(LOCAL_AGENT, amc_query, 2)
#            current_tb = 'PREVIOUS'
#            if rc == 0:
#                if len(res_data) >= 1:
#                    tiebreaker_status = res_data.get('status')
#                    if tiebreaker_status == 0:
#                        debug('User-set tiebreaker is accessible from all ndoes, use it as tiebreaker')
#                        current_tb = 'USERSET'
#                    elif tiebreaker_status == 3:
#                        debug('User-set tiebreaker is inaccessible from all ndoes, use Service VMs as tiebreaker')
#                        current_tb = 'SVMS'

    if current_tb == 'PREVIOUS':
        # Check previous tiebreaker
        if os.path.exists(TIEBREAKER_LIST) == True:
            fd = open(TIEBREAKER_LIST, "r")
            tiebreakerip = [line.replace("\n", '') for line in fd.readlines()]
            fd.close()
            debug('Keep use previous tiebreaker')
            return tiebreakerip
        else:
            current_tb = 'USERSET'

    if current_tb == 'USERSET':
        tiebreakerip = tiebreakerip_usxm
    elif current_tb == 'SVMS':
        (local_svm, remote_svm) = ha_get_local_remote_Service_VM()
        if local_svm.has_key('managementip') and remote_svm.has_key('managementip'):
            tiebreakerip = [local_svm['managementip'], remote_svm['managementip']]
        else:
            debug('ERROR: Failed to get local or remote Service VM')

    # Save to TIEBREAKER_LIST
    if os.path.exists(TIEBREAKER_LIST) == True:
        fd = open(TIEBREAKER_LIST, "r")
        lines = [line.replace("\n", '') for line in fd.readlines()]
        fd.close()
        if lines == tiebreakerip:
            return tiebreakerip
    if tiebreakerip != []:
        fd = open(TIEBREAKER_LIST, "w")
        for the_ip in tiebreakerip:
               fd.write("%s\n" % the_ip)
        fd.flush()
        fd.close()

    return tiebreakerip


def ha_get_scl_timeout():
	debug('Enter ha_get_scl_timeout')

	return HA_STRETCHCLUSTER_LOCK_TIMEOUT


def ha_get_no_quorum_policy():
	debug('Enter ha_get_no_quorum_policy')

	quorum_policy = 'freeze'
	cmd = 'crm_attribute --type crm_config --name no-quorum-policy --query'
	(ret, msg) = runcmd(cmd, print_ret=True)

	if ret == 0:
		if msg.find('value=ignore') >= 0:
			quorum_policy = 'ignore'

	return quorum_policy


def ha_set_no_quorum_policy(quorum_policy, force_flag):
	debug('Enter ha_set_no_quorum_policy')

	node_list = ha_get_node_list()

	if len(node_list) <= 2 and force_flag == False:
		quorum_policy = 'ignore'

	cmd = 'crm_attribute --type crm_config --name no-quorum-policy --update ' + quorum_policy
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

	return ret


def ha_stop_volume(vol_uuid):
	debug('Enter ha_stop_volume %s' % vol_uuid)

	reset_vm('STRETCH_CLUSTER_ha_stop_volume')
	#cmd = 'python /opt/milio/atlas/roles/virtvol/vv-load.pyc usx_stop ' + vol_uuid + ' > /dev/null 2>&1'
	#(ret, msg) = runcmd(cmd, print_ret=True)
	return ret


def ha_set_firsttime_flag():
    debug('Enter ha_set_firsttime_flag')
    cmd = 'touch ' + HA_FIRSTTIME_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)


def ha_remove_firsttime_flag():
    debug('Enter ha_remove_firsttime_flag')
    cmd = 'rm ' + HA_FIRSTTIME_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)


def ha_check_firsttime_flag():
    if os.path.isfile(HA_FIRSTTIME_FLAG):
        return True
    else:
        return False


def set_skip_mount_snapshot_flag():
    debug('Enter set_skip_mount_snapshot_flag')
    cmd = 'touch ' + SKIP_MOUNT_SNAPSHOT_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)


def remove_skip_mount_snapshot_flag():
    debug('Enter remove_skip_mount_snapshot_flag')
    cmd = 'rm ' + SKIP_MOUNT_SNAPSHOT_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)


def check_skip_mount_snapshot_flag():
    if os.path.isfile(SKIP_MOUNT_SNAPSHOT_FLAG):
        return True
    else:
        return False

def ha_set_forcestarting_flag():
    debug('Enter ha_set_forcestarting_flag')
    cmd = 'touch ' + HA_FORCESTARTING_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)


def ha_remove_forcestarting_flag():
    debug('Enter ha_remove_forcestarting_flag')
    cmd = 'rm ' + HA_FORCESTARTING_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)


def ha_check_forcestarting_flag():
    if os.path.isfile(HA_FORCESTARTING_FLAG):
        return True
    else:
        return False

def ha_file_flag_operation(file, operation):
    cmd = ''
    if operation == 'check':
        return os.path.isfile(file)
    elif operation == 'set':
        cmd = 'touch ' + file
    elif operation == 'remove':
        cmd = 'rm ' + file
    else:
        debug("Unsupprt operation.")
        return False
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)
    return True

def ha_disable_wr_hook(dev):
	debug('Enter ha_disable_wr_hook %s' % dev)

	cmd = '/bin/ibdmanager -r a -d ' + dev
	(ret, msg) = ha_retry_cmd(cmd, 2, 3)

	f = open(IBD_AGENT_CONFIG_FILE, "r")
	lines = f.readlines()
	f.close()

	f = open(IBD_AGENT_CONFIG_FILE, "w")
	for the_line in lines:
		# remove wr_hook
		tmp = the_line.split()
		if the_line.find("wr_hook = ") >= 0 and dev == tmp[-1]:
			continue
		else:
			f.write(the_line)
	f.close()

	cmd = '/bin/ibdmanager -r a -u'
	(ret, msg) = ha_retry_cmd(cmd, 2, 3)


def ha_get_crm_config(name):
    cmd = 'crm_attribute --type crm_config --name {} --query --quiet'.format(name)
    rc, msg = ha_retry_cmd(cmd, 2, 3)
    if rc == 0 and len(msg) > 0 and 'Error performing operation' not in msg[0]:
        return msg[0]


# Pre-failover validation for 2 nodes HA group if "no-quorum-policy" is "ignore"
def ha_pre_failover_validation(volume_uuid):
    if ha_get_crm_config('no-quorum-policy') == 'ignore':
        return ha_check_resource_status_on_the_other_node(volume_uuid)


# 0 means the other node is starting or already started the resource
def ha_check_resource_status_on_the_other_node(volume_uuid):
    rc = 1
    other_node_ip = ha_get_other_node_ip()
    if is_reachable(other_node_ip, 1, 1):
        cmd = 'python /opt/milio/atlas/roles/virtvol/vv-load.pyc status'
        (rc, _, _) = remote_exec(other_node_ip, cmd, volume_uuid)
        if rc == 0:
            cmd2 = "ls {}".format(TEARDOWN_FLAG)
            (ret, _, _) = remote_exec(other_node_ip, cmd2, '')
            if ret == 0:
                rc = 2
        elif rc == 1:
            cmd2 = "ps -ef| grep '/opt/milio/atlas/roles/virtvol/vv-load.pyc ha {}' | grep -v grep".format(volume_uuid)
            (rc, _, _) = remote_exec(other_node_ip, cmd2, '')
    return rc


def ha_get_other_node_ip():
    other_node_ip = ''
    recorded_other_node_ip = ''
    total_nodes = ha_get_node_list()
    if len(total_nodes) == 2:
        total_nodes.remove(ha_get_local_node_name())
        remote_node_name = total_nodes[0]
        if check_usxmanager_alive_flag():
            # /usx/inventory/volume/containers?query=.%5Bdisplayname%3D'tis33AA-VOLUME-Testbed825-001'%5D
            url = "/usx/inventory/volume/containers?query=.%5Bdisplayname%3D'{}'%5D".format(remote_node_name)
            rc, msg = ha_query_amc2(LOCAL_AGENT, url, 2)
            try:
                if rc == 0 and len(msg['items']) > 0:
                    for nc in msg['items'][0]['nics']:
                        if nc['storagenetwork']:
                            other_node_ip = nc['ipaddress']
            except Exception as e:
                debug("Catch exception when trying to get other node ip from USX Manager {}".format(e))

    try:
        if os.path.exists(OTHER_NODE_IP) == True:
            with open(OTHER_NODE_IP, 'r') as fd:
                lines = [line.replace("\n", '') for line in fd.readlines()]
                recorded_other_node_ip = lines[0] if lines else ''
        if not other_node_ip:
            other_node_ip = recorded_other_node_ip
        elif recorded_other_node_ip != other_node_ip:
            with open(OTHER_NODE_IP, 'w') as fd:
                    fd.write(other_node_ip)
                    fd.flush()
    except Exception as e:
        debug("Catched exception when trying to get other node ip from local file {}".format(e))

    return other_node_ip


# Return a list since now tiebreakip is a list
# 0 for acquire lock successfully, 1 for lock is already owned by other node, 255 for network timeout
def ha_acquire_stretchcluster_lock(tiebreakerip, resuuid, nodename, timeout):
    debug('Enter ha_acquire_stretchcluster_lock')
    ret = []

    for ip in tiebreakerip:
        rc = ha_acquire_stretchcluster_lock_one(ip, resuuid, nodename, timeout)
        if rc == 255:
            # Need double check network error
            time.sleep(3)
            rc = ha_acquire_stretchcluster_lock_one(ip, resuuid, nodename, timeout)
        ret.append(rc)

    return ret


def ha_acquire_stretchcluster_lock_one(ip, resuuid, nodename, timeout):
    debug('Enter ha_acquire_stretchcluster_lock_one')
    rc = 255

    cmd_1 = 'ibdmanager --role server --module doa -r -a ' + ip + ' -n ' + nodename + ' -l ' + resuuid + ' -t ' + str(timeout)
    cmd_2 = 'ibdmanager -r s -m doa -a ' + ip + ' -n ' + nodename + ' -l ' + resuuid + ' -t ' + str(timeout) + ' lck request'
    for cmd in [cmd_2, cmd_1]:
        (rc, msg) = ha_retry_cmd(cmd, 2, 5)
        if len(msg) > 0:
            if 'invalid option' in msg[0] or 'usage:' in msg[0]:
                continue
            if 'grep lock failed' in msg[0]:
                rc = 1
            elif 'network error' in msg[0]:
                rc = 255
            elif 'grep lock successfully' in msg[0]:
                rc = 0
        status = ('failed', 'successful')[rc == 0]
        debug('doa lock request %s for %s' % (status,ip))
        break

    return rc


# Return a list since now tiebreakip is a list
# 0 for lock is owned by this node, 1 for lock is already owned by other node, 255 for network timeout
def ha_check_stretchcluster_lock(tiebreakerip, resuuid, nodename):
    debug('Enter ha_check_stretchcluster_lock')
    rv = []

    for ip in tiebreakerip:
        cmd_1 = 'ibdmanager --role server --module doa -c -a ' + ip + ' -l ' + resuuid
        cmd_2 = 'ibdmanager -r s -m doa -a ' + ip + ' -n ' + nodename + ' -l ' + resuuid + ' lck check'
        for cmd in [cmd_2, cmd_1]:
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
            rc = 255
            if len(msg) > 0:
                if 'invalid option' in msg[0] or 'usage:' in msg[0]:
                    continue
                for the_line in msg:
                    # owner of the lock: {char* nid}
                    if the_line.find('owner of the lock:') >= 0:
                        tmp = the_line.split()
                        if len(tmp) >= 5:
                            the_node = '{' + nodename + '}'
                            if tmp[4] == the_node:
                                rc = 0
                            else:
                                rc = 1
            rv.append(rc)
            break

    return rv


# Check the connection between node and user-set tiebreaker
# Return 0 if the tiebreaker is inaccessible
def ha_check_tiebreaker():
    debug('Enter ha_check_tiebreaker')
    result = 0

    # Get tiebreakerip from USX Manager
    tiebreakerip = ha_get_tiebreakerip_from_usxm()

    node_name = ha_get_local_node_name()
    ret_list = ha_acquire_stretchcluster_lock(tiebreakerip, node_name, node_name, 5)

    if len(ret_list) == 1:
        if ret_list[0] != 0:
            debug('ERROR: Tiebreaker is inaccessible from this node')
        else:
            debug('INFO: Tiebreaker is accessible from this node')
            result = 1
    else:
        debug('ERROR: Failed to check the connection with tiebreaker')

    return result


def ha_release_stretchcluster_lock(tiebreakerip, resuuid, nodename):
    debug('Enter ha_release_stretchcluster_lock')
    rv = []

    for ip in tiebreakerip:
        cmd_1 = 'ibdmanager --role server --module doa -d -a ' + ip + ' -l ' + resuuid + ' -n ' + nodename
        cmd_2 = 'ibdmanager -r s -m doa -a ' + ip + ' -l ' + resuuid + ' -n ' + nodename + ' lck release'
        for cmd in [cmd_2, cmd_1]:
            (rc, msg) = ha_retry_cmd(cmd, 2, 5)
            rc = 255
            if len(msg) > 0:
                if 'invalid option' in msg[0] or 'usage:' in msg[0]:
                    continue
                if 'release lock failed' in msg[0]:
                    rc = 1
                elif 'network error' in msg[0]:
                    rc = 255
                elif 'release lock successfully' in msg[0]:
                    rc = 0
            rv.append(rc)
            status = ('failed', 'successful')[rc == 0]
            debug('doa lock request %s for %s' % (status,ip))
            break

    return rv


def ha_stretchcluster_config():
    debug('Enter ha_stretchcluster_config')
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    stretchcluster_flag = False
    availability_flag = False
    tiebreakerip = []
    try:
        node_dict = json.loads(s)
        ha_dict = node_dict.get('haconfig')
        if ha_dict is None:
            return (stretchcluster_flag, availability_flag, tiebreakerip)

        flag_robo = False
        flag_strechcluster = False
        if ha_dict.has_key('stretchcluster'):
            flag_stretchcluster = ha_dict.get('stretchcluster')
        if ha_dict.has_key('raid1enabled'):
            flag_robo = ha_dict.get('raid1enabled')
        if (flag_robo is True or flag_stretchcluster is True):
            stretchcluster_flag = True
        else:
            stretchcluster_flag = False

        flag = None
        if ha_dict.has_key('attributes'):
            flag = ha_dict['attributes'].get('preferavailability')
        if flag is None or flag.lower() == 'false':
            availability_flag = False
        else:
            availability_flag = True

        if ha_dict.has_key('attributes'):
            tiebreakerip = [ha_dict['attributes'].get('tiebreakerip')]

        # Always return availability_flag=False for now. Need to change it back
        # when we have time to handle situation availability_flag=True
        # return (stretchcluster_flag, availability_flag, tiebreakerip)
        return (stretchcluster_flag, False, tiebreakerip)
    except:
        debug('ERROR: Exception on ha_stretchcluster_config')
        return (stretchcluster_flag, False, tiebreakerip)


def ha_adjust_quorum_policy(storage_network_down_flag):
	total_num_nodes = 0
	num_maintenance_nodes = 0
	num_active_nodes = 0
	num_online_nodes = 0

	if is_stretchcluster_or_robo():
		# stretchcluster and robo have special handling of no-quorum-policy at
		# usx-daemon.
		return 0

	node_name = ha_get_local_node_name()
	cmd = 'crm node status '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	for the_line in msg:
		if 'Signon to CIB failed' in the_line:
			return 1
		if the_line.find('</node>') >= 0:
			total_num_nodes += 1
		elif the_line.find('name="maintenance" value="on"') >= 0:
			num_maintenance_nodes += 1

	online_node_list = ha_get_online_node_list()
	num_online_nodes = len(online_node_list)

	num_active_nodes = total_num_nodes - num_maintenance_nodes
	debug('total_num_nodes = %d, num_maintenance_nodes = %d, num_active_nodes = %d, num_online_nodes = %d '
		%(total_num_nodes, num_maintenance_nodes, num_active_nodes, num_online_nodes))

	quorum_policy = 'ignore'
	if storage_network_down_flag == True:
		# if storage network is down, set no-quorum-policy=freeze
		debug('WARN: storage network is down, set no-quorum-policy=freeze')
		quorum_policy = 'freeze'
	else:
		if num_active_nodes <= 2:
			quorum_policy = 'ignore'
			if check_usxmanager_alive_flag():
				ha_get_other_node_ip()
		else:
			quorum_policy = 'freeze'
	#else:
	#	if num_active_nodes < 0:
	#		debug('ERROR: num_active_nodes = %d ' % num_active_nodes)
	#		return 1
	#	elif num_active_nodes <= 2:
	#		quorum_policy = 'ignore'
	#	elif num_online_nodes > num_active_nodes / 2:
	#		quorum_policy = 'freeze'
	#	else:
	#		quorum_policy = 'ignore'

	needset = False
	cmd = 'crm_attribute --type crm_config --name no-quorum-policy --query'
	(ret, msg) = runcmd(cmd, print_ret=True)
	if ret == 6:
		needset = True
	else:
		policy_str = 'name=no-quorum-policy value=' + quorum_policy
		msgindex = msg.find(policy_str)
		if msgindex >= 0:
			# the policy has been set correctly
			debug('node %s configure quorum policy %s had been done before' %(node_name, quorum_policy))
			return 0
		else:
			# the policy is different
			needset = True

	if needset:
		cmd = 'crm_attribute --type crm_config --name no-quorum-policy --update ' + quorum_policy
		(ret, msg) = ha_retry_cmd2(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
		if ret != 0:
			if msg.find('Update was older than existing configuration') >= 0:
				return 0
			debug('ERROR : fail to run %s, err=%s, msg=%s' % (cmd, str(ret),
				msg))
			return 1
	return 0


def ha_retry_cmd(cmd, retry_num, timeout):
    debug('Enter ha_retry_cmd: %s %d %d' % (cmd, retry_num, timeout))
    cnt = retry_num
    ret = 0
    msg = ""
    while cnt > 0:
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        if ret == 0:
            break
        cnt -= 1
        if cnt:
            time.sleep(timeout)
    return (ret, msg)


def ha_retry_cmd2(cmd, retry_num, timeout):
	debug('Enter ha_retry_cmd2: %s %d %d' % (cmd, retry_num, timeout))
	cnt = retry_num
	ret = 0
	msg = ""
	while cnt > 0:
		(ret, msg) = runcmd(cmd, print_ret=True)
		if ret == 0:
			break
		watchdog_cmd = 'crm_attribute --type crm_config --name have-watchdog --delete'
		runcmd(watchdog_cmd, print_ret=True, lines=True)
		time.sleep(timeout)
		cnt -= 1
	return (ret, msg)


def ha_unmange_one_resouce(resuuid):
	debug('Enter ha_unmange_one_resouce: %s' % resuuid)
	res_group = resuuid + '_group'
	cmd = 'crm resource unmanage ' + res_group
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
	return 0


def ha_manage_one_resouce(resuuid):
	debug('Enter ha_manage_one_resouce: %s' % resuuid)
	res_group = resuuid + '_group'
	cmd = 'crm resource manage ' + res_group
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
	return 0


def ha_check_resouce_unmanaged(resuuid):
	debug('Enter ha_check_resouce_unmanaged: %s' % resuuid)

	resuuid_ds = resuuid + '_ds'
	resuuid_atl_dedup = resuuid + '_atl_dedup'
	resuuid_ip = resuuid + '_ip'
	unmanaged_flag = False

	cmd = 'crm_mon -r1 | grep heartbeat | grep ' + resuuid + ' | grep -v grep '
	(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
	if ret == 0:
		for line in msg:
			#res_ds        (ocf::heartbeat:ADS):   Started TIS4AUTO-VOLUME-134Testbed20-004 (unmanaged)
			#res_atl_dedup (ocf::heartbeat:dedup-filesystem):      Stopped (unmanaged)
			tmp = line.split()
			res = tmp[0]
			if res in [resuuid_ds, resuuid_atl_dedup, resuuid_ip] and tmp[-1] == '(unmanaged)':
				unmanaged_flag = True
				break

	if ret == 0 and unmanaged_flag == True:
		return (0, True)
	elif ret == 0 and unmanaged_flag == False:
		return (0, False)
	else:
		return (ret, False)


# Check if whole resource group are unmanaged
def ha_check_resouce_group_unmanaged(resuuid):
    debug('Enter ha_check_resouce_group_unmanaged: %s' % resuuid)
    unmanaged_flag = False
    unmanaged_num = 0

    cmd = 'crm_mon -r1 | grep heartbeat | grep ' + resuuid + ' | grep -v grep '
    (ret, msg) = ha_retry_cmd(cmd, 2, 3)
    if ret == 0:
        for line in msg:
            #res_ds        (ocf::heartbeat:ADS):   Started TIS4AUTO-VOLUME-134Testbed20-004 (unmanaged)
            #res_atl_dedup (ocf::heartbeat:dedup-filesystem):      Stopped (unmanaged)
            if '(unmanaged)' in line:
                unmanaged_num += 1

    if len(msg) == 0:
        ret = 1
    else:
        if unmanaged_num == len(msg):
            unmanaged_flag = True

    return (ret, unmanaged_flag)


# If resourceuuid is not None, return the group of the resource, otherwise return all the group
def ha_get_res_group_list(resourceuuid = None):
    out=['']
    grps = []
    cmd = "/usr/sbin/crm configure show|grep '^group'"
    (rc, msg) = ha_retry_cmd(cmd, 5, 3)
    if rc != 0 or (len(msg) > 0 and "Signon to CIB failed" in msg[0]):
        return grps

    #Input: group USX_cd1035aa_group USX_cd1035aa_ds USX_cd1035aa_atl_dedup USX_cd1035aa_atl_nfs USX_cd1035aa_ip \
    for line in msg:
        if resourceuuid is not None:
            if resourceuuid not in line:
                continue
        grp = line.strip('\\ ').split(' ')
        grp.remove('group')
        grp_uuid = re.sub('_group$', '', grp[0])
        grp.insert(0, grp_uuid)
        grps.append(grp)

    #Output: grps[0]: group_uuid, group_name, group_res_list
    #['USX_cd1035aa', 'USX_cd1035aa_group', 'USX_cd1035aa_ds', 'USX_cd1035aa_atl_dedup', 'USX_cd1035aa_atl_nfs', 'USX_cd1035aa_ip']
    return grps

def ha_force_start_res_group(group):
    # The beginning two items are grp_uuid and grp_name, skip them.
    for res in group[2:]:
        (rc, msg) = ha_retry_cmd("crm_resource --force-start -r %s" % res, 1, 5)
        if rc == 0 and len(msg) > 0 and 'returned 0' in msg[0]:
            debug('Successfully force start %s' % res)
            # Let the pacemaker know the resource
            ha_retry_cmd("crm_resource --reprobe -r %s" % res, 1, 5)
        else:
            return 1
    return 0


# This method will only be called in usx_daemon for a empty HA VM when lost quorum
def ha_stretchcluster_start_res(tiebreakerip, nodename, res_timeout, resourceuuid = None):
    debug('Enter ha_stretchcluster_start_res')
    # Set forcestarting flag to avoid be called in usx_deamon again
    ha_set_forcestarting_flag()
    grps = ha_get_res_group_list(resourceuuid)
    scl_timeout = ha_get_scl_timeout()
    for grp in grps:
        volume_uuid = grp[0]
        # Skip unmanaged resource group
        (ret, result) = ha_check_resouce_group_unmanaged(volume_uuid)
        if result:
            continue
        # Acquire lock for both tiebreaker, the result is a list for local and remote Service VM
        # 0: successfully acquired lock
        # 1: other node already owned the lock
        # 255: timeout
        result = ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, res_timeout)
        # If we are using 2 SVMs as tiebreaker, need special logic
        if len(tiebreakerip) == 2:
            # Start resource when both lock is acquired
            if result[0] + result[1] == 0:
                debug('Force start ' + volume_uuid)
                # Force start resource
                ha_force_start_res_group(grp)
                # Reacquire the lock with a much smaller reasonable timeout.
                ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)
                ha_remove_forcestarting_flag()
                return volume_uuid

            if result[0] == 1 or result[1] == 1:
                # Release our lock on local Service VM
                ha_release_stretchcluster_lock([tiebreakerip[0]], volume_uuid, nodename)
                continue

            # Acquire 1 lock(0) and the other is timeout(255)
            if result[0] + result[1] == 255:
                if is_vmmanager_reachable():
                    # Get power status of both Service VMs, 0 for power off
                    (local_svm_power_status, remote_svm_power_status) = ha_get_local_remote_Service_VM_power_status()
                    debug("Service VMs power status: %s,%s" % (local_svm_power_status, remote_svm_power_status))
                    if local_svm_power_status == 0 or remote_svm_power_status == 0:
                        debug('Force start ' + volume_uuid)
                        # Force start resource
                        ha_force_start_res_group(grp)
                        # Reacquire the lock with a much smaller reasonable timeout.
                        ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)
                        ha_remove_forcestarting_flag()
                        return volume_uuid
        else:
            # Sucessfully acquired lock on tiebreaker
            if result[0] == 0:
                debug('Force start ' + volume_uuid)
                # Force start resource
                ha_force_start_res_group(grp)
                # Reacquire the lock with a much smaller reasonable timeout.
                rc = ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)
                ha_remove_forcestarting_flag()
                return volume_uuid
            # Failed to acquired lock, do nothing
            else:
                continue

    ha_remove_forcestarting_flag()
    return None

def ha_unmanage_resources():
	res_group = None
	res_list = []

	local_node = None
	cmd = 'crm_node -n'
	(ret, msg) = runcmd(cmd, print_ret=False, lines=True)
	for the_line in msg:
		tmp = the_line.split()
		if len(tmp) > 0:
			local_node = tmp[0]
			break

	# save PACEMAKER_RSC_LIST for enabling HA
	if local_node != None:
		# search resources on local node
		cmd = 'crm status'
		(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
		if ret != 0:
			return 1
		res_group = None
		res_list = []
		res_group_found = False
		for line in msg:
			if line.find("Resource Group") >= 0 and not res_group_found:
				res_group = line.split()[2]
			elif line.find("Started") >= 0:
				tmp = line.split()
				res = tmp[0]
				started_by = tmp[3]
				if started_by == local_node and res != "iliomon":
					res_list.append(res)
					res_group_found = True

	with open(PACEMAKER_RSC_LIST, "w") as fd:
		if len(res_list) > 0:
			fd.write("%s\n" % res_group)
			for res in reversed(res_list):
				fd.write("%s\n" % res)

	# enter the maintenance mode
	cmd = 'crm node maintenance '
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

	(ret, msg) = runcmd('touch '+ HA_DISABLE_FILE, print_ret=True)
	if ret != 0:
		(ret, msg) = runcmd('touch '+ HA_DISABLE_FILE, print_ret=True)

	for res in res_list:
		cmd = 'crm resource unmanage ' + res
		(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

	# remove HA_DISABLE_FILE
	try:
		os.remove(HA_DISABLE_FILE)
	except:
		pass

	return 0


def ha_manage_resources():
	res_group = None
	res_list = []
	if os.path.isfile(PACEMAKER_RSC_LIST):
		with open(PACEMAKER_RSC_LIST, "r") as fd:
			content = fd.readlines()
			for line in content:
				if line.find('group') >= 0:
					res_group = line
				else:
					res_list.append(line)
	else:
		local_node = None
		cmd = 'crm_node -n'
		(ret, msg) = runcmd(cmd, print_ret=False, lines=True)
		for the_line in msg:
			tmp = the_line.split()
			if len(tmp) > 0:
				local_node = tmp[0]
				break

		if local_node != None:
			# search resources on local node
			cmd = 'crm status'
			(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
			if ret != 0:
				return 1
			res_group = None
			res_list = []
			res_group_found = False
			for line in msg:
				if line.find("Resource Group") >= 0 and not res_group_found:
					res_group = line.split()[2]
				elif line.find("Started") >= 0:
					tmp = line.split()
					res = tmp[0]
					started_by = tmp[3]
					if started_by == local_node and res != "iliomon":
						res_list.append(res)
						res_group_found = True

	if not os.path.exists('/var/run/resource-agents'):
		cmd = 'mkdir -p /var/run/resource-agents'
		(ret, msg) = runcmd(cmd, print_ret=True)

	for res in res_list:
		cmd = 'touch /var/run/resource-agents/' + res
		(ret, msg) = runcmd(cmd, print_ret=True)

		cmd = 'crm resource manage ' + res
		(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

	ret = ha_set_ready_mode()

	return 0


def ha_upgrade_begin():
	debug('Enter ha_upgrade_begin')
	ret = 0
	if ha_check_enabled():
		ret = ha_unmanage_resources()
		time.sleep(2)
		# Make sure a cron job cannot restart corosync & pacemaker
		runcmd('rm -f /run/pacemaker_started', print_ret=True)
		runcmd('killall -9 corosync', print_ret=True)
		runcmd('killall -9 pacemakerd', print_ret=True)
		runcmd('service pacemaker stop', print_ret=True)
		runcmd('service corosync stop', print_ret=True)
		debug('put HA into maintenance mode before upgrade')

	return ret


def ha_upgrade_end():
	debug('Enter ha_upgrade_end')
	ret = 0
	if ha_check_enabled():
		(ret, msg) = ha_retry_cmd('service corosync restart', 2, 2)
    	#if ret != 0:
        #   	return ret
		(ret, msg) = ha_retry_cmd('service pacemaker restart', 2, 2)
		#if ret != 0:
        #   	return ret
                runcmd('touch /run/pacemaker_started', print_ret=True)
		debug('put HA back into normal mode after upgrade')
		ret = ha_manage_resources()
		if ret != 0:
			return ret
	return ret


def ha_logrotate_conf():
	#/var/log/corosync.log {
	#        daily
	#        missingok
	#        rotate 14
	#        size 50M
	#        compress
	#        delaycompress
	#        notifempty
	#}
	tmp_fname = "/tmp/corosync"
	cfile = open(tmp_fname, "w")

	title = "/var/log/corosync.log"
	cfile.write(title + " {\n")
	cfile.write("		daily\n")
	cfile.write("		missingok\n")
	cfile.write("		rotate 14\n")
	cfile.write("		size 50M\n")
	cfile.write("		compress\n")
	cfile.write("		delaycompress\n")
	cfile.write("		notifempty\n")
	cfile.write("		copytruncate\n")
	cfile.write("}\n\n")

	title = "/var/log/atlas-health-check.log"
	cfile.write(title + " {\n")
	cfile.write("		daily\n")
	cfile.write("		missingok\n")
	cfile.write("		rotate 14\n")
	cfile.write("		size 50M\n")
	cfile.write("		compress\n")
	cfile.write("		delaycompress\n")
	cfile.write("		notifempty\n")
	cfile.write("}\n\n")

	title = "/var/log/corosync/*.log"
	cfile.write(title + " {\n")
	cfile.write("		daily\n")
	cfile.write("		missingok\n")
	cfile.write("		rotate 14\n")
	cfile.write("		size 50M\n")
	cfile.write("		compress\n")
	cfile.write("		delaycompress\n")
	cfile.write("		notifempty\n")
	cfile.write("}\n\n")

	cfile.close()
	os.rename(tmp_fname, COROSYNC_ROTATE_CONF)

	return 0


def ha_get_availabe_nodes():
	debug('Enter ha_get_availabe_node')

	num_available_nodes = 0
	total_nodes = 0
	online_nodes = 0
	num_vols = 0

	cmd = 'crm_node -l '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	total_nodes = len(msg)

	cmd = 'crm status'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for line in msg:
			if line.find("Online:") >= 0:
				tmp = line.replace('[', ' ').replace(']', ' ').split()
				online_nodes = len(tmp) - 1
				break

	cmd = 'crm resource status '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	for the_line in msg:
		if the_line.find('Resource Group:') >= 0:
			num_vols += 1

	if online_nodes >= num_vols:
		num_available_nodes = online_nodes - num_vols

	debug('online_nodes = %d, num_vols = %d, num_available_nodes = %d' % (online_nodes, num_vols, num_available_nodes))
	return num_available_nodes


def ha_get_volume_list():
	debug('Enter ha_get_volume_list')

	volume_list = []
	cmd = 'crm resource status'
	(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
	if ret == 0:
		for the_line in msg:
			# Resource Group: vc1202_000tis24-21013-hyb-001-1414050624639_group
			if the_line.find("Resource Group:") >= 0:
				tmp = the_line.split()
				if len(tmp) >= 2:
					res_group = tmp[2]
					vol_uuid = res_group[:-6]
					volume_list.append(vol_uuid)

	return volume_list


def ha_get_stopped_volume_list():
	debug('Enter ha_get_stopped_volume_list')

	volume_list = ha_get_volume_list()
	stopped_volume_list = []
	for the_volume in volume_list:
		cmd = 'crm resource status ' + the_volume + '_group'
		(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
		if ret == 0 and len(msg) == 1:
			if msg[0].find("is NOT running") >= 0:
				stopped_volume_list.append(the_volume)

	return stopped_volume_list


def ha_get_failed_volume_list():
    debug('Enter ha_get_failed_volume_list')

    failed_volume_list = []
    resource_group = ha_get_resource_detail_status()
    for res,services in resource_group.iteritems():
        for service in services:
            if service == 'ADS' and len(services[service]['owner']) == 0:
                failed_volume_list.append(res)

    return failed_volume_list


def ha_get_online_node_list():
	debug('Enter ha_get_online_node_list')

	online_node_list = []
	cmd = 'crm status'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for line in msg:
			if line.find("Online:") >= 0:
				line = line.replace('Online:', '')
				line = line.replace('[', ' ')
				line = line.replace(']', ' ')
				online_node_list = line.split()
				break

	return online_node_list


def ha_get_offline_node_list():
	debug('Enter ha_get_offline_node_list')

	offline_node_list = []
	cmd = 'crm status -r'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for line in msg:
			if line.find("OFFLINE:") >= 0:
				line = line.replace('OFFLINE:', '')
				line = line.replace('[', ' ')
				line = line.replace(']', ' ')
				offline_node_list = line.split()
				break

	return offline_node_list


def ha_get_maintenance_node_list():
	debug('Enter ha_get_maintenance_node_list')

	maintenance_node_list = []
	cmd = 'crm status'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for the_line in msg:
			the_list = the_line.split()
			# Node GUI-LDAP-300338-001-VM: maintenance
			if len(the_list) == 3:
				if the_list[0] == "Node" and the_list[2] == "maintenance":
					the_node = the_list[1][:-1]
					maintenance_node_list.append(the_node)

	return maintenance_node_list


def ha_has_standby():
	debug('Enter ha_has_standby')

	total_nodes = 0
	num_vols = 0
	volume_list = []

	cmd = 'crm node status |grep "node id="'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		total_nodes = len(msg)
		volume_list = ha_get_volume_list()
		num_vols = len(volume_list)
		debug('total_nodes = %d, num_vols = %d' % (total_nodes, num_vols))
		if total_nodes >= num_vols + 1:
			return (True, total_nodes, num_vols)

	return (False, total_nodes, num_vols)


def ha_has_standby_from_usxm():
    debug('Enter ha_has_standby_from_usxm')

    total_nodes = 0
    total_vols  = 0

    # Get hauuid
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    hauuid = None
    try:
        node_dict = json.loads(s)
        ha_config = node_dict.get('haconfig')
        if ha_config is None:
            debug('ERROR: wrong json file %s' %str(node_dict))
            return False

        hauuid = node_dict['haconfig'].get('uuid')
        if hauuid is None:
            debug('ERROR: cannot find hauuid')
            return False

    except ValueError as err:
        debug('ERROR: wrong atlas.json')
        return False

    # Get nodes list
    cmd = "/usr/sbin/crm node status |grep 'node id=' | sed -r 's/.*uname=\"(\S+)\".*/\\1/'"
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) > 0:
        if msg[0] == 'Signon to CIB failed: Transport endpoint is not connected':
            return False
        node_list = msg
        total_nodes = len(node_list)
    else:
        debug('ERROR: failed to get total nodes')
        return False

    # Check how many vols in HA Group on USX Manager
    # curl -X GET http://127.0.0.1:8080/usxmanager/usx/inventory/volume/containers?query=.%5Bhauuid='USX_3d903e69-a8de-38ab-8233-6cf2a234fe82'%5D
    upgrade_flag = 0
    if ' ' not in hauuid:
        amc_query = "/usx/inventory/volume/containers?query=.%5Bhauuid=\'" + hauuid + "\'%5D"
    else:
        # For USX Manager upgraded from old version in which space is allowed in hauuid
        amc_query = "/usx/inventory/volume/containers"
        upgrade_flag = 1
    (rc, res_data) = ha_query_amc2(LOCAL_AGENT, amc_query, 5)
    try:
        if rc == 0:
            # To handle Enable HA for multiple Volumes at the same time
            if upgrade_flag == 0:
                if res_data['count'] < total_nodes:
                    total_nodes = res_data['count']
            for container in res_data['items']:
                if container['usxvm']['vmname'] in node_list:
                    if container.has_key('volumeresourceuuids'):
                        total_vols += 1
        else:
            # Get container info failed on USX Manager
            debug('ERROR: failed to get container info on USX Manager')
            return False
    except:
        # Get container info failed on USX Manager
        debug('ERROR: Exception when try to get container info on USX Manager')
        return False

    # Compare nodes number and volumes number
    if total_nodes - total_vols >= 1:
        debug('total nodes: %d, total vols: %d' %(total_nodes, total_vols))
        return True
    else:
        debug('total nodes: %d, total vols: %d' %(total_nodes, total_vols))
        return False


def ha_query_amc(amcurl, amcquery, retry_num):
	debug('Enter ha_query_amc')

	conn = urllib2.Request(amcurl + amcquery)
	debug("query amc: " + amcurl + amcquery)
	conn.add_header('Content-type','application/json')

	cnt = 0
	rc = 0
	query_count = 1
	res_data = {}
	while cnt < retry_num:
		try:
			res = urllib2.urlopen(conn)
		except:
			debug(traceback.format_exc())
			cnt += 1
			debug('Exception caught on query amc, retry: %d' % cnt)
			time.sleep(5)
			rc = 1
			continue
		debug('POST returned response code: ' + str(res.code))

		rc = 0
		if str(res.code) == "200":
			read_data = res.read()
			res_data = json.loads(read_data)
			debug('query amc response: ' + json.dumps(res_data, sort_keys=True, indent=4, separators=(',', ': ')))
			if res_data.has_key('count'):
				query_count = res_data['count']
				debug('query_count = %d ' % query_count)
				rc = 0
				break

		rc = 1
		res.close()
		time.sleep(5)
		cnt += 1
		debug('retry query amc: %d' % cnt)

	return (rc, query_count, res_data)



def ha_query_amc2(amcurl, amcquery, retry_num):
    debug('Enter ha_query_amc2')

    conn = urllib2.Request(amcurl + amcquery)
    debug("query amc: " + amcurl + amcquery)
    conn.add_header('Content-type','application/json')

    cnt = 0
    rc = 0
    res_data = {}
    while cnt < retry_num:
        try:
            res = urllib2.urlopen(conn)
        except:
            debug(traceback.format_exc())
            cnt += 1
            debug('Exception caught on query amc, retry: %d' % cnt)
            time.sleep(5)
            rc = 1
            continue
        debug('POST returned response code: ' + str(res.code))

        rc = 0
        if str(res.code) == "200":
            read_data = res.read()
            # Avoid exception if data could not be decoded as json
            try:
                res_data = json.loads(read_data)
#                debug('query amc response: ' + json.dumps(res_data, sort_keys=True, indent=4, separators=(',', ': ')))
                break
            except:
                debug("query amc response:")
                debug(read_data)

        rc = 1
        res.close()
        time.sleep(5)
        cnt += 1
        debug('retry query amc: %d' % cnt)

    return (rc, res_data)


def ha_post_amc(amcurl, amcquery, data, retry_num):
    debug('Enter ha_post_amc')

    conn = urllib2.Request(amcurl + amcquery, json.dumps(data))
    debug("query amc: " + amcurl + amcquery)
    conn.add_header('Content-type','application/json')

    cnt = 0
    rc = 0
    res_data = {}
    while cnt < retry_num:
        try:
            res = urllib2.urlopen(conn)
        except:
            debug(traceback.format_exc())
            cnt += 1
            debug('Exception caught on query amc, retry: %d' % cnt)
            time.sleep(5)
            rc = 1
            continue
        debug('POST returned response code: ' + str(res.code))

        rc = 0
        if str(res.code) == "200":
            break

        rc = 1
        res.close()
        time.sleep(5)
        cnt += 1
        debug('retry query amc: %d' % cnt)

    return rc


def ha_remove_orphan(remove_flag):
    debug('Enter ha_remove_orphan')

    if not check_usxmanager_alive_flag() or not is_usxmanager_alive():
        debug('ERROR: could not remove orphan when USX Manager is disconnected')
        return 0

    for i in range(5):
        if ha_get_local_node_name() in ha_get_offline_node_list():
            debug('Local node is still offline, need wait')
            ha_retry_cmd('crm_mon -r1', 1, 1)
            time.sleep(5)
        else:
            break

    offline_node_list = ha_get_offline_node_list()
    debug('offline_node_list: ' + str(offline_node_list))
    maintenance_node_list = ha_get_maintenance_node_list()
    debug('maintenance_node_list: ' + str(maintenance_node_list))
    merged_node_list = list(set(offline_node_list + maintenance_node_list))
    debug('merged_node_list: ' + str(merged_node_list))
    stopped_volume_list = ha_get_stopped_volume_list()
    debug('stopped_volume_list: ' + str(stopped_volume_list))
    failed_volume_list = ha_get_failed_volume_list()
    debug('failed_volume_list: ' + str(failed_volume_list))

    if len(merged_node_list) > 0:
        for the_node in merged_node_list:
            #Query  = .[usxvm[vmname='yj-001-hybrid']]
            #https://10.21.115.105:8443/usxmanager/usx/inventory/volume/containers?query=.%5Busxvm%5Bvmname%3D'yj-001-hybrid'%5D%5D&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false
            amc_node_query = '/usx/inventory/volume/containers?query=.[usxvm[hostname=\'' + the_node + '\']]&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false'
            (rc, offline_node_count, res_data) = ha_query_amc(LOCAL_AGENT, amc_node_query, 5)

            if rc == 0 and offline_node_count == 0 and remove_flag == True:
                cmd = 'crm node delete ' + the_node
                (ret, msg) = runcmd(cmd, print_ret=True)
                cmd = 'crm_node -f -R  ' + the_node
                (ret, msg) = runcmd(cmd, print_ret=True)
                if ret == 0:
                    debug('WARN: deleted HA orphan node %s' % the_node)
                else:
                    debug('WARN: failed to delete HA orphan node %s' % the_node)

    if len(stopped_volume_list + failed_volume_list):
        for the_vol in stopped_volume_list + failed_volume_list:
            (rc, unmanaged_flag) = ha_check_resouce_group_unmanaged(the_vol)
            ha_enabled_flag = False
            #Query = .[uuid='yj-vmm_yj-001-hybrid-1413914728850']
            #https://10.21.115.105:8443/usxmanager/usx/inventory/volume/resources?query=.%5Buuid%3D'yj-vmm_yj-001-hybrid-1413914728850'%5D&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false
            amc_vol_query = '/usx/inventory/volume/resources?query=.[uuid=\'' + the_vol + '\']&&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false'
            (rc, offline_vol_count, res_data) = ha_query_amc(LOCAL_AGENT, amc_vol_query, 5)

            if rc == 0 and offline_vol_count == 0 and remove_flag == True:
                res_tails = ['_ds','_ip','_atl_vscaler','_atl_nfs','_atl_dedup','_atl_iscsi_target','_atl_iscsi_lun']
                for the_tail in res_tails:
                    the_res = the_vol + the_tail
                    cmd = 'crm configure show ' + the_res
                    (ret, msg) = runcmd(cmd, print_ret=True)
                    if ret == 0:
                        sub_cmd = 'crm resource maintenance ' + the_res
                        (sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True)
                        sub_cmd = 'crm resource unmanage ' + the_res
                        (sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True)
                        sub_cmd = 'crm configure delete ' + the_res
                        (sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True)
                        sub_cmd = 'crm resource cleanup ' + the_res
                        (sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True)
                cmd = 'crm configure delete ' + the_vol + '_group'
                (ret, msg) = runcmd(cmd, print_ret=True)
                debug('WARN: deleted HA orphan volume %s' % the_vol)

            if rc == 0:
                if len(res_data.get('items', [])) > 0:
                    ha_enabled_flag = res_data['items'][0].get("ha", False)

            # Sometimes the resource is unmanaged unexpected, we need manage it back
            if rc == 0 and offline_vol_count == 1 and unmanaged_flag == True and ha_enabled_flag == True:
                ha_manage_one_resouce(the_vol)
    return 0


def check_volume_ha_status_from_usxm(the_vol):
    amc_vol_query = '/usx/inventory/volume/resources?query=.[uuid=\'' + the_vol + '\']'
    (rc, offline_vol_count, res_data) = ha_query_amc(LOCAL_AGENT, amc_vol_query, 5)
    if rc == 0:
        if len(res_data.get('items', [])) > 0:
            ha_enabled_flag = res_data['items'][0].get("ha", False)
            return (0, ha_enabled_flag)
    else:
        return (1, False)


def check_vol_ha_flag(volresuuid):
	debug('Enter check_vol_ha_flag')
	amc_vol_query = '/usx/inventory/volume/resources?query=.[uuid=\'' + volresuuid + '\']&&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false'
	(rc, vol_count, res_data) = ha_query_amc(LOCAL_AGENT, amc_vol_query, 5)
	ha_flag = False
	if vol_count == 1:
		ha_flag = res_data['items'][0].get('ha')
		if ha_flag == True:
			debug('volume %s is ha enabled' %volresuuid)
			return 0
	debug('volume %s is NOT ha enabled' %volresuuid)
	return 1


def ha_cleanup_unmanaged_resource():
    debug('Enter ha_cleanup_unmanaged_resource')

    volume_list = ha_get_volume_list()
    cmd = 'crm resource status |grep "FAILED (unmanaged)" | grep -v grep '
    # 'vc1202_tis23-v25-inmem_1428048976448_ds    (ocf::heartbeat:ADS):   FAILED (unmanaged)'
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) >= 1:
        for the_vol in volume_list:
            the_vol_group = the_vol + '_group'
            cmd = 'crm resource cleanup ' + the_vol_group
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return 0


def ha_cleanup_failed_ds_resource_after_preparation():
    debug('Enter ha_cleanup_failed_ds_resource_after_preparation')

    node_name = ha_get_local_node_name()
    cmd = 'crm_mon -r1 | grep ocf::heartbeat:ADS | grep FAILED | grep %s' % node_name
    # USX_14d12499-80b0-3999-8fa4-fbd7c0ac618c_ds   (ocf::heartbeat:ADS):   FAILED TIS45-TB1112-VVM-002 (unmanaged)
    (ret, msg) = ha_retry_cmd(cmd, 1, 5)
    if ret == 0 and len(msg) >= 1:
        p = re.match("(.*_ds)\s*.*",msg[0])
        if p != None:
            cmd = 'crm resource cleanup %s %s' % (p.group(1), node_name)
            (ret, msg) = ha_retry_cmd(cmd, 2, 5)

    return 0


def ha_add_servicename_crm():
	debug('Enter ha_add_servicename_crm')

	volume_list = ha_get_volume_list()
	debug('volume list: %s' % str(volume_list))
	for the_vol in volume_list:
		rc = 0
		query_count = 0
		amc_vol_query = '/usx/inventory/volume/resources?query=.[uuid=\'' + the_vol + '\']&&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false'
		conn = urllib2.Request(LOCAL_AGENT + amc_vol_query)
		conn.add_header('Content-type','application/json')

		res = urllib2.urlopen(conn)
		if str(res.code) == "200":
			read_data = res.read()
			res_data = json.loads(read_data)
			if res_data.has_key('count'):
				vol_count = res_data['count']

		if rc == 0 and vol_count >= 1:
			vol_displayname = res_data['items'][0]['displayname']
			debug('volume %s, displayname is %s ' % (the_vol, vol_displayname))
		res.close()

	return 0


def ha_get_node_list():
	debug('Enter ha_get_node_list')

	node_list = []
	cmd = 'crm_node -l '
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
	if ret == 0:
		# 175408846 TIS4AUTO-VOLUME-134Testbed20-002
		for the_msg in msg:
			tmp = the_msg.split()
			if len(tmp) == 2:
				node_list.append(tmp[1])

	return node_list


def ha_update_mount_status(res_uuid):
    debug("ha_update_mount_status")
    #http://127.0.0.1:8080/usxmanager/upgrades/upgrade/volume/USX_9cb4b7f9-de74-32ca-b32c-37e30f589ca3/mount/status
    cmd = 'curl -k -X POST %s/upgrades/upgrade/volume/%s/mount/status' % (LOCAL_AGENT, res_uuid)
    ha_retry_cmd(cmd, 1, 1)


def ha_enableha_postprocess():
    debug('Enter ha_enableha_postprocess')

    # This must be set first before we restart usx_daemon!
    ha_enable_ha_flag()

    local_node = None
    cmd = 'crm_node -n'
    (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
    if ret != 0:
        debug('ERROR: failed to check cluster nodes')
    for the_line in msg:
        tmp = the_line.split()
        if len(tmp) > 0:
            local_node = tmp[0]
            break

    # set local node cpu utilization
    if local_node != None:
        cmd = 'crm node utilization ' + local_node + ' set cpu 4'
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    # delete colocation rule
    cmd = 'crm configure delete single_dedup_one_node'
    (ret, msg) = ha_retry_cmd(cmd, 2, HA_SLEEP_TIME)

    # get node list to set cpu
    node_list = ha_get_node_list()
    for the_node in node_list:
        #crm node utilization HS-VOL-01 set cpu 4
        cmd = 'crm node utilization ' + the_node + ' set cpu 4'
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    # get vol list to set resource ads cpu
    volume_list = ha_get_volume_list()
    for the_vol in volume_list:
        #crm resource utilization vCenter_HS-DS-02_1440234481610_ds set cpu 4
        ads = the_vol +  '_ds'
        cmd = 'crm resource utilization ' + ads + ' set cpu 4'
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    # set placement-strategy
    cmd = 'crm_attribute --name placement-strategy --update utilization'
    (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
    cmd = 'crm configure property placement-strategy=utilization'
    (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    (volume_type, volume_uuid, ilio_uuid, display_name) = get_volume_info()
    if volume_uuid != None:
        ha_set_volume_running_status(volume_uuid)

    if is_stretchcluster_or_robo():
        tiebreakerip = ha_get_tiebreakerip()
        scl_timeout = ha_get_scl_timeout()
        nodename = ha_get_local_node_name()
        if volume_uuid != None:
            # acquire stretchcluster lock for volume
            ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)

        # restart daemon
        cmd = 'python /opt/milio/atlas/roles/ha/usx_daemon.pyc restart '
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)


def ha_get_resource_detail_status():
    resources = defaultdict(dict)
    cmd = 'crm resource list | grep _group'
    (rc, msg) = ha_retry_cmd(cmd, 1, 3)
    if len(msg) > 0:
        for res in msg:
            p = re.search("(\S+)_group", res)
            if p != None:
                resources[p.group(1)] = defaultdict(dict)
            else:
                return resources
    try:
        for res in resources:
            cmd = 'crm_mon -r1 | grep ocf::heartbeat: | grep ' + res
            (rc, msg) = ha_retry_cmd(cmd, 1, 3)
            if len(msg) >= 4:
                for line in msg:
                    p = re.search("\(ocf::heartbeat:(.+?)\)", line)
                    if p != None:
                        if "Started" in line:
                            resources[res][p.group(1)]["status"] = "Started"
                        elif "FAILED" in line:
                            resources[res][p.group(1)]["status"] = "FAILED"
                        else:
                            resources[res][p.group(1)]["status"] = 'Stopped'
                        if "(unmanaged)" in line:
                            resources[res][p.group(1)]["managed"] = False
                        else:
                            resources[res][p.group(1)]["managed"] = True
                        q = re.search("\[(.*?)\]", line)
                        r = re.search("Started (.*?)$", line)
                        if q != None:
                            resources[res][p.group(1)]["owner"] = q.group(1).split()
                        elif r != None:
                            r = re.search("Started (.*?)$", line)
                            if r != None:
                                resources[res][p.group(1)]["owner"] = [r.group(1)]
                        else:
                            resources[res][p.group(1)]["owner"] = []
    except:
        debug("Exception in get resource detail status")
    return resources


def ha_cleanup_failed_resource():
    debug('Enter ha_cleanup_failed_resource')

    local_node = None
    cmd = 'crm_node -n'
    (ret, msg) = runcmd(cmd, print_ret=False, lines=True)
    if ret != 0:
        debug('ERROR: failed to check cluster nodes')
        return 2
    for the_line in msg:
        tmp = the_line.split()
        if len(tmp) > 0:
            local_node = tmp[0]
            break

    node_used_flag = ha_check_node_used()
    cluster_stop_flag = False

    # If we do have any remote ibd connection, after storage network recover, ibd_upgrade_reset will be triggered
    # For pure sharedstorage(both primary and cache) Volume, check and reset it here
    # To minimize the reset, only for below crm status then check if sharedstorage is attached on the node or not
    # Resource Group: USX_7bf20255-3c9c-37e9-90d8-d966edca0ac8_group
    #   USX_7bf20255-3c9c-37e9-90d8-d966edca0ac8_ds   (ocf::heartbeat:ADS):   Started[ tis11-489Volume004 tis11-489Volume005 ]
    #   USX_7bf20255-3c9c-37e9-90d8-d966edca0ac8_atl_dedup   (ocf::heartbeat:dedup-filesystem):  Started[ tis11-489Volume004 tis11-489Volume005 ]
    #   USX_7bf20255-3c9c-37e9-90d8-d966edca0ac8_atl_nfs    (ocf::heartbeat:nfsserver): Started[ tis11-489Volume004 tis11-489Volume005 ]
    #   USX_7bf20255-3c9c-37e9-90d8-d966edca0ac8_ip    (ocf::heartbeat:IPaddr2):   FAILED (unmanaged)[ tis11-489Volume004 tis11-489Volume005 ]
    if not ha_check_volume_is_starting():
        resource_group = ha_get_resource_detail_status()
        for res,services in resource_group.iteritems():
            reset_flag = 1
            for service in services:
                if service == 'ADS' and not (len(services[service]['owner']) > 1 and local_node in services[service]['owner']):
                    reset_flag = 0
                    break
                if service == 'IPaddr2' and (services[service]['status'] != 'FAILED' or services[service]['managed']):
                    reset_flag = 0
                    break
            if reset_flag == 1:
                res_query = "/usx/inventory/volume/resources?query=.%5Buuid=\'" + res + "\'%5D&fields=raidplans"
                (ret, res_data) = ha_query_amc2(LOCAL_AGENT, res_query, 2)
                if ret == 0 and len(res_data['items']) > 0:
                    if len(res_data['items'][0]['raidplans'][0]['sharedstorages']) > 0 and \
                            len(res_data['items'][0]['raidplans'][0]['raidbricks']) == 0:
                                check_shared_storage_status()
                                if os.path.isfile(SHARED_STORAGE_NOT_FOUND):
                                    reset_vm('pure_sharedstorage_volume_storagenetwork_recover')

    # handle MOVE_SHARED_STORAGE case
    unmanage_flag_files = glob.glob('/var/log/HASM_MOVEDISK_*')
    if len(unmanage_flag_files) > 0:
        resuuid = unmanage_flag_files[0][23:]
        (ret, is_unmanaged) = ha_check_resouce_unmanaged(resuuid)
        if ret == 0 and is_unmanaged == True:
            ha_manage_one_resouce(resuuid)
        (ret, is_unmanaged) = ha_check_resouce_unmanaged(resuuid)
        if ret == 0 and is_unmanaged == False:
            cmd = 'rm ' +  unmanage_flag_files[0]
            (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    unmanage_flag_files = glob.glob('/var/log/HASM_UNMANAGE_*')
    if len(unmanage_flag_files) > 0:
        resuuid = unmanage_flag_files[0][23:]
        ha_manage_one_resouce(resuuid)
        cmd = 'crm node ready'
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
        cmd = 'rm ' +  unmanage_flag_files[0]
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    cmd = 'crm_mon -1 | grep heartbeat |  grep FAILED | grep unmanaged | grep -v grep '
    #res_ip       (ocf::heartbeat:IPaddr2):       FAILED 6666-tis16-22327-vv-005 (unmanaged)
    #res_ip    (ocf::heartbeat:IPaddr2):   FAILED (unmanaged)[ tis11-489Volume004 tis11-489Volume005 ]
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) >= 1:
        for the_line in msg:
            tmp = the_line.split()
            #debug('%s  %s' %(local_node, str(tmp)))
            if len(tmp) in [5,7]:
                if local_node in [tmp[-3], tmp[-2]] and (tmp[1] in ["(ocf::heartbeat:IPaddr2):", "(ocf::heartbeat:ADS):", "(ocf::heartbeat:dedup-filesystem):"]):
                    (ret, total_nodes, num_vols) = ha_has_standby()
                    if ret == True:
                        if tmp[1] in ["(ocf::heartbeat:IPaddr2):", "(ocf::heartbeat:ADS):"]:
                            resuuid = tmp[0][:-3]
                            # crm resource cleanup USX_c4fe63e0-f004-3426-8eb6-d58dfb68267b_group TIS44-233-Testbed3-vol-008
                            cmd = 'crm resource cleanup ' + resuuid + '_group ' + local_node
                            (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
                            #cmd = 'touch /var/log/HASM_UNMANAGE_' + resuuid
                            #(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
                            #cmd = 'crm node maintenance'
                            #(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
                            #ha_unmange_one_resouce(resuuid)
                        else:
                            resuuid = tmp[0][:-10]
                            cmd = 'crm resource cleanup ' + resuuid + '_atl_dedup ' + local_node
                            (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
                        continue

                    # crm_resource -r res_ip -C -H 6666-tis16-22327-vv-004
                    sub_cmd = 'crm_resource -r ' + tmp[0] + ' -C -H ' + tmp[3]
                    (ret, msg) = runcmd(sub_cmd, print_ret=True, lines=True)

    cmd = 'crm_mon -1 | grep heartbeat | grep FAILED | grep "(unmanaged)\[" | grep -v grep '
    # res_ds (ocf::heartbeat:ADS): FAILED (unmanaged)[ c-301452-tis18-vol-006 c-301452-tis18-vol-004 ]
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) >= 1:
        for the_line in msg:
            tmp = the_line.split()
            for the_tmp in tmp:
                if the_tmp == local_node and node_used_flag == False:
                    cluster_stop_flag = True
                    break

    if cluster_stop_flag == True:
        ha_stop_cluster()

    return 0

# Return 0 when the total votes is exactly half of the total number of nodes
# Return 1 when total number of nodes is 0 or total votes is not half of it.
def ha_is_node_split_brain():
    debug('Enter ha_is_node_split_brain')

    total_votes = 0
    total_num_nodes = 0
    maintenance_num_nodes = 0
    cmd = 'crm node status '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret != 0:
        return ret
    for the_line in msg:
        if the_line.find('</node>') >= 0:
            total_num_nodes += 1
        if the_line.find('name="maintenance" value="on"') >= 0:
            maintenance_num_nodes += 1

    total_valid_num_nodes = total_num_nodes - maintenance_num_nodes

    # corosync-quorumtool -s
    # Total votes:   5
    cmd = 'corosync-quorumtool -s  |grep "Total votes" |grep -v grep '
    (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    if ret != 0:
        return ret
    elif ret == 0 and len(msg) == 1:
        tmp = msg[0].split()
        if len(tmp) == 3:
            total_votes = int(tmp[2])
    debug('total number nodes is %d' % total_num_nodes)
    debug('total number maintenance nodes is %d' % maintenance_num_nodes)
    debug('total votes is %d' % total_votes)
    ret = 0
    if total_valid_num_nodes <= 0 or total_votes*2 != total_valid_num_nodes:
        ret = 1

    return ret

def ha_adjust_expected_votes():
    debug('Enter ha_adjust_expected_votes')

    expected_votes = 0
    expected_votes_cfg = 0
    total_num_nodes = 0
    total_num_nodes_end = 0
    cmd = 'crm node status '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret != 0:
        return ret
    for the_line in msg:
        if the_line.find('<node id=') >= 0:
            total_num_nodes += 1
        if the_line.find('</node>') >= 0:
            total_num_nodes_end += 1

    if total_num_nodes_end != total_num_nodes:
        node_name = ha_get_local_node_name()
        cmd = 'crm node utilization ' + node_name + ' set cpu 4'
        ha_retry_cmd(cmd, 1, 1)

    # corosync-quorumtool -s
    # Expected votes:   5
    cmd = 'corosync-quorumtool -s  |grep "Expected votes" |grep -v grep '
    (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    if ret != 0:
        return ret
    elif ret == 0 and len(msg) == 1:
        tmp = msg[0].split()
        if len(tmp) == 3:
            expected_votes = int(tmp[2])

    #root@AA-310651-tis18-vol-001:~# cat /etc/corosync/corosync.conf | grep expected_votes | awk '{print $2}'
    #5
    cmd = "cat %s | grep expected_votes | awk '{print $2}'" % COROSYNC_CONFIG_FILE
    (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    if ret != 0:
        return ret
    elif ret == 0 and len(msg) == 1:
        expected_votes_cfg = int(msg[0])

    # TODO: delete
    #debug("expected_votes %d vs. total_num_nodes %d" % (expected_votes, total_num_nodes))
    #expected_votes = 8
    #total_num_nodes = 7

    debug("expected_votes %d vs. total_num_nodes %d" % (expected_votes, total_num_nodes))
    if expected_votes != total_num_nodes and total_num_nodes > 0:
        cmd = 'corosync-quorumtool -e ' + str(total_num_nodes)
        (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    debug("expected_votes_cfg %d vs. total_num_nodes %d" % (expected_votes_cfg, total_num_nodes))
    if expected_votes_cfg != total_num_nodes:
        cmd = "sed -i -e 's/expected_votes: \S*/expected_votes: %d/' %s" % (total_num_nodes, COROSYNC_CONFIG_FILE)
        (ret, msg) = runcmd(cmd, print_ret=True,lines=True)

    return 0


def ha_has_offline_standby():
	debug('Enter ha_has_offline_standby')

	# get hauuid
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	hauuid = None
	try:
		node_dict = json.loads(s)
		ha_config = node_dict.get('haconfig')
		if ha_config is None:
			debug('ERROR: wrong json file %s' %str(node_dict))
			return False

		hauuid = node_dict['haconfig'].get('uuid')
		if hauuid is None:
			debug('ERROR: cannot find hauuid')
			return False
	except ValueError as err:
		debug('ERROR: wrong atlas.json')
		return False

	#Query  = .[hauuid='eth1-1415436436340']
	#https://10.21.148.233:8443/usxmanager/usx/inventory/volume/containers?query=.%5Bhauuid%3D'eth1-1415436436340'%5D&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false&api_key=f17c79a5-4d44-4c20-9c29-5f40d8c0e7c7
	amc_node_query = '/usx/inventory/volume/containers?query=.[hauuid=\'' + hauuid + '\']&sortby=uuid&order=ascend&page=0&pagesize=100&composite=false'
	(rc, ha_node_count, res_data) = ha_query_amc(LOCAL_AGENT, amc_node_query, 5)

	if rc == 0 and ha_node_count > 1:
		return True

	return False


def ha_reset_node_fake(filename):
	debug('Enter ha_reset_node_fake %s' % filename)

	fd = open('/var/log/' + filename, 'a')
	fd.flush()
	os.fsync(fd)
	fd.close()

	filename = '/var/log/' + filename + '_' + str(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
	fd = open(filename, 'a')
	fd.flush()
	os.fsync(fd)
	fd.close()

	return 0


def ha_reset_node(filename):
	debug('Enter ha_reset_node %s' % filename)

	fd = open('/var/log/' + filename, 'a')
	fd.flush()
	os.fsync(fd)
	fd.close()

	reset_filename = filename + '_reset'
	reset_vm(reset_filename)

	#debug('reset 3')
	#time.sleep(100000)
	return 0


def reset_vm(filename):
    debug('Enter reset_vm %s' % filename)

    try:
        filename = '/var/log/' + filename + '_reset_' + str(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
        fd = open(filename, 'a')
        fd.flush()
        os.fsync(fd)
        fd.close()

        # Log reset reason to kernel dmesg.
        fd = open('/dev/kmsg', 'w')
        traceback.print_stack(None,None,fd)
        dmesg_str = ' '.join(sys.argv) + ':' + str(os.getpid()) + ':' + filename + '\n'
        fd.write(dmesg_str)
        fd.close()

        # Sometimes the log file is not saved to disk, add sync here
        runcmd_nonblock('sync&', True, wait_time=2)
    except Exception as e:
        debug("Exception catched {}".format(e))

    (ret, msg) = runcmd('echo c > /proc/sysrq-trigger', print_ret=False)

    return 0


def ha_stop_cluster():
	debug('Enter ha_stop_cluster')
	runcmd('killall -9 corosync', print_ret=True)
	runcmd('killall -9 pacemakerd', print_ret=True)
	runcmd('service pacemaker stop', print_ret=True)
	runcmd('service corosync stop', print_ret=True)


def ha_set_volume_running_status(volresuuid):
	debug('Enter ha_set_volume_running_status %s' % volresuuid)
	cmd = 'touch /tmp/' + volresuuid + '_' + RUNNING_STATUS
	(ret, msg) = ha_retry_cmd(cmd, 5, 3)
	cmd = 'touch ' + HA_RUNNING_FILE
	(ret, msg) = ha_retry_cmd(cmd, 5, 3)


def ha_set_volume_starting_flag():
    debug('Enter ha_set_volume_starting_flag')
    cmd = 'touch ' + HA_STARTING_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 5, 3)


def ha_remove_volume_starting_flag():
    debug('Enter ha_remove_volume_starting_flag')
    cmd = 'rm -rf ' + HA_STARTING_FLAG
    (ret, msg) = ha_retry_cmd(cmd, 5, 3)


def ha_remove_ha_lock_file():
    debug('Enter ha_lock_file')
    cmd = 'rm -rf ' + HA_LOCK_FILE
    (ret, msg) = ha_retry_cmd(cmd, 5, 3)


def ha_set_start_up_preparation():
    cmd = 'touch ' + HA_START_UP_PREPARATION
    (ret, msg) = ha_retry_cmd(cmd, 5, 3)


def ha_check_start_up_preparation():
    return os.path.exists(HA_START_UP_PREPARATION)


def ha_remove_start_up_preparation():
    cmd = 'rm -rf ' + HA_START_UP_PREPARATION
    (ret, msg) = ha_retry_cmd(cmd, 5, 3)


def ha_check_volume_starting_flag():
    return os.path.isfile(HA_STARTING_FLAG)


def ha_check_volume_running(volresuuid):
	if volresuuid == None:
		return False
	status_file = '/tmp/' + volresuuid + '_' + RUNNING_STATUS
	if os.path.isfile(status_file):
		return True
	else:
		return False

def ha_check_ipaddr2_running():
        if os.path.isfile(IPADDR2_RUNNING_FILE):
                return True
        else:
                return False


def ha_check_node_used():
	if os.path.isfile(HA_RUNNING_FILE):
		return True
	else:
		return False

def ha_check_volume_is_starting():
    if ha_check_forcestarting_flag():
        return True
    cmd = 'ps -ef| grep "/opt/milio/atlas/roles/virtvol/vv-load.pyc ha" | grep -v grep | wc -l'
    (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    if ret == 0:
        if len(msg) > 0 and msg[0] != '0':
            debug('volume resource is starting on the node')
            return True
    return False

def ha_handle_multiple_start_volume(volresuuid):
    debug('Enter ha_handle_multiple_start_volume %s' % volresuuid)

    if ha_check_start_up_preparation():
        debug('ERROR: should not pick up any resource before preparing the node')
        return 3

    local_node = None
    cmd = 'crm_node -n'
    (ret, msg) = runcmd(cmd, print_ret=False, lines=True)
    if ret != 0:
        debug('ERROR: failed to check cluster nodes')
        return 3
    for the_line in msg:
        tmp = the_line.split()
        if len(tmp) > 0:
            local_node = tmp[0]
            break

    node_used_flag = ha_check_node_used()
    volume_running_flag = ha_check_volume_running(volresuuid)
    if node_used_flag == True and volume_running_flag == True:
        debug('WARN: volume resource %s has already run at current node %s' % (volresuuid, local_node))
        return 1
    elif node_used_flag == True and volume_running_flag == False:
        debug('WARN: another volume resource has already run at current node')
        return 3

    cmd = 'ps -ef| grep "/opt/milio/atlas/roles/virtvol/vv-load.pyc ha" | grep -v grep'
    (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    if ret == 0 and len(msg) > 1:
        debug('ERROR: multiple volumes starting on the same node')
        #errlogfile = 'mulitple_volumes_starting'
        #reset_vm(errlogfile)
        return 4

    cmd = 'crm resource status ' + volresuuid + '_ds'
    (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
    if ret == 0 and len(msg) == 1:
        if msg[0].find("is running on:") >= 0:
            tmp_line = msg[0].split()
            running_node = tmp_line[5]
            if running_node != local_node:
                debug('ERROR: volume resource %s has already run at another node %s' % (volresuuid, running_node))
                #errlogfile = volresuuid + '_already_run'
                #reset_vm(errlogfile)
                return 5

    if ha_pre_failover_validation(volresuuid) == 0:
        debug('ERROR: volume resource {} has already started at the other node'.format("volresuuid"))
        return 5

    # Save the output of crm_mon for debug
    ha_retry_cmd('crm_mon -r1', 1, 1)

    return 0


# Add a flag,
# if it is 0 as default, it will check whether current node could reach at least 2 SVM or not,
# if it is 1, if will check whether current node could reach at least half SVM or not
def ha_check_storage_network_status(flag = 0):
    debug('Enter ha_check_storage_network_status')

    offline_node_list = ha_get_offline_node_list()
    num_offline_nodes = len(offline_node_list)
    svm_ip_list = []
    rc = 0

    svm_info = ha_get_service_vm_info()
    # For pure sharedstorage Volume without Service VMs
    if svm_info == {}:
        return 0

    for svm in svm_info.itervalues():
        svm_ip_list.append(svm['serviceip'])
    if len(svm_ip_list) > 0:
        num_reachable_svms = 0
        for the_ip in svm_ip_list:
            cmd = 'ping -c 2 -W 2 ' + the_ip
            (ret, msg) = runcmd(cmd, print_ret=True,lines=True)
            if ret == 0:
                num_reachable_svms += 1

        if flag:
            debug('num of reachable svms is ' + str(num_reachable_svms))
            debug('num of total svms is ' + str(len(svm_ip_list)))
            if num_reachable_svms * 2 < len(svm_ip_list):
                rc = 1
        else:
            # If the Volume totally has 2 Service VMs, it is accesptable that reachablenum = 1
            if num_reachable_svms < min(2, (len(svm_info.keys()) + 1) / 2):
                rc = 1
    return rc


def ha_check_ibd_status():
	debug('Enter ha_check_ibd_status')

	cmd = CMD_IBDMANAGER_STAT
	(ret, msg) = runcmd(cmd, print_ret=False,lines=True)

	total_num_ibds = 0
	num_working_ibds = 0
	for line in msg:
		if line.find("Service Agent Channel") >= 0:
			total_num_ibds += 1
		elif line.find("state:working") >= 0:
			num_working_ibds += 1

	if total_num_ibds > 0 and num_working_ibds == 0:
		debug("ERROR check the ibd working state: "  + str(msg))
		return 1
	else:
		return 0


# Set location for current failover
def ha_set_location(the_vol):
    debug('Enter ha_set_location')

    retry_time = 0
    location_score_string = str(ADS_LOCATION_SCORE) + ':'
    node_name = ha_get_local_node_name()

    # Check if the location is already set there
    the_vol_group = the_vol + '_group'
    the_vol_group_loc = the_vol + '_group_loc'
    cmd = 'crm configure show | grep location | grep %s | grep %s' % (the_vol_group_loc, node_name)
    (ret, msg) = ha_retry_cmd(cmd, 1, 3)
    if ret == 0 and len(msg) > 0 and not 'ERROR' in msg[0]:
        debug('INFO: the group location should be set before')
        return 0

    cmd = 'crm configure delete ' + the_vol_group_loc
    (ret, msg) = ha_retry_cmd(cmd, 1, 3)
    cmd = 'crm configure location ' + the_vol_group_loc + ' ' + the_vol_group + ' ' + location_score_string + ' ' + node_name
    (ret, msg) = ha_retry_cmd(cmd, 5, 5)
    if ret == 1:
       debug('ERROR: failed to change volume group %s location to %s' % (the_vol_group, node_name))
       return 1
    debug('INFO: change volume group %s location to %s' % (the_vol_group, node_name))
    return 0


def ha_check_config_consistency():
    debug('Enter ha_check_config_consistency')

    volume_list = []
    volume_list = ha_get_volume_list()
    location_score_string = str(ADS_LOCATION_SCORE) + ':'
    node_name = ha_get_local_node_name()

    for the_vol in volume_list:
        the_vol_group = the_vol + '_group'
        the_vol_group_loc = the_vol + '_group_loc'
        cmd = 'crm configure show | grep location'
        (ret, msg) = ha_retry_cmd(cmd, 1, 5)
        if ret == 0 and len(msg) > 0:
            for line in msg:
                tmp_array = line.split()
                if tmp_array[0] == 'location' and tmp_array[-1] == node_name:
                    cmd = 'crm configure delete ' + tmp_array[1]
                    (ret, msg) = ha_retry_cmd(cmd, 1, 5)
                    debug('INFO: delete crm resource location %s' % (tmp_array[1]))

    return 0


def ha_check_deletion(flag, voluuid):
	debug('Enter ha_check_deletion')

	supported_flag = ['-rc','-rr']
	if flag not in supported_flag:
		debug('WARN: the flag %s is not supported yet' % flag)
		return 0

	local_node = None
	cmd = 'crm_node -n'
	(ret, msg) = runcmd(cmd, print_ret=False, lines=True)
	if ret != 0:
		debug('ERROR: failed to check cluster nodes')
		return 1
	for the_line in msg:
		tmp = the_line.split()
		if len(tmp) > 0:
			local_node = tmp[0]
			break

	if local_node != None:
		res_tails = ['_ds','_ip','_atl_vscaler','_atl_nfs','_atl_dedup','_atl_iscsi_target','_atl_iscsi_lun']
		res_list = []
		if voluuid !=  None:
			for the_tail in res_tails:
				res_list.append(voluuid + the_tail)

		# search resources on local node
		cmd = 'crm status'
		(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
		if ret != 0:
			debug('ERROR: failed to check cluster status')
			return 1

		res_running = False
		local_running = False
		local_found = False
		remote_found = False
		remote_node = None
		for line in msg:
			if line.find("Started") >= 0:
				tmp = line.split()
				if len(tmp) == 4 and tmp[2] == 'Started':
					res = tmp[0]
					started_by = tmp[3]

					if res in res_list:
						res_running = True

					if started_by == local_node and res != 'iliomon':
						local_running = True

					if started_by == local_node and (res in res_list):
						local_found = True

					if started_by != local_node and (res in res_list):
						remote_found = True
						remote_node = started_by

		if flag == '-rr' and remote_found == True:
			debug('ERROR: resource %s is running in another node %s' % (voluuid, remote_node))
			return ERROR_RESOURCE_IN_ANOTHER_NODE
		elif flag == '-rr' and local_running == True and local_found == False:
			debug('ERROR: node %s has another resource running during volume deletion' % local_node)
			return ERROR_ANOTHER_RESOURCE_RUNNING
		elif flag == '-rc' and local_running == True:
			debug('ERROR: node %s has resource running during container deletion' % local_node)
			return ERROR_RESOURCE_RUNNING
		else:
			return 0

	return 0


def ha_force_failover(voluuid, dest_node):
	debug('Enter ha_force_failover')

	if voluuid == None:
		debug('ERROR: volume uuid cannot be none')

	if dest_node == None:
		debug('ERROR: destination node cannot be none')

	debug('Begin HA force failover %s to %s' % (voluuid, dest_node))
	max_num_retry = 3600
	vol_group = voluuid + '_group'
	vol_group_loc =  vol_group + '_ff_loc'

	# handle the case: volume resource has already run at destination node
	cmd = 'crm resource status ' + vol_group
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0 and len(msg) == 1 and msg[0].find("is running on:") >=0:
		tmp_line = msg[0].split()
		running_node = tmp_line[5]
		if running_node == dest_node:
			debug('WARN: volume resource %s has already run at destination node %s' % (voluuid, dest_node))
			return 0

	delete_loc_cmd = 'crm configure delete ' + vol_group_loc

	cmd = 'crm configure show ' + vol_group_loc
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
		if ret != 0:
			debug('ERROR: failed to delete group location configuration')
			return 1

	cmd = 'crm configure location ' + vol_group_loc + ' ' + vol_group + ' role=Started inf: ' + dest_node
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret != 0:
		debug('ERROR: failed to configure group location')
		(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
		return 1

	cmd = 'crm resource stop ' + vol_group
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret != 0:
		(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
		debug('ERROR: failed to stop resource')
		return 1

	cmd = 'crm resource status ' + vol_group
	retry = 0
	stop_flag = False
	while (retry < max_num_retry):
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		# resource vCenter200_Danzhou-Hybrid01-1422567822152_group is NOT running
		if ret == 0 and len(msg) == 1 and msg[0].find("is NOT running") >=0:
			stop_flag = True
			break
		else:
			retry = retry + 1
			time.sleep(1)

	if stop_flag != True:
		debug('ERROR: failed to stop resource')
		(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
		return 1

	cmd = 'crm resource start ' + vol_group
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret != 0:
		debug('ERROR: failed to start resource')
		(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
		return 1

	cmd = 'crm resource status ' + vol_group
	retry = 0
	start_flag = False
	while (retry < max_num_retry):
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		if ret == 0 and len(msg) == 1 and msg[0].find("is running on:") >=0:
			start_flag = True
			break
		else:
			retry = retry + 1
			time.sleep(1)

	if start_flag != True:
		debug('ERROR: failed to start resource')
		(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
		return 1

	(ret, msg) = runcmd(delete_loc_cmd, print_ret=True, lines=True)
	if ret != 0:
		debug('ERROR: failed to delete group location configuration')
		return 1

	debug('Complete HA force failover %s to %s' % (voluuid, dest_node))
	return 0


def ha_handle_enable_ha_failure(voluuid):
	debug('Enter ha_handle_enable_ha_failure %s' % voluuid)

	cmd = 'ps -ef| grep "/opt/milio/atlas/roles/ha/add_node_to_cluster.pyc" | grep -v grep'
	(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
	if ret == 0 and len(msg) >= 1:
		for the_line in msg:
			tmp = the_line.split()
			if len(tmp) < 2:
				continue
			cmd = 'kill -9  ' + tmp[1]
			(ret, submsg) = runcmd(cmd, print_ret=True)

	cmd = 'python /opt/milio/atlas/roles/ha/delete_node.pyc -rr ' + voluuid
	(ret, msg) = runcmd(cmd, print_ret=True)

	# clean up HA STATUS to VOL_STATUS_UNKNOWN
	cleanup_vol_ha_status()
	return 0

def ha_check_status():
	debug('Enter ha_check_status')

	service_health = True
	dedup_health = True
	ip_health = True

	# json file format:
	#"volumeresources": [
	#	{
	#		"containeruuid": "vc13417_AAA-111-53B-tis18-Hybrid-111",
	#		"dedupfsmountpoint": "/exports/AAA-111-53B-tis18-Hybrid-111",
	#		"exporttype": "NFS",
	#		"serviceip": "10.121.148.51"
	#		"volumetype": "HYBRID"
	#	}
	#]
	(containeruuid, dedupfsmountpoint, exporttype, service_ip, volumetype) = ha_retrieve_config()
	debug('%s %s %s %s %s' %(containeruuid, dedupfsmountpoint, exporttype, service_ip, volumetype))
	if exporttype == 'NFS':
		cmd = 'ps aux | grep nfsd | grep -v grep '
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		if ret != 0:
			debug("WARN: NFS is not running")
			service_health = False

	dedupfs_found = False
	if dedupfsmountpoint != '':
		cmd = 'mount | grep dedup | grep -v grep '
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		if ret != 0:
			debug("WARN: no dedupFS")
			dedup_health = False
		else:
			for the_line in msg:
				the_list = the_line.split(' ')
				the_dev = the_list[0]
				the_mntpoint = the_list[2]
				if the_mntpoint == dedupfsmountpoint:
					dedupfs_found = True

				#TODO: test to read and write the dedupFS
				#dd if=/dev/md3  of=/dev/null  bs=512 count=1 iflag=direct,nonblock
				#sub_cmd = 'dd if=' + the_dev + ' of=/dev/null  bs=512 count=1 iflag=direct,nonblock'
				#(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
				#if sub_ret != 0:
				#	debug("WARN: read error from dedupFS")
				#	dedup_health = False
	if dedupfsmountpoint != '' and dedupfs_found == False:
		debug('WARN: could not find dedupFS %s' % dedupfsmountpoint)
		dedup_health = False

	service_ip_found = False
	if service_ip != '':
		cmd = 'ip addr show | grep "scope global secondary" | grep -v grep '
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		if ret != 0:
			debug('WARN: no service_ip')
			ip_health = False
		else:
			for the_line in msg:
				if the_line.find(' ' + service_ip + '/') >= 0:
					service_ip_found = True
	if service_ip != '' and service_ip_found == False:
		debug('WARN: could not find service_ip %s' % service_ip)
		ip_health = False

	if service_health == True and dedup_health == True and ip_health == True:
		return 0
	else:
		return 1

def ha_stop_service_ip():
	debug('Enter ha_stop_service_ip')

	cmd = 'ip addr show | grep "scope global secondary" | grep -v grep '
	sret = 0
	smsg = ''
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	# inet 10.116.146.108/16 brd 10.116.255.255 scope global secondary eth1
	if ret == 0:
		for the_line in msg:
			items = the_line.split()
			service_ip = items[1]
			dev = items[7]
			# ip addr del 10.116.146.108/16 dev eth1
			cmd = 'ip addr del ' + service_ip + ' dev ' +  dev
			(sret, smsg) = runcmd(cmd, print_ret=True, lines=True)
	return (sret, smsg)



def ha_umount_dedupFS():
	debug('Enter ha_umount_dedupFS')

	rc = 0
	# umount dedupfs
	cmd = 'mount | grep dedup | grep -v grep '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for the_line in msg:
			the_list = the_line.split(' ')
			the_dev = the_list[0]
			sub_cmd = 'umount ' + the_dev
			(sub_ret, sub_msg) = ha_retry_cmd(sub_cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
			if sub_ret != 0:
				rc = 1
			sub_cmd = 'mdadm --stop ' + the_dev
			(sub_ret, sub_msg) = ha_retry_cmd(sub_cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
			if sub_ret != 0:
				rc = 1

	return rc


def ha_storage_network_status(node_dict):
    debug('Enter ha_storage_network_status')
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        return 2

    uuid = ilio_dict.get('uuid')
    nics = ilio_dict.get('nics')
    if nics is None:
        debug('nics is None ')
        return 2
    ipaddr = None
    devname = None
    for nic in nics:
        if nic.get("storagenetwork") is True:
            ipaddr = nic.get("ipaddress")
            devname = nic.get("devicename")
            break
    if devname is None:
        debug('Error getting storage network information.')
        return 2

    (ret, msg) = runcmd('cat /sys/class/net/' + devname + '/operstate', print_ret=False,lines=True)
    if ret != 0:
        return 2
    debug(msg)
    for line in msg:
        if line == "up":
            return 0
        elif line == "down":
            return 1
    return 2


def get_volume_info():
	debug("Enter get_volume_info")
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	volume_type = None
	volume_uuid = None
	ilio_uuid = None
        display_name = None
	try:
		fulljson = json.loads(s)
		ilio_uuid = fulljson['usx']['uuid']
                display_name = fulljson['usx']['displayname']
		if fulljson.has_key('volumeresources'):
			if len(fulljson['volumeresources']) >= 1:
				#"uuid": "USX_a90d31d8-948e-3508-946d-36f826786d77",
				#"volumeservicename": "tis11-hybrid-vv",
				#"volumetype": "HYBRID",
				if fulljson['volumeresources'][0].has_key('volumetype'):
					volume_type = fulljson['volumeresources'][0]['volumetype']
				if fulljson['volumeresources'][0].has_key('uuid'):
					volume_uuid = fulljson['volumeresources'][0]['uuid']
				debug('ilio_uuid %s, volume_uuid %s, volume_type %s ' % (ilio_uuid, volume_uuid, volume_type))
		return (volume_type, volume_uuid, ilio_uuid, display_name)
	except ValueError as err:
		debug('ERROR: wrong atlas.json')
		return (None, None, None, None)

def create(filename):
    fd = os.open(filename, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
    return os.fdopen(fd, 'w')

def get_curr_volume():
    try:
        f = open('/tmp/current_volume', 'r')
        vol_uuid = f.read()
        f.close()
    except:
        return None
    return vol_uuid

def set_curr_volume(vol_uuid):
    old_vol_uuid = get_curr_volume() #get resource uuid
    if old_vol_uuid != None:
        debug('ERROR: volume %s already exist on this VM!' % old_vol_uuid)
        return False
    try:
        f = create('/tmp/current_volume')
        f.write(vol_uuid)
        f.close()
    except:
        debug('ERROR: current_volume already exist!')
        return False
    debug('Created volume flag file for: %s' % vol_uuid)
    return True

def unset_curr_volume():
    check_file = '/tmp/current_volume'
    if os.path.exists(check_file):
        runcmd('rm -f ' + check_file,print_ret=True)
        debug('Remove volume flag file')
        if os.path.exists(HA_STARTING_FLAG):
            ha_remove_volume_starting_flag()
    return True

def cleanup_vol_ha_status():
	debug("Enter cleanup_vol_ha_status")

	# check ha status
	ret = ha_check_enabled()
	if ret == True:
		debug("WARN: HA is enabled, skip")
		return 1

	# check volume info
	(volume_type, volume_uuid, ilio_uuid, display_name) = get_volume_info()
	if volume_type == None or volume_uuid == None or ilio_uuid == None:
		debug('WARN: volume is none')
		return 1
	elif volume_type.upper() in ['SIMPLE_HYBRID', 'SIMPLE_MEMORY', 'SIMPLE_FLASH']:
		debug('WARN: skip because volume type is %s' % volume_type)
		return 1

	stats = {}
	stats['HA_STATUS'] = long(VOL_STATUS_UNKNOWN)
	restapi_url = LOCAL_AGENT + '/usx/status/update'
	json_str = build_json_str(stats, ilio_uuid, ilio_uuid, "VOLUME_CONTAINER")
	cmd = 'curl -k -X POST -H "Content-Type:application/json" -d "%s" %s --connect-timeout 10 --max-time 60 -v' % (json_str, restapi_url)
	(ret, msg) = ha_retry_cmd(cmd, 60, 30);


def update_volume_status(adsname_str, status):
	debug("Enter update_volume_status")
	stats = {}
	stats['VOLUME_EXTENSION_STATUS'] = long(status)
	return send_volume_availability_status(adsname_str, stats, "VOLUME_RESOURCE")


def send_vol_ibd_status():
	volresuuid = None
	iliouuid = None
	USX_DICT = {}
	try:
		fp = open(ATLAS_CONF)
		jsondata = json.load(fp)
		fp.close()
		if jsondata.has_key('usx'): # this is a volume
			USX_DICT['role'] = jsondata['usx']['roles'][0]
			USX_DICT['uuid'] = jsondata['usx']['uuid']
			USX_DICT['usxmanagerurl'] = get_master_amc_api_url()
			USX_DICT['resources'] = jsondata['volumeresources']

			if USX_DICT['resources']: # volume has resource, not HA
				volresuuid = USX_DICT['resources'][0]['uuid']
				iliouuid = USX_DICT['uuid']
			else:
				#debug("WARN : HA standby node, skip")
				return 0
	except:
		debug("ERROR : exception occured, exiting ...")
		return 1

	raidbricks = []
	raidplans = USX_DICT['resources'][0]['raidplans']
	ibd_flag = False
	for the_plan in raidplans:
		if the_plan.has_key('raidbricks'):
			raidbricks = the_plan['raidbricks']
			#debug('raidbricks: ' + str(raidbricks))
			if len(raidbricks) > 0:
				ibd_flag = True
				break

	if ibd_flag == True:
		debug('send_vol_ibd_status for %s in %s' % (volresuuid, iliouuid))
		cmd = 'ps -ef|grep ibdagent | grep -v grep '
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		if ret == 0:
			status = VOL_STATUS_OK
		else:
			status = VOL_STATUS_FATAL

		stats = {}
		stats['VOLUME_STORAGE_STATUS'] = long(status)
		restapi_url = LOCAL_AGENT + '/usx/status/update'
		json_str = build_json_str(stats, iliouuid, volresuuid, "VOLUME_RESOURCE")
		cmd = 'curl -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, restapi_url)
		(ret, out) = runcmd(cmd, print_ret=True, block=False)

	return 0


def update_ha_flag(url, uuid, resuuid, status, cleanup):
	debug('Enter update_ha_flag')

	jobid_file_exists = does_jobid_file_exist(True)
	does_jobid_file_need_deletion = not jobid_file_exists
	send_status("HA", 100, 0, "Enable HA", "HA resources have been configured in a new cluster.", does_jobid_file_need_deletion, block=False)
	if resuuid is None:
		cmd = 'curl -k -X PUT ' + url + '/usx/inventory/volume/containers/' + uuid + '/ha?isha=' + status + '\&api_key=' + uuid + '\&cleanup=' + cleanup
	else:
		cmd = 'curl -k -X PUT ' + url + '/usx/inventory/volume/resources/' + resuuid + '/ha?isha=' + status + '\&api_key=' + uuid + '\&cleanup=' + cleanup
	(ret, out) = runcmd(cmd, print_ret=True)


def send_volume_alert(status, description):
	debug('Enter send_volume_alert')
	ret = get_uuid()
	debug(str(ret))
	if ret.has_key('resource') and ret.has_key('container') and ret.has_key('displayname'):
		#send alert
		send_alert_raid_sync(ret['container'], ret['displayname'], status, None, description)



def is_shared_storage_accessible(dev):
	#cmd = 'dd if=' + dev + ' of=/dev/null bs=512 count=1 iflag=direct,nonblock'
	ioping_cmd = '/usr/bin/ioping'
	if os.path.exists('/usr/bin/ioping'):
		ioping_cmd = '/usr/bin/ioping'
	elif os.path.exists('/usr/local/bin/ioping'):
		ioping_cmd = '/usr/local/bin/ioping'
	else:
		debug('ERROR: cannot find ioping')

	cmd = ioping_cmd + ' -A -D -c 1 -s 512 ' + dev
	ret = runcmd_nonblock(cmd, print_ret=True)
	if ret == 0:
		return True;
	else:
		return False


def is_shared_device_accessible(dev):
    #cmd = 'dd if=' + dev + ' of=/dev/null bs=512 count=1 iflag=direct,nonblock'
    ioping_cmd = '/usr/bin/ioping'
    if os.path.exists('/usr/bin/ioping'):
        ioping_cmd = '/usr/bin/ioping'
    elif os.path.exists('/usr/local/bin/ioping'):
        ioping_cmd = '/usr/local/bin/ioping'
    else:
        debug('ERROR: cannot find ioping')

    cmd = ioping_cmd + ' -A -D -c 1 -s 512 ' + dev
    ret = runcmd_nonblock(cmd, print_ret=True)
    check_file = '/tmp/shared_storage_check_error'
    if ret != 0:
        if not os.path.isfile(check_file):
            ret = runcmd_nonblock(cmd, print_ret=True)
            if ret != 0:
                runcmd('touch ' + check_file, print_ret=True)
                # return True now to give this check one more try
                return True
        else:
            return False
    if os.path.isfile(check_file):
        runcmd('rm -f ' + check_file,print_ret=True)
    return True


def check_shared_storage_status():
    debug('Enter check_shared_storage_status')
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()

    try:
        node_dict = json.loads(s)
        if node_dict.has_key('volumeresources'):
            virtualvols = node_dict.get('volumeresources')
            if virtualvols is None:
                debug('WARN: no volume resources')
                return 0
            for vv in virtualvols:
                for raidplan in vv.get("raidplans"):
                    for sharedstorage in raidplan.get("sharedstorages"):
                        if sharedstorage.get("storagetype") == "DISK" or sharedstorage.get("storagetype") == "FLASH":
                            dev = scsi_to_device(sharedstorage.get("scsibus"))
                            if dev is None:
                                check_file = SHARED_STORAGE_NOT_FOUND
                                if not os.path.isfile(check_file):
                                    runcmd('touch ' + check_file, print_ret=True)
                                    return 1
                            else:
                                if not is_shared_device_accessible(dev):
                                    return 1
    except ValueError as err:
        debug('Exception caught within check_shared_storage_status')

    return 0


def runcmd_nonblock(cmd, print_ret=False, wait_time=5):
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
        time.sleep(wait_time)
        if p.poll() is None:
            return 1
	return 0
    except OSError:
        debug('Exception with Popen')
        return -1

'''
def _send_alert_raid_sync(ilio_id, name, status, description):
    debug('START: Send alert')

    cmd = 'date +%s'
    (ret, epoch_time) = runcmd(cmd)
    epoch_time = epoch_time.rstrip('\n')

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
    	"description"		:"",
    	"service"		:"MONITORING",
    	"alertTimestamp"	:"",
    	"usxtype"		:"VOLUME"
    }

    ad["uuid"] = ilio_id + '-raid-sync-alert-' + str(epoch_time)
    ad["checkId"] = ilio_id + '-raidsync'
    ad["usxuuid"] = ilio_id
    ad["displayname"] = name
    #ad["target"] = "servers." + ilio_id + ".raidsync"
    ad["alertTimestamp"] = epoch_time
    #ad["usxtype"] = 'volume'
    ad['status'] = status
    ad['description'] = description

    code, ret_val = call_rest_api(LOCAL_AGENT + "/alerts", 'POST', json.dumps(ad))
    if code != '200':
        debug("ERROR : Failed to send alert.")
        ret = False
    else:
        ret = True

    debug('END: Send alert')
    return ret
'''

def get_uuid():
    ret = {}
    try:
        fp = open(ATLAS_CONF)
        jsondata = json.load(fp)
        fp.close()
        if jsondata.has_key('usx'): # this is a volume
            ret['container'] = jsondata['usx']['uuid']
            ret['displayname'] = jsondata['usx'].get('displayname')
            if ret['displayname'] is None:
                ret['displayname'] = ret['container']
            #ret['displayname'] = jsondata['usx']['displayname']
            if len(jsondata['volumeresources']) > 0:
                ret['resource'] = jsondata['volumeresources'][0]['uuid']
                ret['displayname'] = jsondata['volumeresources'][0]['displayname']
    except ValueError as err:
        pass

    return ret


def build_json_str(stats, iliouuid, volumeresourceuuid, res_type):
    status_content = ''
    for key, value in stats.iteritems():
        if value != -1: # value = -1 means this status is not enabled for reporting
            status_content += '{\\"name\\":\\"' + key + '\\",\\"value\\":\\"' + volume_status_dict[value] + '\\"},'
    content = status_content.rstrip(',')
    result = ('{\\"usxstatuslist\\":[' + content + '], \\"usxuuid\\":\\"' + volumeresourceuuid + '\\", \\"usxtype\\":\\"' + res_type + '\\", \\"usxcontaineruuid\\":\\"' + iliouuid + '\\"}')
    return result


def send_volume_availability_status(resname, stats, res_type):
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    node_dict = json.loads(s)
    if node_dict is None:
        debug('Error getting Node info')
        return -1
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        debug('Error getting Ilio info')
        return -1
    iliouuid = ilio_dict.get('uuid')
    restapi_url = LOCAL_AGENT + '/usx/status/update'
    json_str = build_json_str(stats, iliouuid, resname, res_type)
    cmd = 'curl -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, restapi_url)
    (ret, out) = runcmd(cmd, print_ret=True, block=False)
    return ret

def ha_modules_version():
    return VERSION

def usx_daemon_version():
    if os.path.exists(USX_DAEMON_VERSION):
        with open(USX_DAEMON_VERSION, 'r') as fd:
            lines = fd.readlines()
            if len(lines) > 0:
                return lines[0]
    return 'UNKNOWN - Before 3.5.1'
