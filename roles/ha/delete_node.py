#!/usr/bin/python

import os, sys
import logging
import tempfile
from subprocess import *
import httplib
import json
import socket
from time import sleep
from ha_util import *

sys.path.insert(0, "/opt/milio/libs/atlas")
from status_update import send_status

ATLAS_CONF = '/etc/ilio/atlas.json'
CONFIG_FILE = '/usr/share/ilio/configured'
ILIO_RES_STOP = '/opt/milio/atlas/roles/ha/ilio_res_stop.pyc'
PACEMAKER_RSC = '/opt/milio/atlas/roles/ha/pacemaker_rsc.pyc'
PACEMAKER_RSC_LIST = '/tmp/pacemaker_rsc.list'
COROSYNC_DEFAULT = '/etc/default/corosync'
HA_DISABLE_FILE = '/tmp/ha_disable'
LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager'

set_log_file('/var/log/usx-atlas-ha.log')

volresuuid = None

def remove_node_from_cluster(node_name, ip_list):
	# Invoke "delete_node.pyc node_name" in another node in the cluster
	for ipaddr in ip_list:
		cmd = 'curl -k -X POST -H "Content-Type:application/json" "http://' + ipaddr + ':8080/usxmanager/commands?command=python%20%2Fopt%2Fmilio%2Fatlas%2Froles%2Fha%2Fdelete_node.pyc&arguments=' + node_name + '&type=os"'
		(ret, msg) = runcmd(cmd, print_ret=True, block=False)
		if ret == 0:
			return ret
	return 1

def resource_is_started(res):
	cnt = 125
	cmd = 'crm resource status'
	res_found = False
	while cnt > 0:
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			if line.find(res) >= 0 and line.find("Resource Group") < 0:
				res_found = True
				if line.find("Started") >= 0:
					return True
				cnt = cnt - 1
				sleep(5)
				break
		if not res_found:
			return True
	return False

def resource_is_stopped(res):
	cnt = 125
	cmd = 'crm resource status'
	res_found = False
	while cnt > 0:
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			if line.find(res) >= 0 and line.find("Resource Group") < 0:
				res_found = True
				if line.find("Stopped") >= 0:
					return True
				elif line.find("Started") >= 0 and line.find("FAILED") >= 0:
					return False
				cnt = cnt - 1
				sleep(5)
				break
		if not res_found:
			return True
	return False

def stop_resources(node_name, res_id, reason, delete_vol):
	res_list = []

	if len(res_id) > 0:
		if os.path.isfile(PACEMAKER_RSC_LIST):
			with open(PACEMAKER_RSC_LIST, "r") as fd:
				content = fd.readlines()
				for line in content:
					if line.find('group') >= 0:
						continue
					else:
						res_list.append(line)
		else:
			cmd = 'crm resource status'
			(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
			if ret != 0:
				return 1
			res_tails = ['_ds','_ip','_atl_vscaler','_atl_nfs','_atl_dedup','_atl_iscsi_target','_atl_iscsi_lun']
			tmp_res_list = []
			for item in res_tails:
				tmp_res_list.append(res_id + item)
			for line in msg:
				if line.find("Resource Group") >= 0:
					continue
				if line.find(res_id) >= 0:
					tmp = line.split()
					res = tmp[0]
					if res in tmp_res_list:
						res_list.append(res)

	# skip volume resource deletion, and let IBD handle it.

	# enter the maintenance mode
	cmd = 'crm node maintenance '
	(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

	(ret, msg) = runcmd('touch '+ HA_DISABLE_FILE, print_ret=True)
	if ret != 0:
		(ret, msg) = runcmd('touch '+ HA_DISABLE_FILE, print_ret=True)
	for res in res_list:
		cmd = 'crm resource maintenance ' + res
		(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
		cmd = 'crm resource unmanage ' + res
		(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
		cmd = 'crm configure delete ' + res
		(ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
		res_path = '/var/run/resource-agents/' + res
		try:
			os.remove(res_path)
		except:
			pass

	ip_list = []

	# Note: need to send request to both online and maitenance nodes
	# collect IP addresses of nodes in the cluster where this node belongs to
	cmd = 'corosync-quorumtool -i'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	for line in msg:
		if line.find("local") < 0:
			tmp = line.split()
			if len(tmp) > 2 and tmp[0].isdigit():
				ip_list.append(tmp[2])

	ha_disable_ha_flag()
	# Make sure corosync cannot be restarted
	cmd = 'rm -f /run/pacemaker_started'
	(ret, msg) = runcmd(cmd, print_ret=True)

	tmp_fname = '/tmp/corosync'
	cfgfile = open(tmp_fname, "w")
	cfginfo='# start corosync at boot [yes|no]\nSTART=no\n'
	cfgfile.write(cfginfo)
	cfgfile.close()
	os.rename(tmp_fname, COROSYNC_DEFAULT)
	sleep(1)
	cmd = 'service corosync stop'
	(ret, msg) = runcmd(cmd, print_ret=True)

	# remove HA_DISABLE_FILE
	try:
		os.remove(HA_DISABLE_FILE)
	except:
		pass

	# clean Pacemaker configuration files for volume resource deletion
	if delete_vol == True:
		cmd = 'service pacemaker stop'
		(ret, msg) = runcmd(cmd, print_ret=True)
		cmd = 'rm -rf /var/lib/pacemaker/cib/*.*'
		(ret, msg) = runcmd(cmd, print_ret=True)

	if len(ip_list) > 0:
		remove_node_from_cluster(node_name, ip_list)
	return 0

def cleanup_resources(reason, res = None):
	if res is None:
		cmd = 'python ' + ILIO_RES_STOP + ' ' + reason
	else:
		cmd = 'python ' + ILIO_RES_STOP + ' ' + reason + ' ' + res
	(ret, msg) = runcmd(cmd, print_ret=True)
	return ret

def send_enable_ha_status(url, uuid, resuuid, status, cleanup):
	if resuuid is None:
		cmd = 'curl -k -X PUT ' + url + '/usx/inventory/volume/containers/' + uuid + '/ha?isha=' + status + '\&api_key=' + uuid + '\&cleanup=' + cleanup
	else:
		cmd = 'curl -k -X PUT ' + url + '/usx/inventory/volume/resources/' + resuuid + '/ha?isha=' + status + '\&api_key=' + uuid + '\&cleanup=' + cleanup
	(ret, out) = runcmd(cmd, print_ret=True)

def send_vol_ha_status(status):
    global volresuuid
    stats = {}
    stats['HA_STATUS'] = long(status)
    if volresuuid:
        send_volume_availability_status(volresuuid, stats, "VOLUME_CONTAINER")
    else:
        debug('Delete node: volresuuid is none.')

#########################################################
#		START HERE 				#
#########################################################
debug('INFO: begin to disable HA')
cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc disable_start'
(ret, msg) = runcmd(cmd, print_ret=True)

cmd = 'ps -ef | egrep "add_node_to_cluster|pacemaker_config" | grep -v grep | awk "\$2 ~ /\d*/ {print \$2}"'
(ret, msg) = ha_retry_cmd(cmd, 2, 5)
if ret == 0:
    for pid in msg:
        cmd = 'kill -9 ' + pid
        (ret, msg) = ha_retry_cmd(cmd, 2, 5)

cfgfile = open(ATLAS_CONF, 'r')
s = cfgfile.read()
cfgfile.close()
try:
	node_dict = json.loads(s)
	ilio_dict = node_dict.get('usx')
	if ilio_dict is None:
		ilio_dict = node_dict
	ha = ilio_dict.get('ha')
	amcurl = LOCAL_AGENT
	uuid = ilio_dict.get('uuid')
	roles = ilio_dict.get('roles')
	if roles is None:
		debug('Error getting role information')
		sys.exit(11)
	role = roles[0]
	does_jobid_file_need_deletion = False
except ValueError as err:
	sys.exit(11)

host_name = ha_get_local_node_name()
if host_name == None:
	debug('Error getting host name')
	sys.exit(1)

node_migrate = False
disable_ha = False
delete_vol = False
resourceid = None
node_name = None

debug('From AMC: ' + ' '.join(sys.argv))

if len(sys.argv) < 2:
    node_name = host_name
else:
    if sys.argv[1] == '-u':
        # migrate HA node from a cluster to another cluster
        node_name = host_name
        node_migrate = True
    elif sys.argv[1] == '-dr':
        # disable HA case
        node_name = host_name
        disable_ha = True
        resourceid = sys.argv[2]
        volresuuid = uuid
        #TODO: need to do some consistency checking here
        debug('INFO: begin to disable HA for resource ' + resourceid)

        ret = ha_check_enabled()
        if ret == False:
            debug("WARN: HA has been already disabled, skip")
        else:
            cmd = 'crm resource status ' + resourceid + '_ds'
            (ret, msg) = ha_retry_cmd(cmd, 1, 1)
            if ret == 0 and len(msg) > 0 \
                    and "is running on: {}".format(node_name) in msg[0]:
                send_vol_ha_status(VOL_STATUS_WARN)
                send_status("HA", 1, 0, "Disable HA", "Disabling HA...", does_jobid_file_need_deletion, block=False)
                ret = ha_unmanage_resources()
                ret = ha_disable_ha_flag()
            else:
                debug("ERROR: Should not disable HA if the resource is not started on this node")
                exit(1)

        send_enable_ha_status(amcurl, uuid, resourceid, "false", "false")

        # set the proper quorum policy
        #ha_set_quorum_policy()
        # do health check
        cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc disable_end'
        (ret, msg) = runcmd(cmd, print_ret=True)

        debug('INFO: complete to disable HA for resource ' + resourceid)
        send_vol_ha_status(VOL_STATUS_UNKNOWN)
        send_status("HA", 100, 0, "Disable HA", "Successfully disabled HA", does_jobid_file_need_deletion, block=False)
        exit(0)
    elif sys.argv[1] == '-rr':
        # TODO: remove volume resources
        delete_vol = True
        node_name = host_name
        resourceid = sys.argv[2]
        debug('INFO: begin to remove volume resources %s and container from cluster ' % resourceid)
        ret = ha_check_deletion('-rr', resourceid)
        if ret != 0:
            exit(ret)
    elif sys.argv[1] == '-rc':
        # remove container ......
        node_name = host_name
        resourceid = ""
        debug('INFO: begin to remove container %s from cluster ' % node_name)
        ret = ha_check_deletion('-rc', None)
        if ret != 0:
            exit(ret)
    else:
        node_name = sys.argv[1]

if host_name == node_name:
    maintenance_mode = ha_check_maintenance_mode()
    if (ha is None or ha is False) and maintenance_mode == False:
        if node_migrate:
            # cannot migrate a non-HA node from a HA cluster
            debug('WARNING: cannot migrate a non-HA node from a HA cluster')
            exit(0)
        debug('WARNING: HA was not eanbled on this volume %s ' % resourceid)
        exit(0)
    else:
        reason = 'delete'
        if disable_ha:
            # disable ha flag
            tmp_fname = '/tmp/new_atlas_conf.json'
            cfgfile = open(tmp_fname, "w")
            node_dict['usx']['ha'] = False
            json.dump(node_dict, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
            cfgfile.close()
            os.rename(tmp_fname, ATLAS_CONF)
            reason = 'disable'
        rc = stop_resources(node_name, resourceid, reason, delete_vol)
    if node_migrate:
        debug('INFO: done with node migration: ' + node_name)
    if disable_ha:
        debug('INFO: finish to disable HA for resource ' + resourceid)
    # No need to send status update to AMC if just remove HA standby node
    if sys.argv[1] != '-rc':
        send_enable_ha_status(amcurl, uuid, resourceid, "false", "false")
    exit(0)

cmd = 'crm_node -l'
(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
if ret != 0:
    exit(ret)

node_id = None
for line in msg:
    tmp = line.split()
    if len(tmp) > 1 and node_name.endswith(tmp[1]):
        node_id = tmp[0]
        break
if node_id is None:
    debug("Failed to find target to delete from cluster")
    exit(0)

# wait for the VM (node_name) to be deleted completely by AMC
sleep(45)

# Remove node_name from cluster
cmd = 'crm node delete ' + node_name
(ret, msg) = runcmd(cmd, print_ret=True)

cmd = 'crm_node -f -R ' + node_id
(ret, msg) = runcmd(cmd, print_ret=True)

cmd = 'cibadmin --delete --xml-text \'<node uname="' + node_name + '"/>\'' 
(ret, msg) = runcmd(cmd, print_ret=True)

cmd = 'cibadmin --delete --xml-text \'<node_state uname="' + node_name + '"/>\'' 
(ret, msg) = runcmd(cmd, print_ret=True)

cmd = 'crm -F node delete ' + node_name
(ret, msg) = runcmd(cmd, print_ret=True)

# set the proper quorum policy
#ha_set_quorum_policy()
cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc disable_end'
(ret, msg) = runcmd(cmd, print_ret=True)

debug('INFO: done with disabling HA')
exit(0)
