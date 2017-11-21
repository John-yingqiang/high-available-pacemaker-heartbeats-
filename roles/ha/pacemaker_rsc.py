#!/usr/bin/python

import os, sys
import logging
import tempfile
from subprocess import *
import httplib
import json
import socket
from time import sleep

sys.path.insert(0, "/opt/milio/libs/atlas")
from status_update import does_jobid_file_exist
from status_update import send_status

sys.path.insert(0, "/opt/milio/atlas/roles/ha")
from ha_util import *
LOG_FILENAME = '/var/log/usx-atlas-ha.log'
set_log_file(LOG_FILENAME)

ATLAS_CONF = '/etc/ilio/atlas.json'
PACEMAKER_RSC_LIST = '/tmp/pacemaker_rsc.list'

def resource_is_started(res, res_group):
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
		if cnt % 10 == 0:
			(ret, msg) = runcmd('crm resource cleanup ' + res, print_ret=True)
			(ret, msg) = runcmd('crm resource start ' + res, print_ret=True)
	return False

def resource_is_stopped(res, res_group):
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
		if cnt % 10 == 0:
			(ret, msg) = runcmd('crm resource cleanup ' + res, print_ret=True)
			(ret, msg) = runcmd('crm resource stop ' + res, print_ret=True)
	return False

def stop_resources(node_name, true_node_name):
	# stop resources that this node_name is running
	cmd = 'crm status'
	(ret, msg) = runcmd(cmd, print_ret=True,lines=True)
	if ret != 0:
		return

	online_nodes = 0
	for line in msg:
		if line.find("Online") >= 0:
			tmp = line.replace('[', ' ').replace(']', ' ').split()
			online_nodes = len(tmp) - 1
			break
	if online_nodes < 2:
		return

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
			if started_by == true_node_name and res != "iliomon":
				res_list.append(res)
				res_group_found = True

	with open(PACEMAKER_RSC_LIST, "w") as fd:
		if len(res_list) > 0:
			fd.write("%s\n" % res_group)
			for res in reversed(res_list):
				fd.write("%s\n" % res)
				cmd = 'crm resource stop ' + res
				(ret, msg) = runcmd(cmd, print_ret=True)
				if not resource_is_stopped(res, res_group):
					debug("Could not stop resource " + res + " by " + true_node_name)

	if len(res_list) > 0:
		cmd = 'crm node standby ' + true_node_name
		(ret, msg) = runcmd(cmd, print_ret=True)

def start_resources(true_node_name):
	res_group = None
	res_list = []
	if not os.path.isfile(PACEMAKER_RSC_LIST):
		return
	with open(PACEMAKER_RSC_LIST, "r") as fd:
		content = fd.readlines()
		for line in content:
			if line.find('group') >= 0:
				res_group = line
			else:
				res_list.append(line)

	online_nodes = 0
	cmd = 'crm status'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for line in msg:
			if line.find("Online") >= 0:
				tmp = line.replace('[', ' ').replace(']', ' ').split()
				online_nodes = len(tmp) - 1
				break
	if online_nodes > 0 and res_group is not None and len(res_group) > 0 and len(res_list) > 0:
		for res in reversed(res_list):
			res = res.rstrip()
			cmd = 'crm resource start ' + res
			(ret, msg) = runcmd(cmd, print_ret=True)
			if not resource_is_started(res, res_group):
				debug("Could not start resource " + res + " by " + true_node_name)

	if len(res_list) > 0:
		cmd = 'crm node online ' + true_node_name
		(ret, msg) = runcmd(cmd, print_ret=True)

#########################################################
#		START HERE 				#
#########################################################

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
		sys.exit(0)
	node_name = ilio_dict.get('uuid')
except ValueError as err:
	sys.exit(11)

cmd = 'crm_node -l'
(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
if ret != 0:
	sys.exit(1)
true_node_name = None
for line in msg:
	tmp = line.split()
	if len(tmp) > 1 and node_name.endswith(tmp[1]):
		true_node_name = tmp[1]
		break
if true_node_name is None:
	debug("Failed to find true node name from cluster")
	sys.exit(1)

if sys.argv[1] == 'stop':
	stop_resources(node_name, true_node_name)
elif sys.argv[1] == 'start':
	start_resources(true_node_name)

sys.exit(0)
