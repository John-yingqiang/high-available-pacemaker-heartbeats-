#!/usr/bin/python

import os, sys, json, shutil
from time import sleep

sys.path.insert(0, "/opt/milio/libs/atlas")
from cmd import *

VERSION = '1.0'

HA_STATUS_OUTPUT = 'ha_status'
ILIO_LOG_DIR = 'ilio_log'
ATLAS_CONF = '/etc/ilio/atlas.json'

##################################################################
#  WARNING: This script is never tested for multi-thread safety  #
#  We may have problems when two nodes running together this     #
#  setup script and get one's configure overwrite the others     #
#  TODO: One solution is to move the setup as the step after all #
#        node in cluster are deployed, and only configure it     #
#        on one machine. The other solution is create a temp     #
#        resource to as a lock (with time-out) that a            #
#        re-configure is in-progress.                            #
##################################################################


##################################################################
#                   START HERE                                   #
##################################################################

import os
import zipfile

def zipdir(path, zip):
	for root, dirs, files in os.walk(path):
		for file in files:
			zip.write(os.path.join(root, file))

try:
	output_dir = ILIO_LOG_DIR
	if not os.path.exists(output_dir):
		os.makedirs(output_dir)
	else:
		shutil.rmtree(output_dir)
		os.makedirs(output_dir)
	filename = output_dir + '/' + HA_STATUS_OUTPUT
	with open(ATLAS_CONF, 'r') as cfgfile:
		s = cfgfile.read()
		node_dict = json.loads(s)
	with open(filename, 'w') as fd:
		fd.write('\t\t\t\t\tATLAS JSON\n')
		fd.write('\t\t\t\t\t-------------------------------------------\n')
		json.dump(node_dict, fd, sort_keys=True, indent=4, separators=(',', ': '))
		fd.write('\n')
		fd.write('\n')

		fd.write('\t\t\t\t\tCOROSYNC CONFIGURATION\n')
		fd.write('\t\t\t\t\t-------------------------------------------\n')
		cmd = 'cat /etc/corosync/corosync.conf'
    		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			fd.write(line+'\n')
		fd.write('\n')

		fd.write('\t\t\t\t\tCOROSYNC QUORUM STATUS\n')
		fd.write('\t\t\t\t\t-------------------------------------------\n')
		cmd = 'corosync-quorumtool -pi'
    		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			fd.write(line+'\n')
		fd.write('\n')

		fd.write('\t\t\t\t\tCOROSYNC CONFIG STATUS\n')
		fd.write('\t\t\t\t\t-------------------------------------------\n')
		cmd = 'corosync-cfgtool -s'
    		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			fd.write(line+'\n')
		fd.write('\n')

		fd.write('\t\t\t\t\tCRM STATUS\n')
		fd.write('\t\t\t\t\t-------------------------------------------\n')
		cmd = 'crm status'
    		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			fd.write(line+'\n')
		fd.write('\n')

		fd.write('\t\t\t\t\tCRM NODE LIST\n')
		fd.write('\t\t\t\t\t-------------------------------------------\n')
		cmd = 'crm node list'
    		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		for line in msg:
			fd.write(line+'\n')
		fd.write('\n')

	# collect logs in /var/log
	log_list = ['ads', 'syslog', 'corosync', 'atlas']
	for item in log_list:
		cmd = 'ls /var/log/' + item + '*'
    		(ret, msg) = runcmd(cmd, print_ret=True)
		for f in msg.split():
			if os.path.isfile(f):
				shutil.copy(f, output_dir)

	zipf = zipfile.ZipFile(ILIO_LOG_DIR + '.zip', 'w')
	zipdir(output_dir, zipf)
	zipf.close()
	shutil.rmtree(output_dir)

except ValueError, e:
	exit(1)
exit(0)
