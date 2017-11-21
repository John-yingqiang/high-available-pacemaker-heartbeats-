#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, sys
import logging
import tempfile
from subprocess import *
import httplib
import json
import socket
import re
import ConfigParser

from time import sleep
sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_storage import *
from atl_ibd import ibd_add_exports, ibd_del_exports
from cmd import *


LOG_FILENAME = '/var/log/usx-manage_agg_res.log'
logging.basicConfig(filename=LOG_FILENAME,
					level=logging.DEBUG,
					format='%(asctime)s %(message)s')

IBDS_CONF = '/etc/ilio/ibdserver.conf'
IBDS_CONF_TMP = '/tmp/agg_ibds.conf'
FSTAB = '/etc/fstab'
FSTAB_BAK = '/etc/fstab.bak'
FSTAB_TMP = '/tmp/agg_fstab'
MEM_DIR = '/mnt/memory/'
DISK_DIR = '/mnt/disk/'


#
# Return filesystem free space in K bytes (1024).
#
def fs_free_space(mount_dir):
	msg = subprocess.check_output('df -k %s' % mount_dir, shell = True)
	debug(msg)
	line = msg.split('\n')[1]
	items = re.split('\s+', line)
	size = items[3]
	return long(size)

def setup_fs(devname, filename):
	mount_dir = os.path.dirname(filename)
	if os.system('yes | mkfs -t ext4 -T largefile4 -m 1 ' + devname) != 0:
		debug('mkfs failed.')
		return 1
	if os.system('mkdir -p ' + mount_dir) != 0:
		debug('mkdir failed.')
		return 1
	if os.system('mount -t ext4 -o rw,noatime,data=writeback %s %s' % (devname, mount_dir)) != 0:
		debug('mount failed.')
		return 1
	size = fs_free_space(mount_dir)
	if os.system('echo "%s %s ext4 rw,noatime,data=writeback 0 0" >>/etc/fstab' % (devname, mount_dir)) != 0:
		debug('update /etc/fstab failed.')
		return 1

	"""
	mkfs -t ext4 /dev/sdb1
	mkdir -p /mnt/disk
	mount /dev/sdb1 /mnt/disk
	#For example, your /dev/sdb1 is 50G, this is 50-1=49G (49*1024*1024*1024) in bytes.
	echo "/dev/sdb1 /mnt/disk ext4 rw,noatime 0 0" >>/etc/fstab
	"""
	return 0

def delete_fs(mountpoint):
	rc = 1
	fstab = open(FSTAB, "r")
	mnts = fstab.readlines()
	fstab.close()

	fstab_tmp = open(FSTAB_TMP, "w")

	for mntentry in mnts:
		if re.search(' ' + re.escape(mountpoint) + ' ', mntentry):
	                debug("Found and Deleting: " + mntentry)
			rc = 0
	                continue
	        fstab_tmp.write(mntentry)

	fstab_tmp.close()

	if rc == 0:
		os.rename(FSTAB, FSTAB_BAK)
		os.rename(FSTAB_TMP, FSTAB)

	return rc


def manage_exports(ibd_dict, action):
	scsi_hotscan()

	if action == 'add':
		sections = [] 

		for export,scsi in ibd_dict.items():
			device_name = scsi_to_device(scsi)
			if device_name == None:
				debug('Cannot find device name for %s:%s' % (export,scsi))
				sys.exit(104)
			mount_file = DISK_DIR + export + '/bigfile'
			if setup_fs(device_name, mount_file) != 0:
				debug('Setup fs failed for ' + device_name + " --> " + mount_file)
				sys.exit(105)
			debug('Setup fs for ' + device_name + ' --> ' + mount_file)
			sections.append({'uuid':export, 'export':mount_file})

		ibd_add_exports(sections)

	elif action == 'delete':
		sections = [] 

		for export,scsi in ibd_dict.items():
			mount_point = DISK_DIR + export
			cmd = 'umount ' + mount_point
			(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
			if ret != 0:
				debug('Could not unmount ' + mount_point)
			if delete_fs(mount_point) != 0:
				debug('Delete fs failed for ' + export)
				sys.exit(106)
			sections.append({'uuid':export})

		ibd_del_exports(sections)

	else:
		return 0

	# update IBD server with new configuration. Do not execute this program in a subprocess by using runcmd().
	ret = os.system('ibdmanager -r s -u')
	if ret != 0:
		debug('Could not update IBD server, rc = ' + str(ret))
		return ret

	return 0


#########################################################
#		START HERE 				#
#########################################################

if len(sys.argv) < 3:
	debug('Usage: manage_res.pyc add/delete AggregatorVO')
	exit(1)

try:
	data = json.loads(sys.argv[2])
	debug('manage_res JSON: ', json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))
	management_id = data['ilioManagementid']
	uuid = data['uuid']
	ibdlist = data['ibdlist']
	if management_id is None or uuid is None or ibdlist is None:
		debug('JSON data error')
		exit(1)
	exit(manage_exports(ibdlist, sys.argv[1]))
except ValueError, e:
	debug('JSON parse exception : ' + str(e))
	exit(1)
exit(0)
