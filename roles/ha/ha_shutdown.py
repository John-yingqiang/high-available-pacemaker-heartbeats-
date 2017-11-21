#!/usr/bin/python
import os, sys
import logging
import tempfile
from subprocess import *
import httplib
import json
import socket
import signal
import fcntl
import mmap
import time
import ctypes
import ctypes.util
from time import sleep

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from ha_util import *
from atl_arbitrator import *

sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
from ha_util import send_volume_alert,reset_vm

sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *

libc = ctypes.CDLL(ctypes.util.find_library('c'))

ATL_ARBITRATOR_PID_FILE = "/var/run/atl_arbitrator.pid"
ATL_ARBITRATOR_DEV_DIR = "/var/run/atl_arbitrator/"

# Must be 512 bytes aligned for Direct IO
# This offset is trying to land in pool nbd private region at
# first 1MB free space, skip the maximum 17k(34 sectors) GPT.
# FIXME: We should use a separate partition as the nbd private region on Pool node.
POISON_OFFSET = 1024 * 30
# Must be 512 bytes aligned for Direct IO
POISON_LENGTH = 512

ARB_ACK_CHECK_INTERVAL = 1 
POISON_PILL = 'letmedie'
POISON_ACK = 'iwilldie'
HEALTH_PILL = 'keepwork'
SHUTDOWN_PREP_PILL = 'shutprep'
SHUTDOWN_PILL = 'shutdown'
TAKEOVER_PILL = 'takeover'
PILL_LEN = 8

CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_STAT_WD = CMD_IBDMANAGER + " -r a -s get_wd"
CMD_IBDMANAGER_STAT_WU = CMD_IBDMANAGER + " -r a -s get_wu"
CMD_IBDMANAGER_STAT_WUD = CMD_IBDMANAGER + " -r a -s get_wud"

LOG_FILENAME = '/var/log/usx-atlas-ha.log'
set_log_file(LOG_FILENAME)

ATLAS_CONF = '/etc/ilio/atlas.json'
SHUTDOWEN_FILE = '/tmp/ha_shutdown'

UPGRADING_PATCH_FLAG = '/etc/ilio/patch_upgrading'
UPGRADED_PATCH_FLAG = '/etc/ilio/patch_upgraded'


def run_e2fsck():
	debug('Enter run_e2fsck.')
	rc = 0
	try:
		mnttab_file = '/etc/ilio/mnttab'
		cmd = 'cat %s' % mnttab_file
		(ret, msg) = runcmd(cmd, print_ret=True)
		dedupfs_lv = msg.split('}}##0##{{')[0]
		e2fsck_cmd = '/opt/milio/bin/e2fsck -f -y -Z 0 %s' % dedupfs_lv
		(sub_rc, sub_msg) = runcmd(e2fsck_cmd, print_ret=True, lines=True)
		if 'Filesystem still has errors' in " ".join(sub_msg):
			debug('e2fsck can\'t fix the dedupfs.')
			rc = 1
	except Exception as err:
		debug('run_e2fsck running exception:%s' % err)
		rc = 1

	if rc != 0:
		send_volume_alert('ERROR', 'Failed to run e2fsck on first shutdown/reboot after upgrading,'
				+ ' check /var/log/usx-atlas-ha.log for details.')
	return rc


def ctypes_alloc_aligned(size, alignment):
	buf_size = size + (alignment - 1)

	raw_memory = bytearray(buf_size)

	ctypes_raw_type = (ctypes.c_char * buf_size)
	ctypes_raw_memory = ctypes_raw_type.from_buffer(raw_memory)
	raw_address = ctypes.addressof(ctypes_raw_memory)
	offset = raw_address % alignment

	offset_to_aligned = (alignment - offset) % alignment
	ctypes_aligned_type = (ctypes.c_char * (buf_size - offset_to_aligned))

	ctypes_aligned_memory = ctypes_aligned_type.from_buffer(raw_memory, offset_to_aligned)
	return ctypes_aligned_memory


#
# Write out the pill
#
def write_pill_one(arb_dev, buf, pill):
	debug('Enter write_pill_one %s for device %s.' %(pill, arb_dev))

	libc.memcpy(buf, ctypes.c_char_p(pill), ctypes.c_int(len(pill)))
	debug('Prepare the pill: ', buf.raw[0:len(pill)])
	fd = os.open(arb_dev, os.O_RDWR|os.O_DIRECT)
	os.lseek(fd, POISON_OFFSET, os.SEEK_SET)
	err_code = libc.write(ctypes.c_int(fd), buf, ctypes.c_int(POISON_LENGTH))
	os.close(fd)
	if err_code == -1:
		debug('ERROR: Can not write the pill %s to the dev: %s.' % (pill, arb_dev))
		return False
	print err_code, ctypes.get_errno()
	debug('Written out the pill %s.' % pill)

	return True


def write_pill(arb_dev_list, pill):
	debug('Enter write_pill %s for devices: %s.' %(pill, str(arb_dev_list))) 
	pill_sent = False
	buf =ctypes_alloc_aligned(1024, 4096)

	for arb_dev in arb_dev_list:
		try:
			rc = write_pill_one(arb_dev, buf, pill)
		except:
			debug(traceback.format_exc())
			rc = False
		if rc == True:
			pill_sent = True

	return pill_sent


def read_pill_one(arb_dev, buf):
	debug('Enter read_pill_one of arb device %s.' % arb_dev)
	
	rc = 0
	#We are using direct IO for poison file because a remote node may change it's content.
	fd = os.open(arb_dev, os.O_RDWR|os.O_DIRECT)
	# Offset 1025 will not work, notice the alignment restriction.
	os.lseek(fd, POISON_OFFSET, os.SEEK_SET)
	err_code = libc.read(ctypes.c_int(fd), buf, ctypes.c_int(POISON_LENGTH))

	if err_code == -1:
		debug('Can not read poison file, error :%d. Skip feed watchdog.' % os.errno)
		# TODO: Need flush log
		return 1

	#data = buf.raw[0:err_code]
	poison = buf.raw[0:len(POISON_PILL)]
	debug("read pill: %s" % poison)
	if poison == POISON_PILL or poison == TAKEOVER_PILL:
		debug('Got a secret pill: %s ' % poison)
	else:
		rc = 1

	#We are fine.
	os.close(fd)

	return rc


def read_pill(arb_dev_list):
	debug('Enter read_pill: %s' % str(arb_dev_list))
	#Allocate 1k length buffer, 4k alignment
	buf =ctypes_alloc_aligned(1024, 4096)

	read_succeed = 1
	for arb_dev in arb_dev_list:
		try:
			rc = read_pill_one(arb_dev, buf)
		except:
			debug('Failed to read pill: arb_dev %s.' % arb_dev)
			#debug(traceback.format_exc())
			rc = 1
		if rc == 0:
			# Peek success if we can reach even just one arb device.
			# TODO: any better quorum like algorithm?
			read_succeed = 0;
			break
	return read_succeed

def get_working_ibds():
	cmd = CMD_IBDMANAGER_STAT_WD
	(ret, msg) = runcmd(cmd, print_ret=True)
	ibd_dev_list = []
	if ret == 0:
		ibd_dev_list = msg.split()	
	return ibd_dev_list

def retry_cmd(cmd, retry_num, timeout):
	debug('Enter retry_cmd: %s %d %d' % (cmd, retry_num, timeout))
	cnt = retry_num
	ret = 0
	msg = ""
	while cnt > 0:
		(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
		if ret == 0:
			break
		time.sleep(timeout)
		cnt -= 1
	return (ret, msg)

def umount_dedupFS():
	debug('Enter umount_dedupFS')
	rc = 0
	# umount snapshots 
	cmd = 'python /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc unmountall'
	(ret, msg) = retry_cmd(cmd, 3, 2)
	if ret != 0:
		rc = 1
	
	# umount dedupfs
	cmd = 'mount | grep "type dedup" | grep -v grep '
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for the_line in msg:
			the_list = the_line.split(' ')
			the_dev = the_list[0]
			# umount dedupFS
			sub_cmd = 'umount ' + the_dev
			(sub_ret, sub_msg) = retry_cmd(sub_cmd, 3, 2)
			if sub_ret != 0:
				rc = 1
			# get top md device
			sub_cmd = 'pvs | grep dedupvg | grep -v grep'
			(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
			if sub_ret == 0:
				sub_list = sub_msg[0].split()
				the_dev = sub_list[0]
			
			# stop dedupvg
			sub_cmd = 'vgchange -an dedupvg'
			(sub_ret, sub_msg) = retry_cmd(sub_cmd, 2, 1)
			if sub_ret != 0:
				rc = 1
			
			# stop top md device 
			sub_cmd = 'mdadm --stop ' + the_dev
			(sub_ret, sub_msg) = retry_cmd(sub_cmd, 3, 2)
			if sub_ret != 0:
				debug('ERROR: failed to stop %s' % the_dev)
				rc = 1

	return rc

def umount_fs_retry(sub_cmd, the_dev):
	umount_success = False
	retry = 0
	max_num_retry = 10
	while (retry < max_num_retry):
		(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
		if sub_ret == 0:
			umount_success = True	
			break
		else:
			retry = retry + 1
			debug('Umount fs failed with %d.' % retry)
			cmd_run = '/usr/bin/lsof | grep "%s"' % the_dev
			runcmd(cmd_run, print_ret=True, lines=True)
			sleep(5)

	if umount_success == False:
		debug('ERROR: failed to umount %s ' % the_dev)
		# TODO: only do core dump in debug mode
		reset_vm('umount_dedup_reset')
		return False

	return True


def umount_dedupFS_retry():
	debug('Enter umount_dedupFS_retry')
	rc = 0

	# umount snapshots 
	cmd = 'python /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc unmountall'
	(ret, msg) = retry_cmd(cmd, 3, 2)
	if ret != 0:
		rc = 1

	# umount dedupfs
	cmd = 'mount | egrep "type dedup|type zfs|type ext4|type btrfs" | egrep -v "grep|usxbase|sda"'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0 and len(msg) > 0:
		mount_dev_dict = {}
		for the_line in msg:
			the_list = the_line.split()
			the_dev = the_list[2]
			the_type = the_list[-2]
			mount_dev_dict[the_dev] = the_type
			
		if milio_config.is_fastfailover:
			for dev, type in mount_dev_dict.items():
				sub_cmd = 'umount %s' % dev
				if not umount_fs_retry(sub_cmd, dev):
					rc = 1
					return

			# Run e2fsck.
			if os.path.exists(UPGRADING_PATCH_FLAG):
				debug('run e2fsck on first shutdown/reboot after upgrading.')
				run_e2fsck()
				os.rename(UPGRADING_PATCH_FLAG, UPGRADED_PATCH_FLAG)

			# Flush write cache
			flush_write_cache()

			# stop the ibdserver
			cmd_stop_ibd = 'ibdmanager -r s -S'
			runcmd(cmd_stop_ibd, print_ret=True, lines=True)
			runcmd('ibdmanager -r s -s get', print_ret=True, lines=True)

			cmd_force_stop_ibd = 'killall -9 ibdserver'
			runcmd(cmd_force_stop_ibd, print_ret=True, lines=True)

			# stop lvm
			sub_cmd = 'pvs 2>/dev/null | grep ibd | grep -v grep'
			(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
			if sub_ret != 0:
				return sub_ret
			for msg in sub_msg:
				sub_list = msg.split()
				the_dev = sub_list[0]
				the_vg = sub_list[1]
				# stop lv
				sub_cmd = 'vgchange -an %s' % the_vg
				(sub_ret, sub_msg) = retry_cmd(sub_cmd, 2, 1)
				if sub_ret != 0:
					rc = 1
				
				# stop top md device 
				sub_cmd = 'mdadm --stop ' + the_dev
				(sub_ret, sub_msg) = retry_cmd(sub_cmd, 3, 2)
				if sub_ret != 0:
					debug('ERROR: failed to stop %s' % the_dev)
					rc = 1
		else:
			for dev, type in mount_dev_dict.items():
				debug('umount %s type %s' % (dev, type))
				if type == 'zfs':
					sub_cmd = '/usr/local/sbin/zpool export usx-zpool'
				else:
					sub_cmd = 'umount %s' % dev

				if umount_fs_retry(sub_cmd, dev) and type != 'zfs':
					# For Diskless VDI volume, sync in memory contents to disk. Check if
					#  sync log is exists to determin whether needs to run simple memory
					#  sync script
					#if os.path.exists('/var/log/simplememory_sync.log'):
					if is_simplified_memory_volume():
						debug('INFO: Simplified Memory Volume, run sync script')
						sub_cmd = 'python /opt/milio/scripts/usx_simplememory_sync.pyc start_backup'
						(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)

					# Run e2fsck.
					if os.path.exists(UPGRADING_PATCH_FLAG):
						debug('run e2fsck on first shutdown/reboot after upgrading.')
						run_e2fsck()
						if milio_config.volume_type != "SIMPLE_HYBRID":
							os.rename(UPGRADING_PATCH_FLAG, UPGRADED_PATCH_FLAG)

					
					# get top md device
					sub_cmd = 'pvs 2>/dev/null | grep dedupvg | grep -v grep'
					(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
					if sub_ret == 0:
						sub_list = sub_msg[0].split()
						the_dev = sub_list[0]

					# stop dedupvg
					# For USX 3.5.0, we moved deduplv to ibdserver target device.
					#  We shouldn't deactivate vg here.
					if not (milio_config.volume_type == "SIMPLE_HYBRID" and is_new_simple_hybrid()):
						sub_cmd = 'vgchange -an dedupvg'
						(sub_ret, sub_msg) = retry_cmd(sub_cmd, 2, 1)
						if sub_ret != 0:
							rc = 1
					
					# stop top md device 
					sub_cmd = 'mdadm --stop ' + the_dev
					(sub_ret, sub_msg) = retry_cmd(sub_cmd, 3, 2)
					if sub_ret != 0:
						debug('ERROR: failed to stop %s' % the_dev)
						rc = 1
	return rc

def stop_loopdev():
	debug('Entering stop loop devices')
	cmd = 'losetup -a'
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for line in msg:
			the_list = line.split(':')
			the_dev = the_list[0]
			sub_cmd = 'losetup -d %s' % the_dev
			(ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
	return ret


def stop_vscaler():
	debug('Enter stop_vscaler')
	rc = 0
	cmd = 'dmsetup table | grep "vscaler" | grep -v grep' 
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for the_line in msg:
			the_list = the_line.split(':')
			the_dev = the_list[0]
			sub_cmd = 'dmsetup remove ' + the_dev
			(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
			if sub_ret != 0:
				rc = 1

	return rc

def stop_volume_vscaler():
	debug('Enter stop_volume_vscaler')
	rc = 0
	cmd = 'dmsetup table | grep "vscaler" | grep -v grep' 
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	if ret == 0:
		for the_line in msg:
			the_list = the_line.split(':')
			the_dev = the_list[0]
			sub_cmd = 'dmsetup remove ' + the_dev
			#(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
			(sub_ret, sub_msg) = retry_cmd(sub_cmd, 3, 5)
			# add a trigger file for QA testing
			if sub_ret != 0 or os.path.exists('/tmp/QATRIGGER5678'):
				(ret, msg) = runcmd('touch /var/log/vscaler_reset', print_ret=False)
				send_volume_alert('ERROR', 'Failed to stop vscaler')
				time.sleep(5);
				reset_vm('stop_volume_reset')
	return rc


def is_simplified_memory_volume():
	debug("Enter is_simplified_memory_volume")
	cfgfile = open(ATLAS_CONF, 'r')
	s = cfgfile.read()
	cfgfile.close()
	is_simple_memory = False
	try:
		fulljson = json.loads(s)
		if fulljson.has_key('volumeresources'):
			if fulljson['volumeresources']:
				volume_type = fulljson['volumeresources'][0]['volumetype']
				if volume_type.upper() == 'SIMPLE_MEMORY':
					is_simple_memory = True
		
		if is_simple_memory:
			debug('This is a simplified Memory Volume')
		else:
			debug('This is NOT a simplified Memory Volume')
		
		return is_simple_memory

	except ValueError as err:
		debug('ERROR: wrong atlas.json')
		return False

def flush_big_buffer():
	debug( "Entering flush_big_buffer()" )
	print "Entering flush_big_buffer()" 
	(volume_type, volume_uuid, ilio_uuid, display_name) = get_volume_info()
	if volume_type == None:
		debug('ERROR: volume type is none')
		sys.exit( 2 )
	elif volume_type.upper() in ['SIMPLE_HYBRID' ]:
		debug('INFO: SIMPLE_HYBRID volume - need to flush big buffer' )
	else:
		debug('INFO: volume type %s which is not SIMPLE_HYBRID - no need to flush big buffer')
		sys.exit(0) 

	if not os.path.exists( '/etc/ilio/big_buffer' ):
		debug('INFO: This volume is not configured for big buffer - no need to flush' )
		sys.exit( 0 )
	
	# check to see if ibdserver is running
	(rc, msg) = runcmd("ibdmanager -r s -s get", print_ret=True, lines=True)
	ibdserver_running = False
	if rc != 0:
		debug('WARN: ibdserver is not running.')
		sys.exit( 2 )

	# now proceed with flushing the big buffer
	flush_write_cache()

def usage():
	print "Usage:" + sys.argv[0] + " shutdown_prep|shutdown_done|umount_dedupFS|stop_vscaler"


#########################################################
#                    START HERE                         #
#########################################################
num_input = 2
if len(sys.argv) < num_input:
	usage()
	sys.exit(1)

if sys.argv[1] == 'shutdown_prep':
	rc = ha_check_enabled()
	if rc == True:
		# Get ibd_dev_list
		(ibd_dev_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
		# write the SHUTDOWN_PREP_PILL
		arb_write_pill(ibd_dev_list, SHUTDOWN_PREP_PILL)

		# stop corosync
		cmd = 'service corosync stop '
		(ret, msg) = runcmd(cmd, print_ret=True)

		# Enable dirty umount
		cmd = 'cmd="echo 1 > /proc/dedup/*/dirty_umount"; /bin/bash -c "$cmd"'
		(ret, msg) = runcmd(cmd, print_ret=True)

	sys.exit(0)

if sys.argv[1] == 'umount_dedupFS':
	rc = umount_dedupFS_retry()
	sys.exit(0)

if sys.argv[1] == 'stop_vscaler':
	rc = stop_volume_vscaler()
	sys.exit(0)

if sys.argv[1] == 'flush_big_buffer':
	rc = flush_big_buffer()
	sys.exit( 0 )

if sys.argv[1] == 'test':
	send_volume_alert('ERROR', 'Failed to stop vscaler')
	sys.exit(0)

cfgfile = open(ATLAS_CONF, 'r')
s = cfgfile.read()
cfgfile.close()
try:
	node_dict = json.loads(s)
	ilio_dict = node_dict.get('usx')
	if ilio_dict is None:
		ilio_dict = node_dict
	ha = ilio_dict.get('ha')
	node_name = ''
	if not ha:
		if ilio_dict.has_key('uuid'):
			node_name = ilio_dict.get('uuid')
		
		if ilio_dict.has_key('roles'):
			roles = ilio_dict.get('roles')
			role = roles[0]
			if len(role) > 0 and role.upper() == "SERVICE_VM":
				debug('stop vscaler for service vm: %s' % node_name)
				stop_vscaler()
		sys.exit(0)
	node_name = ilio_dict.get('uuid')
except ValueError as err:
	sys.exit(11)

# make sure a cron job cannot restart corosync
cmd = 'rm -f /run/pacemaker_started '
(ret, msg) = runcmd(cmd, print_ret=True)

# Get ibd_dev_list
#ibd_dev_list = get_working_ibds()
# write the SHUTDOWN_PREP_PILL
#write_pill(ibd_dev_list, SHUTDOWN_PREP_PILL)

# stop corosync 
cmd = 'service corosync stop '
(ret, msg) = runcmd(cmd, print_ret=True)

# stop nfs 
cmd = 'service nfs-kernel-server stop '
(ret, msg) = runcmd(cmd, print_ret=True)

# stop scst 
cmd = '/etc/init.d/scst stop '
(ret, msg) = runcmd(cmd, print_ret=True)

# umount dedupfs
umount_dedupFS()

# read the pill
retry = 0
max_num_retry = 60
found_flag = False
while (retry < max_num_retry):
	(ibd_dev_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
	rc = 0 if (arb_read_pill(ibd_dev_list, POISON_PILL) or arb_read_pill(ibd_dev_list, TAKEOVER_PILL)) else 1
	debug("read_pill %d times, and return %d." % (retry + 1, rc))
	if rc == 0:
		found_flag = True
		break
	else:
		retry = retry + 1
		sleep(1)

# write POISON_ACK 
#write_pill(ibd_dev_list, POISON_ACK)

if found_flag == True:
	# fast way to stop vscaler: no sync
	cmd = 'python /opt/milio/atlas/roles/aggregate/agexport.pyc -S '
	(ret, msg) = runcmd(cmd, print_ret=True)

	(ibd_dev_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
	# write the SHUTDOWN_PILL
	arb_write_pill(ibd_dev_list, SHUTDOWN_PILL)
	
	cmd = '/usr/local/bin/ibdmanager -r a -S '
	do_system_timeout(cmd, 120)

	# sleep up to 1 minute
	retry = 0
	max_num_retry = 60
	while (retry < max_num_retry):
		ibd_dev_list = get_working_ibds()
		if len(ibd_dev_list) == 0:
			debug("ibd_dev_list is empty.")
			break
		else:
			retry = retry + 1
			sleep(1)
else:
	(ibd_dev_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
	# clean the pill
	arb_write_pill(ibd_dev_list, HEALTH_PILL)

sys.exit(0)
