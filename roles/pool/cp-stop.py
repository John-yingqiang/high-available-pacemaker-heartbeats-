#!/usr/bin/python

import os, sys
import getopt
import logging

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_nbd import *
from atl_arbitrator import *


#
# deactive/remove all volume groups
#
def pv_dev_list(vg_name):
	#pvs -o pv_name,vg_name| grep testvol
	pvs = []
	pv_list_cmd_str = "pvs -o pv_name,vg_name | grep " + vg_name 
	debug(pv_list_cmd_str)
	pv_stream = os.popen(pv_list_cmd_str, 'r', 1)
	pvlines = pv_stream.read().split('\n')
#	print 'pvlines:', pvlines
	for line in pvlines:
		pv_dev = line.split()
		if len(pv_dev) == 2:
			pvs.append(pv_dev[0])
	debug('pvs:', pvs)
	return pvs

def raid_dev_list(md_list):
	raid1s = []
	for i in md_list:
		if i.startswith("/dev/md") == False:
			debug('Skip non-md device: %s' % i)
			continue
		md_list_cmd_str = "mdadm --detail " + i
		debug(md_list_cmd_str)
		md_stream = os.popen(md_list_cmd_str, 'r', 1)
		detail = md_stream.read().split('State\n')
#		print detail[1]
		for j in detail[1].split('\n'):
			attrline = j.split('/')
			if len(attrline) > 1:
				raid1s.append('/' + '/'.join(attrline[1:]))
	debug('raids:', raid1s)
	return raid1s

def stop_vg(vg_name, destory):
	if (len(vg_name) == 0):
		return 0
	debug("Stopping vg %s" % vg_name)
	deactive_vg_cmd_str = 'vgchange -a n ' + vg_name
	rc = do_system(deactive_vg_cmd_str)
	if rc != 0:
		return rc
	if destroy == 0:
		return 0
	remove_vg_cmd_str = 'vgremove ' + vg_name
	do_system(remove_vg_cmd_str)
	return 0

#
# stop all md
#
def stop_md(md_list, destroy):
	for md_dev in md_list:
		if md_dev.startswith("/dev/md") == False:
			debug('Skip non-md device: %s' % md_dev)
			continue
		#get the main device name from a partition device
		md_dev = part_to_main(md_dev)
		stop_md_cmd_str = "mdadm --stop " + md_dev
		rc = do_system(stop_md_cmd_str)
		if rc != 0:
			return rc
	return 0


def stop_pool(vg_name, destroy):
	# stop nbd-server first
	# FIXME: should not kill all nbd-server, it may hosting other pools service!
	cmd_str = "killall -9 ibds"
	rc = do_system(cmd_str)
	if rc != 0:
		debug("Stop ibd-server failed, no ibd-server?")

	while do_system("ps -ef | grep ibds | grep -v grep") == 0:
		debug("Waiting 0.1s for all ibd-server to quit...")
		time.sleep(0.1)

	md5s = pv_dev_list(vg_name)
	rc = stop_vg(vg_name, destroy)
	if rc !=0:
		debug('Could not stop VG!')
		return rc
	md1s = raid_dev_list(md5s)
	stop_md(md5s, destroy)
	nbds = raid_dev_list(md1s)
	rc = stop_md(md1s, destroy)
	if rc != 0:
		debug('Could not stop md!')
		return rc
	rc = arb_stop(vg_name)
	rc = stop_all_nbd(nbds)
	if rc != 0:
		debug('Could not stop nbd!')
		return rc
	return 0

#Parse command line arguments.
def usage():
	print sys.argv[0] + " [-d] <[-p] pool_name>"

debug("Entering cp-stop:")
try:
        opts, args = getopt.getopt(sys.argv[1:], "hdp:", ["help", "pool="])
except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

destroy = 0
pool_name = None
for opt, arg in opts:
        if opt == "-d":
                destroy = 1
        elif opt in ("-p", "--pool"):
                pool_name = arg

if pool_name is None:
	if args:
		debug("Use remaining args as pool: " + args[0])
		pool_name = args[0]
	else:
		usage()
		sys.exit(2)

debug('destroy:', destroy)
debug('stopping pool:', pool_name)

rc = stop_pool(pool_name, destroy)
if (rc != 0):
	debug("Stop pool %s failed with: %s" % (pool_name, rc))
else:
	debug("Stop pool %s succeed with: %s" % (pool_name, rc))

exit(0)



