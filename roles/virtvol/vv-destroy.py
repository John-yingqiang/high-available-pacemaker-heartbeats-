#!/usr/bin/python
import os, sys
import subprocess
import json
from time import sleep

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_md import *
from cmd import *

CMD_IBDMANAGER			= "/bin/ibdmanager"
CMD_IBDMANAGER_STAT_RWWUD	= CMD_IBDMANAGER + " -r a -s get_rwwud"
ATLAS_CONF			= '/etc/ilio/atlas.json'

EXCEPT_EXIT			= 1
INVALID_INPUT_ARG 		= 2
INVALID_UUID	 		= 3
INVALID_TAG	 		= 4
CANNOT_OPEN_FILE 		= 5

def usage():
	debug('Wrong command: %s' % str(sys.argv))
	print('Usage: python /opt/milio/atlas/roles/virtvol/vv-destroy.pyc vol_res_uuid tag')

def run_mdadm(cmd):
	(ret, msg) = runcmd(cmd, print_ret=True)
	if 0 != ret:
		sleep(1)
		(ret, msg) = runcmd(cmd, print_ret=True)
	return (ret, msg)
    
def get_config_from_jsonfile(cfg_file):
	try:
		cfg_file = open(cfg_file, 'r')
		cfg_str = cfg_file.read()
		cfg_file.close()
		cfg = json.loads(cfg_str)
		return cfg
	except:
		return None

def clean_superblock(parent_dev, dev):
	cmd_str = "/sbin/mdadm --manage %s --fail %s" % (parent_dev, dev + "p1")
	run_mdadm(cmd_str)
	cmd_str = "/sbin/mdadm --manage %s --remove %s" % (parent_dev, dev + "p1")
	run_mdadm(cmd_str)
	cmd_str = "/sbin/mdadm --zero-superblock %s" % dev + "p1"
	run_mdadm(cmd_str)
	cmd_str = "/sbin/mdadm --stop %s" % dev + "p1"
	run_mdadm(cmd_str)

def main():
	debug("%s" % sys.argv)

	if len(sys.argv) != 3:
		if len(sys.argv) == 2 and sys.argv[1] == "poweroff":
			sleep(10)
			cmd_str = "/sbin/poweroff -f"
			(ret, msg) = runcmd(cmd_str, print_ret=True)
			return 0
		
		usage()
		return INVALID_INPUT_ARG

	vol_configure = get_config_from_jsonfile(ATLAS_CONF)
	if vol_configure == None:
		debug("vv-destroy failed to load file: %s" % ATLAS_CONF)
		return CANNOT_OPEN_FILE
	
	vol_uuid = sys.argv[1]
	vol_resource = vol_configure["volumeresources"][0]
	cfg_uuid = vol_resource["uuid"]
	if cfg_uuid != vol_uuid:
		debug("%s does not matched the config volume: %s" % (vol_uuid, cfg_uuid))
		return INVALID_UUID

	vol_tag = sys.argv[2]
	taguuids = vol_configure["usx"]["taguuids"]
	found = False
	for cfg_tag in taguuids:
		if cfg_tag == vol_tag:
			found = True
	if found == False:
		debug("%s does not matched the config tag: %s" % (vol_tag, cfg_tag))
		return INVALID_TAG


	cmd_str = CMD_IBDMANAGER_STAT_RWWUD
	out = ['']
	do_system(cmd_str, out)
	rwwud = out[0].split('\n')
	working_ibd = []
	for l in rwwud:
		ls = l.split(' ')
		if len(ls) < 2:
			continue
		devname = ls[1]
		working_ibd.append(devname)
    
	cpool_file = '/etc/ilio/c_pool_infrastructure_' + vol_uuid + '.json'
	c_infrastructure = get_config_from_jsonfile(cpool_file)
	if vol_configure == None:
		debug("vv-destroy failed to load file: %s" % cpool_file)
		return CANNOT_OPEN_FILE

	for storage_type in c_infrastructure:				# disk, memory
		for level_1 in c_infrastructure[storage_type]:		# raid5 level
			raid5_dev = level_1["devname"]
			for level_2 in level_1["children"]:		# raid1 level
				raid1_dev = level_2["devname"]
				clean_superblock(raid5_dev, raid1_dev)
				for level_3 in level_2["children"]:	# ibd level
					ibd_dev = level_3["devname"]
					if ibd_dev in working_ibd:
						cmd_str = "/sbin/mdadm --zero-superblock %s" % ibd_dev + "p1"
						run_mdadm(cmd_str)

        cmd_str = "python /opt/milio/atlas/roles/virtvol/vv-destroy.pyc poweroff"
        subprocess.Popen(cmd_str.split(),
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         close_fds=True)
        return 0

if __name__ == '__main__':
	try:
		rc = main()
	except:
		debug(traceback.format_exc())
		sys.exit(EXCEPT_EXIT)
	sys.exit(rc)

