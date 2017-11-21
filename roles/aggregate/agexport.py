#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os, signal
import ConfigParser
# import logging
import argparse
import json
import traceback
import time
import base64
import errno
import math
# import psutil
import stat
import fnmatch

import sys
from time import sleep

sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_ibd import *
from atl_storage import *
from log import *

sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
from ha_util import runcmd

sys.path.insert(0, "/opt/milio")
from libs import bio

ATLAS_CFG = "/etc/ilio/atlas.json"
IBD_AGENT_CFG = "/etc/ilio/ibdagent.conf"
IBD_SERVER_CFG = "/etc/ilio/ibdserver.conf"
BB_FILE_SIZE = 4294967296
BB_DEVICE_PATH = "/dev/sdc"
BB_FILE_PATH = "/bufdevice"

IBD1_NODE = "/dev/ibd1"
IBD0_NODE = "/dev/ibd0"
SVM_EXPORTS = "/etc/ilio/svm_exports.json"
SVM_EXPORTS_BAK = "/etc/ilio/svm_exports.json.bak"
LVCREATE_BIN = "/sbin/lvcreate"
LVEXTEND_BIN = "/sbin/lvextend"
LVCHANGE_BIN = "/sbin/lvchange"
LVREMOVE_BIN = "/sbin/lvremove"
MEM_DIR = "/mnt/memory/"
RAID1_MD_DEV = "/dev/md0"
ZRAM_MAX_DEVS = 32
ZRAM_DEFAULT_RATIO = 2
CMD_MDSTOP = "/sbin/mdadm --stop"
CMD_MDASSEMBLE = "/sbin/mdadm --assemble --run"
CMD_MDCREATE = "/sbin/mdadm --create --assume-clean --run --force --metadata=1.2"
CMD_MDADD = "/sbin/mdadm --add"
EXPORT_SYMLINK_DIR = "/dev/usx/"

DEFAULT_SNAPSHOT_SIZE = 5
DEFAULT_SPACE_RATIO = 0.9
THINPOOL_METADATA_SIZE = 256 * 1024
LV_CHUNKSIZE = 4096  # KiB
CMD_IBDMANAGER_STAT = CMD_IBDMANAGER + " -r a -s get"

LOG_FILENAME = '/var/log/usx-agexport.log'
LCKFILE = '/tmp/agexport.lck'
set_log_file(LOG_FILENAME)


#
# "deviceoptions": ["WRITEBACK", "WRITETHROUGH", "WRITEAROUND", "BIGBUFFER"]
#

def load_exports():
    if not os.path.isfile(SVM_EXPORTS):
        return {}
    try:
        svm_exports_file = open(SVM_EXPORTS, 'r')
        data = svm_exports_file.read()
        svm_exports_file.close()
        all_exports = json.loads(data)
    except:
        debug(traceback.format_exc())
        return None
    return all_exports


def save_exports(all_exports):
    rc = 0
    data = json.dumps(all_exports, sort_keys=True, indent=4, separators=(',', ': '))
    try:
        svm_exports_file = open(SVM_EXPORTS_BAK, 'w')
        svm_exports_file.write(data)
        svm_exports_file.close()
        os.rename(SVM_EXPORTS_BAK, SVM_EXPORTS)
    except:
        debug(traceback.format_exc())
        rc = 1
    return rc


def exp_subdev_path(subdev):
    if subdev["raidbricktype"].upper() == "SHARED" and subdev["storagetype"].upper() not in ["SNAPSHOTDISK",
                                                                                             "SHARED_VG"] \
            or subdev["storagetype"].upper() in ["WHOLEDISK"]:
        scsibus = subdev["scsibus"]
        # TODO: Do we really need to scan every time?
        scsi_hotscan()
        return scsi_to_device(scsibus)
    elif subdev["storagetype"].upper() in ["WHOLEMEMORY", "MEMORY_LOOP"]:
        return "/dev/" + subdev["uuid"]
    elif subdev["storagetype"].upper() in ["MEMORY"]:
        return MEM_DIR + subdev["storageuuid"] + "/" + subdev["uuid"]
    elif subdev["storagetype"].upper() in ["ZRAM"]:
        return "/dev/" + subdev["uuid"]
    elif subdev["storagetype"].upper() in ["DISK", "FLASH", "MPOOL"]:
        return "/dev/" + subdev["storageuuid"] + "/" + subdev["uuid"]
    elif subdev["storagetype"].upper() in ["SNAPSHOTDISK"]:
        return "/dev/" + subdev["storageuuid"] + "/" + subdev["uuid"]
    elif subdev["storagetype"].upper() in ["SHARED_VG"]:
        return "/dev/" + subdev["storageuuid"] + "/" + subdev["uuid"]
    else:
        debug("ERROR: not supported sub device type!")
        return None


def exppath_hybrid(export):
    return "/dev/mapper/" + export["euuid"]


def exppath_raid1(export):
    return RAID1_MD_DEV


def exppath_plain(export):
    return export["subdevices"][0]["devicepath"]


def exp_to_exppath(export):
    if export["exporttype"].upper() == "HYBRID":
        return exppath_hybrid(export)
    elif export["exporttype"].upper() == "RAID1":
        return exppath_raid1(export)
    elif export["exporttype"].upper() == "PLAIN":
        return exppath_plain(export)
    else:
        debug("ERROR: Unsupported export type: %s" % export["exporttype"])
        return None


#
# Zram methods
#

def alloc_subdevice_zram(subdevice):
    memory_hot_add()
    return start_subdevice_zram(subdevice)


def next_free_zram_dev():
    for i in range(ZRAM_MAX_DEVS):
        zram_dev = "zram" + str(i)
        zram_disksize_file = "/sys/block/" + zram_dev + "/disksize"
        f = open(zram_disksize_file, 'r')
        zram_size = int(f.read())
        if zram_size == 0:
            return zram_dev
    debug("ERROR: no free zram device.")
    return None


def start_subdevice_zram(subdevice, expand_export=False):
    if expand_export == True:
        memory_hot_add()

    cmd_str = "/sbin/modprobe zram num_devices=" + str(ZRAM_MAX_DEVS)
    do_system(cmd_str)
    for i in range(ZRAM_MAX_DEVS):
        zram_dev = next_free_zram_dev()
        if zram_dev == None:
            rc = errno.EBUSY
            break;
        cmd_str = "echo 1 > " + "/sys/block/" + zram_dev + "/reset"
        do_system(cmd_str)
        cmd_str = "echo " + str(os.sysconf('SC_NPROCESSORS_ONLN')) + " > /sys/block/" + zram_dev + "/max_comp_streams"
        rc = do_system(cmd_str)
        if rc != 0:
            debug('ERROR: Failed to set max_comp_streams for ' + zram_dev)
            continue
        if "storageoptions" in subdevice and "ZRAM_RATIO" in subdevice["storageoptions"]:
            zram_ratio = subdevice["storageoptions"]["ZRAM_RATIO"]
        else:
            zram_ratio = ZRAM_DEFAULT_RATIO
        if "storageoptions" in subdevice and "ZRAM_SIZE" in subdevice["storageoptions"]:
            zram_size = subdevice["storageoptions"]["ZRAM_SIZE"]
        else:
            zram_size = int(math.floor(subdevice["raidbricksize"] * zram_ratio))

        if os.path.exists("/sys/block/" + zram_dev + "/mem_limit"):
            cmd_str = "echo " + str(subdevice["raidbricksize"]) + "G" + " > " + "/sys/block/" + zram_dev + "/mem_limit"
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR: set zram mem_limit");
                break;
        else:
            debug("ERROR: kernel does not support zram mem_limit.")
            rc = 1
            break;
        # Set max_sectors_kb to 44 for Xen.
        if check_hypervisor_type() == "Xen":
            debug('Update max_sectors_kb for %s.' % zram_dev)
            zram_max_sector_kb = 44
            cmd_str = 'echo %s > /sys/block/%s/queue/max_sectors_kb' % (zram_max_sector_kb, zram_dev)
            do_system(cmd_str)

        cmd_str = "echo " + str(zram_size) + "G" + " > " + "/sys/block/" + zram_dev + "/disksize"
        rc = do_system(cmd_str)
        if rc == 0:
            break;

    if rc != 0:
        debug("ERROR: can not find free zram device.")
        return rc

    dev_path = subdevice["devicepath"]
    if os.path.islink(dev_path):
        debug("WARNING: dev_path exists, remove it: %s" % dev_path)
        do_system("rm -f " + dev_path)

    cmd_str = 'ln -s ' + "/dev/" + zram_dev + " " + dev_path
    rc = do_system(cmd_str)
    return rc


def stop_subdevice_zram(subdevice):
    if "devicepath" not in subdevice:
        return 0
    zram_dev_path = os.readlink(subdevice["devicepath"])
    zram_dev = os.path.basename(zram_dev_path)
    cmd_str = "echo 1 > " + "/sys/block/" + zram_dev + "/reset"
    rc = do_system(cmd_str)
    return rc


def delete_subdevice_zram(subdevice):
    # zram devcie is already cleared at stop time.
    return 0


#
# Memory methods for tmpfs
#
def setup_memory_tmpfs(memsize, expuuid):
    if memsize <= 0:
        debug('ERROR : Invalid size %s for setting up tmpfs. Needs to be >= 1 (units: GB)' % memsize)
        return None
    memdir = MEM_DIR + expuuid
    if do_system('mkdir -p %s' % memdir) != 0:
        debug('Can not create dir %s' % memdir)
        return None

    # TODO: We might want to add some check here.
    debug('Forcefully umount anything at our mount point %s !' % (memdir))
    do_system('umount %s' % (memdir))

    debug('Creating tmpfs %s with size %sG.' % (memdir, str(memsize)))
    if do_system('mount -t tmpfs -o size=%sG none %s' % (str(memsize), memdir)) != 0:
        debug('Can not create tmpfs %s with size %sG.' % (memdir, str(memsize)))
        return None

    return memdir


def alloc_subdevice_wholememory(subdevice):
    return start_subdevice_wholememory(subdevice)


def start_subdevice_wholememory(subdevice):
    rc = setup_memory_tmpfs(subdevice["raidbricksize"], subdevice["storageuuid"])
    if rc == None:
        debug("ERROR: can not setup memory tmpfs.")
        return 1
    return start_subdevice_memory_loop(subdevice)


#
# Memory on top of tmpfs
#

def alloc_subdevice_memory(subdevice):
    memory_hot_add()
    return start_subdevice_memory(subdevice)


def extend_and_start_subdevice_memory(subdevice):
    memory_hot_add()
    (ret, msg) = runcmd("cat /proc/meminfo | grep MemFree:", print_ret=True, lines=True)
    if ret != 0:
        debug("ERROR: failed to run cat /proc/meminfo")
        return ret
    free_mem_size = 0
    for line in msg:
        if line.strip().startswith("MemFree:"):
            free_mem_size = long(line.split()[1])
            break

    # We are not using devicepath, since MEMORY_LOOP might need that field.
    exp_path = MEM_DIR + subdevice["storageuuid"] + "/" + subdevice["uuid"]

    cur_mem_size = os.stat(exp_path).st_size / 1024
    if long(subdevice["raidbricksize"]) * 1024 * 1024 - cur_mem_size >= free_mem_size:
        debug("ERROR: not enough memory to extend from %d to %d" % \
              (cur_mem_size, long(subdevice["raidbricksize"]) * 1024 * 1024 * 1024))
        return 1

    # cmd_str = '/usr/bin/truncate -c -s %sG %s' % (subdevice["raidbricksize"], exp_path)
    cmd_str = 'fallocate -l %sG %s' % (subdevice["raidbricksize"], exp_path)
    rc = do_system(cmd_str)
    return rc


def start_subdevice_memory(subdevice, expand_export=False):
    if expand_export:
        return extend_and_start_subdevice_memory(subdevice)

    rc = 0
    # We are not using devicepath, since MEMORY_LOOP might need that field.
    exp_path = MEM_DIR + subdevice["storageuuid"] + "/" + subdevice["uuid"]
    cmd_str = 'fallocate -l %sG %s' % (str(subdevice["raidbricksize"]), exp_path)
    rc = do_system(cmd_str)
    if rc != 0:
        return rc
    # Zero the first MB.
    cmd_str = 'dd if=/dev/zero of=' + exp_path + ' bs=1M count=1 conv=notrunc'
    rc = do_system(cmd_str)
    return rc


def stop_subdevice_memory(subdevice):
    return 0


def delete_subdevice_memory(subdevice):
    rc = 0
    exp_path = MEM_DIR + subdevice["storageuuid"] + "/" + subdevice["uuid"]
    if not os.path.isfile(exp_path):
        debug("WARNING: subdevice not exist: %s" % str(subdevice["uuid"]))
        return 0
    cmd_str = "rm -f " + exp_path
    rc = do_system(cmd_str)
    if rc != 0:
        debug("Cannot remove %s" % str(exp_path))
        return rc
    return rc


#
# Memory plus loopback device
#

def alloc_subdevice_memory_loop(subdevice):
    return start_subdevice_memory_loop(subdevice)


def delete_subdevice_memory_loop(subdevice):
    return delete_subdevice_memory(subdevice)


def start_subdevice_memory_loop(subdevice):
    rc = 0
    rc = start_subdevice_memory(subdevice)
    if rc != 0:
        debug("ERROR: can not setup memory file for %s" % subdevice["uuid"])
        return rc

    exp_path = MEM_DIR + subdevice["storageuuid"] + "/" + subdevice["uuid"]
    # Setup loopback
    cmd_str = 'losetup -f'
    out = ['']
    do_system(cmd_str, out)
    lodevname = out[0].strip()
    cmd_str = 'losetup ' + lodevname + ' ' + exp_path
    rc = do_system(cmd_str, out)
    if rc != 0:
        debug("ERROR: can not setup lo device for %s" % subdevice["uuid"])
        return rc

    dev_path = subdevice["devicepath"]
    if os.path.islink(dev_path):
        debug("WARNING: dev_path exists, remove it: %s" % dev_path)
        do_system("rm -f " + dev_path)

    cmd_str = 'ln -s ' + lodevname + " " + dev_path
    rc = do_system(cmd_str)
    return rc


def stop_subdevice_memory_loop(subdevice):
    try:
        lodev = os.readlink(subdevice["devicepath"])
    except:
        debug("WARNING: symlink doesn't exist: %s" % subdevice["devicepath"])
        return 1

    cmd_str = "losetup -d " + lodev
    rc = do_system(cmd_str)
    if rc != 0:
        debug("ERROR: Cannot stop lo device: %s" % lodev)

    cmd_str = "rm -f " + subdevice["devicepath"]
    rc = do_system(cmd_str)
    if rc != 0:
        debug("ERROR: Cannot remove symlink: %s" % subdevice["devicepath"])
    return rc


#
# Disk methods
#

def alloc_subdevice_disk(subdevice):
    rc = 0

    cmd_str = "/sbin/lvcreate -L " + str(subdevice["raidbricksize"]) + "G" + " " + subdevice["storageuuid"] + " -n " + \
              subdevice["uuid"]
    rc = do_system(cmd_str)

    return rc


def alloc_subdevice_snapshot_disk(subdevice):
    debug("alloc_subdevice_snapshot_disk: start (storageuuid:%s)" % subdevice["storageuuid"])
    scsibus = subdevice["scsibus"]
    scsi_hotscan()
    devname = scsi_to_device(scsibus)
    vgname = subdevice["storageuuid"]
    vgpoolname = vgname + '_pool'
    lvname = subdevice["uuid"]
    # AMC changed disk percentge from 125% to 200%, need change the getting method for lvsize.
    lvsize = load_usx_conf()['volumeresources'][0]['raidplans'][0]['volumesize'] - 1
    subdevice["devicepath"] = '/dev/%s/%s' % (vgname, lvname)

    cmd_str = '/sbin/vgcreate %s %s' % (vgname, devname)
    do_system(cmd_str)

    out = ['']
    cmd_str = 'vgs -o vg_free_count --noheadings %s' % vgname
    rc = do_system(cmd_str, out)
    if rc != 0:
        return rc
    free_extents = int(out[0].split(' ')[-1])
    poolsize = int(free_extents * 0.99)

    # Run udevadm first.
    udev_trigger()

    if is_snapshot_enabled(load_usx_conf()):
        cmd_str = 'lvcreate -V %dG -l %d -n %s --thinpool %s/%s' % (lvsize, poolsize, lvname, vgname, vgpoolname)
        rc = do_system(cmd_str)

        # Disable zeroing of thinpool, double the performance!
        cmd_str = 'lvchange -Z n %s/%s' % (vgname, vgpoolname)
        rc = do_system(cmd_str)
    else:
        cmd_str = "lvcreate -L %dG -n %s %s" % (lvsize, lvname, vgname)
        rc = do_system(cmd_str)

    return rc


LVM_ERROR_NOTEXIST = 5


def delete_subdevice_disk(subdevice):
    rc = 0

    out = ['']
    cmd_str = "/sbin/lvremove -f " + subdevice["storageuuid"] + "/" + subdevice["uuid"]
    rc = do_system(cmd_str, out)
    if rc != 0:
        debug("ERROR: Cannot remove the subdevice: %s rc=%s" % (str(subdevice["uuid"]), str(rc)))
        # lvremove return 5 for non-exist LV.
        # However, in busy case, it also return 5!
        # So, we check the error output message directly.
        if "not found" in out[0]:
            debug("WARNING: subdevice does not exist: %s, treat it as deleted." % subdevice["uuid"])
            rc = 0

    return rc


def extend_and_start_subdevice_disk(subdevice):
    (ret, msg) = runcmd("lvs --all --aligned --noheadings --nosuffix", print_ret=True, lines=True)
    if ret != 0:
        debug("ERROR: failed to run lvs")
        return ret
    vg = None
    current_size = None
    for line in msg:
        tmp = line.strip().split()
        if tmp[0] == subdevice["uuid"]:
            vg = tmp[1]
            current_size = tmp[3][:-1]
            break
    if not vg:
        debug("ERROR: failed to find vg for lv %s" % subdevice["uuid"])
        return 1
    if float(subdevice["raidbricksize"]) <= float(current_size):
        return 0

    (ret, msg) = runcmd("vgs --all --aligned --noheadings --nosuffix", print_ret=True, lines=True)
    if ret != 0:
        debug("ERROR: failed to run vgs")
        return ret
    free_disk_size = 0
    for line in msg:
        tmp = line.strip().split()
        if tmp[0] == vg:
            free_disk_size = float(tmp[6][:-1])
            if tmp[6].endswith('t') or tmp[6].endswith('T'):
                free_disk_size = free_disk_size * 1024
            break

    if float(subdevice["raidbricksize"]) - float(current_size) >= free_disk_size:
        debug("ERROR: not enough disk to extend from %sG to %sG" % (current_size, subdevice["raidbricksize"]))
        return 1
    cmd_str = LVEXTEND_BIN + ' -L %sG %s' % (subdevice["raidbricksize"], subdevice["devicepath"])
    rc = do_system(cmd_str)
    return rc


def start_subdevice_disk(subdevice, expand_export=False, error_report=True):
    rc = 0

    # If a shared storage just got attached -> detached -> attached again,
    # the LV might end up with IO Error, we need disable such LV and
    # re-enable them.
    # TODO: should we disable & enable the whole VG? we need to take care of
    # any other LVs on the same VG.
    deactivate_cmd_str = "/sbin/lvchange -a n -y " + subdevice["storageuuid"] + "/" + subdevice["uuid"]
    rc = do_system(deactivate_cmd_str)
    lvname = subdevice["storageuuid"] + "/" + subdevice["uuid"]
    activate_cmd_str = "/sbin/lvchange -a y -y " + lvname

    rc = lvchange_active_sync(lvname)
    if rc == 0:
        rc = do_system('/sbin/lvdisplay {}'.format(lvname))
    if rc != 0:
        debug(
            "WARNING: Cannot start the subdevice %s or it is not exist, will scan and retry." % str(subdevice["uuid"]))

        # Scan and Retry
        # USX-59565, probe the disk partition. Here, we run the command on each disk.
        partprobe_cmd_str = "/sbin/partprobe"
        get_disk_list_cmd_str = "lsscsi | awk {'print $NF'}"
        out = ['']
        do_system(get_disk_list_cmd_str, out)
        for the_disk in out[0].strip().split('\n'):
            partprobe_disk_cmd_str = partprobe_cmd_str + " " + the_disk
            do_system(partprobe_disk_cmd_str)

        pvscan_cmd_str = "/sbin/pvscan"
        rc = do_system(pvscan_cmd_str)

        scan_cmd_str = "/sbin/vgscan"
        rc = do_system(scan_cmd_str)

        # Retry
        rc = do_system(activate_cmd_str)
        if rc != 0 and error_report:
            debug("ERROR: Cannot start the subdevice: %s" % str(subdevice["uuid"]))
            return rc

    if expand_export:
        return extend_and_start_subdevice_disk(subdevice)

    return rc


def start_subdevice_snapshot_disk(subdevice):
    debug("start_subdevice_snapshot_disk: start(uuid:%s) ..." % subdevice["uuid"])
    rc = start_subdevice_disk(subdevice)
    if rc != 0:
        return rc

    vg_name = subdevice["storageuuid"]
    lv_name = subdevice["uuid"]
    orig_lv_path = "/dev/%s/%s" % (vg_name, lv_name)
    mount_snapshot_name = lv_name + "-snapshot-mount"
    backup_snapshot_name = lv_name + "-snapshot-backup"
    backup_snapshot_path = "/dev/%s/%s" % (vg_name, backup_snapshot_name)

    subdevice_snapshot = subdevice
    subdevice_snapshot['uuid'] = backup_snapshot_name
    # Trying to start backup snapshot if it exist. Ignore possible errors
    start_subdevice_disk(subdevice_snapshot, False, False)
    sleep(1)
    # We need these two lines for restoring old values in subdevice
    subdevice_snapshot['uuid'] = lv_name
    subdevice['uuid'] = lv_name

    do_system("lvs")

    if os.path.exists(mount_snapshot_name):
        debug(
            "start_subdevice_snapshot_disk: Legacy mount snapshot detected, cleanup (uuid:%s) ..." % mount_snapshot_name)
        cmd_str = "lvremove -f " + vg_name + "/" + mount_snapshot_name
        rc = do_system(cmd_str)
        if rc != 0:
            debug("cleanup error: %d" % rc)
            return rc
    if os.path.exists(backup_snapshot_path) == False:
        debug("start_subdevice_snapshot_disk: no leftover snapshot(%s) ..." % backup_snapshot_path)
    else:
        if is_snapshot_enabled(load_usx_conf()):
            debug("start_subdevice_snapshot_disk: doing lvrename (uuid:%s) ..." % lv_name)
            cmd_str = "lvremove -f %s/%s" % (vg_name, lv_name)
            rc = do_system(cmd_str)
            cmd_str = "lvrename %s/%s %s" % (vg_name, backup_snapshot_name, lv_name)
            rc = do_system(cmd_str)
            if rc != 0:
                debug("lvrename error: %d" % rc)
                return rc
        else:
            debug("start_subdevice_snapshot_disk: doing lvconvert (uuid:%s) ..." % lv_name)
            cmd_str = "lvconvert --merge " + vg_name + "/" + backup_snapshot_name
            rc = do_system(cmd_str)
            if rc != 0:
                debug("merge error: %d" % rc)
                return rc

    # cmd_str = "lvcreate --snapshot --size %dG --name %s %s" % \
    #	(subdevice["raidbricksize"] / 2 - 1, mount_snapshot_name, orig_lv_path)
    # rc = do_system(cmd_str)
    # if rc != 0:
    #	debug("ERROR: Failed to create mount snapshot!")
    #	return rc

    # subdevice["devicepath"] = '/dev/%s/%s' % (vg_name, mount_snapshot_name)
    return rc


def stop_subdevice_disk(subdevice):
    cmd_str = "/sbin/lvchange -a n " + subdevice["storageuuid"] + "/" + subdevice["uuid"]
    # FIXME: should handle busy case
    rc = do_system(cmd_str)
    if rc != 0:
        time.sleep(5)
        rc = do_system(cmd_str)

    if rc != 0:
        debug("ERROR: Cannot inactivate the subdevice: %s" % str(subdevice["uuid"]))
    return rc


#
# Shared storage methods
#

'''
# no longer needed, to delete. Shared storage doesn't have a valid uuid, use the devicename to fake one for vscaler.
def fix_shared_uuid(subdevice):

	if subdevice["raidbricktype"].upper() != "SHARED":
		return
	if ("uuid" not in subdevice) or (subdevice["uuid"] == None):
		if ("devicepath" in subdevice) and (subdevice["devicepath"] != None):
			subdevice["uuid"] = os.path.basename(subdevice["devicepath"])
		else:
			debug("WARNING: cannot fix shared uuid with empty devicepath!")
	else:
		debug("INFO: shared device already has uuid, skip fix.")
	return
'''


# TODO
def alloc_subdevice_shared(subdevice):
    debug("alloc_subdevice_shared: start (storageuuid:%s)" % subdevice["storageuuid"])
    scsibus = subdevice["scsibus"]
    scsi_hotscan()
    devname = scsi_to_device(scsibus)
    vgname = subdevice["storageuuid"]
    lvname = subdevice["uuid"]
    subdevice["devicepath"] = '/dev/%s/%s' % (vgname, lvname)

    cmd_str = '/sbin/vgcreate %s %s' % (vgname, devname)
    rc = do_system(cmd_str)
    if rc != 0:
        debug('Failed to create VG for shared storage.')
        return rc

    cmd_str = "/sbin/lvcreate -l 100%FREE" + " " + vgname + " -n " + lvname
    rc = do_system(cmd_str)
    return rc


def delete_subdevice_shared(subdevice):
    return 0


def start_subdevice_shared(subdevice):
    rc = start_subdevice_disk(subdevice)
    return rc


def stop_subdevice_shared(subdevice):
    rc = stop_subdevice_disk(subdevice)
    return rc


#
# WHOLEDISK methods
#
def alloc_subdevice_wholedisk(subdevice):
    return 0


def start_subdevice_wholedisk(subdevice):
    return 0


#
# Memory pool methods
#

def alloc_subdevice_mpool(subdevice):
    return alloc_subdevice_disk(subdevice)


def delete_subdevice_mpool(subdevice):
    return delete_subdevice_disk(subdevice)


def start_subdevice_mpool(subdevice):
    rc = start_subdevice_disk(subdevice)
    if rc != 0:
        return alloc_subdevice_mpool(subdevice)
    return rc


def stop_subdevice_mpool(subdevice):
    return stop_subdevice_disk(subdevice)


#
# Plain device methods
#

def subdevice_create_link(subdevice):
    subdevice_link = EXPORT_SYMLINK_DIR + subdevice["uuid"]
    if subdevice_link == subdevice["devicepath"]:
        debug("INFO: devicepath equals to external link, skip symlink.")
        return
    if not os.path.isdir(EXPORT_SYMLINK_DIR):
        do_system("/bin/mkdir " + EXPORT_SYMLINK_DIR)
    if os.path.islink(subdevice_link):
        do_system("rm " + subdevice_link)
    cmd_str = "/bin/ln -s " + subdevice["devicepath"] + " " + subdevice_link
    subdevice["subexportpath"] = subdevice_link
    rc = do_system(cmd_str)
    if rc != 0:
        return rc
    if os.path.islink(subdevice_link) and os.access(subdevice_link, os.W_OK):
        return 0
    return 1


def alloc_export_subdevice(subdevice):
    rc = 0

    if subdevice["raidbricktype"].upper() == "SHARED" and subdevice["storagetype"].upper() not in ["SNAPSHOTDISK",
                                                                                                   "SHARED_VG"]:
        # Deprecated, For backward compatible only.
        rc = alloc_subdevice_wholedisk(subdevice)
    elif subdevice["storagetype"].upper() == "WHOLEDISK":
        rc = alloc_subdevice_wholedisk(subdevice)
    elif subdevice["storagetype"].upper() == "WHOLEMEMORY":
        rc = alloc_subdevice_wholememory(subdevice)
    elif subdevice["storagetype"].upper() == "ZRAM":
        rc = alloc_subdevice_zram(subdevice)
    elif subdevice["storagetype"].upper() == "MPOOL":
        rc = alloc_subdevice_mpool(subdevice)
    elif subdevice["storagetype"].upper() == "MEMORY":
        rc = alloc_subdevice_memory(subdevice)
    elif subdevice["storagetype"].upper() in ["DISK", "FLASH"]:
        rc = alloc_subdevice_disk(subdevice)
    elif subdevice["storagetype"].upper() == "SNAPSHOTDISK":
        rc = alloc_subdevice_snapshot_disk(subdevice)
    elif subdevice["storagetype"].upper() == "SHARED_VG":
        rc = alloc_subdevice_shared(subdevice)
    else:
        debug("ERROR: unsupported storage type: %s, alloc failed." % str(subdevice["storagetype"]))
        return 1

    subdevice_create_link(subdevice)

    return rc


def start_export_subdevice(subdevice, expand_export=False):
    rc = 0
    debug("Starting subdevice: %s of storage type %s " % (str(subdevice), subdevice["storagetype"].upper()))

    if subdevice["raidbricktype"].upper() == "SHARED" and subdevice["storagetype"].upper() not in ["SNAPSHOTDISK",
                                                                                                   "SHARED_VG"]:
        # Deprecated, For backward compatible only.
        rc = start_subdevice_wholedisk(subdevice)
    elif subdevice["storagetype"].upper() == "WHOLEDISK":
        rc = start_subdevice_wholedisk(subdevice)
    elif subdevice["storagetype"].upper() == "WHOLEMEMORY":
        rc = start_subdevice_wholememory(subdevice)
    elif subdevice["storagetype"].upper() == "ZRAM":
        rc = start_subdevice_zram(subdevice, expand_export)
    elif subdevice["storagetype"].upper() == "MPOOL":
        rc = start_subdevice_mpool(subdevice)
    elif subdevice["storagetype"].upper() == "MEMORY":
        rc = start_subdevice_memory(subdevice, expand_export)
    elif subdevice["storagetype"].upper() in ["DISK", "FLASH"]:
        rc = start_subdevice_disk(subdevice, expand_export)
    elif subdevice["storagetype"].upper() == "SNAPSHOTDISK":
        rc = start_subdevice_snapshot_disk(subdevice)
    elif subdevice["storagetype"].upper() == "SHARED_VG":
        rc = start_subdevice_shared(subdevice)
    else:
        debug("ERROR: unsupported storage type: %s, start failed." % str(subdevice["storagetype"]))
        return 1

    if not expand_export:
        subdevice_create_link(subdevice)

    return rc


def stop_export_plain(subdevice, noflush=False):
    debug("Stoping: %s" % subdevice["uuid"])
    if subdevice["raidbricktype"].upper() == "SHARED" and subdevice["storagetype"].upper() not in ["SNAPSHOTDISK",
                                                                                                   "SHARED_VG"]:
        # Deprecated, For backward compatible only.
        rc = stop_subdevice_wholedisk(subdevice)
    elif subdevice["storagetype"].upper() == "MPOOL":
        rc = stop_subdevice_mpool(subdevice)
    elif subdevice["storagetype"].upper() in ["MEMORY", "WHOLEMEMORY"]:
        rc = stop_subdevice_memory(subdevice)
    elif subdevice["storagetype"].upper() in ["ZRAM"]:
        rc = stop_subdevice_zram(subdevice)
    elif subdevice["storagetype"].upper() in ["SHARED_VG"]:
        rc = stop_subdevice_shared(subdevice)
    else:
        rc = stop_subdevice_disk(subdevice)
    return rc


#
# Hybrid methods
#

def get_vscaler_subdevices(export):
    flash_dev = None
    disk_dev = None
    for subdev in export["subdevices"]:
        if subdev["uuid"] == export.get("cachedevuuid"):
            flash_dev = subdev
        else:
            disk_dev = subdev
    return flash_dev, disk_dev


def setup_ibd_config():
    # flash_dev, disk_dev = get_vscaler_subdevices(export)
    # at this point disk_dev["subexportpath"] is going to be the path for ibd
    #    that path will have to be in the ibdserver.conf
    bio.modprobe('ibd')


def verify_big_buffer_resources():
    debug("Entering verify_big_buffer_resources")
    # part_usage = psutil.disk_partitions()
    # disk_use = psutil.disk_usage( "/dev/sdc" )

    # if disk_use.total < 1040478208:
    #    debug( "Error - not enough space on the device for a big buffer file" )
    #    return 1

    # mem_usage = psutil.virtual_memory()

    # if mem_usage.available < 5368709120:
    #    debug( "Error - not enough ram available for big buffer" )
    #    return 1

    MIN_MEM_NEEDED = 5368709
    cmd_free = "free"
    out = ['']
    do_system(cmd_free, out)
    arr_out = out[0].split()
    free_mem = arr_out[9]

    if int(free_mem) < MIN_MEM_NEEDED:
        debug("not enough memory for big buffer usage")
        return 1

    return 0


def create_big_buffer():
    # cmd_str_mklabel = "parted -m -s -- /dev/sdc mklabel gpt"
    # do_system( cmd_str_mklabel )

    # if not os.path.isdir( "/mnt" ):
    #    os.mkdir( "/mnt" )
    # if not os.path.isdir( "/mnt/bb" ):
    #    os.mkdir( "/mnt/bb" )

    # cmd_str_mkfs = "yes | mkfs -V -t ext4 /dev/sdc"
    # do_system( cmd_str_mkfs )

    # cmd_str_mount = "mount /dev/sdc /mnt/bb"
    # do_system( cmd_str_mount )
    if os.path.exists(BB_FILE_PATH):
        debug('remove big buffer file if it exists!')
        os.remove(BB_FILE_PATH)
    cmd_str_create_bb_file = 'fallocate -l %s %s' % (str(BB_FILE_SIZE), BB_FILE_PATH)
    # cmd_str_create_bb_file = "dd if=/dev/zero of=/mnt/bb/bufdevice bs=1M count=4096"
    ret = do_system(cmd_str_create_bb_file)
    if ret != 0:
        debug("not able to create the big buffer file - possibly not enough space??")
        return 1

    return 0


def create_ibdagent_conf(export):
    debug("Entering create_ibdagent_conf")

    if os.path.exists(IBD_AGENT_CFG):
        os.remove(IBD_AGENT_CFG)

    flash_dev, disk_dev = get_vscaler_subdevices(export)
    # config = ConfigParser.ConfigParser()
    # config.add_section("global")
    # ibd_update_config(config, disk_dev["uuid"], "devname", IBD1_NODE)
    # ibd_update_config(config, disk_dev["uuid"], "minor", 16)
    # ibd_update_config(config, disk_dev["uuid"], "ip", "127.0.0.1")
    #
    # cfgfile = open(IBD_AGENT_CFG, 'w')
    # config.write(cfgfile)
    ibdagent_conf = {}
    ibdagent_conf['devuuid'] = disk_dev['uuid']
    ibdagent_conf['cacheip'] = '127.0.0.1'
    ibdagent_conf['devexport'] = IBD1_NODE
    ibdagent_conf['minornum'] = 16
    rc = add_ibd_channel(ibdagent_conf)
    return rc


def create_ibdserver_conf(export):
    debug("Entering create_ibdserver_conf")
    flash_dev, disk_dev = get_vscaler_subdevices(export)
    # at this point disk_dev["subexportpath"] is going to be the path for ibd
    #    that path will have to be in the ibdserver.conf
    bio.modprobe('ibd')

    if os.path.exists(IBD_SERVER_CFG):
        os.remove(IBD_SERVER_CFG)

    # config = ConfigParser.ConfigParser()
    #
    # ibd_update_config(config, "global", "num_workers", 50)
    # ibd_update_config(config, "global", "ds_type", "split_ds")
    # ibd_update_config(config, "global", "ds_poolsz", 64)
    # ibd_update_config(config, "global", "ds_pagesz", 4)
    # ibd_update_config(config, "global", "io_type", "bio")
    # ibd_update_config(config, "global", "io_poolsz", 512)
    # ibd_update_config(config, "global", "io_pagesz", 8)
    # ibd_update_config(config, "global", "io_bufdevice", BB_FILE_PATH)
    # ibd_update_config(config, "global", "io_bufdevicesz", 4)
    #
    # # ibd_update_config( config, "buftest", "exportname", "/dev/ibd1" ) # this is a placeholder for now
    exportname = "/dev/usx/" + disk_dev["uuid"]
    ibd_exp_name = os.readlink(exportname)
    if not can_open_dev(ibd_exp_name):
        return 1
    #
    # ibd_update_config(config, disk_dev["uuid"], "exportname", ibd_exp_name)
    # ibd_update_config(config, disk_dev["uuid"], "direct_io", 1)
    #
    # cfgfile = open(IBD_SERVER_CFG, 'w')
    # config.write(cfgfile)
    config_support_vdi()
    # rc = ibd_kick_server()
    # if rc != 0:
    #     debug('Failed to notify ibdserver about new config.')
    #     return 1
    modify_drw_config_cache_vdi(disk_dev["uuid"], ibd_exp_name, BB_FILE_PATH, 4)
    return 0


def vv_find_working_ibd():
    debug('Enter vv_find_working_ibd')

    cmd = CMD_IBDMANAGER_STAT
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    debug("check the ibd working state: " + str(msg))

    the_ibd = None
    the_uuid = None
    the_state = False
    ibd_found = False
    working_ibd_list = []
    working_uuid_list = []
    for line in msg:
        if line.find("state:working") >= 0:
            the_state = True

    if the_state == True:
        return 0

    return 1


# debug("working_ibd_list: " + str(working_ibd_list))
# debug("working_uuid_list: " + str(working_uuid_list))
# return (working_ibd_list, working_uuid_list)

def start_ibdserver_ibdagent():
    debug("Entering start_ibdserver_ibdagent")
    # cmd_str_start_ibdserver = "/usr/local/bin/ibdserver"
    # do_system( cmd_str_start_ibdserver )

    # cmd_str_verify_ibdserver_running = "/usr/local/bin/ibdmanager -r s -s get"

    if os.path.exists(IBD0_NODE):
        cmd_str = "rm -rf /dev/ibd0"
        do_system(cmd_str)
        # os.remove( IBD0_NODE )
    if os.path.exists(IBD1_NODE):
        cmd_str = "rm -rf /dev/ibd1"
        do_system(cmd_str)
        # os.remove( IBD1_NODE )

    device0 = os.makedev(44, 0)
    os.mknod(IBD0_NODE, stat.S_IFBLK, device0)
    device1 = os.makedev(44, 16)
    os.mknod(IBD1_NODE, stat.S_IFBLK, device1)

    ibd_kick_server()

    cmd_str_start_agent = "/usr/local/sbin/ibdagent"
    do_system(cmd_str_start_agent)

    # not sure if I can call this from here
    started = 0
    while not started:
        ret_val = vv_find_working_ibd()
        if ret_val == 0:
            started = 1
        # if "/dev/ibd1" in working_ibd:
        #    started = 1
        else:
            time.sleep(1)
            # debug( "starting the clear-cache script" )
            # cmd_drop_caches = ['python', '/opt/milio/atlas/scripts/clear-cache.pyc' ]
            # ret = subprocess.Popen( cmd_drop_caches, close_fds = True )
            # if ret != 0:
    # debug( "WARNING: not able to run the drop caches script" )

    return 0


def fix_ibd_link(new_export):
    debug("Entering fix_ibd_link()")
    for subdev in new_export["subdevices"]:
        if "storagetype" == "WHOLEDISK":
            subdev_for_disk = subdev
            break
    subdevice_link = EXPORT_SYMLINK_DIR + subdev["uuid"]
    cmd_str_rm = "rm " + subdevice_link
    rc = do_system(cmd_str_rm)
    cmd_str_ln = "/bin/ln -s " + "/dev/ibd1" + " " + subdevice_link
    rc = do_system(cmd_str_ln)
    if rc != 0:
        debug("ibd_fix_link() - Unable to create link to ibd1")
        return rc
    return rc


def load_conf_for_simh():
    try:
        with open(ATLAS_CFG, 'r') as e:
            load_data = e.read()
            json_conf = json.loads(load_data)
    except:
        json_conf = {}

    return json_conf


def setup_lv_simh(dev_name):
    vgname = 'dedupvg'
    lvname = 'deduplv'
    config = load_conf_for_simh()
    if not config:
        return 1
    cmd_str = 'dd if=%s of=/etc/ilio/gpt.backup bs=1M count=1' % dev_name
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        return rc

    # Cleanup any GPT leftover
    cmd_str = 'dd if=/dev/zero of=%s bs=1M count=1' % dev_name
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        return rc
    # Modify LVM config file
    cmd_str = "sed -i -e 's/md_chunk_alignment = 1/md_chunk_alignment = 0/'" + \
              " -e 's/data_alignment_detection = 1/data_alignment_detection = 0/'" + \
              " -e 's/data_alignment_offset_detection = 1/data_alignment_offset_detection = 0/'" + \
              " /etc/lvm/lvm.conf"
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        debug("ERROR: Cannot fix lvm.conf!")
        return rc

    # Setup VG/LV
    # Make sure the lv start at 1MB of the underlying PV for backwork compatibility
    cmd_str = 'pvcreate -ff -y -v --dataalignment 512 --dataalignmentoffset 512 %s' % dev_name
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        debug("ERROR: pvcreate failed!")
        return rc

    cmd_str = 'vgcreate %s %s' % (vgname, dev_name)
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        debug("ERROR: vgcreate failed!")
        return rc

    cmd_str = 'vgs -o vg_free_count --noheadings %s' % vgname
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        return rc
    free_extents = int(msg[0].split(' ')[-1])
    cmd_str = 'vgs -o vg_extent_size --noheadings --units=k --nosuffix %s' % vgname
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        return rc
    extent_size = int(float(msg[0].split(' ')[-1]))

    # Run udevadm first
    udev_trigger()

    if not is_snapshot_enabled(config):
        cmd_str = 'lvcreate -v -l %d --contiguous y --zero n -n %s %s' % (free_extents, lvname, vgname)
    else:
        # Reserve for thinpool metadata
        metadata_size = int(free_extents * 0.001) * extent_size  # KiB
        if metadata_size < THINPOOL_METADATA_SIZE:
            metadata_size = THINPOOL_METADATA_SIZE

        # FIXME: '--zero n' will cause the snapshots first 4k got zeroed latter, lvm bug?
        free_extents = free_extents - metadata_size / extent_size
        lvsize = free_extents * extent_size
        lvsize = lvsize - int(milio_config.snapshot_space) * 1024 * 1024
        debug('original volume size is {size}G'.format(size=milio_config.original_volumesize))
        debug('created volume size is {size}G'.format(size=lvsize / 1024 / 1024))
        if int(lvsize / 1024 / 1024) < 1:
            errormsg('lvsize less than 1G')
            return 1
        cmd_str = 'lvcreate -v -V %dk -l %d --poolmetadatasize %dk --chunksize %dk -n %s --thinpool %s/%s' \
                  % (lvsize, free_extents, metadata_size, LV_CHUNKSIZE, lvname, vgname, vgname + 'pool')

    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    if rc != 0:
        return rc
    if is_snapshot_enabled(config):
        # Disable zeroing of thinpool, double the performance!
        cmd_str = 'lvchange -Z n dedupvg/dedupvgpool'
        rc, msg = runcmd(cmd_str, print_ret=True, lines=True)

    # Log the result partition table.
    cmd_str = 'lvs -a -o +seg_start_pe,seg_pe_ranges'
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)

    return rc


def create_sdb_lv(export):
    for subdev in export["subdevices"]:
        if 'WHOLEDISK' in subdev['storagetype']:
            export_uuid = subdev['uuid']
    exportname = "/dev/usx/" + export_uuid
    ibd_exp_name = os.readlink(exportname)
    rc = setup_lv_simh(ibd_exp_name)
    if rc != 0:
        return rc
    os.system('rm -rf %s' % exportname)
    cmd_str = 'ln -s /dev/dedupvg/deduplv /dev/usx/%s' % export_uuid
    rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
    return rc


def alloc_export_hybrid(new_export):
    debug("Entering alloc_export_hybrid...")
    rc = 0

    if len(new_export["subdevices"]) != 2:
        debug("subdevice number mismatch!")
        return 1
    for subdev in new_export["subdevices"]:
        rc = alloc_export_subdevice(subdev)
        if rc != 0:
            debug("Alloction failed.")
            delete_export(new_export)
            return 1

    devopt = new_export['deviceoptions']
    for opt in devopt:
        if "logdev_name=" in opt.lower():
            flash_dev, disk_dev = get_vscaler_subdevices(new_export)
            disk_dev['devicepath'] += '2'
            subdevice_create_link(disk_dev)
            break

    if "BIGBUFFER" in new_export["deviceoptions"]:
        debug("Found BIGBUFFER in deviceoptions")
        # setup_ibd_config()
        rc = verify_big_buffer_resources()
        if rc != 0:
            debug("alloc_export_hybrid - error on verifying resources for ibd")
            return rc
        rc = create_big_buffer()
        if rc != 0:
            debug("alloc_export_hybrid - error on creating the big buffer")
            return rc
        rc = create_sdb_lv(new_export)
        if rc != 0:
            debug('alloc_export_hybrid - error on creating sdb lv.')
            return rc
        rc = create_ibdserver_conf(new_export)
        if rc != 0:
            debug("alloc_export_hybrid - errro on creating the ibdserver conf")
            return rc
        rc = create_ibdagent_conf(new_export)
        if rc != 0:
            debug("alloc_export_hybrid - error on creating ibdagent conf file")
            return rc
        rc = start_ibdserver_ibdagent()
        if rc != 0:
            debug("alloc_export_hybrid - error on starting ibd")
            return rc
        rc = fix_ibd_link(new_export)
        if rc != 0:
            debug("alloc_export_hybrid - error on fixing ibd link")
            return rc
            # fix ibd symlink
            # 1. setup ibdserver/agent config
            # 2. create bigbuf
            # 3. start ibdserver/agent process
            # 4. Fix symlink, re-link to /dev/ibd1
    else:
        debug("Did not find BIGBUFFER in deviceoptions")

    rc = setup_vscaler(new_export)
    if rc != 0:
        debug("vscaler creation failed!")
        delete_export(new_export)
    return rc


def delete_export_hybrid(export):
    flash_dev, disk_dev = get_vscaler_subdevices(export)

    if flash_dev == None:
        debug("ERROR: Missing flash device.")
        return 1

    # Destroy vscaler
    cmd_str = "/opt/milio/scripts/vscaler_destroy -f " + flash_dev["subexportpath"]
    rc = do_system(cmd_str)
    if rc != 0:
        debug("WARNING: vscaler destroy failed!")

    return rc


def start_lv_simh():
    rc = vgchange_active_sync('dedupvg', LOG_FILENAME)
    if rc != 0:
        debug('ERROR: cannot start lv for simple hybird!')
        return rc
    # check whether can open the /dev/dedupvg/deduplv
    dev = '/dev/dedupvg/deduplv'
    if not can_open_dev(dev):
        return 1
    return 0



def start_export_hybrid(export, expand_export=False):
    debug("Entering start_export_hybrid()")
    rc = 0
    flash_dev = None
    disk_dev = None

    flash_dev, disk_dev = get_vscaler_subdevices(export)

    for subdev in export["subdevices"]:
        rc = start_export_subdevice(subdev, expand_export)
        if rc != 0:
            debug("ERROR: subdevice start failed.")
            return 1

    devopt = export['deviceoptions']
    for opt in devopt:
        if "logdev_name=" in opt.lower():
            flash_dev, disk_dev = get_vscaler_subdevices(export)
            disk_dev['devicepath'] += '2'
            rc = subdevice_create_link(disk_dev)
            if rc != 0:
                return rc
            break

    writeback = False
    if "WRITEBACK" in devopt:
        writeback = True
    if "BIGBUFFER" in export["deviceoptions"]:

        # Some actions for upgrading.
        rc = UpgradeStatus().upgrade_boot_up()
        if rc != 0:
            debug('start_export_hybrid() - error on upgrade ibdserver after upgrading')
            return rc

        if is_new_simple_hybrid():
            debug('start_export_hybrid(), enable LV')
            rc = start_lv_simh()
            if rc != 0:
                debug('try to enbale LV failed!')
                return rc
            rc = change_ibdserver_configure_support_new_simple(export)
            if rc != 0:
                return rc
        debug("start_export_hybrid() - found BIGBUFFER in deviceoptions")
        rc = create_ibdagent_conf(export)
        if rc != 0:
            debug("start_export_hybrid() - error on creating ibdagent conf file")
            return rc
        rc = start_ibdserver_ibdagent()
        if rc != 0:
            debug("start_export_hybrid() - error on start of ibdserver and ibdagent")
            return rc
        rc = fix_ibd_link(export)
        if rc != 0:
            debug("start_export_hybrid() - error on fixing ibd link")
            return rc
    else:
        debug("start_export_hybrid() - did not find BIGBUFFER in deviceoptions")

    # Setup vscaler
    if flash_dev == None or disk_dev == None:
        debug("ERROR: Missing subdevices.")
        return 1

    if "subexportpath" not in flash_dev and expand_export:
        subdevice_create_link(flash_dev)
    if "subexportpath" not in disk_dev and expand_export:
        subdevice_create_link(disk_dev)

    # only writeback mode can use vscaler_load
    if not writeback:
        return setup_vscaler(export)
    else:
        cmd_str = "/opt/milio/scripts/vscaler_load " + flash_dev["subexportpath"]
        rc = do_system(cmd_str)
        if rc != 0:
            debug("ERROR: vscaler load failed!")
            if flash_dev["storagetype"].upper() in ALL_MEMORY_STORAGE_TYPES or flash_dev[
                "storagetype"].upper() == "FLASH":
                debug("Try to recreate vscaler for memory devices.")
                return setup_vscaler(export)
        tune_vscaler(export)
        return rc


def change_ibdserver_configure_support_new_simple(export):
    rc = 0
    device_uuid = None
    for device_detail in export['subdevices']:
        if device_detail.get('storagetype') in ['WHOLEDISK']:
            device_uuid = device_detail['uuid']
    try:
        if device_uuid is None:
            raise IOError('can\'t get the device uuid , Please check it.')
        conf_parser = ConfigParser.ConfigParser()
        conf_parser.read(IBD_SERVER_CFG)
        if milio_settings.enable_new_ibdserver:
            device_uuid += '-drw'
        if conf_parser.has_section(device_uuid):
            if not conf_parser.get(device_uuid, 'exportname') in ['/dev/dedupvg/deduplv']:
                conf_parser.set(device_uuid, 'exportname', '/dev/dedupvg/deduplv')
                with open(IBD_SERVER_CFG, 'w') as save_conf:
                    conf_parser.write(save_conf)
        else:
            raise IOError('no {device_uuid} in the configure file!'.format(device_uuid=device_uuid))

    except Exception as e:
        debug('{error_msg}'.format(error_msg=e))
        rc = 1
    return rc


def stop_export_hybrid(export, noflush=False):
    flash_dev = None
    disk_dev = None

    flash_dev, disk_dev = get_vscaler_subdevices(export)

    # if noflush == True or \
    #		(flash_dev != None and flash_dev["storagetype"].upper() not in ALL_MEMORY_STORAGE_TYPES):
    if noflush == True:
        # Try fast remove, don't flush even in write back mode.
        # dev.vscaler.vmem-jin-4+vdisk-jin-4.fast_remove = 1
        (ret, msg) = runcmd("/sbin/dmsetup table", print_ret=True, lines=True)
        for line in msg:
            if flash_dev["uuid"] in line and disk_dev["uuid"] in line and "WRITE_BACK" in line:
                set_vscaler_parameter(export, "fast_remove", "1")
                break

    cmd_str = "/sbin/dmsetup remove " + export["euuid"]
    retry = 0
    max_num_retry = 15
    while (retry < max_num_retry):
        out = ['']
        rc = do_system(cmd_str, out)
        if rc == 0:
            break;
        elif "No such device or address" in out[0]:
            debug("WARN: vscaler does not exist, skip stop!")
            rc = 0;
            break;
        else:
            retry = retry + 1
            time.sleep(4)
    if rc != 0:
        debug("WARNING: cannot stop vscaler %s" % str(export["euuid"]))
        if "Device or resource busy" in out[0]:
            debug("ERROR: vscaler busy, skip stop sub devices!")
            return errno.EBUSY

    for subdev in export["subdevices"]:
        rc = stop_export_plain(subdev)
        if rc != 0:
            debug("WARNING: cannot stop subdevice: %s" % str(subdev))
    return rc


def set_vscaler_parameter(export, key, value):
    flash_dev = None
    disk_dev = None
    rc = 0

    flash_dev, disk_dev = get_vscaler_subdevices(export)

    # Try fast remove, don't flush even in write back mode.
    # dev.vscaler.vmem-jin-4+vdisk-jin-4.fast_remove = 1
    debug(export)
    try:
        cachedev_str = flash_dev["uuid"] + "+" + disk_dev["uuid"]
        sysctl_var = "dev.vscaler." + cachedev_str + "." + key
        cmd_str = "/sbin/sysctl " + sysctl_var + "=" + value
        rc = do_system(cmd_str)
    except:
        debug(traceback.format_exc())
        debug("WARNING: vscaler tunning failed! cmd:%s" % cmd_str)
        rc = 1

    if rc != 0:
        debug("WARNING: vscaler tunning failed with rc: %s" % str(rc))

    return rc


def tune_vscaler(export):
    debug("Setting vscaler run time parameters...")
    set_vscaler_parameter(export, "dirty_thresh_pct", "80")
    set_vscaler_parameter(export, "max_clean_ios_set", "2")
    set_vscaler_parameter(export, "max_clean_ios_total", "4")
    set_vscaler_parameter(export, "reclaim_policy", "1")
    return


def setup_vscaler(export):
    debug('Entering setup_vscaler...')
    # Setup vscaler
    flash_dev, disk_dev = get_vscaler_subdevices(export)

    if flash_dev == None or disk_dev == None:
        debug("Missing subdevices.")
        return 1

    # Remove any junk data on the flash device
    delete_export_hybrid(export)

    if "deviceoptions" not in export or (len(export["deviceoptions"]) == 0):
        if flash_dev["storagetype"].upper() in ALL_MEMORY_STORAGE_TYPES:
            # Default do WRITETHROUGH for ALL memory related storages.
            export["deviceoptions"] = ["WRITETHROUGH"]
        else:
            export["deviceoptions"] = ["WRITEBACK"]

    if "WRITEBACK" in export["deviceoptions"]:
        options = "back"
    elif "WRITETHROUGH" in export["deviceoptions"]:
        options = "thru"
    elif "WRITEAROUND" in export["deviceoptions"]:
        options = "around"
    else:
        if "BIGBUFFER" in export["deviceoptions"]:
            debug('WARNING: unsupported hybrid cache mode, fallback to around for BIGBUFFER')
            options = "around"
        else:
            debug("WARNING: unsupported hybrid cache mode, fallback to thru")
            options = "thru"

    cmd_str = "yes | /opt/milio/scripts/vscaler_create -p " + \
              options + " " + export["euuid"] + " " + flash_dev["subexportpath"] + " " + disk_dev["subexportpath"]
    rc = do_system(cmd_str)
    if rc == 0:
        tune_vscaler(export)
    return rc


def get_snapcloneenabled(new_export):
    clone_enabled = new_export.get('p_snapcloneenabled')
    if clone_enabled == None:
        return True
    return clone_enabled


#
# Raid1 methods
#
def setup_raid1(new_export):
    flash_dev, disk_dev = get_vscaler_subdevices(new_export)

    clone_enabled = get_snapcloneenabled(new_export);

    if (flash_dev == None or disk_dev == None) and clone_enabled:
        debug("Missing subdevices.")
        return 1

    if clone_enabled:
        cmd_str = CMD_MDCREATE + ' ' + RAID1_MD_DEV + " -N " + RAID1_MD_DEV + \
                  " --bitmap=internal --bitmap-chunk=64M " + " --level=raid1 --raid-devices=2 " + \
                  flash_dev["subexportpath"] + ' ' + disk_dev["subexportpath"]
    else:
        if flash_dev != None:
            cmd_str = CMD_MDCREATE + ' ' + RAID1_MD_DEV + " -N " + RAID1_MD_DEV + \
                      " --bitmap=internal --bitmap-chunk=64M " + " --level=raid1 --raid-devices=2 " + \
                      "missing" + ' ' + flash_dev["subexportpath"]
        elif disk_dev != None:
            cmd_str = CMD_MDCREATE + ' ' + RAID1_MD_DEV + " -N " + RAID1_MD_DEV + \
                      " --bitmap=internal --bitmap-chunk=64M " + " --level=raid1 --raid-devices=2 " + \
                      "missing" + ' ' + disk_dev["subexportpath"]
        else:
            debug("No suitable disk for creating RAID1 array")
            return 1

    out = ['']
    rc = do_system(cmd_str, out)
    if 'Device or resource busy' in out[0]:
        debug('Device or resource busy occured during setup raid1')
        return 1

    if rc == 0:
        # try to wait the md device
        debug('DEBUG:try to wait the raid1 device.')
        while True:
            if os.path.exists(RAID1_MD_DEV):
                break
            debug("DEBUG: Waiting md [%s]..." % RAID1_MD_DEV)
            time.sleep(0.1)
    return rc


def setup_raid1_missing(new_export):
    if not get_snapcloneenabled(new_export):
        debug("Can't do setup_raid1_missing() when 'snapcloneenabled' != 0")
        return 0
    flash_dev, disk_dev = get_vscaler_subdevices(new_export)

    if flash_dev == None or disk_dev == None:
        debug("Missing subdevices.")
        return 1

    cmd_str = CMD_MDCREATE + ' ' + RAID1_MD_DEV + " -N " + RAID1_MD_DEV + \
              " --bitmap=internal " + " --level=raid1 --raid-devices=2 " + \
              "missing" + ' ' + disk_dev["subexportpath"]

    rc = do_system(cmd_str)
    return rc


def start_raid1(export):
    flash_dev, disk_dev = get_vscaler_subdevices(export)

    if flash_dev == None or disk_dev == None:
        debug("Missing subdevices.")
        return 1

    if get_snapcloneenabled(export):
        cmd_str = CMD_MDASSEMBLE + ' ' + RAID1_MD_DEV + ' ' + disk_dev["subexportpath"]

        rc = do_system(cmd_str)

        if rc != 0:
            debug("ERROR: Failed to assemble with disk dev.")
            stop_raid1(export)
            debug("WARNING: Recreating the raid1 device.")
            rc = setup_raid1_missing(export)
            if rc != 0:
                return rc

    cmd_str = CMD_MDADD + ' ' + RAID1_MD_DEV + ' ' + flash_dev["subexportpath"]

    rc = do_system(cmd_str)
    if rc == 0:
        # try to wait the md device
        debug('DEBUG:try to wait the raid1 device.')
        while True:
            if os.path.exists(RAID1_MD_DEV):
                break
            debug("DEBUG: Waiting md [%s]..." % RAID1_MD_DEV)
            time.sleep(0.1)
    return rc


def stop_raid1(export):
    cmd_str = CMD_MDSTOP + ' ' + RAID1_MD_DEV
    rc = do_system(cmd_str)
    return rc


def alloc_export_raid1(new_export):
    debug("Entering alloc_export_raid1...")
    rc = 0

    clone_enabled = get_snapcloneenabled(new_export)

    if len(new_export["subdevices"]) != 2 and clone_enabled:
        debug("subdevice number mismatch!")
        return 1
    for subdev in new_export["subdevices"]:
        # Skip snapshot disk if snapcloneenabled == 0
        if not clone_enabled and subdev["storagetype"].upper() == "SNAPSHOTDISK":
            continue

        rc = alloc_export_subdevice(subdev)
        if rc != 0:
            debug("Alloction failed.")
            delete_export(new_export)
            return 1

    rc = setup_raid1(new_export)
    if rc != 0:
        debug("raid1 creation failed!")
        delete_export(new_export)
    return rc


def start_export_raid1(export, expand_export=False):
    # TODO: should distinguise start and alloc!
    debug("Entering start_export_raid1...")
    rc = 0

    if len(export["subdevices"]) != 2:
        debug("subdevice number mismatch!")
        return 1
    for subdev in export["subdevices"]:
        if subdev["uuid"] != export["cachedevuuid"]:
            debug("skip reinit disk dev.")
            rc = start_export_subdevice(subdev, expand_export)
            if rc != 0:
                debug('ERROR: start disk device failed!')
                return rc
            continue
        rc = alloc_export_subdevice(subdev)
        if rc != 0:
            debug("Alloction failed.")
            return 1

    rc = start_raid1(export)
    if rc != 0:
        debug("raid1 start failed!")
    return rc


def stop_export_raid1(export, noflush=False):
    stop_raid1(export)

    clone_enabled = get_snapcloneenabled(export)

    for subdev in export["subdevices"]:
        if not clone_enabled and subdev["storagetype"].upper() == "SNAPSHOTDISK":
            continue
        rc = stop_export_plain(subdev)
        if rc != 0:
            debug("WARNING: cannot stop subdevice: %s" % str(subdev))
    return rc


#
# General methods
#

def export_create_link(export):
    internal_link = exp_to_exppath(export)
    export_link = EXPORT_SYMLINK_DIR + export["euuid"]
    if export_link == internal_link:
        debug("INFO: devicepath equals to external link, skip symlink.")
        return
    if not os.path.isdir(EXPORT_SYMLINK_DIR):
        do_system("/bin/mkdir " + EXPORT_SYMLINK_DIR)
    if os.path.islink(export_link):
        do_system("rm " + export_link)
    cmd_str = "/bin/ln -s " + internal_link + " " + export_link
    do_system(cmd_str)
    return


def alloc_export(new_export):
    rc = 0

    memory_hot_add()

    if new_export["exporttype"].upper() == "HYBRID":
        rc = alloc_export_hybrid(new_export)
    elif new_export["exporttype"].upper() == "RAID1":
        rc = alloc_export_raid1(new_export)
    elif new_export["exporttype"].upper() == "PLAIN":
        if len(new_export["subdevices"]) != 1:
            debug("ERROR: Plain type should only have 1 subdevice.")
            return 1
        rc = alloc_export_subdevice(new_export["subdevices"][0])
    else:
        debug("ERROR: Not supported exporttype: %s" % str(new_export["exporttype"]))
        return errno.EINVAL

    if rc != 0:
        debug('alloc_export failed.')
        return rc

    export_create_link(new_export)

    file_dev = EXPORT_SYMLINK_DIR + new_export["euuid"]
    set_new_disk(file_dev)

    return rc


def stop_export(export, noflush=False):
    rc = 0

    debug("Stopping %s" % str(export["euuid"]))
    if export["exporttype"].upper() == "HYBRID":
        rc = stop_export_hybrid(export, noflush)
        return rc
    elif export["exporttype"].upper() == "RAID1":
        rc = stop_export_raid1(export, noflush)
        return rc
    else:
        rc = stop_export_plain(export["subdevices"][0], noflush)
        return rc


def delete_export(export):
    rc = 0
    debug("Freeing export: %s" % str(export))

    rc = stop_export(export, noflush=True)
    if rc != 0:
        debug("WARNING: Stop export failed on %s!" % str(export["euuid"]))
        if rc == errno.EBUSY:
            debug("WARNING: export busy on %s ,skip delete subdevices!" % str(export["euuid"]))
            return rc

    for subdev in export["subdevices"]:
        if subdev["raidbricktype"].upper() == "SHARED":
            debug("INFO: skip delete shared storage.")
            rc = 0
        elif subdev["storagetype"].upper() in ["MEMORY", "WHOLEMEMORY"]:
            rc = delete_subdevice_memory(subdev)
        elif subdev["storagetype"].upper() in ["ZRAM"]:
            rc = delete_subdevice_zram(subdev)
        else:
            rc = delete_subdevice_disk(subdev)
        if rc != 0:
            debug("WARNING: Failed to remove subdevice: %s" % str(subdev))
    return rc


def start_export(export, expand_export):
    rc = 0

    if export["exporttype"].upper() == "HYBRID":
        rc = start_export_hybrid(export, expand_export)
    elif export["exporttype"].upper() == "RAID1":
        rc = start_export_raid1(export, expand_export)
    elif export["exporttype"].upper() == "PLAIN":
        if len(export["subdevices"]) != 1:
            debug("ERROR: Plain type should only have 1 subdevice.")
            return 1
        rc = start_export_subdevice(export["subdevices"][0], expand_export)
    else:
        debug("ERROR: start_export: Not supported exporttype: %s" % str(export["exporttype"]))
        return 1
    if not expand_export:
        export_create_link(export)
    return rc


def extend_export(export):
    debug("Extending %s" % str(export["euuid"]))

    if export["exporttype"].upper() == "HYBRID":
        rc = stop_export_hybrid(export)
    elif export["exporttype"].upper() == "RAID1":
        rc = stop_export_raid1(export)
    elif export["exporttype"].upper() == "PLAIN":
        rc = stop_export_plain(export["subdevices"][0])
    else:
        rc = stop_export(export, True)

    if rc != 0:
        debug('ERROR: Failed to stop export %s .' % (export["euuid"]))
        return rc
    rc = agg_export_one(export, True)
    if rc != 0:
        debug('ERROR: Failed to export one %s .' % (export["euuid"]))
    return rc


def export_all(all_exports):
    for exp_uuid in all_exports:
        debug('Exporting lv: %s' % exp_uuid)
        rc = start_export(all_exports[exp_uuid], False)
        if rc != 0:
            debug('Export of item: %s failed!' % str(exp_uuid))
    return 0


#
# Misc methods
#

def is_role_volume():
    debug("Entering is_role_volume()")
    configure = load_usx_conf()
    if configure.has_key('usx'):
        roles = configure["usx"]["roles"]
    else:
        roles = configure["roles"]
    if "VOLUME" in roles:
        return True
    return False


#
# Add new exports to ibd config. Size unit is in MB.
#
def ibd_add_export(export):
    exp_path = export["exportpath"]
    rc = modify_drw_config_dis_cache(export['euuid'], exp_path)
    if rc != 0:
        debug('ibd try to add resource failed, Please check ibd state or ibd commands.')
        return rc
    rc = ibd_kick_server()
    if rc != 0:
        debug('Failed to notify ibdserver about new config.')
    return rc


def ibd_del_export(export):
    config = None
    ## Load the ibd config
    try:
        config = ConfigParser.ConfigParser()
        config.read(IBDS_CONF)
    except:
        debug("IBD server config load error!")
        return 1

    if config == None:
        return 1

    exp_uuid = export["euuid"]
    if config.has_section(exp_uuid):
        exp_path = config.get(exp_uuid, IBD_KEY_EXPORT)
        ret = config.remove_section(exp_uuid)
        if ret == False:
            # It is OK if the section is already gone.
            debug('WARNING: Failed to remove export %s from ibdserver config. Maybe it is already deleted.' % exp_uuid)
    else:
        debug('WARNING: Cannot find export to delete: ' + exp_uuid)
        return 1

    ret = save_ibd_config_and_online(config)

    wait = True
    while wait:
        wait = False
        (rc, msg) = runcmd("ibdmanager -r s -s get|grep uuid:", print_ret=True, lines=True)
        for line in msg:
            if exp_uuid in line:
                wait = True
                break
        if wait:
            time.sleep(2)
    return ret


def save_ibd_config_and_online(config):
    ##Notify ibdserver about the new configuration.
    try:
        f = open(IBDS_CONF, 'w')
        config.write(f)
        f.close()
    except:
        debug("Cannot update ibd config file!")
        return 1
    ret = ibd_kick_server()
    if ret != 0:
        debug('Failed to notify ibdserver about new config.')
        return 1
    return 0


#
# Return filesystem free space in MB bytes (1024).
#
def fs_free_space(mount_dir):
    msg = subprocess.check_output('df -k %s' % mount_dir, shell=True)
    debug(msg)
    line = msg.split('\n')[1]
    items = re.split('\s+', line)
    size = items[3]
    return long(size) / 1024


#
# Translate storageuuid to exportuuid by lookup local atlas.json.
#
def lookup_exportuuid(storageuuid):
    f = open(ATLAS_CFG, 'r')
    data = f.read()
    f.close()
    j = json.loads(data)
    for export in j["export"]:
        if export["storageuuid"] == storageuuid:
            return export["uuid"]
    return None


def zram_total_size():
    exports = load_exports()
    if exports == None:
        debug("No exports")
        return 0

    totalzramsize = 0
    for exp_uuid, export in exports.items():
        for subdev in export["subdevices"]:
            if subdev["storagetype"].upper() == "ZRAM":
                totalzramsize += subdev["raidbricksize"]
    return totalzramsize


#
# Should only be called AFTER save new exports change to JSON file
#
def update_usxmanager_capacity(subdev):
    # Update usxmanager about the free capacity change in GB.
    # curl -k -X PUT http://127.0.0.1:8080/usxmanager/usx/inventory/servicevm/exports/agg-34_DISK_ibd_0/50
    storagetype = subdev["storagetype"].upper()
    if storagetype not in ["DISK", "SNAPSHOT", "FLASH", "MEMORY", "ZRAM"] or \
                    subdev["raidbricktype"].upper() == "SHARED":
        debug("Skip update capacity for %s:%s." % (subdev["storageuuid"], storagetype))
        return
    try:
        import_uuid = lookup_exportuuid(subdev["storageuuid"])
    except:
        debug("lookup storageuuid failed! skip update freecapacity!")
        return
    if subdev["storagetype"].upper() in ["MEMORY", "ZRAM"]:
        exp_path = MEM_DIR + subdev["storageuuid"] + "/" + subdev["uuid"]
        tmpfs_size = fs_free_space(os.path.dirname(exp_path))
        tmpfs_size_GB = int(tmpfs_size) / 1024
        free_size = tmpfs_size_GB - zram_total_size()
        size_str = str(free_size)
    else:
        size = lvm_free_space(subdev["storageuuid"])
        size_str = str(int(size))
    cmd_str = "curl -k -X PUT http://127.0.0.1:8080/usxmanager/usx/inventory/servicevm/exports/" + import_uuid + '/' + size_str
    do_system(cmd_str)
    return


#
# Api methods
#

# FIXME: should handle disk/memory
def agg_get_size(exp_uuid):
    try:
        size = fs_free_space(os.path.dirname(exp_path))
        size_str = str(int(size))
        # 'aggr_get_size' is the unique magic API string for pool.
        # JSON Input unit is in GB, Our output Size unit is in MB.
        debug('aggr_get_size ' + exp_uuid + ' ' + size_str)
        print 'aggr_get_size ' + exp_uuid + ' ' + size_str
        return 0
    except:
        debug(traceback.format_exc())
        debug('ERROR: Cannot get export size.')
        pass
    return 1


def agg_export_one(export, expand_export=False):
    # Fix the device path
    for subdevice in export["subdevices"]:
        path = exp_subdev_path(subdevice)
        if path:
            subdevice["devicepath"] = path
        else:
            debug('ERROR: Failed to find path for %s .' % (subdevice["uuid"]))
            return 1
    return start_export(export, expand_export)


def agg_export():
    ## Load the ibd config
    exports = load_exports()
    if exports == None:
        debug("Nothing to export")
        return 0
    # No need to fix devicepath, just assume the saved devicepath is still correct.
    ret = export_all(exports)
    if ret != 0:
        debug('Failed to configure ibdserver for new LV.')
        return 1
    return 0


def agg_extend(new_export):
    exp_uuid = new_export['euuid']
    ## Load all exports
    old_exports = load_exports()
    if exp_uuid not in old_exports:
        debug('WARNING: Export %s does not exist, delete failed.' % exp_uuid)
        return errno.ENODEV

    old_export = old_exports[exp_uuid]

    # Stop ibd old_export? or vol side?
    # rc = ibd_del_export(old_export)
    # if rc != 0:
    #     return rc

    rc = del_sac_channel(exp_uuid)
    if rc != 0:
        return rc
    # Extend
    rc = extend_export(new_export)
    if rc == 0:
        # Save new size
        old_exports = load_exports()
        if exp_uuid not in old_exports:
            debug('ERROR: Export %s disappered! extend failed.' % exp_uuid)
            return errno.ENODEV
        # update new size
        old_exports[exp_uuid] = new_export
        rc = save_exports(old_exports)
        if rc != 0:
            debug("ERROR: Cannot not save updated exports!")
            return errno.EACCES

    new_export["exportpath"] = exp_to_exppath(new_export)
    if 0 != ibd_add_export(new_export):
        return 1
    for subdev in new_export["subdevices"]:
        update_usxmanager_capacity(subdev)

    # Start
    return rc


def agg_destroy(exp_uuid):
    ## Load all exports
    old_exports = load_exports()
    if exp_uuid not in old_exports:
        debug('WARNING: Export %s does not exist, delete failed.' % exp_uuid)
        return errno.ENODEV

    export = old_exports[exp_uuid]

    rc = del_sac_channel(exp_uuid)
    if rc != 0:
        return rc

    ret = delete_export(export)
    if ret != 0:
        debug('ERROR: Failed to delete export %s .' % (exp_uuid))
        return (ret)

    old_exports = load_exports()
    if exp_uuid not in old_exports:
        debug('ERROR: Export %s disappered! delete failed.' % exp_uuid)
        return errno.ENODEV
    del old_exports[exp_uuid]
    rc = save_exports(old_exports)
    if rc != 0:
        debug("ERROR: Cannot not save updated exports!")
        return errno.EACCES

    for subdev in export["subdevices"]:
        update_usxmanager_capacity(subdev)
    debug("Delete succeed.")
    return 0


def agg_create(new_export):
    rc = 0

    # Check existing exports
    old_exports = load_exports()
    # Skip this checking if snapclone is disabled
    if new_export["euuid"] in old_exports and get_snapcloneenabled(new_export):
        debug('Export %s already exist, create failed.' % new_export["euuid"])
        return errno.EEXIST

    # Fix options
    new_export["deviceoptions"] = [x.upper() for x in new_export["deviceoptions"]]

    # Fix the device path
    for subdevice in new_export["subdevices"]:
        subdevice["devicepath"] = exp_subdev_path(subdevice)

    new_export["exportpath"] = exp_to_exppath(new_export)

    rc = alloc_export(new_export)
    if rc != 0:
        debug('Cannot allocate space for export: %s.' % (new_export["euuid"]))
        return errno.ENOSPC

    # Allocate done, record the detail.
    old_exports = load_exports()

    # Skip this checking if snapclone is disabled
    if new_export["euuid"] in old_exports and get_snapcloneenabled(new_export):
        debug('ERROR: Export %s added by someone else, create failed.' % new_export["euuid"])
        return errno.EEXIST

    old_exports[new_export["euuid"]] = new_export

    rc = save_exports(old_exports)
    if rc != 0:
        debug("ERROR: Cannot not save new exports!")
        return errno.EACCES

    if not is_role_volume():
        ret = ibd_add_export(new_export)
        if ret != 0:
            debug("Failed to add export :" + str(new_export["euuid"]) + ' ret is:' + str(ret))
            return errno.ECONNABORTED

    for subdev in new_export["subdevices"]:
        update_usxmanager_capacity(subdev)

    return rc


def agg_stop(noflush):
    old_exports = load_exports()
    rc = 0

    debug("Stopping ibdserver.")
    cmd_str = "killall -9 ibdserver"
    do_system(cmd_str)
    time.sleep(1)

    for exp_uuid in old_exports:
        export = old_exports[exp_uuid]
        rc = stop_export(export, noflush)
        if rc != 0:
            debug("WARNING: can not stop: %s" % str(exp_uuid))
    return rc


def usage():
    debug('Wrong command: %s' % str(sys.argv))
    print sys.argv[0] + ' <[-c] [-d] [-e] [-S]> <[JSON] [uuid]>. create, export, stop or destroy LV.'
    print '              -c <JSON> create new export, require base64 urlsafe encoded json input'
    print '              -E <JSON> extend an existing export to new size.'
    print '              -d <uuid> delete exist export'
    print '              -e start ALL exports'
    print '              -S [-f] stop all exports, -f for fast stop WITHOUT flush data'
    print '              -s get export size'
    return


def lock():
    fp = open(LCKFILE, "w")
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return False
    fp.write(str(os.getpid()))
    fp.close()
    pid = open(LCKFILE).readline().rstrip()
    debug('PID %s wrote lckfile %s' % (pid, LCKFILE))
    return pid


def unlock():
    if os.path.exists(LCKFILE):
        debug("PID %s unlocking.." % os.getpid())
        os.remove(LCKFILE)


def chkps(cpid):
    out = [' ']
    cmd_str = 'ps -ax  | grep -i %s | grep -v grep | awk \'{print $1}\'' % (cpid)
    rc = do_system(cmd_str, out)
    if out[0] != "":
        return 1
    return 0


def islocked():
    out = [' ']
    pid = 0
    if os.path.isfile(LCKFILE):
        pid = open(LCKFILE).readline().rstrip()
        rc = chkps(pid)
        if (rc):
            return pid
        else:
            debug("Lockfile found with no matching process, unlocking...")
            unlock()
    else:
        debug("No Lock File exists")
    return pid


# Since export, stop, destroy, create and extend
# all reset the zram device any of them running
# at the same time has the potential threat of
# trying to reset zram device one after the other
# Hence wait_if_busy function prevents it
def wait_if_busy():
    rc = islocked()
    while (rc):
        debug('PID %s already in progress, waiting..' % str(rc))
        time.sleep(5)
        rc = islocked()
    lock()
    return 0


def main():
    ##Parse command line args
    rc = 0
    if len(sys.argv) < 2:
        usage()
        return errno.EINVAL
    if sys.argv[1] == '-s':
        exp_dev = sys.argv[2]
        rc = agg_get_size(exp_dev)
    elif sys.argv[1] == '-e':
        # Export all devices or the provided one.
        if len(sys.argv) == 2:
            rc = agg_export()
        elif len(sys.argv) == 3:
            # For shared storage exports on Volume.
            data = sys.argv[2]
            data = base64.urlsafe_b64decode(data)
            debug("Decoded json input:")
            debug(data)

            try:
                export = json.loads(data)
            except:
                debug(traceback.format_exc())
                debug("ERROR: JSON load error!")
            rc = agg_export_one(export)
            if rc != 0:
                debug("ERROR: export failed!")
                return rc
            exp_uuid = export['euuid']
            old_exports = load_exports()
            if old_exports == None:
                old_exports = {}
            if exp_uuid in old_exports:
                debug('INFO: Export %s exist, no need to record. ' % exp_uuid)
                return 0
            old_exports[exp_uuid] = export
            rc = save_exports(old_exports)
            if rc != 0:
                debug("ERROR: Cannot not save updated exports!")
                return errno.EACCES
        else:
            usage()
            return errno.EINVAL
    elif sys.argv[1] == '-S':
        # Stop all devices
        if len(sys.argv) < 2:
            usage()
            return errno.EINVAL
        noflush = False
        if len(sys.argv) == 3:
            if sys.argv[2] == '-f':
                noflush = True

        rc = agg_stop(noflush)
    elif sys.argv[1] == '-d':
        # Delete device
        if len(sys.argv) != 3:
            usage()
            return errno.EINVAL
        exp_name = sys.argv[2]
        rc = agg_destroy(exp_name)
    elif sys.argv[1] in ['-c', '-E']:
        # Create/Extend device
        if len(sys.argv) < 3:
            usage()
            return errno.EINVAL
        if sys.argv[2] == '-f':
            # Load Device JSON from file.
            try:
                f = open(sys.argv[3], 'r')
                data = f.read()
                f.close()
            except:
                debug('Open file error: %s!' % sys.argv[2])
                return (2)
        else:
            # Device json in command line args
            data = sys.argv[2]
            data = base64.urlsafe_b64decode(data)
            debug("Decoded json input:")
            debug(data)

        try:
            new_export = json.loads(data)
            debug(json.dumps(new_export, sort_keys=True, indent=4, separators=(',', ': ')))
        except:
            debug(traceback.format_exc())
            debug("ERROR: JSON load error!")
            return errno.EINVAL

        if sys.argv[1] == '-c':
            rc = agg_create(new_export)
        elif sys.argv[1] == '-E':
            rc = agg_extend(new_export)
        else:
            usage()
            return errno.EINVAL
    else:
        usage()
        return errno.EINVAL
    return (rc)


if __name__ == "__main__":
    rc = 100
    wait_if_busy()
    debug("Entering agexport: ", sys.argv)
    try:
        rc = main()
    except:
        debug(traceback.format_exc())
        rc = 101
    unlock()
    debug("Exiting with: %s" % str(rc))
    exit(rc)
