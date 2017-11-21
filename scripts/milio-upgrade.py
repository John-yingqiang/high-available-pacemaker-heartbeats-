#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
"""
For svm/vm side upgrading, the script will be called by upgrading script.
"""
import os
import sys 
import os.path
sys.path.insert(0, "/opt/milio/libs/atlas")
from cmd import *
from atl_util import *
from log import *
sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *
BB_FILE_SIZE = 4294967296
BB_FILE_PATH = "/bufdevice"
BB_FILE_PATH_BK = "/bufdevice_orig"

UPGREP_VERSION = '/etc/ilio/snapshot-version'

def update_lvm_conf():
    debug("Enter update_lvm_conf")
    if is_new_simple_hybrid():
        debug('This is a new simple hybrid setup , not doing procedure of skip lv /dev/sdb!')
        return 0
    if not os.path.isfile("/etc/lvm/lvm.conf.orig"):
        debug("milio_upgrade: Setting up filter in /etc/lvm.lvm.conf")
        runcmd("cp /etc/lvm/lvm.conf /etc/lvm/lvm.conf.orig")
        #Updating /etc/lvm/lvm.conf
        runcmd("sed 's!^\s*filter.*!    filter = [\"a|/dev/sda|\", \"a|/dev/xvda|\", \"a|/dev/mapper|\", \"r/.*/\" ]!g' /etc/lvm/lvm.conf.orig >/etc/lvm/lvm.conf")
        #Update initramfs
        runcmd("update-initramfs -u -k `uname -r`")
        debug("milio-upgrade: initramfs updated")
    else:
        # Make sure we have correct filter
        (ret, msg) = runcmd('grep \'filter = \["a|/dev/sda|", \"a|/dev/xvda|\", "a|/dev/mapper|", "r/.*/" \]\' /etc/lvm/lvm.conf ')
        if msg == "":
            debug("milio-upgrade: filter in /etc/lvm/lvm.conf incorrect. Changing...")
            runcmd("cp /etc/lvm/lvm.conf /etc/lvm/lvm.conf.orig")
            #Updating /etc/lvm/lvm.conf
            runcmd("sed 's!^\s*filter.*!    filter = [\"a|/dev/sda|\", \"a|/dev/xvda|\", \"a|/dev/mapper|\", \"r/.*/\" ]!' /etc/lvm/lvm.conf.orig >/etc/lvm/lvm.conf")
            #Update initramfs
            runcmd("update-initramfs -u -k `uname -r`")
            debug("vol_init: initramfs updated")

    return 0

def remove_buf_device():
    debug("Enter remove_buf_device")
    debug("Stop ibdserver")
    cmd_stop_ibdserver = "/bin/ibdmanager -r s -S"
    rc = do_system(cmd_stop_ibdserver)
    if rc != 0:
        debug("Stop ibdserver failed")
        return 1
    debug("Remove bufdevice file")
    cmd_remove_bb_file = " ".join(["/bin/mv", BB_FILE_PATH, BB_FILE_PATH_BK])
    rc = do_system(cmd_remove_bb_file)
    if rc != 0:
        debug("Remove bufdevice file failed")
        return 1
    debug("Create new bufdevice file")
    cmd_str_create_bb_file = 'fallocate -l %s %s' % ( str(BB_FILE_SIZE), BB_FILE_PATH)
    rc = do_system(cmd_str_create_bb_file)
    if rc != 0:
        debug("Create new bufdevice file failed")
        return 1

    debug("Start ibdserver")
    cmd_str_start_ibdserver = "/bin/ibdserver"
    rc = do_system(cmd_str_start_ibdserver)
    if rc != 0:
        debug("Start ibdserver failed")
        return 1

    return rc

if __name__ == "__main__":
    debug("Enter milio-upgrade")
    # Check whether it's simple hybrid setup first.
    configure = load_usx_conf()
    if configure == None:
        debug("Load configure error!")
        sys.exit(1)
    if configure['volumeresources'][0]['volumetype'].upper() == "SIMPLE_HYBRID":
        rc = do_system('vgdisplay dedupvg')
        if rc == 0:
            rc = update_lvm_conf()
            if rc == 0 and not is_snapshot_enabled(configure) and os.path.exists(BB_FILE_PATH):
                cmd="ls -l /dev/mapper | grep dm- | awk '{print $11, $9;}' | sort | cut -c 7-"
                (res, msg) = runcmd(cmd)
                ln=msg.strip("\n").split("\n")
                if len(ln) == 4:
                    (num, vl) = ln[3].split(" ")
                    if vl != "dedupvg-deduplv":
                        debug("ERROR: wrong volume sequence!")
                        rc = remove_buf_device()   
                    if rc != 0:
                        debug("Milio-upgrade failed")
                        sys.exit(1)
            else:
                debug("Update lvm conf fail, or snapshot enabled, or not use ibd on this setup")
    sys.exit(0)     
