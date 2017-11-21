#!/usr/bin/python

"""
USX Diskless VDI ILIO bootstrap role script

To setup ILIO for the first time as DISKLESS:

**    precondition: '/usr/share/ilio/configured' file DOES NOT exist

    1. Get the amount of memory available in ILIO

       ilio cache vram_memory --compression=true

       expected return:

       {'vram_available_memory': 22020096, 'compression': True, 'vram_number_of_vms': 21}

       Extract the number for vram_available_memory, this is the amount of memory in kB, you will use this number in the next setup.

    2. Perform DedupFS setup

       ilio cache auto_setup --vram=22020096 --export-type=nfs --device= --compression=true --non-persistent=false --name=ILIO_VirtualDesktops --format-on-boot=false --storage-type=

       the "vram=" parameter must be what the vram_memory command above returned as the "vram_available_memory" parameter ( in the case of above example, it is 22020096)

After ILIO is configured for the first time:

**    precondition: '/usr/share/ilio/configured/ file exists

    Run the steps in "To setup ILIO for the first time as DISKLESS"
    
To reconfigure ILIO (for testing)

    1. Remove the "configured" script
    
       rm -f  /usr/share/ilio/configured

    2. Remove milio log file
    
       rm -f /var/log/usx-milio.log

       This is so that your test run is not contaminated by logs from previous runs

    3. Run the steps in "TO SETUP ILIO FOR FIRST TIME AS DISKLESS ("/usr/share/ilio/configured" DOES NOT EXIST)" as above.
"""

import httplib
import ConfigParser
import json
import os, sys
from pprint import pprint
import subprocess
import socket
#import logging
import time
import re
import ast

sys.path.insert(0, "/opt/milio/")
from libs.cmd import runcmd
from libs.atlas.status_update import * 

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from log import *

# Configuration files
VDI_CFG = '/etc/ilio/atlas.json'

# Commands
CMD_ONLINE_SNAPCLONE = '/opt/milio/scripts/onlinesnap_setup.sh'
CMD_VM_COUNT = '/opt/milio/scripts/checkvmcount_setup.sh'
CMD_GENERATE_PASSWD = '/opt/milio/scripts/iliopasswdchange'
CMD_CHANGE_PASSWD = '/bin/echo -e "%s\\n%s" | passwd poweruser'

# Device
SNAPCLONEDEV = '/dev/sdb'

# Log files
LOG_FILENAME = '/var/log/vdi_diskless_start.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))
''' 

def info(*args):
    msg = " ".join([str(x) for x in args])
    print >> sys.stderr, msg

def usage():
    debug("Usage:" + sys.argv[0] + " config|start")
    debug("      config - configure VDI to be diskless ILIO")
    debug("               auto configure Dedup FS")
    debug("               NFS mount Dedup FS to datastore")
    debug("               start online-snapclone cronjob")
    debug("               start VM count check")
    debug("               change login password")
    debug(" ")
    debug("      start - Assumes this VDI node has been configured")
    debug("              auto configure Dedup FS")

def load_cfg():
    """
    Process Atlas JSON configuration file
    Return value: dictionary of configuration parameters
    """
    cfg_dict = {}
    debug("Load Atlas JSON config file")
    if os.access(VDI_CFG, os.R_OK): # /etc/ilio/atlas.json file exists
        fp = open(VDI_CFG)
        jsondata = json.load(fp)
        cfg_dict['role'] = jsondata.get('roles')[0]
        cfg_dict['uuid'] = jsondata.get('uuid')
        cfg_dict['license'] = jsondata.get('license')
        cfg_dict['amcurl'] = jsondata.get('amcurl')
        fp.close()
    else:
        debug("ERROR : %s is not found on this ILIO" % VDI_CFG)
    return cfg_dict

def setup_dedup_fs():
    """
    Set up Dedup filesystem for diskless VDI
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    vram_size = 62914560 # Fix TISILIO-3188: Hard code /dev/zram0 size to be 60GB

#     # Get the amount of memory available in this ILIO
#     debug("Get the amount of memory available in this ILIO")
#     cmd = 'ilio cache vram_memory --compression=true' # enable compression, vram memory returns 60GB
#     ret, msg = runcmd(cmd, print_ret = True)
#     if ret == 0: 
#         debug("Get vram available memory")
#         output = str(msg.strip())
#         vram_dict = ast.literal_eval(output) # http://stackoverflow.com/questions/988228/converting-a-string-to-dictionary
#         vram_size = vram_dict['vram_available_memory']
#         debug("vram available memory obtained %sKB" % vram_size)
#     else:
#         debug("vram info not found on this ILIO")
#         retVal = ret

    # Perform DedupFS setup
    debug("Set up compressed diskless ilio")
    cmd = 'ilio cache auto_setup --vram=%s --export-type=nfs --device= --compression=true --non-persistent=false --name=ILIO_VirtualDesktops --format-on-boot=false --storage-type=' % vram_size
    ret, msg = runcmd(cmd, print_ret = True)
    if ret != 0:
        debug("ERROR : Error creating filesystem. -- " + msg)
        retVal = ret
    if os.system('mount | grep dedup | grep -v grep') != 0 :
        debug("ERROR : Dedup FS is not mounted! Configuration failed!")
        retVal = 1
        
    return retVal

def nfs_mount_datastore():
    """
    Call REST API to NFS mount the Dedup FS
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    out = ['']
    
    debug("Mount ILIO Dedup FS as an NFS datastore")
    cfg_dict = load_cfg()
    if not cfg_dict: # configuration dictionary is empty
        debug("ERROR : Parsing JSON configuration failed!")
        retVal = 1
    else:
        restapi_url = cfg_dict['amcurl']
        restapi_url += '/iliods/vdidiskless/vmware/export' # construct REST API URL
        json_str = '{\\"datastoreNameOnEsxHost\\":\\"AtlantisILIOFreeTrial-Datastore\\",\\"iliouuid\\":\\"%s\\"}' % cfg_dict['uuid'] # construct JSON string {"iliouuid":"<ilio uuid>"}
        cmd =  r'curl -s -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, restapi_url) 
        rc = do_system(cmd, out)
        if rc != 0:
            debug('REST API call to mount NFS datastore failed!')
            send_status("CONFIGURE", 95, 1, "Bootstrap", 'ERROR : Mount datastore failed', True)
            retVal = rc
        else: # rc == 0
            if out: # list not empty
                status = json.loads(out[0]).get('status') # get return status from curl command call
                if status != 0: # non-zero status, send an event with error message
                    send_status("CONFIGURE", 95, 1, "Bootstrap", 'ERROR : Mount datastore failed', True)
            
        
    return retVal

def check_online_snapclone():
    """
    Add cronjob for online snapclone
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    
    debug("Checking online snapclone")
    rc = do_system(CMD_ONLINE_SNAPCLONE)
    if rc != 0:
        debug('ERROR : online snapclone setup script failed!')
        retVal = rc
        
    return retVal
    
def check_vm_count():
    """
    Add cronjob to check number of VMs allowed
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    debug("Checking VM count")
    rc = do_system(CMD_VM_COUNT)
    if rc != 0:
        debug('ERROR : check vm count script failed!')
        retVal = rc
    
    return retVal

def change_password():
    """
    Change default password to a randomly generated new password
    Return value: 0 successful
                  other unsuccessful
    """   
    retVal = 0
    out = ['']
    cfg_dict = load_cfg()
    if not cfg_dict: # configuration dictionary is empty
        debug("ERROR : Parsing JSON configuration failed")
        retVal = 1
    else:
        debug("Changing poweruser password")
        # Generating new password
        token = cfg_dict['license']
        cmd = CMD_GENERATE_PASSWD + ' ' + token # from /etc/ilio/atlas.json 'license':'eXV3ZWlqQGF0bGFudGlzY29tcHV0aW5nLmNvbQ=='
        debug("--Generating new password...")
        ret, msg = runcmd(cmd, print_ret = True)
        if ret != 0:
            debug("ERROR : Generating new password failed -- " + msg)
            retVal = ret
        password = msg.strip('\n')[-16:] # the last 16 characters of the return value of executing password generation command is the password       
        if '\\' in password:
            password = password.replace('\\', '\\\\\\') # fix ATLANTIS-1959, escape special escape sequences: \a \b \f \n, etc
        if '"' in password:
            password = password.replace('"', '\\"') # fix ATLANTIS-1959, escape the double quote characters in password to ensure change password shell command execute successfully 
        # Change old password to newly generated password
        cmd = CMD_CHANGE_PASSWD % (password, password)
        debug("--Changing password...")
        ret, msg = runcmd(cmd, print_ret = True)
        if ret != 0:
            debug("ERROR : Change password failed -- " + msg)
            retVal = ret
    
    return retVal

def setup_backup_device():
    """
    Partition and format online-snapclone backup disk
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    snapclonedev_partition = SNAPCLONEDEV + '1'
    
    cmd = 'fdisk -l | grep ' + SNAPCLONEDEV
    debug("Checking if snapclone disk partition is presented")
    ret, msg = runcmd(cmd, print_ret = True)
    if ret != 0: 
        debug("WARNING : This seems to be a diskless ILIO, but the snapclone disk DOES NOT exist. If this ILIO is a nonpersistent diskless ILIO, you will not be able to create snapclones on it")
        retVal = 0
    else: # partition exists
        cmd = 'mount | grep ' + snapclonedev_partition
        debug("Checking if snapclone disk is mounted")
        ret, msg = runcmd(cmd, print_ret = True)
        if ret == 0:
            debug("INFO : snapclone partition is already configured and mounted, skipping steps related to partitioning, formatting and mounting snapclone partition.")
            retVal = ret
        else: # partition exist but not mounted
            cmd = '(echo o; echo n; echo p; echo 1; echo ; echo; echo w) | fdisk ' + SNAPCLONEDEV
            debug("Partitioning snapclone device %s" % SNAPCLONEDEV)
            ret, msg = runcmd(cmd, print_ret = True)
            if ret != 0:
                debug("WARNING : Failed to partition the snapclone disk; you will not be able to create snapclone backups")
                retVal = 0
            else: # patitioned and ready to format, using mount point: /mnt/images
                cmd = 'mkdir -p /mnt/images; umount /mnt/images; mke2fs -j -m 1 ' + snapclonedev_partition + ' && mount -t ext3 ' + snapclonedev_partition + ' /mnt/images'
                debug("Format disk as ext3 and mount it, after first unmounting the mount point if it is mounted")
                ret, msg = runcmd(cmd, print_ret = True)
                if ret != 0:
                    debug("WARNING : Failed to format and/or mount the snapclone disk; you will not be able to create snapclone backups")
                    retVal = 0
                else: # disk successfully partitioned, formated and mounted
                    retVal = ret

    return retVal

def mount_backup_device():
    """
    Mount online-snapclone backup disk
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    snapclonedev_partition = SNAPCLONEDEV + '1'
    mount_point = '/mnt/images'
    debug("Mounting backup device %s on mount point %s" % (snapclonedev_partition, mount_point))
    cmd = 'mkdir -p ' + mount_point + '; mount -t ext3 ' + snapclonedev_partition + ' ' + mount_point
    ret, msg = runcmd(cmd, print_ret = True)
    if ret != 0:
        debug("WARNING : Failed to mount backup device %s on mount point %s" % (snapclonedev_partition, mount_point))
        retVal = ret
    return retVal
    
def reset_vmcount_file():
    """
    Reset vm count upon reboot
    Return value: 0 successful
                  other unsucessful
    """
    retVal = 0
    timestamp = time.strftime("%H:%M:%S")
    VMCOUNT_FILE = '/usr/share/ilio/vmcount_new.txt'
    debug("Resetting VM count...")
    # Regardless whether the vmcount_new file exist or not, overwrite the file with one line to reset count and update timestamp
    cmd = "date '+%T.%N'" # calling shell cmd 'date'
    ret, msg = runcmd(cmd, print_ret = True)
    if ret == 0: 
        timestamp = msg.strip()
    try:
        fp = open(VMCOUNT_FILE, 'w+')
    except IOError:
        debug("Faile to open %s to write" % VMCOUNT_FILE)
        retVal = 1
    else:
        wline = 'violation_count=0    date=' + timestamp + ' ESXVMCOUNT=0 XENVMCOUNT=0'
        fp.write(wline)
        fp.close()
        
    return retVal

def output_status_to_console():
    """
    Output results of "top" and "dstat" commands to virtaul console 2 and 3
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0
    #cmd1 = 'nohup top > /dev/tty2 &'
    #cmd1 = 'top >/dev/tty2 2>&1'
    cmd1 = 'openvt -f -c 2 top'
    cmd2 = 'openvt -f -c 3 -- dstat --time --cpu --net --disk --sys --load --proc --top-cpu'
    debug("Output status to tty2 and tty3...")
    rc1 = do_system(cmd1)
    if rc1 != 0:
        debug('ERROR : output status to tty2 failed!')
    rc2 = do_system(cmd2)
    if rc2 != 0:
        debug('ERROR : output status to tty3 failed')
    
    retVal = rc1 & rc2 
    return retVal

def restore_snapclone_backup():
    """
    Run online-snapclone Restore command
    Return value: 0 successful
                  other unsuccessful
    """
    retVal = 0  
#     cmd1 = 'rm -rf /var/log/snapclone_restore.log'
#     debug("Cleanup snapclone restore log")
#     ret, msg = runcmd(cmd, print_ret = False)
#     if ret != 0:
#         debug("ERROR : Remove snapclone log failed")
#     cmd2 = '/opt/milio/scripts/partclone.sh -o restore >> /var/log/snapclone_restore.log 2>&1'
    cmd2 = '/opt/milio/scripts/partclone.sh -o restore'
    debug("Running snapclone restore operation...")
    ret, msg = runcmd(cmd2, print_ret = False)
    if ret != 0:
        debug("ERROR : snapclone restore operation failed")
        retVal = ret

    return retVal

def vdi_config():
    """
    Configure ILIO as Diskless VDI for the first time
        1. Change default ILIO password
        2. Setup Dedup file system
        3. Setup online snapclone disk partition
        4. Configure online snapclone cronjob
        5. Configure VM amount limit checking cronjob
        6. NFS mount Dedup FS as a datastore
        7. Display system status on tty2 and tty3
    """
    debug("Configuring diskless VDI")

    # Change poweruser password
    rc = change_password()
    if rc != 0:
        debug("Changing poweruser password failed")
        sys.exit(1)
   
    # Setup Dedup file system
    rc = setup_dedup_fs()
    if rc != 0:
        debug("Setup Diskless ILIO failed")
        sys.exit(1)
        
    # Setup online snapclone disk partition
    rc = setup_backup_device()
    if rc != 0:
        debug("Setup Online Snapclone backup disk failed")
        sys.exit(1)
        
    # Configure online snapclone
    rc = check_online_snapclone()
    if rc != 0:
        debug("Adding Online Snapclone cronjob failed")
        sys.exit(1)
        
    # Run VM count check
    rc = check_vm_count()
    if rc != 0:
        debug("Adding VM count cronjob failed")
        sys.exit(1)
   
    # Run system status commands
    rc = output_status_to_console()
    if rc != 0:
        debug("Output status to console tty2/tty3 failed")
        # still allow system to bootstrap successfuly if output status fails
        
    # Configure NFS datastore
    rc = nfs_mount_datastore()
    if rc != 0:
        debug("Mount NFS datastore failed")
        # ILIO configuration is still successful even though NFS mount failed; user can manually mount the datastore
        #sys.exit(1)
    
    debug("Diskless VDI has been setup successfully")
    debug("==== END VDI DISKLESS NODE CONFIG/STAT : Successful! ====")
    return 0

def vdi_start():
    """
    If ILIO has been configured 
        1. Setup the Dedup FS for the diskless ILIO
        2. Mount snapclone backup device
        3. Reset VM count persistent file
        4. Display system status on tty2 and tty3
        5. Perform snapclone restore operation
    """
    debug("Starting diskless VDI")
    # Setup Dedup File system
    rc = setup_dedup_fs()
    if rc != 0:
        debug("Setup Diskless ILIO failed")
        sys.exit(1)

    # Mount snapclone backup device
    rc = mount_backup_device()
    if rc != 0:
        debug("Mount snapclone backup device failed")
        # still allow system to bootstrap successfully if mount fails
                
    # Reset VM count file
    rc = reset_vmcount_file()
    if rc != 0:
        debug("Reset vmcount failed")
        # still allow system to bootstrap successfully if reset vm count fails

    # Run system status commands
    rc = output_status_to_console()
    if rc != 0:
        debug("Output status to console tty2/tty3 failed")
        # still allow system to bootstrap successfuly if output status fails
    
    # Snapclone restore operation
    rc = restore_snapclone_backup()
    if rc != 0:
        debug("Snapclone restore operation failed")
        # still allow system to bootstrap successfully if snapclone restore operation fails
    
    debug("Diskless VDI has been setup successfully")
    debug("==== END VDI DISKLESS NODE CONFIG/STAT : Successful! ====")
    return 0

debug("==== BEGIN VDI DISKLESS NODE CONFIG/START ====")

cmd_options = {
    "config",
    "start",
}

if len(sys.argv) < 2:
    usage()
    debug("ERROR: Incorrect number of arguments. Need at least one argument which is either 'config' or 'start'")
    sys.exit(1)

cmd_type = sys.argv[1]
debug(cmd_type)

if cmd_type in cmd_options:
    if cmd_type.lower().strip().startswith('config'):
        debug("Script has been called in 'config' mode, so assuming that we need to do first-time configuration!")
        try:
            rc = vdi_config()
        except:
            debug('An exception has occurred during configuration , exiting....')
            sys.exit(1)
    else:
        debug("Script has been called in 'start' mode, so assuming that first-time configuration is already done!")
        try:
            rc = vdi_start()
        except:
            debug('An exception has occurred during startup, exiting.... ')
            sys.exit(1)
else:
    usage()
    debug("ERROR: Incorrect argument '%s'. Argument has to be either 'config' or 'start'" % cmd_type)
    sys.exit(1)
