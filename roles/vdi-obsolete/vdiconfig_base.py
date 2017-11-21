#!/usr/bin/python
"""
USX ILIO VDI role configuration base class


"""
import os, sys
import logging
from subprocess import *
import time
import re
import ast

sys.path.insert(0, "/opt/milio/")
from libs.cmd import runcmd
from libs.atlas.status_update import *

###############################################################################
# Hard coded error return code
#
#
#
###############################################################################

CONFIG_SUCCESS                = 0
MISSING_ATLAS_CONFIG          = 1
INCORRECT_ARGUMENTS           = 2
CONFIGURATION_FAILED          = 3
STARTUP_FAILED                = 4

CONSTRUCT_DEDUPFS_FAILED      = 10
MOUNT_DEDUPFS_FAILED          = 11
MOUNT_DATASTORE_FAILED        = 12
DATA_DISK_NOT_PRESENTED       = 13

SETUP_ONLINESNAPCLONE_FAILED  = 20
SETUP_BACKUP_DEVICE_FAILED    = 21
SETUP_CHECK_VM_COUNT_FAILED   = 22
MOUNT_BACKUP_DEVICE_FAILED    = 23
SNAPCLONE_RESTORE_FAILED      = 24

GENERATE_PASSWD_FAILED        = 30
CHANGE_PASSWD_FAILED          = 31
DISPLAY_STATUS_FAILED         = 33

OPEN_VMCOUNT_FILE_FAILED      = 40  
 
# REST API URL
MOUNT_RESTAPI_URL = '/usxmanager/usxds/vdi/export'
 
# Commands
CMD_GENERATE_PASSWD = '/opt/milio/scripts/iliopasswdchange'
CMD_CHANGE_PASSWD = '/bin/echo -e "%s\\n%s" | passwd poweruser'
CMD_ONLINE_SNAPCLONE = '/opt/milio/scripts/onlinesnap_setup.sh'
CMD_VM_COUNT = '/opt/milio/scripts/checkvmcount_setup.sh'
CMD_SNAPCLONE_RESTORE = '/opt/milio/scripts/partclone.sh -o restore'

# Devices
SNAPCLONEDEV = '/dev/sdb'
MOUNT_POINT = '/mnt/images'

# Files
VMCOUNT_FILE = '/usr/share/ilio/vmcount_new.txt'

class VDIConfigBase(object):
    """
    The VDIConfigBase is the base class for 
     all VDI role configuration script
    """
    def __init__(self, data):
        self.cfgdata = data
        


    def debug(self, *args):
        logging.debug("".join([str(x) for x in args]))
        print("".join([str(x) for x in args]))


     
    def info(self, *args):
        msg = " ".join([str(x) for x in args])
        print >> sys.stderr, msg

###############################################################################
# Set up Dedup file system; NFS mount dedup fs in VM Manager datastore
#
#
#
###############################################################################

    def setup_dedup_fs(self, type, data_disk='/dev/sdb'):
        # may need add non-persistent=true/false flag 
        #  for disk-backed dedup fs configuration
        """
        Setup Dedup file system for ILIO; 
         the command to be used differs depending on the type of ILIO
        """       
        retVal = CONFIG_SUCCESS

        # Get the amount of memory available in this ILIO
        cmd = 'ilio cache vram_memory --compression=true' # enable compression, vram memory returns 60GB
        ret, msg = runcmd(cmd, print_ret = True)
	if ret == 0: 
	    debug("Get vram available memory")
	    output = str(msg.strip())
	    vram_dict = ast.literal_eval(output)
	    vram_size = vram_dict['vram_available_memory']
	    debug("vram available memory obtained %sKB" % vram_size)
	    if vram_size==0:
		debug("vram size is 0")
		return CONSTRUCT_DEDUPFS_FAILED
	else:
	    debug("vram info not found on this ILIO")
	    return ret
        
        if type.lower() == 'vdi_diskless_free':
            self.debug("Set up Free Diskless ILIO...")
    	    vram_size = 62914560
            cmd = (('ilio cache auto_setup --vram=%s --export-type=nfs' + 
                   ' --device= --compression=true --non-persistent=false' +
                   ' --name=ILIO_VirtualDesktops --format-on-boot=false' +
                   ' --storage-type=') % vram_size)
        elif type.lower() == 'vdi_diskless':
            self.debug("Setup Diskless ILIO")
            cmd = (('ilio cache auto_setup --vram=%s --export-type=nfs' + 
                   ' --device= --compression=true --non-persistent=false' +
                   ' --name=ILIO_VirtualDesktops --format-on-boot=false' +
                   ' --storage-type=') % vram_size)
        elif type.lower() == 'vdi_diskbacked_free':
            self.debug("Set up Free Diskbacked ILIO...")
            # non-persistent=true for vScaler in write back mode
            cmd = (('ilio cache auto_setup --name=ILIO_VirtualDesktops' +
                   ' --device=%s --storage-type=local --export-type=nfs' + 
                   ' --non-persistent=false')
                   % data_disk) 
        elif type.lower() == 'vdi_diskbacked':
            self.debug("Setup Disk-backed ILIO")
            cmd = (('ilio cache auto_setup --name=ILIO_VirtualDesktops' +
                   ' --device=%s --storage-type=local --export-type=nfs' + 
                   ' --non-persistent=false')
                   % data_disk) 

        self.debug(cmd)        
        ret, msg = runcmd(cmd)
        self.debug(msg)
        if ret != 0:
            self.debug("ERROR : Failed to create Dedup file system!")
            retVal = CONSTRUCT_DEDUPFS_FAILED
        if os.system('mount | grep dedup | grep -v grep') != 0:
            self.debug("ERROR : Dedup file system is not mounted!")
            retVal = MOUNT_DEDUPFS_FAILED
        
        return retVal


    
    def mount_nfs_datastore(self, type, url, uuid):
        """
        Inovke REST API to NFS mount the Dedup file system
        """
        retVal = CONFIG_SUCCESS
        api_url = url + MOUNT_RESTAPI_URL
        json_str = ""
        cmd = ""
                
        self.debug('Mount NFS datastore in VM Manager...')
	"""
        if type.lower() == 'vdi_diskless_free':
            # construct JSON string with hard coded datastore name
            json_str = (('{\\"datastoreNameOnEsxHost\\":' + 
                         '\\"AtlantisILIOFreeTrial-Datastore\\",' + 
                         '\\"iliouuid\\":\\"%s\\"}') % uuid) 
            cmd =  ((r'curl -s -k -X POST -H "Content-Type:application/json"' + 
                     ' -d "%s" %s') % (json_str, api_url))
        elif type.lower() == 'vdi_diskless':
            json_str = (('{\\"datastoreNameOnEsxHost\\":' + 
                         '\\"AtlantisILIO-Datastore\\",' + 
                         '\\"iliouuid\\":\\"%s\\"}') % uuid) 
        elif type.lower() == 'vdi_diskbacked':
            json_str = (('{\\"datastoreNameOnEsxHost\\":' + 
                         '\\"AtlantisILIO-Datastore\\",' + 
                         '\\"iliouuid\\":\\"%s\\"}') % uuid) 
        elif type.lower() == 'vdi_diskbacked_free':
            json_str = (('{\\"datastoreNameOnEsxHost\\":' + 
                         '\\"AtlantisILIOFreeTrial-Datastore\\",' + 
                         '\\"iliouuid\\":\\"%s\\"}') % uuid) 
	"""

        json_str = (('{\\"iliouuid\\":\\"%s\\"}') % uuid) 
        cmd =  ((r'curl -s -k -X POST -H "Content-Type:application/json"' + 
                     ' -d "%s" %s') % (json_str, api_url))
        
        self.debug(cmd)
        ret, msg = runcmd(cmd)
        self.debug(msg)
        if ret != 0:
            self.debug("ERROR : REST API call to mount datastore" + 
                       " in VM Manager failed!")
            send_status("CONFIGURE", 95, 1, "Bootstrap", 
                        "ERROR! Mount datastore failed", True)
            retVal = MOUNT_DATASTORE_FAILED
        else: # REST API call executed successfully, need check the 
              # return status for the datastore mount operation
              # get the returned status from REST API call
            status = json.loads(msg).get('status') 
            if status != 0: # non-zero return status, indicates error
                send_status("CONFIGURE", 95, 1, "Bootstrap", 
                            "ERROR! Mount datastore failed", True)
                retVal = MOUNT_DATASTORE_FAILED
                
        return retVal



###############################################################################
# Online snapclone related operations
#
#
#
###############################################################################

    def check_online_snapclone(self):
        """
        Add cronjob to perform online snapclone backup
        """
        retVal = CONFIG_SUCCESS
        
        self.debug("Running online snapclone setup...")
        ret, msg = runcmd(CMD_ONLINE_SNAPCLONE)
        self.debug(msg)
        if ret != 0:
            self.debug("ERROR : Setup online snapclone cronjob failed!")
            retVal = SETUP_ONLINESNAPCLONE_FAILED

        return retVal
    


    def setup_backup_device(self):
        """
        Partition and format online snapclone backup disk
        """ 
        retVal = CONFIG_SUCCESS
        snapclonedev_partition = SNAPCLONEDEV + '1'
        
        cmd = 'fdisk -l | grep ' + SNAPCLONEDEV
        self.debug("Checking if snapclone disk partition is presented...")
        self.debug(cmd)
        ret, msg = runcmd(cmd)
        self.debug(msg)
        if ret != 0:
            self.debug("WARNING : This seems to be a diskless ILIO," +
                       " but the snapclone disk DOES NOT exist. If this" + 
                       " ILIO is a non-persistent diskless ILIO, you will" +
                       " not be able to create snapclones on it.")
            retVal = SETUP_BACKUP_DEVICE_FAILED
        else: # partition exists
            cmd = 'mount | grep ' + snapclonedev_partition
            self.debug("Checking if snapclone disk is mounted...")
            self.debug(cmd)
            ret, msg = runcmd(cmd)
            self.debug(msg)
            if ret != 0: # partition exists but not mounted
                cmd = ('(echo o; echo n; echo p; echo 1; echo; echo; echo w)' + 
                       ' | fdisk ' + SNAPCLONEDEV)
                self.debug("Partitioning snapclone device %s..." % SNAPCLONEDEV)
                self.debug(cmd)
                ret, msg = runcmd(cmd)
                self.debug(msg)
                if ret != 0:
                    self.debug("WARNING : Failed to partition the snapclone;" +
                               " you will not be able to create" +
                               " snapclone backups.")
                    retVal = SETUP_BACKUP_DEVICE_FAILED
                else: # partitioned and ready to be formatted
                    cmd = ('mkdir -p /mnt/images; umount /mnt/images;' +
                           ' mke2fs -j -m 1 ' + snapclonedev_partition + 
                           ' && mount -t ext3 ' + snapclonedev_partition + 
                           ' /mnt/images')
                    self.debug("Formatting disk as ext3 and mount it, after" +
                               " first umounting the mount point /mnt/images" + 
                               " if it is mounted...")
                    self.debug(cmd)
                    ret, msg = runcmd(cmd)
                    self.debug(msg)
                    if ret != 0:
                        self.debug("WARNING : Failed to format and/or mount" +
                                   " the snapclone disk; you will not be" +
                                   " able to create snapclone backups.")
                        retVal = SETUP_BACKUP_DEVICE_FAILED
            else: 
                self.debug("INFO : snapclone partition is already configured" + 
                           " and mounted, skipping steps related to" +
                           " partitioning, formatting and mounting snapclone" +
                           " partition.")
        
        return retVal 



    def mount_backup_device(self):
        """
        Mount online snapclone backup disk
        """
        retVal = CONFIG_SUCCESS
        snapclonedev_partition = SNAPCLONEDEV + '1'
        
        self.debug("Mounting backup device %s on mount point %s..." % 
                   (snapclonedev_partition, MOUNT_POINT))
        cmd = ('mkdir -p ' + MOUNT_POINT + '; mount -t ext3 ' + 
               snapclonedev_partition + ' ' + MOUNT_POINT)
        self.debug(cmd)
        ret, msg = runcmd(cmd)
        self.debug(msg)
        if ret != 0:
            self.debug("WARNING : Failed to mount backup device %s on" + 
                       " mount point %s" % (snapclonedev_partition, 
                                            MOUNT_POINT))
            retVal = MOUNT_BACKUP_DEVICE_FAILED
        
        return retVal



    def restore_snapclone_backup(self):
        """
        Perform online snapclone restore operation
        """
        retVal = CONFIG_SUCCESS
        self.debug("Running snapclone restore operation...")
        self.debug(CMD_SNAPCLONE_RESTORE)
        ret, msg = runcmd(CMD_SNAPCLONE_RESTORE)
        #self.log(msg)
        if ret != 0:
            self.debug("ERROR : snapclone restore operation failed!")
            retVal = SNAPCLONE_RESTORE_FAILED
            
        return retVal



###############################################################################
# VM count restriction related operations
#
#
#
###############################################################################

    def check_vm_count(self):
        """
        Add cronjob to enforce the restriction on number 
         of VMs allowed in the datastore
        """
        retVal = CONFIG_SUCCESS
         
        self.debug("Setup VM count checking...") 
        ret, msg = runcmd(CMD_VM_COUNT)
        self.debug(msg)
        if ret != 0:
            self.debug("ERROR : Setup check vm count cronjob failed!")
            retVal = SETUP_CHECK_VM_COUNT_FAILED
         
        return retVal
 
 
 
    def reset_vm_count(self):
        """
        Reset VM count in file upon reboot, regarless file exists or not
        """
        retVal = CONFIG_SUCCESS
         
        timestamp = time.strftime("%H:%M:%S")
         
        self.debug("Resetting VM count...")
        cmd = "date '+%T.%N'"
        ret, msg = runcmd(cmd)
        if ret == 0:
            timestamp = msg.strip()
        try:
            fp = open(VMCOUNT_FILE, 'w+')
        except IOError:
            self.debug("ERROR : Failed to open %s to write!" % 
                       (VMCOUNT_FILE))
            retVal = OPEN_VMCOUNT_FILE_FAILED
        else:
            wline = ("violation_count=0    date=" + timestamp + 
                     " ESXVMCOUNT=0 XENVMCOUNT=0")
            fp.write(wline)
            fp.close()
         
        return retVal 



###############################################################################
# Miscellaneous bootstrap related operations
#
#
#
###############################################################################

    def change_password(self, token):
        """
        Change default system password to a randomly generated 
         new password based on user registration code
         
         from /etc/ilio/atlas.json 
         SAMPLE: 'license':
           'QXRsYW50aXNJTElPX2hpZGVub3JpdEBhdGxhbnRpc2NvbXB1dGluZy5jb20='; 
         base 64 enconding of "AtlantisILIO_user@company.com"
        """
        retVal = CONFIG_SUCCESS
        
        self.debug("Changing poweruser password...")
        cmd = CMD_GENERATE_PASSWD + ' ' + token 
        self.debug("---Generating new password...")
        #self.debug(cmd)
        ret, msg = runcmd(cmd)
        #self.debug(msg)
        if ret != 0:
            self.debug("ERROR : Generating new password failed!")
            retVal = GENERATE_PASSWD_FAILED
        password = msg.strip('\n')[-16:] # the last 16 characters of the 
                                         #  return value of executing 
                                         #  the password generation command 
                                         #  is the actual password
        if '\\' in password:
            # fix ATLANTIS-1959, escape special escape sequences: 
            # \a \b \f \n, etc
            password = password.replace('\\', '\\\\\\') 
        if '"' in password:
            # fix ATLANTIS-1959, escape the double quote characters in password
            # to ensure change password shell command execute successfully 
            password = password.replace('"', '\\"') 
        cmd = CMD_CHANGE_PASSWD % (password, password)
        self.debug("---Changing default password to the generated on...")
        #self.debug(cmd)
        ret, msg = runcmd(cmd)
        #self.debug(msg)
        if ret != 0:
            self.debug("ERROR : Changing password failed!")
            retVal = CHANGE_PASSWD_FAILED
        
        return retVal


    
    def display_status(self):
        """
        Output results of "top" and "dstat" commands to 
         virtual terminal 2 and virtual terminal 3
        """ 
        retVal = CONFIG_SUCCESS

        cmd1 = 'openvt -f -c 2 top'
        cmd2 = ('openvt -f -c 3 -- dstat --time --cpu --net --disk --sys' + 
                ' --load --proc --top-cpu')
        
        self.debug("Output system statuses to tty2 and tty3...")
        self.debug(cmd1)
        ret, msg = runcmd(cmd1)
        self.debug(msg)
        if ret != 0:
            self.debug("ERROR : output \"top\" to tty2 failed")
            retVal = DISPLAY_STATUS_FAILED
        self.debug(cmd2)
        ret, msg = runcmd(cmd2)
        self.debug(msg)
        if ret != 0:
            self.debug("ERROR : output \"dstat\" to tty3 failed")
            retVal = DISPLAY_STATUS_FAILED
        
        return retVal



###############################################################################
# Role configuration operations
#
#
#
###############################################################################
              
    def vdi_config(self):
        """
        VDI initial configuration method
        """
        raise NotImplementedError()


    
    def vdi_start(self):
        """
        VDI subsequent start method, called if VDI is configured
        """
        raise NotImplementedError()


        
    def usage(self):
        self.debug("Usage:" + sys.argv[0] + " config|start")
        self.debug("      config - Configure the ILIO VDI for the first time")
        self.debug(" ")
        self.debug("      start - Assumes this ILIO VDI" +
                   " has been configured, start it up")
        self.debug(" ")
        

    
    def entry(self):
        self.debug("==== BEGIN VDI NODE CONFIG/START ====")
        
        cmd_options = {
            "config",
            "start",
        }
        
        if len(sys.argv) < 2:
            self.usage()
            self.debug("ERROR: Incorrect number of arguments. Need at least" + 
                       " one argument which is either 'config' or 'start'")
            sys.exit(INCORRECT_ARGUMENTS)
        
        cmd_type = sys.argv[1]
        #self.debug(cmd_type)
        
        if cmd_type in cmd_options:
            if cmd_type.lower().strip().startswith('config'):
                self.debug("Script has been invoked in 'config' mode," + 
                    " so assuming that we need to do first-time configuration!")
                try:
                    self.vdi_config()
                except:
                    self.debug("An exception has occurred" + 
                        " during configuration, exiting...")
                    sys.exit(CONFIGURATION_FAILED)
            else:
                self.debug("Script has been invoked in 'start' mode," +
                    " so assuming that the first-time configuration" +
                    " is already done!")
                try:
                    self.vdi_start()
                except:
                    self.debug("An exception has occurred during startup," +
                        " exiting...")
                    sys.exit(STARTUP_FAILED)
        else:
            self.usage()
            self.debug(("ERROR: Incorrect argument '%s'. Argument has to be" +
                " either 'config' or 'start'") % cmd_type)
            sys.exit(INCORRECT_ARGUMENTS)
