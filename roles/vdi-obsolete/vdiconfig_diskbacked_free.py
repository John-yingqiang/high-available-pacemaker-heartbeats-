#!/usr/bin/python
"""
USX Free Disk-backed ILIO VDI role configuration subclass

"""
import os, sys
import logging
from subprocess import *

from vdiconfig_base import *

class VDIConfigDiskbackedFree(VDIConfigBase):
    
    def __init__(self, data):
        self.myfullrole = data['role'].lower()
        self.amcurl = data['amcurl']
        self.uuid = data['uuid']
        self.token = data['license']
        self.log = '/var/log/' + 'usx-' + self.myfullrole + '_config.log'
        logging.basicConfig(filename=self.log,level=logging.DEBUG,format='%(asctime)s %(message)s')



    def vdi_config(self):
        """
        Configure ILIO as a Free Disk-backed VDI for the first time
        1. Change default ILIO password
        2. Setup Dedup file system
        5. Configure VM amount limit checking cronjob
        6. NFS mount Dedup FS as a datastore
        7. Display system status on tty2 and tty3        
        """
        self.debug("Entering Free Diskbacked VDI Configuration")
        
        
        # Change poweruser password
        rc = self.change_password(self.token)
        if rc != CONFIG_SUCCESS:
            self.debug("Change password failed! Error code: %s" % rc)
            sys.exit(rc)
        
        # Setup Dedup file system
        rc = self.setup_dedup_fs(self.myfullrole)
        if rc != CONFIG_SUCCESS:
            self.debug("Setup Dedup file system failed! Error code: %s" % rc)
            sys.exit(rc)
 
        # Configure VM count checking 
        rc = self.check_vm_count()
        if rc != CONFIG_SUCCESS:
            self.debug("Configure VM count checking cronjob failed!" + 
                       " Error code: %s" % rc)
            sys.exit(rc)
          

        # Display system statuses to virtual consoles
        rc = self.display_status()
        if rc != CONFIG_SUCCESS:
            self.debug("Display system statuses failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
 
        # Mount NFS datastore
        rc = self.mount_nfs_datastore(self.myfullrole, self.amcurl, self.uuid)
        if rc != CONFIG_SUCCESS:
            self.debug("Mount NFS datastore in VM Manager failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, notification of failure has been sent to UI
            #  user can manually mount the datastore 
        
        self.debug("Free Disk-backed VDI has been setup successfully")
        self.debug("==== END VDI NODE CONFIG/STAT : Successful! ====")


    
    def vdi_start(self):
        """
        If ILIO has been configured as a Free Diskless VDI
        1. Setup Dedup file system
        	2. Mount snapclone backup device
        3. Reset VM count file
        4. Display system status on tty2 and tty3
        	5. Perform snapclone restore operation
        """
        self.debug("Entering Free Diskbacked VDI Start Up...")
        
        # Setup Dedup file system
        rc = self.setup_dedup_fs(self.myfullrole)
        if rc != CONFIG_SUCCESS:
            self.debug("Setup Dedup file system failed! Error code: %s" % rc)
            sys.exit(rc)        
        
        # Reset VM count file
        rc = self.reset_vm_count()
        if rc!= CONFIG_SUCCESS:
            self.debug("Reset VM count file failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
        
        # Display system statuses to virtual consoles
        rc = self.display_status()
        if rc != CONFIG_SUCCESS:
            self.debug("Display system statuses failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
            
            
        self.debug("Free Diskbacked VDI has been setup successfully")
        self.debug("==== END VDI NODE CONFIG/START : Successful! ====") 
