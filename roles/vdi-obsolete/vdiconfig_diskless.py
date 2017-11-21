#!/usr/bin/python
"""
USX Diskless ILIO VDI role configuration subclass
"""
import os, sys
import logging

from vdiconfig_base import *

class VDIConfigDiskless(VDIConfigBase):
    
    def __init__(self, data):
        self.myfullrole = data['role'].lower()
        self.amcurl = data['amcurl']
        self.uuid = data['uuid']
        self.license = data['license']
        self.log = '/var/log/' + 'usx-' + self.myfullrole + '_config.log'
        logging.basicConfig(filename=self.log,level=logging.DEBUG,format='%(asctime)s %(message)s')
        
        
        
    def vdi_config(self):
        """
        Configure ILIO as a Free Diskless VDI for the first time
        1. Change default ILIO password
        2. Setup Dedup file system
        3. Setup online snapclone disk partition
        4. Configure online snapclone cronjob
        6. NFS mount Dedup FS as a datastore
        7. Display system status on tty2 and tty3
        """
        self.debug("Configuring Diskless VDI...")
        

        # Setup Dedup file system
        rc = self.setup_dedup_fs(self.myfullrole)
        if rc != CONFIG_SUCCESS:
            self.debug("Setup Dedup file system failed! Error code: %s" % rc)
            sys.exit(rc)
 
        # Configure online snapclone            
        rc = self.check_online_snapclone()
        if rc != CONFIG_SUCCESS:
            self.debug("Configure online snapclone cronjob failed!" + 
                       " Error code: %s" % rc)
            sys.exit(rc)

        # Setup online snapclone disk partition
        rc = self.setup_backup_device()
        if rc != CONFIG_SUCCESS:
            self.debug("Setup backup disk failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through

        # Display system statuses to virtual consoles
        rc = self.display_status()
        if rc != CONFIG_SUCCESS:
            self.debug("Display system statuses failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
 
        # Mount NFS datastore
	"""
        rc = self.mount_nfs_datastore(self.myfullrole, self.amcurl, self.uuid)
        if rc != CONFIG_SUCCESS:
            self.debug("Mount NFS datastore in VM Manager failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, notification of failure has been sent to UI
            #  user can manually mount the datastore 
	"""
        
        self.debug("Free Diskless VDI has been setup successfully")
        self.debug("==== END VDI NODE CONFIG/START : Successful! ====")

    def vdi_start(self):
        """
        If ILIO has been configured 
            1. Setup the Dedup FS for the diskless ILIO
            2. Mount snapclone backup device
            4. Display system status on tty2 and tty3
            5. Perform snapclone restore operation
        """        
        self.debug("Starting Diskless VDI...")

        # Setup Dedup file system
        rc = self.setup_dedup_fs(self.myfullrole)
        if rc != CONFIG_SUCCESS:
            self.debug("Setup Dedup file system failed! Error code: %s" % rc)
            sys.exit(rc)        
        
        # Mount snapclone backup device
        rc = self.mount_backup_device()
        if rc != CONFIG_SUCCESS:
            self.debug("Mount snapclone backup disk failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
        
        # Display system statuses to virtual consoles
        rc = self.display_status()
        if rc != CONFIG_SUCCESS:
            self.debug("Display system statuses failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
            
        # Perfrom snapclone restore operation
        rc = self.restore_snapclone_backup()
        if rc != CONFIG_SUCCESS:
            self.debug("Snapclone restore operation failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through 
            
        self.debug("Free Diskless VDI has been setup successfully")
        self.debug("==== END VDI NODE CONFIG/START : Successful! ====") 
