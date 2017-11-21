#!/usr/bin/python
"""
USX Disk-backed ILIO VDI role configuration subclass
"""
import os, sys
import logging

from vdiconfig_base import *

class VDIConfigDiskbacked(VDIConfigBase):
    
    def __init__(self, data):
        self.myfullrole = data['role'].lower()
        self.amcurl = data['amcurl']
        self.uuid = data['uuid']
        self.license = data['license']
        self.log = '/var/log/' + 'usx-' + self.myfullrole + '_config.log'
        logging.basicConfig(filename=self.log,level=logging.DEBUG,format='%(asctime)s %(message)s')

        
    def vdi_config(self):
        """
        Configure ILIO as a Disk-backed VDI for the first time
        2. Setup Dedup file system
        6. NFS mount Dedup FS as a datastore
        7. Display system status on tty2 and tty3
        """
        self.debug("Configuring Disk-backed VDI...")
        
            
        # Setup Dedup file system
        rc = self.setup_dedup_fs(self.myfullrole)
        if rc != CONFIG_SUCCESS:
            self.debug("Setup Dedup file system failed! Error code: %s" % rc)
            sys.exit(rc)
 
          
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
        
        self.debug("Diskbacked VDI has been setup successfully")
        self.debug("==== END VDI NODE CONFIG/START : Successful! ====")


    def vdi_start(self):
        """
        If ILIO has been configured 
            1. Setup the Dedup FS for the disk-backed ILIO
            4. Display system status on tty2 and tty3
        """        
        self.debug("Starting Disk-backed VDI...")

        
        # Setup Dedup file system
        rc = self.setup_dedup_fs(self.myfullrole)
        if rc != CONFIG_SUCCESS:
            self.debug("Setup Dedup file system failed! Error code: %s" % rc)
            sys.exit(rc)        
        

        # Display system statuses to virtual consoles
        rc = self.display_status()
        if rc != CONFIG_SUCCESS:
            self.debug("Display system statuses failed!" + 
                       " Error code: %s" % rc)
            # Don't quit, still allow bootstrap to go through
            
            
        self.debug("Diskbacked VDI has been setup successfully")
        self.debug("==== END VDI NODE CONFIG/START : Successful! ====") 
