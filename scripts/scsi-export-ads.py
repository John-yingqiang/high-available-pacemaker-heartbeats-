#! /usr/bin/env python
'''
Configure a new SCSI export on a fresh ADS volume DedupFS

OR

Start an existing configuration.

Author: Kartikeya Iyer

See the usage() function for more details and documentation on this script.

'''
import os,sys
import json
from types import *

sys.path.insert(0, "/opt/milio/")
from libs.exporters import scst as scsi
from libs.atlas.cmd import runcmd

# Import the atlas common log. This logs to /var/log/usx-milio-atlas.log
from libs.atlas.log import debug

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *

MYNAME=sys.argv[0]

# The globals which hold the command line args
DEDUP_MNT=None
IQN=None
NEEDS_CONFIG=None

# All IQNs passed in must have this prefix. 
# The IQN suffix will be the unique identifier for the
# ADS volume exported by this ILIO.
VALID_IQN_PREFIX="iqn.com.atlantiscomputing.usx:"

# Other globals
DEDUP_MNT_SIZE=None
LUN_FILENAME="LUN1"
SCSI_CONFIG_FILENAME="/etc/scst.conf"
VOL_CFG = "/etc/ilio/atlas.json"

#
# Helpful message for the clueless
#
def usage():
	debug("Usage: " + MYNAME + " <DEDUP_FILESYSTEM_MOUNTPOINT> <IQN> <CONFIGURATION_NEEDED>")
	debug("  Creates a SCSI LUN export of a mounted Dedup file system.")
	debug("  The Dedup file system is exported as a single LUN, the size")
	debug("  of which is the size of the entire file system. The SCSI")
	debug("  export is a fileio export.")
	debug("         ")
	debug("  This script logs messages in /var/log/usx-milio-atlas.log")
	debug("         ")
	debug("  Options:")
	debug("        DEDUP_FILESYSTEM_MOUNTPOINT : The full path to the mount point")
	debug("           of the ADS volume's dedup filesystem which needs to be exported.")
	debug("           This argument is mandatory.")
	debug("         ")
	debug("         IQN : A string for the IQN which identifies  the LUN to be")
	debug("           created on DEDUP_FILESYSTEM_MOUNTPOINT. ")
	debug("           The IQN _must_ have the following prefix:")
	debug("              iqn.com.atlantiscomputing.flexcloud:")
	debug("           This argument is mandatory.")
	debug("         ")
	debug("         CONFIGURATION_NEEDED : Whether or not to perform first-time config.")
	debug("            Governs whether or not we want to configure a new")
	debug("            SCSI export on a fresh ADS volume, or start an already ")
	debug("            configured SCSI export on an existing ADS volume.")
	debug("            Values:")
	debug("                   True : Configure a new SCSI export and assume that the")
	debug("                          ADS volume has been freshly created. Using this")
	debug("                          option overwrites any existing data in any ")
	debug("                          existing LUN on the ADS dedupFS volume.")
	debug("                   ")
	debug("                   False: Assume that the SCSI export has already been ")
	debug("                          configured, and just start the SCSI export")
	debug("         ")

'''
#
# loads a json file in a dictionary
# code does not care about the conversion of some of the keys, as in the load_conf
# function at atlas/roles/ads/ads-load.py
#
def _load_conf(fname, setup_info):
	#
	# retrieve all configuration info from a Json file 
	#
	try:
		cfg_file = open(fname, 'r')
		cfg_str = cfg_file.read()
		cfg_file.close()
		setup_info['configure'] = json.loads(cfg_str)

	except:
		debug("CAUTION: Cannot load configure json file:", fname)
		return 1
	return 0
'''

#
# Gets the command-line arguments passed in (if any), and validates whether
# argument 1 is a valid dedup file system mount point or not. Argument 2 is
# the IQN for the LUN to be created.
# 
# If this function is successful in getting and verifying the dedup
# mountpoint, it sets the DEDUP_MNT global. Otherwise DEDUP_MNT is None.
#
# If this function is successful in getting the IQN, it sets the IQN
# global variable. If unsuccessful, IQN remains None.
#
def get_and_validate_commandline_args():
	global DEDUP_MNT
	global IQN
	global NEEDS_CONFIG
	try:
		###### Get the mountpoint from the command line
		dedup_mtpt = sys.argv[1]

		if dedup_mtpt is None:
			debug("ERROR : No valid dedup mount point found, got null value. ")
			return

		dedup_mtpt = dedup_mtpt.strip()
		if not dedup_mtpt:
			debug("ERROR : No valid dedup mount point parameter found, string empty. ")
			return

		cmd = "mount | grep " + dedup_mtpt + " | egrep -i 'dedup|zfs|ext4|btrfs' "
		ret, msg = runcmd(cmd, print_ret=True)
		if ret != 0:
			debug("ERROR : Mount point '"+dedup_mtpt+"' does not seem to be a valid dedup mount point. ")
			return

		# If we got here, the passed-in mount point is a valid DedupFS mount point.
		DEDUP_MNT = dedup_mtpt
		debug("Getting and validating dedup FS mountpoint succeeded for: "+DEDUP_MNT)

		###### Get the string for the IQN. 
		lun_iqn = sys.argv[2]

		if lun_iqn is None:
			debug("ERROR : No valid IQN parameter passed, got null value. ")
			return

		lun_iqn = lun_iqn.strip()
		if not lun_iqn:
			debug("ERROR : No valid IQN parameter found, string empty. ")
			return

		if not lun_iqn.startswith(VALID_IQN_PREFIX):
			debug("ERROR : Invalid IQN prefix in IQN parameter, expecting IQN to start with: "+VALID_IQN_PREFIX)
			return

		# if we got here, the passed-in IQN string is valid.
		IQN = lun_iqn
		debug("IQN String to be used for SCSI export of LUN: "+IQN)

		### Get whether we need to be configured or not
		config_needed = sys.argv[3]
		if config_needed is None:
			debug("ERROR : Null parameter passed in for NEEDS_CONFIG. This is a mandatory parameter.")
			return
		config_needed = config_needed.strip()
		if not config_needed:
			debug("ERROR : Empty string passed in for NEEDS_CONFIG. This is a mandatory parameter.")
			return

		config_needed = config_needed.lower()

		yes_config = {
			"true",
			"yes",
			"1",
			"config",
			"configure",
		}

		no_config = {
			"false",
			"no",
			"0",
			"start",
		}

		if (config_needed not in yes_config) and (config_needed not in no_config):
			debug("ERROR : Invalid argument '"+config_needed+"' passed in for NEEDS_CONFIG. Needs to be 'True' or 'False'.")
			return

		if config_needed in yes_config:
			debug("Setting NEEDS_CONFIG to True as per user request")
			NEEDS_CONFIG = True
		else:
			debug("Setting NEEDS_CONFIG to False as per user request")
			NEEDS_CONFIG = False

	except:	
		debug("ERROR : Exception validating command line arguments.")
		DEDUP_MNT=None
		IQN=None
		return



#
# Get the available size, in MB, of a validated DedupFS mount point.
#
# This command uses the 'df -m' command to get the available space,
# in MB, of the given dedup mountpoint. In the output of df, the
# available space is field 4.
#
# After getting a valid size in MB, this function subtracts a certain
# safety factor (which can be specified as a parameter to this function)
# from the reported available space, and returns this number as the 
# available space in MB. This is for safety reasons, to make sure 
# that there is no issue due to rounding errors and 1000 vs 1024
# in the calculation of the MB size. The default safety factor is 2 MB.
# Pass in 0 for this parameter to use the full reported available size.
#
# Parameters:
#		mtpt: Full path to dedupFS mountpoint for which we need the avail size
#
#		safety_factor_MB : The number of MB to subtract from the reported
#			available size, to allow for rounding errors and 1000 vs 1024
#			calculations. Defaults to 2.
#
# Returns:
#		Available Size in MB (minus 2MB) of the Dedup FS mountpoint
#		Python None object on errors
#
def get_mountpoint_size(mtpt, safety_factor_MB=2):
        try:
		if mtpt is None:
			debug("ERROR : No valid dedup mount point passed to size get function. ")
			return None

		mtpt = mtpt.strip()
		if not mtpt:
			debug("ERROR : Empty string for dedup mount point passed to size get function. ")
			return None

		debug("Running size get for dedup mountpoint: "+mtpt)
		cmd = "df -m | grep "+mtpt+" | awk '{print $4}'"
		ret, msg = runcmd(cmd, print_ret = True)
		if ret != 0:
			debug("ERROR : Size check for '"+mtpt+"' returned error code: "+str(ret))
			return None

		if msg is None:
			debug("ERROR : Size check for '"+mtpt+"' returned null data: "+str(ret))
			return None

		msg = msg.strip()

		debug("MSG returned from size check: "+msg)
		size_long = long(msg)
		debug("size in MB from size check: "+str(size_long))

		# Reduce the size received by the specified safety factor
		debug("Safety factor (MB) : "+str(safety_factor_MB))
		size_long -= safety_factor_MB
		debug("size in MB from size check after subtract of safety factor: "+str(size_long))

		if (size_long <= 1024):
			debug("ERROR : Available Size check for '"+mtpt+"' returned  "+str(ret)+" MB which is less than 1GB, it does not make sense to create a SCSI LUN here.")
			return None

		# If we got here, we have a valid size for the LUN
		debug("Returning final size in MB from size check after subtract of safety factor: "+str(size_long))
		return size_long

        except:
		debug("ERROR : Exception getting available size of dedup mount point. Cannot determine dedup mount point size. ")

	# Should never get here except on error
        return None


#
# Configure a new SCSI LUN on the dedupFS.
# This assumes that we have a fresh ADS volume
#
def configure_scsi_export():
	debug("--- Entering 'configure' workflow! ---")
	debug("Using validated dedup FS mountpoint: "+DEDUP_MNT)
	debug("Using SCSI LUN IQN: "+IQN)
        DEDUP_MNT_SIZE = None
        # First check if the user has specified default iscsi export size
        #local_setup_info = {}
        iscsi_export_size = None
        #rc = load_conf(VOL_CFG, local_setup_info)
        #if rc == 0:
        #        setup_info = local_setup_info['configure']
        setup_info = load_atlas_conf()
        if setup_info is not None:
            if setup_info['volumeresources']:
                if setup_info['volumeresources'][0].has_key('iscsiexportsize'):
                    iscsi_export_size = setup_info['volumeresources'][0]['iscsiexportsize']
        if iscsi_export_size is not None:
            DEDUP_MNT_SIZE = long(iscsi_export_size) * 1024
            debug("INFO: User has specifed the iscsi export size as "+ str(DEDUP_MNT_SIZE) +" MB.")
        else:
        # Get the available size, in MB, of the dedup mount point
            debug("Getting size validated dedup FS mountpoint:"+DEDUP_MNT)
            DEDUP_MNT_SIZE = get_mountpoint_size(DEDUP_MNT)

        if DEDUP_MNT_SIZE is None:
		debug("ERROR : Failed Getting Available size to create a SCSI LUN on DedupFS. Exiting")
		sys.exit(2)

	# Create the sparse file for the LUN on the dedup FS.
	debug("Setting up fileio LUN file of size " + str(DEDUP_MNT_SIZE) + " MB on validated dedup FS mountpoint: "+DEDUP_MNT)
	lun_file_path = None
	try:
		lun_file_path = os.path.join(DEDUP_MNT, LUN_FILENAME)
	except:
		lun_file_path = None

	if lun_file_path is None or not lun_file_path:
		debug("ERROR : Failed getting a valid LUN file path to create a SCSI LUN on DedupFS. Exiting")
		sys.exit(3)

	debug("Using LUN file path "+ lun_file_path + " for creating fileio LUN file of size " + str(DEDUP_MNT_SIZE) + " MB on validated dedup FS mountpoint: "+DEDUP_MNT)
	cmd = "dd if=/dev/zero of="+lun_file_path+" bs=1M count=1 seek="+str(DEDUP_MNT_SIZE)
	debug("DD CMD: "+cmd)
	debug("Creating fileio LUN file of size " + str(DEDUP_MNT_SIZE) + " MB on validated dedup FS mountpoint: "+DEDUP_MNT)
	ret, msg = runcmd(cmd, print_ret = True)
	if ret !=0:
		debug("ERROR : Failed creating LUN file for SCSI LUN on DedupFS. Error message was: "+msg)
		debug("ERROR : Exiting due to Failed creation of LUN file for SCSI LUN on DedupFS.")
		sys.exit(4)

	# Create the scst export for the LUN created above
	debug("Creating SCSI config file and exporting "+lun_file_path+" on IQN "+IQN)
	scsi.export([lun_file_path], IQN, '', "file", True)
	debug("--- FINISHED 'configure' workflow! ---")



#
# Start an existing SCSI configuration after first making sure that the
# SCSI config file contains valid entries for our ADS dedupFS and 
# LUN.
#
def start_scsi_export():
	debug("--- Entering 'start' workflow ---")
	debug("Using validated dedup FS mountpoint: "+DEDUP_MNT)
	debug("Using SCSI LUN IQN: "+IQN)
	# Check whether our IQN is present in the scsi config file
	debug("Checking whether IQN '"+IQN+"' is present in "+SCSI_CONFIG_FILENAME)
	cmd = "cat "+SCSI_CONFIG_FILENAME+" | grep 'TARGET ' | awk '{print $2}' | grep -w '"+IQN+"'"
	ret, msg = runcmd(cmd, print_ret=True)
	if ret != 0:
		debug("ERROR : Failed Checking whether IQN '"+IQN+"' is present in "+SCSI_CONFIG_FILENAME+", not starting config")
		sys.exit(2)
	else:
		debug("Yes, IQN '"+IQN+"' is present in "+SCSI_CONFIG_FILENAME)


	# Check whether the LUN device for our ADS volume export is present in the 
	# scsi config file. 
	debug("Checking whether LUN device '"+LUN_FILENAME+"' is present in "+SCSI_CONFIG_FILENAME)
	cmd = "cat "+SCSI_CONFIG_FILENAME+" | grep DEVICE -A 1 | grep -w '"+LUN_FILENAME+"'"
	ret, msg = runcmd(cmd, print_ret=True)
	if ret != 0:
		debug("ERROR : Failed Checking whether LUN device '"+LUN_FILENAME+"' is present in "+SCSI_CONFIG_FILENAME)
		sys.exit(3)
	else:
		debug("Yes, LUN device '"+LUN_FILENAME+"' is present in "+SCSI_CONFIG_FILENAME)


	# Check whether the LUN device is actually present on the dedup fs of this ADS volume
	debug("Checking whether LUN backing entity '"+LUN_FILENAME+"' exists on "+DEDUP_MNT)
	lun_file_path = None
	try:
		lun_file_path = os.path.join(DEDUP_MNT, LUN_FILENAME)
	except:
		lun_file_path = None

	if lun_file_path is None or not lun_file_path:
		debug("ERROR : Failed getting a valid LUN file path to create a SCSI LUN on DedupFS. Exiting")
		sys.exit(3)

	if not os.path.isfile(lun_file_path):
		debug("ERROR : Nonexistent LUN backing entity at "+lun_file_path)
		sys.exit(4)
	else:
		debug("OK! Found LUN backing entity '"+LUN_FILENAME+"' on "+DEDUP_MNT)

	# If we got here, everything checks out. So let's do the export
	debug("Starting SCSI export...")
	scsi.export([lun_file_path], IQN, '', "file", False)
	debug("--- Finished 'start' workflow ---")


		

#
# main()
#
debug(" ")
debug("======== START : "+MYNAME+" ========")
# check for proper command line args
debug("Checking for valid number of command line arguments...")

if len(sys.argv) < 4:
	usage()
	debug("ERROR: Incorrect number of arguments.")
	sys.exit(1) 
		
# Parse the command line args and validate mount point.
debug("Getting and validating command line arguments...")
get_and_validate_commandline_args()

# Sanity check
if DEDUP_MNT is None:
	usage()
	debug("ERROR : Failed Getting and validating dedup FS mountpoint. Exiting")
	sys.exit(1)

if IQN is None:
	usage()
	debug("ERROR : Failed Getting valid IQN for SCSI LUN to be created on Dedup FS mountpoint. Exiting")
	sys.exit(1)

if NEEDS_CONFIG is None:
	usage()
	debug("ERROR : Failed Getting valid parameter for NEEDS_CONFIG. Exiting")
	sys.exit(1)

if NEEDS_CONFIG:
	configure_scsi_export()
else:
	start_scsi_export()

# Verify proper export of SCSI Target IQN
debug("Verifying that IQN '"+IQN+"' is exported correctly...")
if not scsi.verify_iqn_export(IQN):
	debug("ERROR : Could not verify that IQN '"+IQN+"' was exported correctly! You may need to re-run this script. Exiting!")
	# TODO : Cleanup necessary? If running in configure mode, just re-running this will create the LUn etc, so it should be unnecessary.
	sys.exit(4)
else:
	debug("IQN '"+IQN+"' is exported correctly. Status=OK")


# Verify proper export of LUN device which we exported
filename=scsi.uniqueexportname(LUN_FILENAME)
debug("Verifying that LUN device '"+filename+"' at '"+DEDUP_MNT+"' is exported correctly...")
if not scsi.verify_device_export(filename):
	debug("ERROR : Failed Verifying that LUN device '"+filename+"' at '"+DEDUP_MNT+"' is exported correctly. You may need to re-run this script. Exiting!")
	# TODO : Cleanup necessary? If running in configure mode, just re-running this will create the LUn etc, so it should be unnecessary.
	sys.exit(5)
else:
	debug("LUN device '"+filename+"' at '"+DEDUP_MNT+"' is exported correctly. Status=OK")

debug("======== END : "+MYNAME+"  ========")
sys.exit(0)
