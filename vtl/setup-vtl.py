#! /usr/bin/env python
'''
setup-vtl.py - set up a Virtual Tape Library (VTL) setup on a dedup volume.

Copyright (c) 2015 Atlantis Computing Inc. All rights reserved.

Originally written by: Kartikeya Iyer - kartik@atlantiscomputing.com

15-SEP-2015 : The first cut of this script does not have any command line
parsing capabilities, nor the ability to specify a custom setup without
changing the source. This will all come later, if needed. It also does not
have the ability to expose the VTL devices through anything other than iSCSI.
While Fibrechannel/FCoE have been discussed, currently we do not have the
hardware to test FC/FCoE, so this has been held in abeyance. Also, absolutely
no testing whatsoever has been done on combinations of robot devices and tape
drives; we stick to the tested and known to work combination of the STK L700
robot and the IBM ULT3580-TD5 tape drive.

This script logs to: /var/log/setup-vtl.log

This script sets up a VTL config with the following virtual devices:
    1 x Robot/changer device (StorageTek L700)
    1 x Tape Drive (IBM ULT3580-TD5)
    as many slots as needed containing one LTO-5 tape each. The number of
    slots needed depends on the free size available in the Dedup volume.

It then sends out the Robot device as well as the tape drive out over iSCSI
using the SCST subsystem.

On the system on which the backup software is running, you will need to
connect to the exported iSCSI devices using that system's iSCSI initiator,
at which point the Robot/changer device and tape drive devices will be
visible in the backup system's device list. You can then use the backup
software of your choice to create backups from the backup system to this
USX VM.

If you need more info on how tape backup systems work, please see the
REFERENCES section below.

This script includes an API designed to interact with the "mtx" system
command to load/unload tapes to/from tape drives and tape slots.

This script may be used either as a library which can be included in your
own VTL setup infrastructure, or used as-is; see the "Main" section of this
script for how it is used to set up and export a VTL infrastructure.

This script is designed to work on USX VMs with build version USX-3.0 or
greater. It also requires a few pre-requisites to be installed on the
USX VM (including their dependencies):
    fcoe-utils
    sgutils
    pynetifaces
    mtx
    sg3utils
    mt-st

If you are going to be modifying this script, please pay attention to the
TODO and FIXME comments scattered throughout this script.

Script return values:
    0 - Everything went well.
    1 - Wrong VM version OR type: Could not find a required milio lib function
    2 - Wrong VM version: failed VM release version check.
    3 - Other runtime errors. See log file for details.

*** WARNING ***
DANGER: After running this script multiple times, discovered that the SCSI
host ID (first tuple of "[XX:YY:ZZ:AA]" SCSI ID i.e the "XX" part) keeps 
increasing every time mhvtl runs. 
Googled how to decrement this Host ID but to no effect. Rebooting the system
seems to be the only way to reset this counter, according to
  http://forums.fedoraforum.org/showthread.php?t=259060
In SCSI drivers kernel source file (drivers/scsi/hosts.c), it is defined as
  static atomic_t scsi_host_next_hn;


KNOWN BACKUP SOFTWARE CAVEATS
================================
Symantec BackupExec
---------------------
1. To use the virtual tapes created by this script, in the BackupExec GUI
   you will need to erase all tapes, otherwise the tapes are moved into the
   "retired media" and are not usable for backup, because BE doesn't
   recognize the tape format.
   UPDATE: This should be solved by the added code in here to erase all
   configured slots on first creation.

2. Setting a tape barcode/label in Symantec BackupExec has no effect on the
   actual tape label on the USX VM. It may look like it succeeded at first,
   but the tape label will not be changed on the USX VM, and using this tape
   in a backup job will show that it has reverted to the originally configured
   barcode/label.


REFERENCES
============
MAIN: https://sites.google.com/site/linuxvtl2/
FORUM: http://mhvtl-community-forums.966029.n3.nabble.com/
http://mhvtl-a-linux-virtual-tape-library.966029.n3.nabble.com/

mhvtl on Ubuntu:
http://www.dataprotection.nu/blog/index.php/86
http://mhvtl-a-linux-virtual-tape-library.966029.n3.nabble.com/Quick-setup-script-for-Ubuntu-11-10-mhvtl-and-mhvtl-gui-td3928515.html

MHVTL config pdf (long) for CENTOS:
https://alexandreborgesbrazil.files.wordpress.com/2013/09/how-to-configure-a-free-vtl1.pdf


Some useful info:
http://www.pedroliveira.pt/install-virtual-tape-library-bacula/


Config howto (+ build for Centos):
http://mhvtl-a-linux-virtual-tape-library.966029.n3.nabble.com/MHVTL-0-18-4-scst-iSCSI-Target-Centos-5-4-STEP-BY-STEP-td1684094.html

Another HOWTO:
http://www.ithierarchy.com/ITH/node/25

MHVTL with iSCSI (used in Windows):
http://mhvtl-a-linux-virtual-tape-library.966029.n3.nabble.com/Success-I-got-MHVTL-to-work-on-Windows-via-iSCSI-td1684636.html

Popular tape libraries (circa 2011, it looks like):
http://searchdatabackup.techtarget.com/Top-tape-library-systems-Spectra-Logic-midrange-and-enterprise-winner


General references on Tape backup
=======================================
Linux tape backup with mt & tar : http://www.cyberciti.biz/faq/linux-tape-backup-with-mt-and-tar-command-howto/
Tape drive naming conventions: http://www.cyberciti.biz/faq/tape-drives-naming-convention-under-linux/

'''

# TODO : Command lines and parsing
# TODO : Handle multiple tape drives. We DO NOT handle multiple changers for now.
# TODO : Handle SCST export types for Fibrechannel and FCoE, once we have the hardware

import os
import sys, tempfile, re, traceback
from types import *
from subprocess import *
import signal
import time
import string
import math
import ConfigParser
from pkg_resources import parse_version

# Next two are for the IQN - we need a unique identifier for this node.
import netifaces as ni
import uuid

# milio/Atlas library functions
sys.path.insert(0, "/opt/milio")
from libs.atlas.cmd import runcmd
from libs.atlas.log import debug

# Before we go further, check if we have a required library function
# in milio.libs.atlas.log. If we don't have this function, we are not
# running on the correct version of the VM.
# The function being imported exists in USX-3
# All messages in the 'except' clause, if it is triggered,
# will be printed to /var/log/milio-atlas.log on a pre USX-3 ILIO.
# On other VMs, it may just be output to the console.
try:
    from libs.atlas.log import set_log_file
except:
    debug("ERROR : VTL Setup script : Wrong VM version, or wrong milio libs version. Cannot continue VTL setup!")
    sys.exit(1)



######## GLOBAL CONSTANTS #########
MYNAME = os.path.basename(__file__)

# This script logs to this file.
LOGFILE="/var/log/setup-vtl.log"

# The VTL user and group on the system. This user and group should exist
# beforehand.
SYS_VTL_USER = "vtl"
SYS_VTL_GROUP = "vtl"

# The file on the USX containing the USX Build release version number
RELEASE_VERSION_FILE="/etc/ilio/release-version"

# The minimum required USX build version for this script
MINIMUM_REQUIRED_RELEASE_VERSION = "USX-3.0.1.0"

# The second field in the output of 'lsscsi -g', which holds the SCSI device
# type name for Changer/Robot/Library devices.
ROBOT_SCSI_DEVTYPE_STRING = "mediumx"

# The second field in the output of 'lsscsi -g', which holds the SCSI device
# type name for Tape Drive devices.
TAPEDRIVE_SCSI_DEVTYPE_STRING = "tape"

# The expected number of fields in the output of the "lsscsi -g" command.
# We have this just in case the lsscsi command changes in the future.
LSSCSI_COMMAND_EXPECTED_NUM_FIELDS = 7

# mtx  status for tape drives and tape slots - what word in the mtx output
# designates a full or empty drive/slot?
# Change these if the output of "mtx status" ever changes.
MTX_STATUS_FULL_DESIGNATOR = "Full"
MTX_STATUS_EMPTY_DESIGNATOR = "Empty"

# SCST SCSI export types - iSCSI, Fibrechannel, FCoE
SCST_ISCSI_SETUP_TYPE = "iscsi" # Uses the SCST iSCSI target driver
SCST_FCOE_SETUP_TYPE = "fcoe" # Uses the SCST fcst target driver
SCST_FC_SETUP_TYPE = "fc" # Uses the SCST QLogic target driver
SCST_SETUP_TYPES = (SCST_ISCSI_SETUP_TYPE,
    SCST_FCOE_SETUP_TYPE,
    SCST_FC_SETUP_TYPE,
    )

# The MHVTL config directory
MHVTL_CONF_DIR = "/etc/mhvtl/"

##### USX VTL Config file globals
# A file, created by the USX framework, which holds USX-specific VTL configs.
# For instance, one parameter is "expected_dedup_ratio", which governs by how
# much the VTL is overprovisioned, using the expected dedup ratio.
# This file lives in MHVTL_CONF_DIR
USX_VTL_CONF_FILENAME = "usx-vtl.conf"
USX_VTL_CONF_FILEPATH = MHVTL_CONF_DIR+"/"+USX_VTL_CONF_FILENAME
# Now define the sections and the various setting keys here. They will be used
# in the code below.
# TODO: If you add more sections and/or settings to the USX VTL config file,
# then remember to declare them here. It's a better approach than hardcoding
# it in the code
USX_VTL_CONF_SECTIONHEADER_STRING = "usx-vtl"
USX_VTL_CONF_DEDUP_PERCENT_SETTING_KEY = "expected_dedup_percentage"
# The default deduplication percentage to use if not otherwise specified.
USX_VTL_DEFAULT_DDP_PCT_STR = "50"

# The mhvtl.conf file. See mhvtl docs for what this contains.
MHVTL_CONF_FILENAME = "mhvtl.conf"

# The mhvtl config file configuring the Robot and Tape Drive objects.
MHVTL_DEVICECONF_FILENAME = "device.conf"

# The mhvtl config file containing the slot definitions for a Robot/Library
MHVTL_LIBRARYCONTENTS_FILENAME = "library_contents.10"

# Like it says.
SCST_CONF_FILENAME = "/etc/scst.conf"

MHVTL_INIT_SCRIPT="/etc/init.d/mhvtl"

iSCSI_KERNEL_MODS_NEEDED =("mhvtl",
    "scst_tape",
    "scst_changer",
    "iscsi_scst",
    "scst",
    )

FIBRECHANNEL_KERNEL_MODS_NEEDED =("mhvtl",
    "scst_tape",
    "scst_changer",
    "qla2xxx_scst",
    "qla2x00tgt",
    "scst",
    )

FCOE_KERNEL_MODS_NEEDED =("mhvtl",
    "scst_tape",
    "scst_changer",
    "fcst",
    "scst",
    )


SCST_SETUP_TYPE_KERNMOD_DICT = {SCST_ISCSI_SETUP_TYPE:iSCSI_KERNEL_MODS_NEEDED,
    SCST_FC_SETUP_TYPE:FIBRECHANNEL_KERNEL_MODS_NEEDED,
    SCST_FCOE_SETUP_TYPE:FCOE_KERNEL_MODS_NEEDED,
    }

# These are the userspace programs for which we check existence in current
# PATH. If they exist, it means that MHVTL and dependencies were installed
# properly.
USERSPACE_PROGRAMS = ("vtltape",
    "vtllibrary",
    "/etc/init.d/mhvtl",
    "mktape",
    "vtlcmd",
    "mtx",
    )


#### WARNING WARNING WARNING ######
# As of mhvtl source  commit  f993138c7fc64304684d94def69a7714c3c283bf
# made on 14-Apr-2015, the only actual working robot device and tape drive 
# combo, consistently seen to work by me, seems to be:
# Robot/Library: STK L700
#   Tapes:  IBM ULT3580-TD5
#           Ultrium 5-SCSI
# We need to thoroughly test other Robot/Tape drive combos.

###### Tape Drive emulations supported by MHVTL as of 03-Sep-2015.
# Data from:
# https://sites.google.com/site/linuxvtl2/home/list_of_tape_emulations
# Vendors and types gleaned from mhvtl source code and the comments in the
# default/skeleton library_contents file of mhvtl. Capacities from Googling.
#
# The tape drives we configure in MHVTL need to be from this dict.
#
# The key of the dict is the drive model (Product ID), and the value is a
# tuple.
# The first tuple element is the drive manufacturer (Vendor ID).
#
# The second tuple element is the trailing Identifier to be used for the
# tape slots for this particular drive.
#
# The third tuple element is the capacity/density in GB of this drive/slot
# type. The capacities have been taken from the Internet/google for a
# particular drive. Where possible, native/uncompressed capacities have been
# used for the drive. Where there is a range of possible capacities/abilities,
# the most conservative (i.e. lesser one) has been used (e.g IBM 3592 series)
#
# WARNING: THIS DICT CANNOT TOLERATE SPACES IN FIELDS OTHER THAN
# DEVICE MODEL / PRODUCT ID (key) FIELD. It messes up the SCSI device
# list processing.
#
# Given a drive model, we will use the corresponding vendor ID when writing
# the MHVTL device.conf file, and use the corresponding trailing identifier
# in the barcodes for the slots we create for this tape. The capacity/size is
# used to create the mhvtl.conf file, as well as decide how many tape slots
# are needed for this particular drive (corresponding to how much free space
# is available on the DedupFS volume.
#
# It's as clear as mud, I know, but what to do - this whole tape business is
# pretty complicated, to say the least.
#
# For LTO Tape drives. The library_contents trailing identifier is one of
#   L1,L2,L3,L4,L5 corresponding to LTO-1 through LTO-5 respectively.
#   For our purposes, we use LTO-5, which is L5.
#   LTO-5 has been set to 1000GB instead of the actual 1500GB, and
#   LTO-6 has been set to 2000GB instead of the actual 2500GB.
#   https://en.wikipedia.org/wiki/Linear_Tape-Open#Tape_specifications
#
# For AIT tape drives, the library_contents trailing ID is "X4"
#
# For IBM 3592 drives, the library_contents trailing ID is one of JA for
#   3592+, JB for 3592E05+, JC for 3592E06+, JK for 3592E07+
#
# Other drives have the ID set similarly as per the comment in the
# Skeleton/example library_contents file of mhvtl.
TAPE_DRIVE_DICT = {"ULT3580-TD1":("IBM", "L1", 100),# NOT TESTED
    "ULT3580-TD2":("IBM", "L2", 200),# WORKS, tested with STK L700 Robot
    "ULT3580-TD3":("IBM", "L3", 400),# NOT TESTED
    "ULT3580-TD4":("IBM", "L4", 800),# NOT TESTED
    "ULT3580-TD5":("IBM", "L5", 1000),# WORKS, tested with STK L700 Robot
    "ULT3580-TD6":("IBM", "L6", 2000),# NOT TESTED
    "ULTRIUM-TD1":("IBM", "L1", 100),# NOT TESTED
    "ULTRIUM-TD2":("IBM", "L2", 200),# NOT TESTED
    "ULTRIUM-HH2":("IBM", "L2", 200),# NOT TESTED
    "ULTRIUM-TD3":("IBM", "L3", 400),# NOT TESTED
    "ULTRIUM-HH3":("IBM", "L3", 400),# NOT TESTED
    "ULTRIUM-TD4":("IBM", "L4", 800),# NOT TESTED
    "ULTRIUM-HH4":("IBM", "L4", 800),# NOT TESTED
    "ULTRIUM-TD5":("IBM", "L5", 1000),# NOT TESTED
    "ULTRIUM-HH5":("IBM", "L5", 1000),# NOT TESTED
    "ULTRIUM-TD6":("IBM", "L6", 2000),# NOT TESTED
    "ULTRIUM-HH6":("IBM", "L6", 2000),# NOT TESTED
    "Ultrium 1-SCSI":("HP", "L1", 100),# NOT TESTED
    "Ultrium 2-SCSI":("HP", "L2", 200),# NOT TESTED
    "Ultrium 3-SCSI":("HP", "L3", 400),# NOT TESTED
    "Ultrium 4-SCSI":("HP", "L4", 800),# NOT TESTED
    "Ultrium 5-SCSI":("HP", "L5", 1000),# NOT WORKING with L700, Tape does not show up in Windows
    "Ultrium 6-SCSI":("HP", "L5", 1000),# NOT TESTED
    "SDX-300C":("AIT", "X4", 50),# NOT TESTED
    "SDX-500C":("AIT", "X4", 50),# NOT TESTED
    "SDX-500V":("AIT", "X4", 50),# NOT TESTED
    "SDX-700C":("AIT", "X4", 100),# NOT TESTED
    "SDX-700V":("AIT", "X4", 100),# NOT TESTED
    "SDX-900V":("AIT", "X4", 200),# NOT TESTED
    "03592J1A":("IBM", "JA", 300),# NOT TESTED
    "03592E05":("IBM", "JB", 300),# NOT TESTED
    "03592E06":("IBM", "JC", 1000),# NOT TESTED
    "T10000C":("STK", "TA", 5000),# NOT TESTED
    "T10000B":("STK", "TA", 1000),# NOT TESTED
    "T10000A":("STK", "TA", 500),# NOT TESTED
    "T9840D":("STK", "TW", 75),# NOT TESTED
    "T9840C":("STK", "TX", 40),# NOT TESTED
    "T9840B":("STK", "TY", 20),# NOT TESTED
    "T9840A":("STK", "TZ", 20),# NOT TESTED
     "T9940B":("STK", "TU", 200),# NOT TESTED
    "T9940A":("STK", "TV", 60),# NOT TESTED
    "DLT7000":("QUANTUM", "D7", 35),# NOT TESTED
    "DLT8000":("QUANTUM", "D7", 40),# NOT TESTED
    "SDLT320":("QUANTUM", "S3", 160),# NOT TESTED
    "SDLT600":("QUANTUM", "S3", 300),# NOT TESTED
    }


# Library (Changer Robot) device dict.
# The key is the Robot device model. The value is a tuple.
# The first tuple element is the Vendor. This field goes in as vendor ID.
# The second tuple is the model series name, if present/available. This
# field is not currently used in this script.
#
# This data was gleaned by going through the mhvtl source code as of 
# 03-Sep-2015. Some of the model names I assigned myself, since the code
# either does not mention it or does not care what string this field contains.
# Some model names are only a partial string, since this is all the code
# needs to create the device. So if this looks funny, there you have it...
#
# WARNING: THIS DICT CANNOT TOLERATE SPACES IN FIELDS OTHER THAN
# DEVICE MODEL/PRODUCT ID (key) FIELD (and unused MODEL SERIES field).
# It messes up the SCSI device list processing.
# TODO : Add a new tuple as the second tuple, which contains trailing IDs
# from the tape Drive dict - this will give us a compatibility check for
# a particular Robot/Drive combo. 
# E.g. T10000B drive does not seem to work with L700 robot, it does not
# show up as a drive in the Windows device manager. I suspect that this
# might be due to a Robot/drive incompatibility, but I'm not certain.
ROBOT_DICT = { "NEO":("Overland","Neo 8000"),
    "T-680":("Spectra","T-Series"), # NOT TESTED
    "3573-TL":("IBM","TS3100 series"), # NOT TESTED
    "03584L23":("IBM","03584 Series"), # NOT TESTED
    "03584D22":("IBM","03584 Series"), # NOT TESTED
    "03584L22":("IBM","03584 Series"), # NOT TESTED
    "03584D23":("IBM","03584 Series"), # NOT TESTED
    "03584L32":("IBM","03584 Series"), # NOT TESTED
    "03584D32":("IBM","03584 Series"), # NOT TESTED
    "03584L52":("IBM","03584 Series"), # NOT TESTED
    "03584D52":("IBM","03584 Series"), # NOT TESTED
    "03584L53":("IBM","03584 Series"), # NOT TESTED
    "03584D53":("IBM","03584 Series"), # NOT TESTED
    "L700":("STK","L Series"), # WORKS
    "L700e":("STK","L Series"), # NOT TESTED
    "L180":("STK","L Series"), # NOT TESTED
    "L20":("STK","L Series"), # NOT TESTED
    "L40":("STK","L Series"), # NOT TESTED
    "L80":("STL","L Series"), # NOT TESTED
    "SL500":("STK","SL Series"), # DOES NOT WORK, mhvtl upstream commit  f993138c7fc643 of 14-Apr-2015. Complains about missing density when used.
    "EML":("HP","EML E-Series"), # NOT TESTED
    "MSL":("HP","MSL Series"), # NOT TESTED
    "QUANTUM":("Scalar","Quantum 6-00423013"), # NOT TESTED
    }
######## End : GLOBAL CONSTANTS #########



######## Config File Templates (template ALL the things!!) ########

# The USX VTL config file template. This template is used to create the file
# if it doesn't already exist.
USX_VTL_CONF_TEMPLATE=string.Template("""
#############################################################################
#
# USX VTL Config file.
#
# This file is used to store USX-specific configuration items which will be
# used in the setup of the VTL infrastructure.
#
# Example configurations:
#   [usx-vtl]
#   expected_dedup_percentage = 50
#
#############################################################################

# Section header - DO NOT CHANGE
[${SECTIONHEADER_STRING}]

# Config parameter for how much deduplication we are expecting on this system.
# Defaults to 50 unless explicitly changed. If you change this, you will need
# to re-run the setup-vtl.py[c] script.
${DEDUP_PERCENT_SETTING_KEY} = ${DEFAULT_DDP_PCT_STR}

""")

# The main mhvtl.conf file.
MHVTL_CONF_TEMPLATE="""
##############################################################################
#
# AUTOMATICALLY GENERATED BY setup-vtl.py[c].
#
# Any manual changes may be overwritten.
#
# CONSIDER YOURSELF WARNED!
#
##############################################################################

# Home directory for config file(s)
MHVTL_CONFIG_PATH=/etc/mhvtl

# Default media capacity (500 M)
#CAPACITY=500
# Kartik : Changed default to 1048576 MB (1TB) for our default LTO-5 Tapes
CAPACITY=1048576

# Set default verbosity [0|1|2|3]
VERBOSE=0

# Set kernel module debuging [0|1]
VTL_DEBUG=0
"""

# The MHVTL device.conf config file template.
# This file specifies the main Virtual Library Changer device, and the
# individual tape drives present in the tape library.
DEVICE_CONF_TEMPLATE=string.Template("""
##############################################################################
#
# AUTOMATICALLY GENERATED BY setup-vtl.py[c].
#
# Any manual changes may be overwritten.
#
# CONSIDER YOURSELF WARNED!
#
##############################################################################

VERSION: 5

# VPD page format:
# <page #> <Length> <x> <x+1>... <x+n>
# NAA format is an 8 hex byte value seperated by ':'
# Note: NAA is part of inquiry VPD 0x83
#
# Each 'record' is separated by one (or more) blank lines.
# Each 'record' starts at column 1
# Serial num max len is 10.
# Compression: factor X enabled 0|1
#     Where X is zlib compression factor    1 = Fastest compression
#                       9 = Best compression
#     enabled 0 == off, 1 == on
#
# fifo: /var/tmp/mhvtl
# If enabled, data must be read from fifo, otherwise daemon will block
# trying to write.
# e.g. cat /var/tmp/mhvtl (in another terminal)

Library: 10 CHANNEL: 00 TARGET: 00 LUN: 00
 Vendor identification: ${LIBRARY_VENDOR}
 Product identification: ${LIBRARY_DEVICE_MODEL}
 Unit serial number: ATLVTL_C
 NAA: 10:22:33:44:ab:00:00:00
 Home directory: ${DEDUP_MNT}
 PERSIST: False
 Backoff: 400
# fifo: /var/tmp/mhvtl

""")

DRIVE_TEMPLATE = string.Template("""
Drive: ${DRIVE_SEQ_PLUS_10} CHANNEL: 00 TARGET: ${DRIVE_SEQ_2PADDED} LUN: 00
 Library ID: 10 Slot: ${DRIVE_SEQ_2PADDED}
 Vendor identification: ${TAPEDRIVE_VENDOR}
 Product identification: ${TAPEDRIVE_MODEL}
 Unit serial number: ATLVTL_D${DRIVE_SEQ_2PADDED}
 NAA: 10:22:33:44:ab:00:${DRIVE_SEQ_2PADDED}:00
# Compression: factor 1 enabled 1
# Compression type: lzo
 Compression: factor 1 enabled 0
 Backoff: 400
# fifo: /var/tmp/mhvtl

""")


# The MHVTL 'library_contents.10' config  file Template.
# This file defines the MHVTL tape library Slots, drives and pickers.
# The actual Library Changer device and the drive device are specified
# in device.conf (see above).
LIBRARY_CONTENTS_TEMPLATE = """
##############################################################################
#
# AUTOMATICALLY GENERATED BY setup-vtl.py[c].
#
# Any manual changes may be overwritten.
#
# CONSIDER YOURSELF WARNED!
#
##############################################################################

#Drive 1:
#
#Picker 1:
#
#MAP 1:
#
# Slot 1 - ?, no gaps
# Slot N: [barcode]
# [barcode]
# Barcode max length is 12 chars
# a barcode is comprised of three fields: [Leading] [identifier] [Trailing]
# Leading "CLN" -- cleaning tape
# Leading "W" -- WORM tape
# Leading "NOBAR" -- will appear to have no barcode
# If the barcode is at least 8 character long, then the last two characters are Trailing
# Trailing "S3" - SDLT600
# Trailing "X4" - AIT-4
# Trailing "L1" - LTO 1, "L2" - LTO 2, "L3" - LTO 3, "L4" - LTO 4, "L5" - LTO 5
# Trailing "LT" - LTO 3 WORM, "LU" -  LTO 4 WORM, "LV" - LTO 5 WORM
# Trailing "L6" - LTO 6, "LW" - LTO 6 WORM
# Trailing "TA" - T10000+
# Trailing "TZ" - 9840A, "TY" - 9840B, "TX" - 9840C, "TW" - 9840D
# Trailing "TV" - 9940A, "TU" - 9940B
# Trailing "JA" - 3592+
# Trailing "JB" - 3592E05+
# Trailing "JC" - 3592E06+
# Trailing "JK" - 3592E07+
# Trailing "JW" - WORM 3592+
# Trailing "JX" - WORM 3592E05+ & 3592E06
# Trailing "JY" - WORM 3592E07+
# Trailing "D7" - DLT7000 media (DLT IV)
#
# Number of slots depends on tape drive model specified as well as dedup size
# (free space) available to host the virtual tape data files.
# Last slot will be a
# Cleaner tape just in case the backup software requires one.
# Slots go here, example (for 3TB DedupFS Vol):
#Slot 1: ATLVTP01L5
#Slot 2: ATLVTP02L5
#Slot 3: ATLVTP03L5
#Slot 4: CLNATLVTP

"""

LIBRARY_CONTENTS_DRIVE_TEMPLATE=string.Template("Drive ${DRIVE_SEQ}:")

LIBRARY_CONTENTS_PICKER_TEMPLATE="""

Picker 1:

"""

LIBRARY_CONTENTS_MAP_TEMPLATE=string.Template("MAP ${DRIVE_SEQ}:")

# The Slot template to populate the library_contents MHVTL file.
# The tape Barcode (tape volume name) template is ATLVTP, followed by a
# leading-zero-padded 4-character tape sequence number, followed by the
# trailing tape ID. This trailing ID depends on the tape drive specified.
LIBRARY_CONTENTS_SLOT_TEMPLATE=string.Template("Slot ${SEQNUM}: ATLVTP${PADDED_SEQNUM}${TRAIL_ID}")
LIBRARY_CONTENTS_SLOT_CLEANER_TEMPLATE=string.Template("Slot ${SEQNUM}: CLNATLVTP")

# The iSCSI SCST config for exposing the Library Changer device and the tape
# drive using iSCSI. This template uses subtemplates as defined below.
# It has been split up like this to allow for adding multiple changer and tape
# drive devices if required in the future.
iSCSI_SCST_CONF_TEMPLATE = string.Template("""
# NOTE: Replace SCSI IDs in DEVICE lines with actual IDs of lib & tape drive devices as per lsscsi -g
# SCSI IDs look like this: 3:0:0:0
HANDLER dev_changer {
    ${CHANGER_HANDLER_SUBSECTION}
}

HANDLER dev_tape {
    ${TAPEDRIVE_HANDLER_SUBSECTION}
}

TARGET_DRIVER iscsi {
enabled 1

    ${TARGET_SUBSECTION}
}

""")

# iSCSI SCST Subtemplates for the HANDLER and TARGET subsections of scst.conf.
# Used for both changer device as well as tape drive device; just substitute
# the correct SCSI device ID and device type/name.
# The strings built up in these subtemplates will be used in the iSCSI SCST
# config template above, to write the final actual scst.conf file.
# For example:
#  If your changer device has SCSI ID 3:0:0:0, then you get the following:
#    ${VTLDEV_SCSI_ID} = "3:0:0:0"
#    ${VTL_DEV_TYPE_STR} = "chg0"
#  in the corresponding conf subsections, and in the main SCST_CONF template,
#  when we substitute these subsection values, we would get:
#    ${CHANGER_HANDLER_SUBSECTION} = "DEVICE 3:0:0:0"
#    first line of ${TARGET_DEFS} = "TARGET iqn.com.atlantiscomputing.vtl:chg0"
#    last line ${TARGET_DEFS} = "LUN 0 3:0:0:0"
# It has been split up like this to allow for adding multiple changer and tape
# drive devices if required in the future.
iSCSI_SCST_CONF_HANDLER_SUBSECTION_TEMPLATE = string.Template("""DEVICE ${VTLDEV_SCSI_ID}""")
iSCSI_SCST_CONF_TARGET_SUBSECTION_TEMPLATE = string.Template("""
    TARGET iqn.com.atlantiscomputing.vtl.${NODE_ID}:${VTL_DEV_TYPE_STR} {
        InitialR2T No
        ImmediateData Yes
        FirstBurstLength 131072
        MaxRecvDataSegmentLength 131072
        enabled 1
        LUN 0 ${VTLDEV_SCSI_ID}
    }
""")

# TODO : SCST templates for FC and FCoE

######## END : Config File Templates ########

class AtlScsiDeviceInfo(object):
    '''
    A class to hold the SCSI information for the SCSI objects in which
    we're interested for this VTL implementation:
        Changer/library/robot devices
        Tape Drive devices

    This information comes from the system's SCSI proc interface, and we
    use the 'lsscsi -g' command to get the raw info used to populate the
    info in objects of this class.

    The instance variables of this class are basically the different fields
    present in the output of the 'lsscsi -g' command.

    Instance variables:
        scsi_id             - scsi_host,channel,target_number,LUN tuple.
                              It is placed in brackets and each element
                              is colon separated. As gathered from the system,
                              it looks like this: "[3:0:1:0]". In this
                              instance variable, we store it as a plain string
                              after stripping the brackets. Thus, in the
                              instance variable, it looks like this: "3:0:1:0"

        dev_type            - SCSI peripheral type; rather than using the
                              formal name (e.g. "direct access device") a
                              shorter name is used, e.g. "mediumx", "tape".

        dev_vendor          - The device Vendor ID string.

        dev_model           - The device Model ID string.

        dev_revnum          - The device Revision Number string.

        primary_nodename    - The SCSI Primary nodename. The primary device
                              node name is associated with the upper level SCSI
                              driver that "owns" the device. Examples of upper
                              level SCSI drivers are sd (for disks), and st (for
                              tapes). Some SCSI devices have peripheral types
                              that either don't have upper level drivers to
                              control them, or the associated driver module is
                              not loaded. Such devices have '-' given for their
                              primary device node name. For our VTL devices,
                              this generally looks like:
                                Robot/Changer devices - /dev/sch<n>
                                Tape Drive devices - /dev/st<n>
                                  where <n> is 0, 1, 2, ...

        generic_nodename    - The device SCSI Generic (sg) nodename e.g. /dev/sg<n>

    TODO : Add  an instance variable which contains a list of tape drives 
    associated with each robot device object, and functions to get and set the
    same. We can use the SCSI BUS ID (first component X of SCSI ID "X:Y:Z:A)
    to set up the associations.
    '''
    def __init__(self, scsi_id="", dev_type="", vendor="", model="", revnum="", primary_nodename="", generic_nodename=""):
        '''
        The constructor
        '''
        scsi_id = scsi_id.replace('[', '')
        scsi_id = scsi_id.replace(']', '')
        self.scsi_id = scsi_id
        self.dev_type = dev_type
        self.vendor = vendor
        self.model = model
        self.revnum = revnum
        self.primary_nodename = primary_nodename
        self.generic_nodename = generic_nodename

    #### Define accessor methods for the instance variables of interest.
    ## Getters
    def get_scsi_id(self):
        return self.scsi_id

    def get_dev_type(self):
        return self.dev_type

    def get_vendor(self):
        return self.vendor

    def get_model(self):
        return self.model

    def get_revnum(self):
        return self.revnum

    def get_primary_nodename(self):
        return self.primary_nodename

    def get_generic_nodename(self):
        return self.generic_nodename

    ## Setters.
    ## If they were successfully able to change the Instance variable
    ## in question, they return True.
    ## Otherwise, the instance variable is NOT modified, and the setter
    ## returns False.
    ## Setters will not allow explicitly setting empty parameters.
    ## COPYPASTA!!!
    def set_scsi_id(self, param):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            param = param.replace(']', '')
            param = param.replace('[', '')
            self.scsi_id = param
            return True
        except:
            print_exception()
            return False


    def set_dev_type(self, param):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.dev_type = param
            return True
        except:
            print_exception()
            return False

    def set_vendor(self):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.vendor = param
            return True
        except:
            print_exception()
            return False

    def set_model(self):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.model = param
            return True
        except:
            print_exception()
            return False

    def set_revnum(self):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.revnum = param
            return True
        except:
            print_exception()
            return False

    def set_primary_nodename(self):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.primary_nodename = param
            return True
        except:
            print_exception()
            return False

    def set_generic_nodename(self):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.generic_nodename = param
            return True
        except:
            print_exception()
            return False

    def dumpinfo(self):
        '''
        Print the info contained in this object
        '''
        try:
            debug("     --- SCSI Object dump ---")
            debug("         Device SCSI ID..: "+self.scsi_id)
            debug("         Device SCSI type: "+self.dev_type)
            debug("         Device Vendor...: "+self.vendor)
            debug("         Device Model....: "+self.model)
            debug("         Device Revision.: "+self.revnum)
            debug("         Primary nodename: "+self.primary_nodename)
            debug("         Generic nodename: "+self.generic_nodename)
        except:
            debug("ERROR : AtlScsiDeviceInfo.dumpinfo() : Exception dumping object data. Exception information follows (if available).")
            print_exception()


class AtlTapeDrive(object):
    '''
    A class to hold the info of the Tape Drives to be used in the VTL config.

    TODO : Document instance variables.

    TODO : Add a function to return the associated Robot/CHanger/Library
    device with which a tape object is associated. We can use the SCSI Bus
    ID (the X part of a SCSI ID "X:Y:Z:A") for this.
    '''

    def __init__(self, seq=1, model="", vendor="", trailID="", capacity_GB=0):
        '''
        Konstructor!
        '''
        self.seq = seq
        self.model = model
        self.vendor = vendor
        self.trailID = trailID
        self.capacity_GB = capacity_GB

    ##### Accessors
    ### Getters
    def get_seq(self):
        return self.seq

    def get_model(self):
        return self.model

    def get_vendor(self):
        return self.vendor

    def get_trailID(self):
        return self.trailID

    def get_capacity_GB(self):
        return self.capacity_GB

    ## Setters.
    ## If they were successfully able to change the Instance variable
    ## in question, they return True.
    ## Otherwise, the instance variable is NOT modified, and the setter
    ## returns False.
    ## Setters will not allow explicitly setting empty parameters.
    ## COPYPASTA!!!
    def set_seq(self, param):
        try:
            if param is None or not isinstance(param, (int, long)) or param <= 0:
                return False
            self.seq = param
            return True
        except:
            print_exception()
            return False

    def set_model(self, param):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.model = param
            return True
        except:
            print_exception()
            return False

    def set_vendor(self, param):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.vendor = param
            return True
        except:
            print_exception()
            return False

    def set_trailID(self, param):
        try:
            if param is None or not isinstance(param, basestring) or not param.strip():
                return False
            self.trailID = param
            return True
        except:
            print_exception()
            return False

    def set_capacity_GB(self, param):
        try:
            if param is None or not isinstance(param, (int, long)) or param <= 0:
                return False
            self.capacity_GB = param
            return True
        except:
            print_exception()
            return False

    def dumpinfo(self):
        try:
            '''
            Print the info contained in this object
            '''
            debug("     --- Tape Drive Object dump ---")
            debug("         Drive Sequence #: "+str(self.seq))
            debug("         Drive Vendor....: "+self.vendor)
            debug("         Drive Model.....: "+self.model)
            debug("         VTL Tape ID Str.: "+self.trailID)
            debug("         Capacity (GB)...: "+str(self.capacity_GB))
        except:
            debug("ERROR : AtlTapeDrive.dumpinfo() : Exception dumping object data. Exception information follows (if available).")
            print_exception()


'''
Get the IP address assigned to eth0, or the first valid IP address assigned
to this ILIO.
In practice, this will return the IP address assigned to eth0.
This function also sets the global variable INTERFACE_IPS which is a
dictionary containing eth_name -> IP mappings.

Parameters:
    None

Returns:
    String containing first valid IP Address, if one was found
    Returns empty string on errors
'''
def get_first_valid_ipaddress():
    ret = ""
    interface_ips = {}
    try:
        # Get list of all network interfaces in the system
        ifaces = ni.interfaces()
        # remove the 'lo' interface from this list
        ifaces.remove('lo')

        debug("Getting IP Addresses...")
        for iface in ifaces:
            ifacestr = str(iface)
            ipaddr = ni.ifaddresses(iface)[2][0]['addr']
            if ipaddr is None or not ipaddr:
                continue
            interface_ips[ifacestr] = ipaddr
            debug("   "+ifacestr+" = "+ipaddr)


        if len(interface_ips) <= 0:
            debug("ERROR : Could not get a valid IP Address on any interface.")
            return ""

        # Set ret to the first IP address in the dictionary. This is for safety
        ret = interface_ips.values()[0]

        # Now get the IP address of eth0. If no eth0 (how did that happen??),
        # We just return the first available IP address we got above
        # Later we can modify this to get the IP address of a particular NIC
        if interface_ips.has_key('eth0'):
            myret = interface_ips.get('eth0')
            if myret is None or not myret:
                debug("WARNING: Did not get IP Address of eth0.")
            else:
                debug("INFO : Got IP Address for eth0: "+myret)
                ret = myret

        return ret
    except:
        debug("ERROR : Exception getting IP addresses assigned to this ILIO. Exception info follows (if available)")
        print_exception()
    return ret



def signal_handler(signum, frame):
    '''
    Function to trap POSIX signals sent to this program.
    According to http://www.gnu.org/savannah-checkouts/gnu/libc/manual/html_node/Termination-Signals.html
    we cannot hook the SIGKILL function.
    SIGSTOP/SIGSTP is also not explicitly handled because not all systems define
    this signal.
    The signals which we currently handle are:
        SIGINT
        SIGQUIT
        SIGTERM
        SIGHUP

    Parameters:
        signum : Signal Number
        frame: Object referring to the current stack frame

    Returns:
        This function does not return anything.

    Reference:
        http://docs.python.org/2/library/signal.html
    '''

    debug("WARNING : Termination signal received! For safety, and to prevent the system config being in an inconsistent state, IGNORING termination signal.")


def print_exception():
    '''
    If an exception exists, print it.
    Then clear the exception info
    '''
    if sys.exc_info()[0]:
        debug("EXCEPTION: Exception info: "+str(sys.exc_info()[0])+"\n"+str(sys.exc_info()[1])+"\n"+traceback.format_exc())
    else:
        debug("EXCEPTION : No exception info found, even though there was an exception. Excepinception? Sorry...")

    # Clear the exception
    sys.exc_clear()


def read_usx_vtl_config_file(path_to_file):
    '''
    Reads the USX VTL Config file at the given path, and returns a 
    Dictionary object containing the contents, if any, of the read
    config. This file contains USX-specific configuration items used
    to setup the VTL infrastructure. 

    path_to_file is usually USX_VTL_CONF_FILEPATH

    The returned dictionary object contains key-value pairs which are
    pretty much exact representations of the contents of the config
    file.

    Parameters:
        path_to_file    - String. Path to the USX VTL config file.

    Returns:
        Dictionary object containing key-value pairs corresponding to the
        data in the config file.
        Returns an empty dictionary object on errors.
    '''
    if path_to_file is None or not path_to_file or not path_to_file.strip():
        debug("ERROR : read_usx_vtl_config_file : Path to file was null or empty!")
        return {}

    try:
        # Does the file exist?
        # Yes, I'm aware of TOCTTOU bugs. That's what the try-catch is for.
        if not os.path.isfile(path_to_file):
            debug("WARNING : read_usx_vtl_config_file : File "+path_to_file+" does not exist. Returning empty dict.")
            return {}

        retval = {}
        debug("INFO : read_usx_vtl_config_file : Reading config file: "+path_to_file)
        conf = ConfigParser.ConfigParser()
        conf.read(path_to_file)

        ###### Now get the settings specified in the file.
        # Expected Dedup ratio. This goes into the dict as an int.
        dedup_expected_int = -1
        try:
            dedup_expected = conf.get(USX_VTL_CONF_SECTIONHEADER_STRING, USX_VTL_CONF_DEDUP_PERCENT_SETTING_KEY)
            if dedup_expected is None or not dedup_expected:
                debug("WARNING : read_usx_vtl_config_file : Failed to get value for Expected Dedup ratio, not adding it to output dict.")
            else:
                try:
                    dedup_expected_int = int(dedup_expected)
                    if dedup_expected_int < 0 or dedup_expected_int > 100: # Error
                        debug("WARNING : read_usx_vtl_config_file : Expected Dedup ratio outside expected range (0-100), not adding it to output dict.")
                        dedup_expected_int = -666
                except:
                    debug("WARNING : read_usx_vtl_config_file : Expected Dedup ratio does not seem to be an integer, not adding it to output dict.")
        except:
            debug("WARNING : read_usx_vtl_config_file : Exception getting value for Expected Dedup ratio, not adding it to output dict.")

        if dedup_expected_int >= 0: # < 0 Indicates error
            debug("INFO : read_usx_vtl_config_file : Setting "+USX_VTL_CONF_DEDUP_PERCENT_SETTING_KEY+" to "+dedup_expected)
            retval[USX_VTL_CONF_DEDUP_PERCENT_SETTING_KEY] = dedup_expected_int

        #### Now parse any other config settings here, in a similar way.

        return retval

    except:
        debug("ERROR : read_usx_vtl_config_file : Exception reading USX VTL config file. Exception info follows (if available).")
        print_exception()
        return {}


def write_usx_vtl_default_conf_file(path_to_file, overwrite=False):
    '''
    Using the template declared in this script, write the default USX VTL
    config file to the specified path (path is usually USX_VTL_CONF_FILEPATH).

    Parameters:
        path_to_file    - The path and filename of the config file. This is
                          usually USX_VTL_CONF_FILEPATH.
        overwrite       - Boolean. Overwrite file if it already exists.
                          Defaults to False.

    Returns:
        True    - Successfully wrote file to specified path
        False   - Failed to write file; error and/or exception occurred.
    '''
    if path_to_file is None or not path_to_file or not path_to_file.strip():
        debug("ERROR : write_usx_vtl_default_conf_file : Path to file was null or empty!")
        return False
    try:
        # Does the file exist?
        # Yes, I'm still aware of TOCTTOU bugs.
        if  os.path.isfile(path_to_file):
            if overwrite:
                debug("WARNING : write_usx_vtl_default_conf_file : USX VTL config File '"+path_to_file+"' exists, and overwrite=True. OVERWRITING existing file.")
            else:
                debug("WARNING : write_usx_vtl_default_conf_file : USX VTL config File '"+path_to_file+"' exists, and overwrite=False. NOT OVERWRITING existing file.")
                return False

        usx_vtl_conf = USX_VTL_CONF_TEMPLATE.substitute(SECTIONHEADER_STRING=USX_VTL_CONF_SECTIONHEADER_STRING, DEDUP_PERCENT_SETTING_KEY=USX_VTL_CONF_DEDUP_PERCENT_SETTING_KEY, DEFAULT_DDP_PCT_STR = USX_VTL_DEFAULT_DDP_PCT_STR)
        debug("INFO : write_usx_vtl_default_conf_file : Writing USX VTL config File '"+path_to_file+"' (overwrite="+str(overwrite)+")...")
        debug("         Writing the following data to USX VTL config file: "+path_to_file)
        debug("-------------- BEGIN : DATA BEING WRITTEN TO:  "+path_to_file+" -----------------")
        debug(usx_vtl_conf)
        debug("-------------- END : DATA BEING WRITTEN TO:  "+path_to_file+" -----------------")
        with open(path_to_file, 'w') as f:
            f.write(usx_vtl_conf)

        return True
    except:
        debug("ERROR : write_usx_vtl_default_conf_file : Exception writing USX VTL default config file. Exception info follows (if available).")
        print_exception()
        return False


def get_dedup_dir():
    '''
    Gets the mount point of the DedupFS, if it is mounted.

    Parameters:
        None

    Returns:
        String containing mount point of DedupFS if it is mounted.
        Empty string if DedupFS is not mounted, or on errors.
    '''
    cmd = "mount | egrep 'ext4|dedup|btrfs' | grep 'exports'|grep -v 'grep' | cut -d' ' -f3"
    ret, msg = runcmd(cmd)
    if ret != 0:
        debug("ERROR : Could not find Dedup directory mountpoint")
        return ""

    if msg is None or not msg:
        debug("ERROR : Searching for Dedup directory mountpoint returned nothing")
        return ""

    # Remove newline from msg
    msg = msg.strip()
    return msg

def get_mount_point_type(mountpoint):
    cmd_mount = "mount | grep '%s'|grep -v 'grep'|awk  '{print $5}'" % mountpoint
    ret, res = runcmd(cmd_mount)
    if ret != 0 or res is None:
        return None
    return res.strip()

def get_dedup_available_size_in_kbytes(dedup_mountpoint):
    '''
    Gets the size, in kilobytes of the free space available on the specified
    DedupFS mountpoint. Uses df for this.

    Parameters:
        dedup_mountpoint    - The mount point of the DedupFS whose free size
                              we want.

    Returns:
        String containing the free size in kilobytes.
        Returns empty string on errors or exceptions.
    '''
    if dedup_mountpoint is None or not dedup_mountpoint.strip():
        debug("ERROR : get_dedup_available_size_in_kbytes : No valid mountpoint parameter found!")
        return ""

    mount_type = get_mount_point_type(dedup_mountpoint)
    try:
        ret,res = runcmd("df -t %s --output=avail -k %s | tail -n1" % (mount_type, dedup_mountpoint.strip()))
        if ret != 0 or res is None or not res.strip():
            debug("ERROR : get_dedup_available_size_in_kbytes : Failed to get available size of dedupFS mounted at: "+dedup_mountpoint)
            return ""

        # if we got here, we have a valid res. Try to convert it to a number, to make sure that it is valid.
        res = res.strip()
        long(res)

        # If we got here without getting an exception, it is a valid number. Return it.
        debug("INFO : get_dedup_available_size_in_kbytes : Available (free) size of '"+dedup_mountpoint+"' in kB: "+res)
        return res

    except:
        debug("ERROR : get_dedup_available_size_in_kbytes : Exception getting available size of Dedup mountpoint!")
        return ""


def check_vm_release_version():
    '''
    Check whether the VM release version in RELEASE_VERSION_FILE is
    greater than or equal to MINIMUM_REQUIRED_RELEASE_VERSION. 

    This function is required because the VTL functionality (and especially
    its dependencies) is only meant for USX release versions above USX-3.0

    This function uses the parse_version() funtion from the pkg_resources
    library to perform the version check comparison.

    Parameters:
        None

    Returns:
        True    - Release version >= Minimum required version, alles klaar :)
        False   - Release version < Minimum required version, cannot proceed :(
    '''
    try:
        vm_ver = ""
        with open(RELEASE_VERSION_FILE, 'r') as f:
            vm_ver = f.readline().strip()

        if vm_ver is None or not vm_ver:
            debug("ERROR : check_vm_release_version : Could not determine VM Release version!")
            return False

        debug("INFO : check_vm_release_version : VM release version from file....: "+vm_ver)
        debug("INFO : check_vm_release_version : Minimum required release version: "+MINIMUM_REQUIRED_RELEASE_VERSION)
        isvalid = parse_version(vm_ver) >= parse_version(MINIMUM_REQUIRED_RELEASE_VERSION)
        if not isvalid:
            debug("ERROR : check_vm_release_version : VM Release version is NOT greater than or equal to minimum required release version!")
        else:
            debug("INFO : check_vm_release_version : VM Release version is greater than or equal to minimum required release version.")

        return isvalid

    except:
        debug("ERROR : check_vm_release_version : Exception checking release version. Exception was:")
        debug((sys.exc_info()[0]+sys.exc_info()[1]+sys.exc_info()[2]) if sys.exc_info()[0] else "No Exception information gleaned from system.")
        return False


def is_modprobed(kernmod):
    '''
    Checks (using 'lsmod | grep') whether the specified kernel module is loaded.

    Parameters:
        kernmod  - String with name of kernel module to be checked.

    Returns:
        True    - Kernel module is loaded.
        False   - Failed to check whether kernel module is loaded.
    '''
    if kernmod is None or not kernmod:
        debug("ERROR : is_modprobed : No valid kernel module name specified.")
        return False

    try:
        ret,res = runcmd("lsmod | grep -w "+ kernmod + " | grep -v grep")
        if ret == 0:
            debug("INFO : is_modprobed : Successfully found loaded kernel module: "+kernmod)
            return True
        else:
            debug("INFO : is_modprobed : Could NOT find loaded kernel module: "+kernmod)
            return False
    except:
        debug("ERROR : is_modprobed : Exception checking for loaded kernel module, exception info follows if available. ")
        print_exception()
        return False



def modprobe(kernmod):
    '''
    Loads (using 'modprobe') the specified kernel module.

    Parameters:
        kernmod  - String with name of kernel module to be loaded.
                   Note that it should be the module filename WITHOUT the
                   '.ko' extension, otherwise modprobe will fail.
                   E.g if you want to load 'fcst.ko', you pass in 'fcst'.

    Returns:
        True    - Successfully loaded kernel module, or module already loaded.
        False   - Failed to load kernel module.
    '''
    if kernmod is None or not kernmod:
        debug("ERROR : modprobe : No valid kernel module name specified.")
        return False

    try:
        if is_modprobed(kernmod):
            debug("INFO : modprobe : Kernel module already loaded: "+kernmod)
            return True

        ret,res = runcmd("modprobe "+kernmod)
        if ret == 0:
            debug("INFO : modprobe : Load succeeded, verifying load of kernel module: "+kernmod)
            if is_modprobed(kernmod):
                debug("INFO : modprobe : Successfully loaded kernel module: "+kernmod)
                return True

        debug("ERROR : modprobe : Failed to load kernel module: "+kernmod)
        return False
    except:
        debug("ERROR : modprobe : Exception loading kernel module, exception was: ")
        return False


def rmmod(kernmod):
    '''
    Unloads (using 'rmmod') the specified kernel module.

    Parameters:
        kernmod  - String with name of kernel module to be unloaded.
                   Note that it should be the module filename WITHOUT the
                   '.ko' extension, otherwise rmmod will fail.
                   E.g if you want to unload 'fcst.ko', you pass in 'fcst'.

    Returns:
        True    - Successfully unloaded kernel module.
        False   - Failed to unload kernel module.
    '''
    if kernmod is None or not kernmod:
        debug("ERROR : rmmod : No valid kernel module name specified.")
        return False

    try:
        # First see if the module is loaded. If it's not, there's nothing to
        # unload.

        if not is_modprobed(kernmod):
            debug("WARNING : rmmod : Kernel module '"+kernmod+"' does not appear to be loaded. Nothing to unload. Returning success.")
            return True
        # If we got here, we need to unload it.
        ret,res = runcmd("rmmod "+kernmod)
        if ret == 0:
            debug("INFO : rmmod : Unload succeeded, verifying unload of  kernel module: "+kernmod)
            if not is_modprobed(kernmod):
                debug("INFO : rmmod : Kernel module '"+kernmod+"' successfully unloaded.")
                return True

        debug("ERROR : rmmod : Failed to load kernel module: "+kernmod)
        return False
    except:
        debug("ERROR : rmmod : Exception unloading kernel module, exception was: ")
        return False



def does_progam_exist(progname):
    '''
    Checks whether the given program name exists on the system and is in path.
    Uses the system 'which' command for this. Note that this will fail if the
    program exists in a location which is not in the PATH variable of this 
    process.

    Parameters:
        progname    - Program name to check. 

    Returns:
        True    - Program was found in PATH by 'which'.
        False   - Failed to find program, perhaps due to errors/exceptions.
    '''
    if progname is None or not progname:
        debug("ERROR : does_program_exist : Program name to check is null or empty!")
        return False
    try:
        ret,res = runcmd("which "+progname)
        if ret == 0:
            debug("INFO : does_progam_exist : Found in current PATH: "+progname)
            return True
        else:
            debug("ERROR : does_program_exist : Could not find "+progname)
            return False
    except:
        debug("ERROR : does_program_exist : Exception checking program existence! Exception was:")
        return False


def is_program_running(prog):
    '''
    Checks whether the given program is running. Does this by using
    "ps aux | grep <prog>".

    Parameters:
        prog    - String. Name of process to check.

    Returns:
        0   - Program is running.
        1   - Program is not running.
        2   - Error/Exception checking whether program is running.
    '''
    if prog is None or not prog  or not prog.strip():
        debug("ERROR : is_program_running : prog name parameter was null or empty.")
        return 2
    try:
        retval = 2
        ret,res = runcmd("ps aux | grep -w "+prog+" | grep -v grep")
        if ret == 0:
            debug("INFO : is_program_running : Program '"+prog+"' IS running.")
            retval = 0
        else:
            debug("INFO : is_program_running : Program '"+prog+"' IS NOT running.")
            retval = 1

        if retval == 2:
            debug("ERROR : is_program_running : Could not determine whether '"+prog+"' is running.")

        return retval

    except:
        debug("ERROR : is_program_running : Exception checking whether program was running. Exception info follows (if available).")
        print_exception()
        return 2


def check_mhvtl_installed(scst_setup_type="iscsi"):
    '''
    Checks whether mhvtl and its dependencies are installed.
    It does the following checks:
        1. Can the following kernel modules be modprobed:
            a. mhvtl
            b. SCST tape, iSCSI, fcst & Qlogic FC  kernel modules
            c. Other kernel modules as defined in globals above
        2. Do the following userspace programs exist:
            a. vtltape
            b. vtllibrary
            c. /etc/init.d/mhvtl
            d. mktape
            e. vtlcmd
            f. mtx
            g. Any other programs as defined in USERSPACE_PROGRAMS above
        3. Do the System VTL users (as specified by the globals called
            "SYS_VTL_USER" and "SYS_VTL_GROUP" exist on the system?

        Parameters:
            scst_setup_type - String containing one of:
                          "iscsi" - iSCSI SCST setup required of VTL devices
                          "fc" - Fibrechannel SCST setup required of VTL devices
                          "fcoe" - FCoE SCST setup required of VTL devices
                          If not specified, defaults to iscsi.

        Returns:
            True - everything in the list above was found, vtl and deps OK.
            False - Could not verify one or more required components exist.
    '''
    if scst_setup_type is None or not scst_setup_type.strip():
        debug("ERROR : check_mhvtl_installed : No SCST setup type specified!")
        return False
    try:
        retval = True
        kernret = True
        userret = True
        debug("INFO : check_mhvtl_installed : Checking whether VTL and dependencies are correctly installed...")

        uret = False
        gret = False
        ret,res = runcmd("cat /etc/passwd | grep -w "+SYS_VTL_USER+" | grep -v grep")
        if ret != 0:
            debug("WARNING : check_mhvtl_installed : System VTL user '"+SYS_VTL_USER+"' not found. VTL MAY NOT WORK!")
        else:
            uret = True
        ret,res = runcmd("cat /etc/group | grep -w "+SYS_VTL_GROUP+" | grep -v grep")
        if ret != 0:
            debug("WARNING : check_mhvtl_installed : System VTL GROUP '"+SYS_VTL_GROUP+"' not found. VTL MAY NOT WORK!")
        else:
            gret = True

        userret = uret and gret

        kernmods = None
        try:
           kernmods = SCST_SETUP_TYPE_KERNMOD_DICT[scst_setup_type]
        except:
            debug("ERROR : check_mhvtl_installed : Exception getting list of kernel mods for setup type '"++"'. Are you sure this type is correct?")
            return False

        if kernmods is None:
            debug("ERROR : check_mhvtl_installed : List of kernel mods for setup type '"++"' is Null. This should not have happened.")
            return False

        debug("INFO : check_mhvtl_installed : Checking whether kernel modules are correctly installed for setup type "+scst_setup_type)
        for kmod in kernmods:
            if not modprobe(kmod):
                kernret = False

        if kernret is False:
            debug("ERROR : check_mhvtl_installed : Failed to verify loading of one or more kernel modules!")
        else:
            debug("INFO : check_mhvtl_installed : Verified successful loading of all required kernel modules!")

        # Check whether our required programs are found
        progret = True
        debug("INFO : check_mhvtl_installed : Checking whether required userspace programs are correctly installed...")
        for prog in USERSPACE_PROGRAMS:
            if not does_progam_exist(prog):
                debug(    "setting progret to false")
                progret = False
        if progret is False:
            debug("ERROR : check_mhvtl_installed : Failed to verify existence of one or more userspace programs!")
        else:
            debug("INFO : check_mhvtl_installed : Verified existence of all required userspace programs.")

        retval = kernret and progret and userret
        if retval is False:
            debug("ERROR : check_mhvtl_installed : Failed to verify correct installation of VTL dependencies!")
        else:
            debug("INFO : check_mhvtl_installed : Verified correct installation of VTL dependencies.")

        return retval

    except:
        debug("ERROR : check_mhvtl_installed : Exception verifying correct installation of VTL dependencies!")
        return False


def slots_required(dedup_avail_size_kb, tape_cap_GB, expected_dedup_pct):
    '''
    For a given DedupFS size, figure out how many slots are required for the
    given tape type.
    'Slots' here refers to the virtual slots in the VTL. Each slot contains a
    (possibly barcoded) tape in an actual tape library.

    This function accepts a parameter for the expected deduplication 
    percentage, and adds extra slots based on this. This basically leads to
    slot overprovisioning, to better utilize the data savings of the DedupFS.

    For example, if we have:
          tape_cap_GB = 1000
          dedup_avail_size_kb (converted to GB) = 2000
        Then ordinarily the number of slots required = 2
        However, if expected_dedup_pct = 50, then we have:
          tape_cap_GB = 1000
          dedup_size (GB) = 2000 + (2000 * 50%) = 3000
        and thus, slots required = 3

    Parameters:
        dedup_avail_size_kb - String indicating size of the DedupFS, in KB,
                              available to us for tape storage.

        drive_capacity_GB   - Integer. The capacity in GB of the tape drive
                              model which we're going to use with the slots.

        expected_dedup_pct  - Integer. The expected percentage of deduplication
                              we expect on the DedupFS. This is usually given
                              in the USX VTL config file present at 
                              USX_VTL_CONF_FILEPATH.

    Returns:
        Positive int (>=0) indicating number of slots available for the given
        tape capacity and given DedupFS size.
        Returns -1 on errors.
    '''
    if dedup_avail_size_kb is None or not dedup_avail_size_kb.strip():
        debug("ERROR : slots_required : Dedup avail size null or empty!")
        return -1
    if tape_cap_GB is None:
        debug("ERROR : slots_required : Tape drive capacity is null or empty!")
        return -1

    try:
        if tape_cap_GB <= 0 :
            debug("ERROR : slots_required : Invalid tape capacity. Need a positive integer > 0 ")
            return -1

        # If we got here, we have a valid tape capacity
        debug("INFO : slots_required : Tape capacity (GB) is: "+str(tape_cap_GB))

        # First, convert the size we got in kB to GB. We convert to GB because we
        # can use the LTO_TAPE_CAPACITY_GB_DICT to figure out how many tape slots
        # are needed.
        ddp_sz_kB = int(dedup_avail_size_kb)
        ddp_sz_GB = ddp_sz_kB / (1024*1024)
        if ddp_sz_GB <= 0:
            debug("ERROR : slots_required : Failed to convert Dedup avail size into GB; got result <= 0 which is meaningless!")
            return -1

        debug("INFO : slots_required : Available Dedup size (GB) is : "+str(ddp_sz_GB))
        debug("INFO : slots_required : Expected Dedup percentage is : "+str(expected_dedup_pct))

        # Calculate the effective filesystem size (GB) taking into account the
        # expected deduplication percentage. We round down, to ensure that we
        # don't overestimate too much (though I suppose it makes no difference)
        efsGB = int(math.floor(ddp_sz_GB + ((float(ddp_sz_GB * expected_dedup_pct)) / 100.0)))
        debug("INFO : slots_required : Effectively available in GB  : "+str(efsGB))


        # Now divide the effective size (GB) by the tape capacity in GB.
        # That's the number of slots.
        # We have some rounding to take care of here. If the dedup size is not
        # an exact integral multiple to tape_cap_GB, then the division will
        # result in truncation of the result. This means that we will actually
        # have one slot LESS than required. So we use math.ceil to do the 
        # upward rounding on the division (after first converting the numbers
        # to floats), and then we cast the result to int.
        # Note that if the dedup size is less than the
        # tape capacity, the division will return 0. In this case, we just set
        # it to 1; it means that 1 tape slot is all that is needed.
        slots = int(math.ceil(float(efsGB) / float(tape_cap_GB)))
        if slots == 0:
            debug("WARNING : slots_required : slots is 0. This may mean that tape capacity > effectively available dedup FS size. Setting slots to 1.")
            slots = 1

        debug("INFO : slots_required : Number of tape slots needed for tape drive capacity "+str(tape_cap_GB)+" GB on Volume with free size "+str(ddp_sz_GB)+" GB and expected dedup rate of "+str(expected_dedup_pct)+"% is: "+str(slots))
        return slots

    except:
        debug("ERROR : slots_required : Exception getting number of slots required from Dedup Available size!")
        return -1


def create_mhvtl_config(robot, drive_type_list,  dedup_mnt, dedup_avail_size_kb):
    '''
    It first sets the ownership of the dedup_mnt directory to 
    SYS_VTL_USER:SYS_VTL_GROUP. Then, it creates the mhvtl config files.
    Creates the mhvtl config from the MHVTL config file templates and the
    supplied parameters.
    It creates the following mhvtl config files:
        mhvtl.conf
        device.conf (with one Library device with ID 10)
        library_contents.10

    *** WARNING : This function overwrites any existing config files.

    *** WARNING : If there was any error creating any of the config files,
    this funtion does NOT clean up after itself. You may be left with an
    inconsistent and/or nonfunctional config.

    ***** YOU HAVE BEEN WARNED! *****

    Parameters:
        robot            - The model name of the Library/robot/changer device.
        drive_type_list  - List of Tape Drive model (string) in the robot. 
        dedup_mnt        - The Dedup mountpoint. The virtual tape data files
                           live within the slot subdirectories in this dir.

    Returns:
        True    - Successfully created config.
        False   - Error creating config.
    '''
    global ROBOT_DICT
    global TAPE_DRIVE_DICT
    if robot is None or drive_type_list is None or dedup_mnt is None or dedup_avail_size_kb is None or not robot.strip() or not (len(drive_type_list) > 0) or not dedup_mnt.strip() or not dedup_avail_size_kb.strip():
        debug("ERROR : create_mhvtl_config : One or more required parameters was null or empty!")
        return False
    drive_obj_list = []
    robot_vendor = ""
    totalslotsneeded = 0
    trailid_list = []
###### TODO : Delete everything in /opt/mhvtl. Found MHVTL bug where even if you specify dir of tapes, if it exists in /opt/mhvtl then mhvtl uses that.
    # Set the ownership of the dedup_mnt dir to SYS_VTL_USER:SYS_VTL_GROUP
    try:
        ret,res = runcmd("chown -R "+SYS_VTL_USER+":"+SYS_VTL_GROUP+" "+dedup_mnt)
        if ret != 0:
            debug("ERROR : create_mhvtl_config : Failed to set ownership of "+dedup_mnt+" to "+SYS_VTL_USER+":"+SYS_VTL_GROUP+", return code was "+str(ret)+" and output was: "+res)
            return False
        debug("INFO : create_mhvtl_config : Successfully set ownership of "+dedup_mnt+" to "+SYS_VTL_USER+":"+SYS_VTL_GROUP+", return code was "+str(ret))
    except:
        debug("ERROR : create_mhvtl_config : Exception setting ownership of  "+dedup_mnt+" to "+SYS_VTL_USER+":"+SYS_VTL_GROUP)
        debug(sys.exc_info()[2])
        return False
    try:
        # Look up the robot device and tape drive details. We'll need this.
        debug("INFO : create_mhvtl_config : Looking up Library/Robot device details for: "+robot)
        robot_vendor,robot_series = ROBOT_DICT[robot]
        if robot_vendor is None or not robot_vendor.strip():
            debug("ERROR : create_mhvtl_config : Failed Looking up Library/Robot device details for: "+robot)
            return False
        robot_vendor = robot_vendor.strip()
    except:
        debug("ERROR : create_mhvtl_config : Exception Looking up Library/Robot/Changer device details for: "+robot)
        debug(sys.exc_info()[2])
        return False

    try:
        drive_seq = 0
        # Process the drive type list
        debug("INFO : create_mhvtl_config : Processing tape drive list and creating Tape Drive objects...")
        for drive_type in drive_type_list:
            drive_seq = drive_seq + 1
            # Can't have more than 99 drives; it messes up the config files
            if drive_seq > 99:
                debug("WARNING : Already processed 99 drives, cannot process any more. breaking out of tape drive list processing loop.")
                break

            debug("     Looking up Tape Drive details for: "+drive_type)
            drive_vendor,trailid,drive_cap_GB = TAPE_DRIVE_DICT[drive_type]
            if drive_vendor is None or trailid is None or not drive_vendor.strip() or not trailid.strip():
                debug("     ERROR : Failed Looking up Tape Drive details for: "+drive_type)
                debug("     Skipping current drive record and proceeding to next one!")
                drive_seq = drive_seq - 1
                continue
            drive_vendor = drive_vendor.strip()
            trailid = trailid.strip()
            debug("     Creating Tape Drive Object with following data: ")
            debug("         Seq#="+str(drive_seq)+", Drive Type='"+drive_type+"', VendorID='"+drive_vendor+"', TrailID='"+trailid+"', TapeCapGB="+str(drive_cap_GB))
            try:
                tapedrive = AtlTapeDrive(drive_seq, drive_type, drive_vendor, trailid, drive_cap_GB)
                if tapedrive is None or not tapedrive:
                    debug("     ERROR : Error creating Tape Drive object for seq#="+str(drive_seq)+", Model="+drive_type)
                    debug("     Skipping current drive record and proceeding to next one!")
                    drive_seq = drive_seq - 1
                    continue
                # If we got this far, we successfully create the tape drive object.
                debug("         Created Tape Drive Object with following data. Adding it to tape drive list. ")
                tapedrive.dumpinfo()
                drive_obj_list.append(tapedrive)
            except:
                debug("     ERROR : Exception creating Tape Drive object for seq#="+str(drive_seq)+", Model="+drive_type)
                debug("     Skipping current drive record and proceeding to next one!")
                drive_seq = drive_seq - 1
                continue

    except:
        debug("ERROR : create_mhvtl_config : Exception processing tape drive list")
        print_exception()
        return False


    try:
        ###### Process USX VTL config file
        ## Read the USX VTL config file (if we have it) to get the expected
        ## dedup ratio (and other settings when implemented).
        ddp_pct = int(USX_VTL_DEFAULT_DDP_PCT_STR)
        # Does the file exist?
        # Yes, still aware of TOCTTOU bugs. 
        if not os.path.isfile(USX_VTL_CONF_FILEPATH):
            # If the file doesn't exist, create it.
            debug("INFO : create_mhvtl_config : USX VTL default config file does not exist. Setting expected deduplication percentage to default value of "+USX_VTL_DEFAULT_DDP_PCT_STR+" and creating the file...")
            if not write_usx_vtl_default_conf_file(USX_VTL_CONF_FILEPATH):
                debug("WARNING : create_mhvtl_config : USX VTL default config file did not exist, and failed to create it. Continuing.")
        else:
            # Read the data from file
            confdict = read_usx_vtl_config_file(USX_VTL_CONF_FILEPATH)
            if confdict is None or not confdict:
                debug("WARNING : create_mhvtl_config : Failed to read data from USX VTL default config file. Setting expected deduplication percentage to default value of "+USX_VTL_DEFAULT_DDP_PCT_STR+"%.")
            else:
                # We have the data; process it
                # Read the default deduplication percentage expected for this setup.
                try:
                    ddp_pct = confdict[USX_VTL_CONF_DEDUP_PERCENT_SETTING_KEY]
                    debug("INFO : create_mhvtl_config : Setting expected Deduplication percentage to "+str(ddp_pct)+" as read from USX VTL config file.")
                except:
                    debug("WARNING : create_mhvtl_config : Exception reading value of expected deduplication percentage from USX VTL default config file. Setting expected deduplication percentage to default value of "+USX_VTL_DEFAULT_DDP_PCT_STR+"%.")
                    ddp_pct = int(USX_VTL_DEFAULT_DDP_PCT_STR)

        debug("INFO : create_mhvtl_config : Using "+str(ddp_pct)+" as value of expected deduplication percentage.")


        ######## Now process the tape drive objects list and write the config files
        debug("INFO : create_mhvtl_config : Processing Tape Drive Objects and writing config files...")
        libconts_drivesec = ""
        libconts_mapsec = ""
        devconf_tapedrivesec = ""

        # Process Tape drive objects, calculate slots needed
        # Write the Tape drives section of the device.conf template
        # Write the Drives and Map sections of the library_contents template
        debug("     Processing Tape Drive Objects and writing config files...")
        for drive in drive_obj_list:
            try:
                seq = drive.get_seq()
                model = drive.get_model()
                vendor = drive.get_vendor()
                trailid = drive.get_trailID()
                capacity_GB = drive.get_capacity_GB()
                # Check for the required tape data we need to create the templates
                if vendor is None or trailid is None or model is None or seq is None or capacity_GB is None \
                        or not vendor or not trailid or seq <= 0 or capacity_GB <= 0 :
                    debug("         ERROR : Failed to obtain valid data for Tape Drive record being processed.")
                    debug("         Skipping this Tape drive, continuing on to next Tape Drive record if any.")
                    continue
                # Compute how many slots are needed for the given Dedupfs size
                slots = slots_required(dedup_avail_size_kb, capacity_GB, ddp_pct)
                if slots <= 0:
                    debug("         ERROR : Failed to calculate valid number of slots required. Expected positive value but got: "+str(slots))
                    debug("         Skipping drive with sequence number="+str(seq)+" and model="+model+" and capacity="+capacity_GB+" GB, continuing on to next record if any.")
                    continue
                debug("         Slots required for Tape type '"+model+"' for Dedup with available size of "+ddp_size_kb_str+" kB is: "+str(slots))
                # Now, for as many slots as needed for this drive type, we
                # append as many trailid entries to the trailid_list
                # FIXME : There is a bug here. If we have multiple drives of
                # the same type, then we end up having more slots than the 
                # volume can hold. This may be why mtx next fails.
                # FIXME : We are not taking deduplication into account.
                # How much overprovisioning should we cater for?
                for i in range(1, slots+1):
                    trailid_list.append(trailid)

                # Now write the tape drive section of device.conf
                seq_2padded = str(seq).zfill(2)
                seqstr = str(seq)
                devconf_tapedrivesec = devconf_tapedrivesec + DRIVE_TEMPLATE.substitute(DRIVE_SEQ_PLUS_10=str(seq+10), DRIVE_SEQ_2PADDED=seq_2padded, TAPEDRIVE_VENDOR=vendor, TAPEDRIVE_MODEL=model)
                devconf_tapedrivesec = devconf_tapedrivesec+"\n"

                # Now write the Drives section of library_contents
                libconts_drivesec = libconts_drivesec + LIBRARY_CONTENTS_DRIVE_TEMPLATE.substitute(DRIVE_SEQ=seqstr)
                libconts_drivesec = libconts_drivesec + "\n"

                # Now write the MAP section library_contents
                libconts_mapsec = libconts_mapsec + LIBRARY_CONTENTS_MAP_TEMPLATE.substitute(DRIVE_SEQ=seqstr)
                libconts_mapsec = libconts_mapsec+"\n"
            except:
                debug("         ERROR : Exception processing current drive Object. Exception info follows (if available).")
                print_exception()
                debug("         Skipping drive with sequence number="+str(seq)+" and model="+model+" and capacity="+str(capacity_GB)+" GB, continuing on to next record if any.")
                continue
        # End : for drive in drive_obj_list

        # Sanity check to see if we have the needed data
        if libconts_drivesec is None or libconts_mapsec is None or devconf_tapedrivesec is None \
                or not libconts_drivesec.strip() or not libconts_mapsec.strip() or not devconf_tapedrivesec.strip():
                    debug("ERROR : create_mhvtl_config : failed to correctly parse one or more required data items. Cannot write configs.")
                    return False

        #### Write mhvtl.conf
        fname = MHVTL_CONF_DIR+MHVTL_CONF_FILENAME
        debug("         Writing the following data to device.conf config file: "+fname)
        debug("-------------- BEGIN : DATA BEING WRITTEN TO:  "+fname+" -----------------")
        debug(MHVTL_CONF_TEMPLATE)
        debug("-------------- END : DATA BEING WRITTEN TO:  "+fname+" -----------------")
        debug("     Writing : "+fname)
        with open(fname, "w") as f:
            f.write(MHVTL_CONF_TEMPLATE)

        #### write device.conf
        # Collate everything written so far.
        fname = MHVTL_CONF_DIR+MHVTL_DEVICECONF_FILENAME
        debug("     Writing : "+fname)
        deviceconflibstr = DEVICE_CONF_TEMPLATE.substitute(LIBRARY_VENDOR=robot_vendor, LIBRARY_DEVICE_MODEL=robot, DEDUP_MNT=dedup_mnt)
        deviceconfstr = deviceconflibstr + "\n" + devconf_tapedrivesec
        debug("         Writing the following data to device.conf config file: "+fname)
        debug("-------------- BEGIN : DATA BEING WRITTEN TO:  "+fname+" -----------------")
        debug(deviceconfstr)
        debug("-------------- END : DATA BEING WRITTEN TO:  "+fname+" -----------------")

        # Finally, write the actual device.conf file to the system
        with open(fname, "w") as f:
            f.write(deviceconfstr)

        ##### write library_contents
        fname = MHVTL_CONF_DIR+MHVTL_LIBRARYCONTENTS_FILENAME
        debug("     Writing : "+fname)
        libcontsmain = LIBRARY_CONTENTS_TEMPLATE
        libconts_pickersec = LIBRARY_CONTENTS_PICKER_TEMPLATE
        # Build up the "Slots" section of the library_contents file
        i = 0
        slotstr = ""
        for trailid in trailid_list:
            i = i+1
            padded_i = str(i).zfill(4)
            slotstr = slotstr + LIBRARY_CONTENTS_SLOT_TEMPLATE.substitute(SEQNUM=i, PADDED_SEQNUM=padded_i, TRAIL_ID=trailid)
            slotstr = slotstr+"\n"

        # Add a final slot for a cleaner tape.
        i = i + 1
        slotstr = slotstr + LIBRARY_CONTENTS_SLOT_CLEANER_TEMPLATE.substitute(SEQNUM=i)
        slotstr = slotstr+"\n"

        # Now put all the bits together
        libconts = libcontsmain +"\n"+libconts_drivesec+"\n"+libconts_pickersec+"\n"+libconts_mapsec+"\n" + slotstr

        debug("         Writing the following data to library_contents config file: "+fname)
        debug("-------------- BEGIN : DATA BEING WRITTEN TO:  "+fname+" -----------------")
        debug(libconts)
        debug("-------------- END : DATA BEING WRITTEN TO:  "+fname+" -----------------")

        # Now write the file
        with open(fname, "w") as f:
            f.write(libconts)

        debug("INFO : create_mhvtl_config : Successfully created all required config files.")
        return True


    except:
        debug("ERROR : create_mhvtl_config : Exception creating one or more VTL config files!")
        print_exception()
        return False


def stop_mhvtl():
    '''
    Stops the mhvtl service and checks whether any vtl processes are running.

    Parameters:
        None

    Returns:
        True    - Successfully stopped mhvtl.
        False   - Failed stopping mhvtl and/or errors occurred.
    '''
    try:
        # First unload any loaded tapes in the first drive of all robots.
        # TODO : Modify below code to handle ALL loaded tape drives of
        # all robots. Since right now we only support a single robot with
        # a single tape drive, it's not an issue.
        robot_objs = get_scsi_info_vtl(ROBOT_SCSI_DEVTYPE_STRING)
        for robot in robot_objs:
            sg_dev = robot.get_generic_nodename()
            if sg_dev is None or not sg_dev or not sg_dev.strip():
                continue
            unload_tape_slot_from_drive(sg_dev)

        debug("INFO : stop_mhvtl : stopping mhvtl....")
        ret,res = runcmd(MHVTL_INIT_SCRIPT+" stop")
        if ret != 0:
            debug("WARNING : stop_mhvtl : Service stop indicated failure. VTL might still be running.")

        # Need to wait a little for things to settle down.
        time.sleep(6)

        # We need to ignore the name of this script in ps, since it itself is
        # originally called start-vtl.py[c].
        debug("INFO : stop_mhvtl : Checking for any running vtl processes...")
        ret,res = runcmd("ps aux | grep -i vtl | grep -v grep | grep -v "+MYNAME)
        if ret == 0:
            # Uh oh, something is still running...
            debug("ERROR : stop_mhvtl : Found some running vtl processes: "+res)
            debug("ERROR : stop_mhvtl : Failed to stop mhvtl!")
            return False

        debug("INFO : stop_mhvtl : Did not find any running vtl processes. This is good :-)")

        return True
    except:
        debug("ERROR : stop_mhvtl : Exception stopping mhvtl! Exception info follows (if available).")
        print_exception()
        return False


def silent_rmmod(kernmod):
    '''
    Quietly try to rmmod the given kernel module. Don't display any messages.

    Parameters:
        None

    Returns:
        Nothing.
    '''
    try:
        if kernmod is None or not kernmod.strip():
            return
        runcmd("rmmod "+kernmod)
    except:
        return
    finally:
        return



def stop_scst():
    '''
    Stops SCST, and attempts to remove as many loaded scst modules as it can.

    Even if SCST is running in a particular export mode - iscsi, fc, fcoe
    (each of which has its own set of loaded SCST kernel modules), this 
    function will attempt an unload of all possible kernel modules. This is
    necessitated by the fact that sometimes SCST will not stop unless the 
    irrelevant modules (i'm looking at you, qla2x) are unloaded, even if
    they're not being actually used. Sigh.

    Parameters:
        None

    Returns:
        True    - Stopped SCST service (even if some modules could not be
                  unloaded).
        False   - Could not stop SCST service, and/or errors encountered.
    '''
    try:
        try:
            # First, try to unload as many SCST kernel modules as we can. We
            # use the rmmod_silent() function since we don't want any debug 
            # messages displayed - this is just to unload any unused SCST modules
            # so that the service actually stops instead of complaining that some
            # module is still using it. Of course, modules actually being used
            # will not actually be unloaded, but we don't care about that at this
            # point.
            for kkey in SCST_SETUP_TYPE_KERNMOD_DICT:
                for ddata in SCST_SETUP_TYPE_KERNMOD_DICT[kkey]:
                    silent_rmmod(ddata)
        except:
            print "" # Ignore any exceptions for this.

        # Now actually try to stop the SCST service. It's OK if it's not
        # running and you try to stop it, because it still returns 0.
        debug("INFO : stop_scst : Stopping SCST service...")
        ret,res = runcmd("service scst stop")
        if ret != 0:
            debug("ERROR : stop_scst : SCST service stop failed with message: "+res)
            return False

        # wait a while...
        time.sleep(3)

        # If the above worked, we do a silent rmmod again. COPYPASTA!!!
        try:
            for kkey in SCST_SETUP_TYPE_KERNMOD_DICT:
                for ddata in SCST_SETUP_TYPE_KERNMOD_DICT[kkey]:
                    silent_rmmod(ddata)
        except:
            print "" # Ignore any exceptions for this.

        return True
    except:
        debug("ERROR : stop_scst : Exception stopping SCST. Exception info follows (if available).")
        print_exception()
        return False


def start_mhvtl():
    '''
    Modprobe the mhvtl kernel module and start the mhvtl service.

    Parameters:
        None

    Returns:
        True    - Successfully started the mhvtl service.
        False   - Could not start mhvtl and/or errors/exceptions.
    '''
    try:
        if not is_modprobed("mhvtl"):
            debug("INFO : start_mhvtl : Loading mhvtl kernel module...")
            if not modprobe("mhvtl"):
                debug("ERROR : start_mhvtl : Failed to load kernel module! Cannot start mhvtl!")
                return False

        debug("INFO : start_mhvtl : Starting mhvtl service...")
        ret,res = runcmd(MHVTL_INIT_SCRIPT + " start")
        # Need to wait a while for the SCSI subsystem to fully initialize all
        # the VTL SCSI devices
        time.sleep(3)
        if ret == 0:
            debug("INFO : start_mhvtl : Successfully started mhvtl service. Output was: "+res)
            return True

        # If we got here, we failed.
        debug("Error : start_mhvtl : FAILED to start mhvtl service. Return code was "+str(ret)+" andOutput from start command was: "+res)
        return False

    except:
        debug("ERROR : start_mhvtl : Exception starting mhvtl. Exception info follows (if available).")
        print_exception()
        return False


def get_scsi_info_vtl(vtl_device_type):
    '''
    This function gets the output of 'lsscsi -g' on the system for all
    existing VTL devices of the specified type (either of Changer/Robot/
    Library devices, OR Tape Drive devices). It then processes this info
    and returns a list of AtlScsiDeviceInfo objects corresponding to the
    requested device type for all such currently present devices currently
    registered in the kernel's SCSI subsystem.

    Obviously, this function is only useful if mhvtl is actually properly
    up and running, otherwise this function won't be able to return anything
    useful.

    Parameters:
        vtl_device_type - A string containing the value of either the
                          ROBOT_SCSI_DEVTYPE_STRING global variable,
                          or the TAPEDRIVE_SCSI_DEVTYPE_STRING global.
                          If the passed in type is not one of these, the
                          function won't like it.

    Returns:
        On Succes, returns a list of AtlScsiDeviceInfo objects populated with
            the SCSI info for the required VTL device type.
        On Failure, returns a list of whatever objects it could successfully
            build up - a possibly incorrect or incomplete list. I thought that
            this would be better than just returning an empty list. This way,
            whatever devices we managed to correctly gather can still be used
            for further processing. Right or wrong, it's what I chose...
    '''
    retval = []
    errord = False
    if vtl_device_type is None or not vtl_device_type or (vtl_device_type != ROBOT_SCSI_DEVTYPE_STRING and vtl_device_type != TAPEDRIVE_SCSI_DEVTYPE_STRING):
        debug("ERROR : get_scsi_info_vtl : Invalid parameter for vtl_device_type. Expected either '"+ROBOT_SCSI_DEVTYPE_STRING+"' or '"+TAPEDRIVE_SCSI_DEVTYPE_STRING+"'. Returning empty list.")
        return []

    try:
        debug("INFO : get_scsi_info_vtl : Getting system SCSI information for all system devices of SCSI devtype: "+vtl_device_type)
        ret,res = runcmd("lsscsi -g | grep -w "+vtl_device_type+" | grep -v grep")
        if ret !=0:
            debug("WARNING : get_scsi_info_vtl : FAILED getting system SCSI information for all system devices of SCSI devtype: '"+vtl_device_type+"'. Returnng empty list. This might just mean that there were no SCSI devices of type "+vtl_device_type+" configured on the system.")
            return []

        # Now go through the lsscsi output line-by-line and build up our
        # AtlScsiDeviceInfo objects.
        debug("INFO : get_scsi_info_vtl : Processing obtained system SCSI information SCSI devtype: "+vtl_device_type)
        for ln in [s.strip() for s in res.splitlines()]:
            debug("     Processing SCSI device info record: "+ln)
            fieldz = ln.strip().split()
            numfields = len(fieldz)
            debug("         # fields in current output record: "+str(numfields))
            # Check if we have the correct number of output fields from the
            # 'lsscsi -g' command. Probably don't need this, but better safe..
            if numfields < LSSCSI_COMMAND_EXPECTED_NUM_FIELDS or numfields > (LSSCSI_COMMAND_EXPECTED_NUM_FIELDS + 1):
                debug("         WARNING: get_scsi_info_vtl : Number of fields in SCSI subsytem output record being processed does not match expected number of fields.")
                debug("             Expected "+str(LSSCSI_COMMAND_EXPECTED_NUM_FIELDS)+" but got "+str(numfields)+".")
                debug("             Ignoring this output record and proceeding to next record if available...")
                errord = True
                continue

            # Now check if we've got ONE MORE than the expected numer of fields. 
            # If we do, then likely one of the fields has gotten double-split
            # due to a space in the field e.g."Ultrium 5-SCSI". 
            # Since this is usually the name field, let's munge it.
            # Note, if any other field does this, this munge won't work.
            # But it's late, I'm tired, and so far the only tape field
            # with a pesky space in it is the model field for the HP Ultrium
            # tapes. So the hell with it.
            # TODO: FIXME : Find a better way of dealing with this munge
            if numfields == LSSCSI_COMMAND_EXPECTED_NUM_FIELDS + 1:
                fieldz[3] = fieldz[3]+" "+fieldz[4]
                fieldz[4] = fieldz[5]
                fieldz[5] = fieldz[6]
                fieldz[6] = fieldz[7]

            # If we got here, we've got the correct number of output fields.
            # Process them and build up the data for the AtlScsiDeviceInfo
            # object
            scsi_id = fieldz[0].strip()
            dev_type = fieldz[1].strip()
            vendor = fieldz[2].strip()
            model = fieldz[3].strip()
            revnum = fieldz[4].strip()
            primary_nodename = fieldz[5].strip()
            generic_nodename = fieldz[6].strip()

            # One final sanity check
            if dev_type != vtl_device_type:
                debug("         WARNING : get_scsi_info_vtl : Record device type does not match requested device type.")
                debug("             Expected '"+vtl_device_type+"' but got '"+dev_type+"'")
                debug("             Ignoring this output record and proceeding to next record if available...")
                errord = True
                continue

            # If we still got here, we've got everything we need.
            # Let's build up our Object!
            try:
                myobj = AtlScsiDeviceInfo(scsi_id, dev_type, vendor, model, revnum, primary_nodename, generic_nodename)
                if myobj is None or not myobj:
                    debug("         WARNING : get_scsi_info_vtl : Failed building up SCSI object for current output record.")
                    debug("             Ignoring this output record and proceeding to next record if available...")
                    errord = True
                    continue
                # whew! If we got here, it's all good. Add it to our output list
                debug("         INFO : get_scsi_info_vtl : Successfully built up object for current SCSI record. Object dump follows.")
                myobj.dumpinfo()
                retval.append(myobj)
            except:
                debug("         WARNING : get_scsi_info_vtl : Exception building up SCSI object for current output record.")
                debug("             Ignoring this output record and proceeding to next record if available...")
                errord = True
                continue

        ### End : for ln in [s.strip() for s in res.splitlines()]
        errorstatus = " ERRORS :-( check logs for errors." if errord else "NO errors :-)"
        debug("INFO : get_scsi_info_vtl : Done processing obtained system SCSI information SCSI devtype: '"+vtl_device_type+"' with "+errorstatus)
        return retval

    except:
        debug("ERROR : get_scsi_info_vtl : Exception getting system SCSI info for SCSI device type '"+vtl_device_type+"'. Exception info follows (if available).")
        print_exception()
        return retval


def fibrechannel_scst_setup(robot_obj_list, tapedrive_obj_list):
    '''
    '''
    # TODO:
    return False


def fcoe_scst_setup(robot_obj_list, tapedrive_obj_list):
    '''
    '''
    # TODO:
    return False


def iscsi_scst_setup(robot_obj_list, tapedrive_obj_list):
    '''
    Create a configuration setup to export our VTL devices using SCST's
    iSCSI target driver.

    Parameters:
        robot_obj_list      - A list of AtlScsiDeviceInfo objects for the SCSI
                          Robot/Library/Changer devices active in the system.

        tapedrive_obj_list  - A list of AtlScsiDeviceInfo objects for the SCSI
                           Tape Drive devices active in the system.

    Returns:
        True    - Successfully created an iSCSI SCST config setup.
        False   - Failed creating iSCSI SCST config setup; errors and/or
                  exceptions occurred.
    '''
    if robot_obj_list is None or tapedrive_obj_list is None or not robot_obj_list or not tapedrive_obj_list:
        debug("ERROR : iscsi_scst_setup : One or more required parameters was null or empty!")
        return False
    try:
        maintemplate_str = ""
        target_subsection_str = ""
        changer_handler_subsection_str = ""
        tapedrive_handler_subsection_str = ""
        changer_target_str = ""
        tapedrive_target_str = ""

        # First, get the IP address of this node. We'll need it for the IQN.
        # TODO : Get the USX Node ID and use it in place of IP address.
        # Can accept it from the command line, maybe? And pass it down the call stack?
        ipaddy = get_first_valid_ipaddress()
        if ipaddy is None or not ipaddy:
            debug("WARNING : iscsi_scst_setup : Failed to get IP Address! We use this value in the IQN. Will use a UUID in the IQN in place of this value")
            ipaddy = uuid.uuid4().hex

        debug("INFO : iscsi_scst_setup : Using the following Node ID as part of the iSCSI IQN: "+ipaddy)


        # Handle the robot SCSI object list
        debug("INFO: iscsi_scst_setup : Processing Robot/Changer/Library SCSI Device object list...")
        for robot in robot_obj_list:
            try:
                scsi_id = robot.get_scsi_id()
                pri_nodename = robot.get_primary_nodename().split('/')[-1] # Get the "sch0" part of "/dev/sch0"
                if scsi_id is None or pri_nodename is None or not scsi_id.strip() or not pri_nodename.strip():
                    debug("WARNING : iscsi_scst_setup : Error handling current Robot SCSI object record. Some expected data was null or empty.")
                    debug("     Ignoring this Robot SCSI record. Proceeding to process next Robot SCSI object record, if available.")
                    continue

                changer_handler_subsection_str = changer_handler_subsection_str + iSCSI_SCST_CONF_HANDLER_SUBSECTION_TEMPLATE.substitute(VTLDEV_SCSI_ID=scsi_id)
                changer_handler_subsection_str = changer_handler_subsection_str + "\n"
                changer_target_str = changer_target_str + iSCSI_SCST_CONF_TARGET_SUBSECTION_TEMPLATE.substitute(NODE_ID=ipaddy, VTL_DEV_TYPE_STR=pri_nodename, VTLDEV_SCSI_ID=scsi_id)
                changer_target_str = changer_target_str + "\n"
            except:
                debug("WARNING : iscsi_scst_setup : Exception handling current Robot SCSI object record. Exception info follows(if available).")
                print_exception()
                debug("     Ignoring this Robot SCSI record Proceeding to process next Robot SCSI object record, if available.")
                continue

        # Handle the tape drive SCSI  object list. COPYPASTA!!!
        debug("INFO: iscsi_scst_setup : Processing Tape Drive SCSI Device object list...")
        for tapedrive in tapedrive_obj_list:
            try:
                scsi_id = tapedrive.get_scsi_id()
                pri_nodename = tapedrive.get_primary_nodename().split('/')[-1] # Get the "st0" part of "/dev/st0"
                if scsi_id is None or pri_nodename is None or not scsi_id.strip() or not pri_nodename.strip():
                    debug("WARNING : iscsi_scst_setup : Error handling current Tape Drive SCSI object record. Some expected data was null or empty.")
                    debug("     Ignoring this Tape Drive SCSI record. Proceeding to process next Tape Drive SCSI object record, if available.")
                    continue

                tapedrive_handler_subsection_str = tapedrive_handler_subsection_str + iSCSI_SCST_CONF_HANDLER_SUBSECTION_TEMPLATE.substitute(VTLDEV_SCSI_ID=scsi_id)
                tapedrive_handler_subsection_str = tapedrive_handler_subsection_str + "\n"
                tapedrive_target_str = tapedrive_target_str + iSCSI_SCST_CONF_TARGET_SUBSECTION_TEMPLATE.substitute(NODE_ID=ipaddy, VTL_DEV_TYPE_STR=pri_nodename, VTLDEV_SCSI_ID=scsi_id)
                tapedrive_target_str = tapedrive_target_str + "\n"
            except:
                debug("WARNING : iscsi_scst_setup : Exception handling current Robot SCSI object record. Exception info follows(if available).")
                print_exception()
                debug("     Ignoring this Robot SCSI record Proceeding to process next Robot SCSI object record, if available.")
                continue

        debug("INFO: iscsi_scst_setup : Preparing SCST config file...")
        if changer_handler_subsection_str is None or not changer_handler_subsection_str.strip():
            debug("ERROR : iscsi_scst_setup : Could not build up valid Changer/Robot HANDLER config subsection!")
            return False

        if tapedrive_handler_subsection_str is None or not tapedrive_handler_subsection_str.strip():
            debug("ERROR : iscsi_scst_setup : Could not build up valid Tape Drive HANDLER config subsection!")
            return False

        if changer_target_str is None or not changer_target_str.strip():
            debug("ERROR : iscsi_scst_setup : Could not build up valid Robot/Changer TARGET config subsection!")
            return False

        if tapedrive_target_str is None or not tapedrive_target_str.strip():
            debug("ERROR : iscsi_scst_setup : Could not build up valid Tape Drive TARGET config subsection!")
            return False

        target_subsection_str = changer_target_str + "\n"+tapedrive_target_str
        maintemplate_str = iSCSI_SCST_CONF_TEMPLATE.substitute(CHANGER_HANDLER_SUBSECTION = changer_handler_subsection_str, TAPEDRIVE_HANDLER_SUBSECTION = tapedrive_handler_subsection_str, TARGET_SUBSECTION = target_subsection_str)
        if maintemplate_str is None or not maintemplate_str.strip():
            debug("ERROR : iscsi_scst_setup : Could not build up valid SCSI configuration for given devices!")
            return False

        # If we got this far, we have what looks like a valid SCSY config file.
        # Write eet!
        fname = SCST_CONF_FILENAME
        debug("INFO : iscsi_scst_setup : Writing the following data to SCST config file for iSCSI export: "+fname)
        debug("-------------- BEGIN : DATA BEING WRITTEN TO:  "+fname+" -----------------")
        debug(maintemplate_str)
        debug("-------------- END : DATA BEING WRITTEN TO:  "+fname+" -----------------")
        debug("INFO : iscsi_scst_setup : Writing to SCST config file for iSCSI export: "+fname)
        with open(fname, "w") as f:
            f.write(maintemplate_str)

        # If we got here, we correctly wrote the scsi configuration.
        debug("INFO : iscsi_scst_setup : Successfully created SCST config file for iSCSI export: "+fname)
        return True
    except:
        debug("ERROR : iscsi_scst_setup : Exception creating iSCSI SCST config setup! Exception info follows(if available).")
        print_exception()
        return False


def create_scst_config(scst_setup_type, robot_obj_list, tapedrive_obj_list):
    '''
    Create the SCST config depending upon the given SCST export type.
    Even though we receive a list of robot/Changer objects, we actually
    only support a single Robot/changer for now.

    Parameters:
        scst_setup_type - One of the values in SCST_SETUP_TYPES. String.
        robot_obj_list  - A list of AtlScsiDeviceInfo objects of type 
                          ROBOT_SCSI_DEVTYPE_STRING.
                          we currently only support a single Robot per
                          VTL setup.
        tapedrive_obj_list   - A list of AtlScsiDeviceInfo objects of type
                          TAPEDRIVE_SCSI_DEVTYPE_STRING.

    Returns:
        True    - Successfully created the SCST config for the requested SCST
                  export type.
        False   - Failed to create the SCST config, errors and/or exceptions.
    '''
    if scst_setup_type is None or robot_obj_list is None or tapedrive_obj_list is None or not scst_setup_type or not robot_obj_list or not tapedrive_obj_list:
        debug("ERROR : create_scst_config : One or more required parameters was null or empty.")
        return False
    try:
        # Check scst_setup type
        if scst_setup_type not in SCST_SETUP_TYPES:
            debug("ERROR : create_scst_config : Invalid setup type: "+scst_setup_type+", expected one of "+str(SCST_SETUP_TYPES).strip('[]'))
            return False

        # Stop SCST again, just to be safe, before we write the SCST config
        stop_scst()

        if scst_setup_type == SCST_ISCSI_SETUP_TYPE:
            return iscsi_scst_setup(robot_obj_list, tapedrive_obj_list)
        elif scst_setup_type == SCST_FC_SETUP_TYPE:
            return fibrechannel_scst_setup(robot_obj_list, tapedrive_obj_list)
        elif scst_setup_type == SCST_FCOE_SETUP_TYPE:
            return fcoe_scst_setup(robot_obj_list, tapedrive_obj_list)
        else:
            # Should never get here. This situation is exactly thus:
            # "Oh my god how did this happen i am not good with computer"
            debug("ERROR : create_scst_config : cannot create SCST setup. You reached a code path you should never have reached. Congratulations. You win the internets for today.")
            return False

    except:
        debug("ERROR : create_scst_config : Exception creating SCST config. Exception info follows (if available).")
        print_exception()
        return False


def start_scst(scst_setup_type):
    '''
    Start the SCST service after modprobing the relevant kernel modules as per
    scst_setup_type.

    Parameters:
        scst_setup_type - One of the values in SCST_SETUP_TYPES. String.

    Returns:
        True    - Successfully started the SCST service for the requested SCST
                  export type.
        False   - Failed to start SCST, errors and/or exceptions occurred.
    '''
    if scst_setup_type is None or not scst_setup_type:
        debug("ERROR : start_scst : One or more required parameters was null or empty.")
        return False

    try:
        # Check scst_setup type
        if scst_setup_type not in SCST_SETUP_TYPES:
            debug("ERROR : start_scst : Invalid setup type: "+scst_setup_type+", expected one of "+str(SCST_SETUP_TYPES).strip('[]'))
            return False

        # Modprobe the correct kernmods depending on the SCST export type
        kernmod_list = SCST_SETUP_TYPE_KERNMOD_DICT[scst_setup_type]
        if kernmod_list is None or not kernmod_list:
            # Oh god how did this happen i am not good with computer
            debug("ERROR : start_scst : Unable to get list of kernel modules for setup type: "+scst_setup_type+", this really should not have happened. What did you do??")
            return False
        debug("INFO : start_scst : Loading required kernel modules for SCST export config type: "+ scst_setup_type)
        for kernmod in kernmod_list:
            if not modprobe(kernmod):
                debug("ERROR : start_scst : FAILED Loading required kernel module '"+kernmod+"' for SCST export config type: "+ scst_setup_type)
                return False

        # If we got here, we've modprobed everything we need. Now start the SCST service.
        debug("INFO : start_scst : Starting SCST service for SCST export config type: "+ scst_setup_type)
        ret,res = runcmd("service scst restart")
        # Let's wait just a bit
        time.sleep(3)
        if ret != 0:
            debug("ERROR : start_scst : FAILED starting SCST service for SCST export config type: "+ scst_setup_type+", return code was: "+str(ret)+" and response was: "+res)
            return False

        # TODO : Verify SCSI export of things in scst.conf

        debug("INFO : start_scst : SUCCESSFULLY Started SCST services for SCST export config type: "+ scst_setup_type)
        return True

    except:
        debug("ERROR : create_scst_config : Exception creating SCST config. Exception info follows (if available).")
        print_exception()
        return False


def is_mhvtl_running():
    '''
    Check whether the mhvtl service is properly running. we do this by
    checking whether "vtllibrary" as well as "vtltape" are running (at 
    least one process of each).

    TODO: Enahncement: 
        Check the number of processes against the config and verify.

    Parameters:
        None

    Returns:
        True    - VTL service is running.
        False   - VTL service not running, not running correctly, or error.
    '''
    try:
        librunning = is_program_running("vtllibrary")
        taperunning = is_program_running("vtltape")
        if (librunning == 0 and taperunning == 0):
            debug("INFO : is_mhvtl_running : VTL processes are running.")
            return True

        debug("INFO : is_mhvtl_running : VTL service not  running (or not running correctly).")
        return False

    except:
        debug("ERROR : is_mhvtl_running : Exception getting mhvtl running status. exception info follows (if available).")
        print_exception()
        return False


def get_active_slots():
    '''
    If VTL is running, get the list of active tape slots (no cleaner tapes)
    currently present in the running config. It does this by reading all
    'Slot N:' lines in the active library_contents config file.

    Parameters:
        None

    Returns:
        On success: Dict containing key=Slot# (string) and value=volname where
                    volname is the Volume Label of the tape assigned to that
                    particular slot.
                    E.g. {"1":"ATLVTP0001L5",}
        On Failure/Error/Exception: Returns empty dict. Caller must check.
    '''
    try:
        if not is_mhvtl_running():
            debug("ERROR : get_active_slots : VTL service does not seem to be running. No use getting active slots.")
            return {}

        # Get the list of configured slots from the library_contents file.
        ret,res = runcmd("cat "+MHVTL_CONF_DIR+"/"+MHVTL_LIBRARYCONTENTS_FILENAME+" | grep ^Slot | grep -v CLN | grep -v grep")
        if ret != 0 or not res or not res.strip():
            debug("ERROR : get_active_slots : Failed to get active slots.")
            return {}

        # Now split the output on newlines
        debug("INFO : get_active_slots : Processing slots info read from system...")
        retval = {}
        mylines = res.splitlines()

        # Process each output line and build up the ouput dict object.
        for myline in mylines:
            myline = myline.strip()
            if not myline:
                continue
            linesplt = myline.split(":")
            if not len(linesplt) >= 2:
                continue
            slotstr = linesplt[0].strip()
            slot_volstr = linesplt[1].strip()
            slotstrsplt = slotstr.split()
            if not len(slotstrsplt) >= 2:
                continue
            slot = slotstrsplt[1].strip()
            if not slot:
                continue
            debug("       Adding Key(slotnum)="+slot+", value(tapevol)="+slot_volstr+" to output dict.")
            retval[slot] = slot_volstr

        # Done processing.
        debug("INFO : get_active_slots : Finished processing slot info read from system.")
        return retval

    except:
        debug("ERROR : get_active_slots : Exception getting active slots on system. exception info follows (if available).")
        print_exception()
        return {}


def get_drive_info(robot_dev):
    '''
    Gets the status info for the currently active tape drives on the specified
    robot device currently active in the system.

    It does this by issuing the "mtx -f <robot_dev> status" command and
    extracting the "Data Transfer Element" lines of the output.

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.
    Returns:
        On Success  - Returns a dict of strings containing the info for each
                      tape drive device associated with the given Robot dev.
                      The key of the dict is the drive number (zero-based),
                      and the value is either the word "Empty" or a string
                      of the form "Full (Storage Element X Loaded)" where
                      X is 1,2, etc. This is basically the output of 
                      "mtx -f <robot_dev> | grep -w 'Data Transfer Element'"
                      converted into a dict.
        On Failure  - Returns an empty dict. Caller must check.
    '''
    if robot_dev is None or not robot_dev or not robot_dev.strip():
        debug("ERROR : get_drive_info : Robot device parameter was null or empty.")
        return {}
    try:
        if not is_mhvtl_running():
            debug("ERROR : get_drive_info : VTL service does not seem to be running. Cannot get active drives.")
            return {}

        # Get the list of tape drives and their status string
        ret,res = runcmd("mtx -f "+robot_dev+" status | grep -w 'Data Transfer Element' | grep -v grep")
        if ret != 0 or not res or not res.strip():
            debug("ERROR : get_drive_info : Failed to get active tape drive status info.")
            return {}

        # Now process each line of output. Code shamelessly copypasta'd
        # from get_active_slots above; it's a coincidence that the output
        # to be processed is similar. If I need to do something like this
        # one more time, I swear that I'll convert it into a function, honest!
        debug("INFO : get_drive_info : Processing active tape drive info read from system...")
        retval = {}
        mylines = res.splitlines()

        # Process each output line and build up the ouput dict object.
        for myline in mylines:
            myline = myline.strip()
            if not myline:
                continue
            linesplt = myline.split(":")
            if not len(linesplt) >= 2:
                continue
            drivestr = linesplt[0].strip()
            drive_statstr = linesplt[1].strip()
            drivestrsplt = drivestr.split()
            if not len(drivestrsplt) >= 2:
                continue
            # We want the last element
            drive = drivestrsplt[-1].strip()
            if not drive:
                continue
            debug("       Adding Key(drivenum)="+drive+", value(tapevol)="+drive_statstr+" to output dict.")
            retval[drive] = drive_statstr

        # Done processing.
        debug("INFO : get_drive_info : Finished processing active tape drive info read from system.")
        return retval
    except:
        debug("ERROR : get_drive_info : Exception getting drive info for specified robot device. Exception info follows (if available).")
        print_exception()
        return {}


def get_tape_slot_loaded_in_drive(robot_dev, drive_num_str):
    '''
    Checks whether a given tape drive device is empty. If not, which tape slot
    has been loaded into it. 

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.
        drive_num_str   - The Drive number of the drive in the robot device
                      (starts from 0) in which to load tape from slot. String.

    Returns:
        MTX_STATUS_EMPTY_DESIGNATOR - if the specified tape drive is empty
        Tape slot number (string)   - If the drive contains a tape loaded from a slot
        "" (empty string)           - Errors/Exceptions.
    '''
    if robot_dev is None or drive_num_str is None or not robot_dev or not drive_num_str or not robot_dev.strip() or not drive_num_str.strip():
        debug("ERROR : get_tape_slot_loaded_in_drive : One or more required parameters was null or empty.")
        return ""

    try:
        drivedict = get_drive_info(robot_dev)
        if drivedict is None or not drivedict:
            debug("ERROR : get_tape_slot_loaded_in_drive : Unable to get drive info for robot device "+robot_dev)
            return ""

        # Now check drivedict for the given drive number.
        if not drive_num_str in drivedict:
            debug("ERROR : get_tape_slot_loaded_in_drive : Robot device '"+robot_dev+"' does not contain drive with index number "+drive_num_str)
            return ""
        result = drivedict[drive_num_str]
        if result is None or not result or not result.strip():
            debug("ERROR : get_tape_slot_loaded_in_drive : Failed to get info for Robot device '"+robot_dev+"', tape drive index "+drive_num_str)
            return ""

        result = result.strip()

        # Now check whether the result indicates empty or full.
        # The output of "mtx status" (full output line in drivedirct) for a
        # given drive index looks like this:
        # Empty drive:
        #  Empty
        # Drive is loaded with a tape from a slot:
        #  Full (Storage Element 1 Loaded):VolumeTag = ATLVTP0001L5
        # We parse the result for anything resembling the above.
        if result.startswith(MTX_STATUS_EMPTY_DESIGNATOR):
            debug("INFO : get_tape_slot_loaded_in_drive : Robot '"+robot_dev+"' drive "+drive_num_str+" is empty.")
            return MTX_STATUS_EMPTY_DESIGNATOR

        if result.startswith(MTX_STATUS_FULL_DESIGNATOR):
            debug("INFO : get_tape_slot_loaded_in_drive : Robot '"+robot_dev+"' drive "+drive_num_str+" contains: "+result)
            # Parse result to extract tape slot loaded.
            if "Loaded" not in result:
                debug("ERROR : get_tape_slot_loaded_in_drive : Robot device '"+robot_dev+"', tape drive index "+drive_num_str+" seems to have data but no loaded slot. This is strange, to say the least.")
                return ""

            retval = result.split(":")[0].split(' ')[-2]
            debug("INFO : get_tape_slot_loaded_in_drive : Robot '"+robot_dev+"' drive "+drive_num_str+" contains tape slot: "+retval)
            return retval

        # if we got here, the result contains something we weren't expecting.
        debug("ERROR : get_tape_slot_loaded_in_drive : Robot device '"+robot_dev+"', tape drive index "+drive_num_str+" contains unexpected data! Cannot determine if a slot is in use or not.")
        return ""

    except:
        debug("ERROR : get_tape_slot_loaded_in_drive : Exception getting drive info for specified robot device/drive combo. Exception info follows (if available).")
        print_exception()
        return ""


def unload_tape_slot_from_drive(robot_dev, drive_num_str="", slot_num_str="", force_unload=False):
    '''
    Unloads from Tape Drive# <drive_num> of Tape Robot/changer/library device
    <robot_dev> the tape currently loaded, using the "mtx" program.

    The mtx command looks something like this:
      mtx -f /dev/sg3 unload 1 0

    With this function, you can force a loaded tape to be unloaded into the
    specified slot; you just pass in the slot you want the currently loaded
    tape unloaded to, via the slot_num_str parameter, and set force_unload
    to True. Note that you'll ALSO have to explicitly specify the drive:
        unload_tape_from_slot("/dev/sg3", "0", "666", True)

    It's a bit complicated, so see the unload section of the mtx man page
    for more details.

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.

        drive_num_str   - The Drive number of the drive in the robot device
                      (starts from 0) in which to unload tape slot. String.
                          Defaults to "".

        slot_num_str    - The tape Slot number into which to stick the unloaded
                          tape (starts from 1). String. Defaults to "".

        force_unload    - Force a loaded tape to be unloaded even if it is NOT
                          actually from the requested slot as passed in via the
                          slot_num_str parameter. Also needs the drive_num_str
                          parameter to be passed in, if set to True.
                          *** WARNING ***
                          force_unload=True could potentially have dangerous
                          consequences and/or repercussions. You have been
                          warned!

    Returns:
        True    - Succeeded in unloading tape as specified, or  drive was
                  already empty.
        False   - Could not unload specified slot from drive on robot.
    '''
    if robot_dev is None or not robot_dev.strip():
        debug("ERROR : unload_tape_slot_from_drive : One or more required parameters was null or empty.")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : unload_tape_slot_from_drive : VTL is not running; no point trying to unload anything.")
            return False


        # Now see if the given drive has anything loaded.
        statdr = "0"
        if drive_num_str is None or not drive_num_str or not drive_num_str.strip():
            statdr = "0"
        else:
            statdr = drive_num_str
        drivestat = get_tape_slot_loaded_in_drive(robot_dev, statdr)
        if drivestat is None or not drivestat.strip():
            debug("WARNING : unload_tape_slot_from_drive : Could not get status for drive number "+statdr+" on robot device "+robot_dev+". This is unusual. Will proceed trying to unload, but this may fail.")

        # Now check what drivestat contains.
        drivestat = drivestat.strip()
        if drivestat == MTX_STATUS_EMPTY_DESIGNATOR:
            debug("INFO : unload_tape_slot_from_drive : Nothing loaded in drive number "+statdr+" of robot device "+robot_dev+", nothing to unload :-)")
            return True

        slot_to_unload = ''
        drive_to_unload = ''

        # If we have a force unload, get the relevant params.
        if force_unload:
            slot_num_str = slot_num_str.strip()
            if not slot_to_unload:
                debug("WARNING : unload_tape_slot_from_drive : Force Unload specified but no explicit slot given, force_unload will not have any effect.")
            else:
                if slot_num_str != drivestat:
                    debug("WARNING : unload_tape_slot_from_drive : Force Unload specified. Requested slot "+slot_num_str+" differs from loaded slot "+drivestat+", proceeding to unload loaded tape into requested slot "+drivestat)
                    slot_to_unload = drivestat
                    if statdr != "0":
                        drive_to_unload = statdr

        else:
            if slot_num_str and (slot_num_str != drivestat):
                # We were specified slot_num_str (so it's not empty), and it
                # doesn't match the slot of the currently loaded tape.
                debug("ERROR : unload_tape_slot_from_drive : Tape loaded in drive "+drive_num_str+" is originally loaded from slot "+drivestat+", but we were requested to unload it to slot "+slot_num_str+" and force_unload NOT specified. Cannot unload. NOT unloading tape drive "+drive_num_str)
                return False

        # If we got here, we have SOMETHING to unload. Let's do eet!
        debug("INFO : unload_tape_slot_from_drive : Unloading loaded tape to slot '"+slot_to_unload+"' (blank slot means unload to original slot) (original slot was: "+drivestat+", force_unload="+str(force_unload)+") from tape drive '"+drive_to_unload+"' (blank drive means drive 0) on robot device "+robot_dev)
        ret,res = runcmd("mtx -f "+robot_dev+" unload "+slot_to_unload+" "+drive_to_unload)
        if ret != 0:
            debug("ERROR : unload_tape_slot_from_drive : FAILED Unloading loaded tape to slot '"+slot_to_unload+"' (blank slot means unload to original slot) (original slot was: "+drivestat+", force_unload="+str(force_unload)+") from tape drive '"+drive_to_unload+"' (blank drive means drive 0) on robot device "+robot_dev)
            return False

        # If we got here, we successfully unloaded everything
        debug("INFO : unload_tape_slot_from_drive : SUCCEEDED Unloading loaded tape to slot '"+slot_to_unload+"' (blank slot means unload to original slot) (original slot was: "+drivestat+", force_unload="+str(force_unload)+") from tape drive '"+drive_to_unload+"' (blank drive means drive 0) on robot device "+robot_dev)
        return True

    except:
        debug("ERROR : unload_tape_slot_from_drive : Exception unload tape slot. Exception info follows (if available).")
        print_exception()
        return False


def load_tape_slot_into_drive(robot_dev, drive_num_str, slot_num_str, force_load=False):
    '''
    Loads Tape Drive# <drive_num> of Tape Robot/changer/library device
    <robot_dev> with tape in Tape Slot# <slot_num> using the
    "mtx" program.

    The mtx command looks something like this:
      mtx -f /dev/sg3 load 1 0

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.

        drive_num_str   - The Drive number of the drive in the robot device
                          (starts from 0) in which to load tape from slot.
                          String.

        slot_num_str    - The tape Slot number to load (starts from 1). String.

        force_load      - Boolean. If the given tape drive is already loaded
                          with a slot, unload that slot and load the requested
                          one.
                          *** WARNING ***
                          force_load=True could potentially have dangerous
                          consequences and/or repercussions. You have been
                          warned!

    Returns:
        True    - Succeeded in loading specified tape slot into specified
                  drive on specified Robot device.
        False   - Could not load specified slot into drive on robot.
    '''
    if robot_dev is None or drive_num_str is None or slot_num_str is None or not robot_dev.strip() or not drive_num_str.strip() or not slot_num_str.strip():
        debug("ERROR : load_tape_slot_into_drive : One or more required parameters was null or empty.")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : load_tape_slot_into_drive : VTL is not running; no point trying to load anything.")
            return False

        slot_num_str = slot_num_str.strip()
        # Check whether given slot is actually present in active config.
        activeslots_dict = get_active_slots()
        if slot_num_str not in activeslots_dict:
            debug("ERROR : load_tape_slot_into_drive : Requested slot "+slot_num_str+" is not in list of active slots. Cannot load it into tape drive.")
            return False

        # Now see if the given drive has anything loaded.
        drivestat = get_tape_slot_loaded_in_drive(robot_dev, drive_num_str)
        if drivestat is None or not drivestat.strip():
            debug("WARNING : load_tape_slot_into_drive : Could not get status for drive number "+drive_num_str+" on robot device "+robot_dev+". This is unusual. Will proceed trying to load, but this may fail.")

        # Now check what drivestat contains.
        drivestat = drivestat.strip()
        was_full = False
        if drivestat != MTX_STATUS_EMPTY_DESIGNATOR:
            was_full = True
            # check if slot loaded is same as requested slot. If so, nothing 
            # to load.
            if drivestat == slot_num_str:
                debug("INFO : load_tape_slot_into_drive : Drive "+drive_num_str+" on robot device "+robot_dev+" already has slot "+drivestat+" loaded. No need to load anything :-)")
                return True

            # If we got here, we have a slot loaded which is NOT the requested 
            # slot. We now need to check whether we need to force unload.
            # If slot loaded and force_load=False, return with error.
            if force_load == False:
                debug("ERROR : load_tape_slot_into_drive : Drive "+drive_num_str+" on robot device "+robot_dev+" is NOT empty, and force_load=False. Aborting load of slot "+slot_num_str+" into drive!")
                return False
            else:
                # If slot loaded and force_load=True, unload currently loaded slot.
                debug("WARNING : load_tape_slot_into_drive : Drive "+drive_num_str+" on robot device "+robot_dev+" is NOT empty, and force_load=True. Will attempt to unload loaded slot "+drivestat+" from drive before proceeding...")
                if not unload_tape_slot_from_drive(robot_dev, drive_num_str, drivestat):
                    debug("ERROR : load_tape_slot_into_drive : Drive "+drive_num_str+" on robot device "+robot_dev+" was NOT empty, force_load=True, and FAILED to unload loaded slot "+drivestat+". Aborting load of slot "+slot_num_str+" into drive!")
                    debug("    DIRE WARNING : YOUR SYSTEM MAY NOW BE IN AN INCONSISTENT STATE! Recommend reboot and performing setup of VTL infrastructure from scratch.")
                    return False
                else:
                    debug("INFO : load_tape_slot_into_drive : Drive "+drive_num_str+" on robot device "+robot_dev+" was NOT empty, force_load=True, and SUCCESSFULLY unloaded loaded slot "+drivestat)

        # If drive was originally full, verify drive empty.
        if was_full:
            drivestat = get_tape_slot_loaded_in_drive(robot_dev, drive_num_str)
            if drivestat is None or not drivestat.strip():
                debug("WARNING : load_tape_slot_into_drive : verify empty : Could not get status for drive number "+drive_num_str+" on robot device "+robot_dev+". This is unusual. Will proceed trying to load, but this may fail.")
            drivestat = drivestat.strip()
            if drivestat != MTX_STATUS_EMPTY_DESIGNATOR:
                debug("ERROR : load_tape_slot_into_drive : FAILED Verifying previously loaded tape drive "+drive_num_str+" on robot device "+robot_dev+" is now empty. It may still contain a loaded slot. Cannot load requested slot.")
                return False
            else:
                debug("INFO : load_tape_slot_into_drive : Verified that previously loaded tape drive "+drive_num_str+" on robot device "+robot_dev+" is now empty.")


        # If we still got here, chances are we can load requested slot.
        # Load specified slot into drive.
        ret,res = runcmd("mtx -f "+robot_dev+" load "+slot_num_str+" "+drive_num_str)
        if ret != 0:
            debug("ERROR : load_tape_slot_into_drive : FAILED loading slot "+slot_num_str+" into tape drive "+drive_num_str+" on robot device "+robot_dev+". Return code was: "+str(ret)+", and output of load command was: "+res)
            return False

        # FIXME : Verify loaded slot, maybe?
        # If we got here, the mtx load returned 0.
        debug("INFO : load_tape_slot_into_drive : SUCCESSFULLY loading slot "+slot_num_str+" into tape drive "+drive_num_str+" on robot device "+robot_dev+". Return code was: "+str(ret)+", and output of load command was: "+res)
        return True

    except:
        debug("ERROR : load_tape_slot_into_drive : Exception loading specified tape slot into drive on device. Exception info follows (if available).")
        print_exception()
        return False


def load_first_tape_slot_into_drive(robot_dev, drive_num_str="0"):
    '''
    Load the first available tape slot into the Tape drive on the specified
    Robot/Changer/Library device.

    This function is essentially the following mtx command:
      mtx -f <robot_dev> first [<drivenum>]

    According to the mtx man page:
        first [<drivenum>]
           Loads drive <drivenum> from the first slot in the media changer.
           Unloads the drive if there is already media in it (note: you may
           need to eject the tape using your OS's tape control commands
           first). Note that this command may not be what you want on large
           tape libraries -- e.g. on Exabyte 220, the first slot is usually
           a cleaning tape. If <drivenum> is omitted, defaults to first drive.

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.

        drive_num_str   - The Drive number of the drive in the robot device
                          (starts from 0) in which to load tape from slot.
                          String. This is optional. If not specified, it
                          defaults to 0, and is akin to issuing the command
                          to load the first tape slot into the first drive
                          on the specified robot.

    Returns:
        True    - Succeeded in loading first tape slot into specified
                  drive on specified Robot device.
        False   - Could not load first slot into drive on robot.
    '''
    if robot_dev is None or drive_num_str is None or not robot_dev.strip() or not drive_num_str.strip():
        debug("ERROR : load_first_tape_slot_into_drive : One or more required parameters was null or empty.")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : load_first_tape_slot_into_drive : VTL is not running; no point trying to load anything.")
            return False

        # Load it using the mtx command. This also takes care of unload if
        # needed, so we don't need to do much of anything except run mtx.
        ret,res = runcmd("mtx -f "+robot_dev+" first "+drive_num_str)
        if ret != 0:
            debug("ERROR : load_first_tape_slot_into_drive : Failed to load first tape slot into drive "+drive_num_str+" on robot device "+robot_dev+". Return code was "+str(ret)+" and output was: "+res)
            return False
        debug("INFO : load_first_tape_slot_into_drive : SUCCEEDED loading first tape slot into drive "+drive_num_str+" on robot device "+robot_dev)
        return True
    except:
        debug("ERROR : load_first_tape_slot_into_drive : Exception loading first tape slot into drive on device. Exception info follows (if available).")
        print_exception()
        return False


def load_next_tape_slot_into_drive(robot_dev, drive_num_str="0"):
    '''
    Load the next available tape slot into the Tape drive on the specified
    Robot/Changer/Library device.

    This function is essentially the following mtx command:
      mtx -f <robot_dev> next [<drivenum>]

    According to the mtx man page:
        next [<drivenum>]
            Unloads the drive and loads the next tape in sequence. If the
            drive was empty, loads the first tape into the drive.

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.

        drive_num_str   - The Drive number of the drive in the robot device
                          (starts from 0) in which to load tape from slot.
                          String. This is optional. If not specified, it
                          defaults to 0, and is akin to issuing the command
                          to load the next tape slot into the first drive
                          on the specified robot.

    Returns:
        True    - Succeeded in loading next tape slot into specified
                  drive on specified Robot device.
        False   - Could not load next slot into drive on robot, or no more
                  media slots left.
    '''
    if robot_dev is None or drive_num_str is None or not robot_dev.strip() or not drive_num_str.strip():
        debug("ERROR : load_next_tape_slot_into_drive : One or more required parameters was null or empty.")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : load_next_tape_slot_into_drive : VTL is not running; no point trying to load anything.")
            return False

        # Load it using the mtx command. This also takes care of unload if
        # needed, so we don't need to do much of anything except run mtx.
        ret,res = runcmd("mtx -f "+robot_dev+" next "+drive_num_str)
        if ret != 0:
            debug("ERROR : load_next_tape_slot_into_drive : Failed to load next tape slot into drive "+drive_num_str+" on robot device "+robot_dev+". Return code was "+str(ret)+" and output was: "+res)
            return False
        debug("INFO : load_next_tape_slot_into_drive : SUCCEEDED loading next tape slot into drive "+drive_num_str+" on robot device "+robot_dev)
        return True
    except:
        debug("ERROR : load_next_tape_slot_into_drive : Exception loading next tape slot into drive on device. Exception info follows (if available).")
        print_exception()
        return False



def load_last_tape_slot_into_drive(robot_dev, drive_num_str="0"):
    '''
    Load the last available tape slot into the Tape drive on the specified
    Robot/Changer/Library device.

    Note that in this script/library, the way the VTL setup is done means
    that the last slot is a cleaner tape. So using this function along with
    the rest of the functions in this library to set up the VTL means that
    you will effectively be loading the cleaner tape when you call this
    function. Of course, if this script's functions subsequently change so
    that the last slot configured is no longer a cleaning tape, this entire
    paragraph does not apply.

    This function is essentially the following mtx command:
      mtx -f <robot_dev> last [<drivenum>]

    According to the mtx man page:
        last [<drivenum>]
            Loads drive <drivenum> from the last slot in the media changer.
            Unloads the drive if there is already a tape in it. (Note: you
            may need to eject the tape using your OS's tape control commands
            first).

    Parameters:
        robot_dev       - The SCSI Generic Device (sg) nodename of the Robot.
                          It is a string of the form "/dev/<SOMETHING>" where
                          SOMETHING is a string such as "sg0", "sg1" etc.

        drive_num_str   - The Drive number of the drive in the robot device
                          (starts from 0) in which to load tape from slot.
                          String. This is optional. If not specified, it
                          defaults to 0, and is akin to issuing the command
                          to load the last tape slot into the first drive
                          on the specified robot.

    Returns:
        True    - Succeeded in loading last tape slot into specified
                  drive on specified Robot device.
        False   - Could not load next slot into drive on robot.
    '''
    if robot_dev is None or drive_num_str is None or not robot_dev.strip() or not drive_num_str.strip():
        debug("ERROR : load_last_tape_slot_into_drive : One or more required parameters was null or empty.")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : load_last_tape_slot_into_drive : VTL is not running; no point trying to load anything.")
            return False

        # Load it using the mtx command. This also takes care of unload if
        # needed, so we don't need to do much of anything except run mtx.
        ret,res = runcmd("mtx -f "+robot_dev+" last "+drive_num_str)
        if ret != 0:
            debug("ERROR : load_last_tape_slot_into_drive : Failed to load last tape slot into drive "+drive_num_str+" on robot device "+robot_dev+". Return code was "+str(ret)+" and output was: "+res)
            return False
        debug("INFO : load_last_tape_slot_into_drive : SUCCEEDED loading last tape slot into drive "+drive_num_str+" on robot device "+robot_dev)
        return True
    except:
        debug("ERROR : load_last_tape_slot_into_drive : Exception loading last tape slot into drive on device. Exception info follows (if available).")
        print_exception()
        return False


def erase_tape_in_drive(tpdrv):
    '''
    Erases the tape in the tape drive having the specified SCSI Primary 
    nodename (/dev/st0, /dev/st1 etc). Note that for this function to work,
    the tape drive must already have a tape loaded from a slot.

    This function uses the "mt" command to erase the tape. The command looks
    like this:
        mt -f /dev/st0 erase

    Parameters:
        tpdrv   - The SCSI Primary Nodename of the tapedrive device on which
                  to erase the loaded tape.

    Returns:
        True    - Erased tape loaded in specified tape drive
        False   - Failed to erase tape in specified drive; error or exception.
    '''
    if tpdrv is None or not tpdrv:
        debug("ERROR : erase_tape_in_drive : Tape drive primary nodename param is null or empty!")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : erase_tape_in_drive : VTL is not running; no point trying to erase anything.")
            return False

        ret,res = runcmd("mt -f "+tpdrv+" erase")
        if ret == 0:
            debug("INFO : erase_tape_in_drive : Successfully erased tape in drive "+tpdrv)
            return True

        debug("WARNING : erase_tape_in_drive : Failed to erase tape in drive "+tpdrv+", return code was "+int(ret)+" and output was: "+res)
        return False

    except:
        debug("ERROR : erase_tape_in_drive : Exception erasing tape in drive. Exception info follows (if available).")
        print_exception()
        return False


def erase_all_slots(robot_obj, tpdrv_obj):
    '''
    Erases tapes in all active slots in the active MHVTL config.

    Parameters:
        robot_obj   - The AtlScsiDeviceInfo object for a Robot device. This
                      is usually the first robot device in the system.
        tpdrv_obj   - The AtlScsiDeviceInfo object for a Tape Drive device.
                      This is usually the first tape drive in the system.

    Returns:
        True    - Succeeded in erasing tapes in all active slots.
        False   - Failed to erase tapes in all active slots; error or
                  exception occurred.
    '''
    if robot_obj is None or not robot_obj or tpdrv_obj is None or not tpdrv_obj:
        debug("ERROR : erase_all_slots : One or more required parameters was null or empty!")
        return False
    try:
        # First check if VTL is running. No point doing this if VTL is not running.
        if not is_mhvtl_running():
            debug("WARNING : erase_all_slots : VTL is not running; no point trying to erase anything.")
            return False

        robot_sg_dev = robot_obj.get_generic_nodename()
        if robot_sg_dev is None or not robot_sg_dev:
            debug("ERROR : erase_all_slots : Failed to get SCSI Generic device nodename for Robot device!")
            return False

        tape_scsi_dev = tpdrv_obj.get_primary_nodename()
        if tape_scsi_dev is None or not tape_scsi_dev:
            debug("ERROR : erase_all_slots : Failed to get SCSI primary nodename for Tape Drive device!")
            return False

        slotsd = get_active_slots()
        if slotsd is None or not slotsd:
            debug("WARNING : erase_all_slots : No active slots found. Since VTL is running, this may be an error.")
            return False

        errord = False
        debug("INFO : erase_all_slots : Processing active slots...")
        for slot in slotsd:
            debug("    Processing slot "+slot+", containing tape "+slotsd[slot])
            # Load slot
            if not load_tape_slot_into_drive(robot_sg_dev, "0", slot):
                debug("        Processing slot "+slot+", containing tape "+slotsd[slot]+", Failed to load slot. Moving to next slot.")
                errord = True
                continue

            # Erase tape
            if not erase_tape_in_drive(tape_scsi_dev):
                debug("        Processing slot "+slot+", containing tape "+slotsd[slot]+", Failed to erase tape. Unloading and Moving to next slot.")
                errord = True

            # If unloading failed, it's a problem. we could force, but that 
            # may leave the system in an inconsistent state.
            if not unload_tape_slot_from_drive(robot_sg_dev, "0", slot):
                debug("        Processing slot "+slot+", containing tape "+slotsd[slot]+", Failed to unload tape. Cannot continue.")
                errord = True
                break

        if errord:
            debug("WARNING : erase_all_slots : Failed to erase one or more slots!")
        else:
            debug("INFO : erase_all_slots : Successfully erased active slots.")

        return not errord

    except:
        debug("ERROR : erase_all_slots : Exception erasing tapes in all active slots. Exception info follows (if available).")
        print_exception()
        return False


def abend(message = "Aborting due to errors!"):
    '''
    Abort running this script after displaying the given message.
    The script will exit with return status code 1.

    Parameters:
        message - String indicating message to display. Optional.

    Returns:
        Nothing. It exits the program with status code 3.
    '''
    debug(message)
    debug("================= ABORT : Setup VTL : "+MYNAME+" : ABNORMAL TERMINATION =================")
    sys.exit(3)


############################### MAIN #################################
if __name__ == "__main__":
    try:

        # TODO : Command lines and parsing
        # TODO : Handle multiple tape drives. We DO NOT handle multiple changers for now.
        # TODO : Handle SCST export types for Fibrechannel and FCoE, once we have the hardware
        scst_setup_type = SCST_ISCSI_SETUP_TYPE
        drive_type_list = ["ULT3580-TD5"]
        robot = "L700"

        set_log_file(LOGFILE)
        ### Set Signal Handlers
        debug(" ")
        debug("================== Setup VTL : START : "+MYNAME+" : Starting run =======================")
        debug("INFO : Main : setting signal handlers...")
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)

        debug("INFO : Version Check : Performing VM version check...")
        if not check_vm_release_version():
            debug("ERROR : Version Check : Failed release version check!")
            debug("======== ABORT : Setup VTL : VM Version Check FAILED =========")
            sys.exit(2)
        else:
            debug("INFO : Version Check : PASSED release version check.")

        # Check whether mhvtl is installed correctly
        if not check_mhvtl_installed(scst_setup_type):
            abend("ERROR : Main : failed to verify correct installation of dependencies. Aborting run!")

        # Check if a dedupFS is mounted
        ddir = get_dedup_dir()
        if ddir is None or not ddir.strip():
            abend("ERROR : Main : DedupFS does not seem to be mounted!")

        ddir = ddir.strip()
        debug("INFO : Main : Dedup mountpoint found at: "+ddir)

        # Get the dedupFS size
        ddp_size_kb_str = get_dedup_available_size_in_kbytes(ddir)
        if ddp_size_kb_str is None or not ddp_size_kb_str.strip():
            abend("ERROR : Main : Could not get Dedup mountpoint available size!")
        ddp_size_kb_str = ddp_size_kb_str.strip()
        debug("INFO : Main : Dedup mountpoint available size (kB) is: "+ddp_size_kb_str)

        ## Compute how many slots are needed for the given Dedupfs size
        #slots = slots_required(ddp_size_kb_str, drive_type)
        #if slots <= 0:
        #    abend("ERROR : Main : Failed to calculate valid number of slots required. Expected positive value but got: "+str(slots))
        #debug("INFO : Main : Slots required for Tape type '"+drive_type+"' for Dedup with available size of "+ddp_size_kb_str+" kB is: "+str(slots))

        # Shut down any running mhvtl service/processes, and stop SCST
        debug("INFO : Main : Stopping any running VTL services...")
        if not stop_mhvtl():
            debug("ERROR : Main : Failed to stop VTL services. If we continue with the VTL setup process, we risk the system getting into an inconsistent state, from which the only way to recover is a reboot.")
            abend("ERROR : Main : Aborting due to failure to stop VTL services.")
        debug("INFO : Main : Stopping any running SCST services...")
        if not stop_scst():
            debug("ERROR : Main : Failed to stop SCST services. If we continue with the VTL setup process, we risk the system getting into an inconsistent state, from which the only way to recover is a reboot.")
            abend("ERROR : Main : Aborting due to failure to stop SCST services.")

        # Set up the MHVTL config according to tape drive model and slots required
        if not create_mhvtl_config(robot, drive_type_list, ddir, ddp_size_kb_str):
            abend("ERROR : Main : Failed to create VTL config!")

        # Start the mhvtl service
        debug("INFO : Main : Starting VTL services...")
        if not start_mhvtl():
            abend("ERROR : Main : Failed to start mhvtl service.")

        # Get the SCSI IDs of the MHVTL devices ('lsscsi -g' is your friend)
        robot_objs = get_scsi_info_vtl(ROBOT_SCSI_DEVTYPE_STRING)
        tapedrive_objs = get_scsi_info_vtl(TAPEDRIVE_SCSI_DEVTYPE_STRING)

        if robot_objs is None or not robot_objs or tapedrive_objs is None or not tapedrive_objs:
            abend("ERROR : Main : Failed to build up Robot object list and/or Tapedrive object list.")

        # Before we start, erase the tapes in all the slots. This may help
        # with mitigating the Symantec Backupexec issue where it moves the
        # tapes to "Retired Media" because it can't recognize the tape; the
        # only way to get SBE to use the tapes is to erase them in the SBE
        # GUI, so we do an erase here in the hopes that it will help.
        erase_all_slots(robot_objs[0], tapedrive_objs[0])

        # Using the "mtx" program, load the first tape slot into the first
        # drive of the first robot device
        first_robot_obj = robot_objs[0]
        if first_robot_obj is None or not first_robot_obj:
            abend("ERROR : Main : Failed to get first robot device object from list.")

        first_robot_sg_dev = first_robot_obj.get_generic_nodename()
        if first_robot_sg_dev is None or not first_robot_sg_dev or not first_robot_sg_dev.strip():
            abend("ERROR : Main : Failed to get SCSI Generic Nodename ('sg' device name) for first robot device object in list.")

        if not load_first_tape_slot_into_drive(first_robot_sg_dev):
            abend("ERROR : Main : Failed to load first tape slot into first drive on Robot sg device node "+first_robot_sg_dev)

        # Erase the tape in the drive. We just do this for safety.
        erase_tape_in_drive(tapedrive_objs[0].get_primary_nodename())

        # Create an SCST config to export out the Library Changer device as
        # well as the Tape Drive device, depending on the SCST export type
        # (iSCSI, FC, FCoE) requested.
        debug("INFO : Main : Creating SCST config for SCST export type: "+scst_setup_type)
        if not create_scst_config(scst_setup_type, robot_objs, tapedrive_objs):
            abend("ERROR : Main : Failed to create SCST Config for SCST export type: "+scst_setup_type)


# TODO : Will also provide a command line option to send them out as SCST QLogic FC devices.

        # (Re)start SCST
        debug("INFO : Main : Starting SCST...")
        if not start_scst(scst_setup_type):
            abend("ERROR : Main : Failed to start SCST services for SCST export type: "+scst_setup_type)

        debug("================== Setup VTL : END : "+MYNAME+" : Successfully finished run =======================")
    except:
        debug("ERROR : Main : Exception running code in Main. Exception info follows (if available).")
        print_exception()
        abend("EXCEPTION IN MAIN!")

############ END MAIN ############
##################### END FILE #######################
