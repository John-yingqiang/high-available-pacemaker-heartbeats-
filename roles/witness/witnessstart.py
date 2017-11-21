#!/usr/bin/python
import json
import os, sys
from pprint import pprint
import logging
import time
import re
import base64

sys.path.insert(0, "/opt/milio/")
from libs.atlas.cmd import runcmd

sys.path.insert(0, "/opt/milio/libs/atlas/")
from log import *
LOG_FILENAME = '/var/log/usx-witnessstart.log'
set_log_file(LOG_FILENAME)

def ibdserver_start():
    ret = 0
    if os.system('/bin/ibdserver >> %s 2>&1' % (LOG_FILENAME)) != 0:
        debug('ERROR : Failed to start ibdserver')
        ret = 103

    # If we got here, ibd-server is supposed to have started up. Check whether it's really running.
    sleepsecs=3
    debug('Waiting %s seconds for ibdserver to fully start up...' % str(sleepsecs))
    time.sleep(sleepsecs)
    debug('Checking if ibdserver is running...')
    if os.system('ps aux | grep ibdserver | grep -v grep') != 0:
        debug('ERROR : Device ibdserver does NOT seem to be running! Exiting!')
        ret = 104

    return ret
#
# Helpful message for the clueless
#
def usage():
    debug("Usage:" + sys.argv[0] + " config|start")
    debug("      config - start ibdserver")
    debug(" ")
    debug("      start - start ibdserver")

"""
Main
"""
debug("==== BEGIN WITNESS NODE CONFIG/START ====  ")
NEEDS_CONFIG=False
cmd_options = {
	"config",
	"start",
}

if len(sys.argv) < 2:
    usage()
    debug("ERROR: Incorrect number of arguments. Need at least one argument which is either 'config' or 'start'")
    exit(1)

cmd_type = sys.argv[1]

if cmd_type is None or not cmd_type:
    usage()
    debug("ERROR: Incorrect argument - %s. Argument has to be either 'config' or 'start'" % cmd_type)
    exit(1)

if cmd_type in cmd_options:
    if cmd_type.lower().strip().startswith('config'):
        NEEDS_CONFIG = True
        debug("Script has been called in 'config' mode, so assuming that we need to do first-time configuration!")
    else:
        debug("Script has been called in 'start' mode, so assuming that first-time configuration is already done!")
else:
    usage()
    debug("ERROR: Incorrect argument '%s'. Argument has to be either 'config' or 'start'" % cmd_type)
    exit(1)

# Open the Atlas JSON file and read the data into JSON object
try:
    myfile = open('/etc/ilio/atlas.json', 'r')
    data1 = myfile.read()
    debug('data1 is: %s' % data1)
except:
    debug('ERROR : Failed opening JSON file to read config data, cannot continue!')
    sys.exit(2)

if data1 is None or not data1:
    debug( 'ERROR : No data available in Atlas json file, exiting')
    sys.exit(91)

device_dict = json.loads(data1)
if not device_dict:
    debug( 'ERROR : No JSON data read from Atlas json file, exiting')
    sys.exit(92)

pprint(device_dict)
myfile.close

# Get the ILIO Role
try:
    ilio_role = device_dict["roles"][0]
except KeyError:
    debug( 'ERROR : ===NO ROLE FOUND=== : KeyError : roles not present in JSON')
    ilio_role = None

if not ilio_role:
    debug( 'ERROR : ILIO Role does not seem to be defined in Atlas JSON, exiting')
    sys.exit(93)

debug('ILIO ROLE = %s' % ilio_role)

# If this ILIO's role is not an Aggregator node, exit with error
if not ilio_role.lower() == 'usx_witness_vm':
    debug('ERROR : ILIO Role defined in Atlas JSON does not seem to be witness, exiting!')
    sys.exit(94)

if NEEDS_CONFIG:
    rc = ibdserver_start()
    if rc != 0:
        debug("ERROR: Failed to start ibdserver %s" % rc)
        sys.exit(rc)
else:
    rc = ibdserver_start()
    if rc != 0:
        debug("ERROR: Failed to start ibdserver %s" % rc)
        sys.exit(rc)

debug('Verified ibdserver is running.')
debug('Witness node was successfully set up')
debug("==== END WITNESS NODE CONFIG/START : Successful! ==== ")
