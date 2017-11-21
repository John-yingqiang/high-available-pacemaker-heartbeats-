#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os, signal
import ConfigParser
#import logging
import argparse
import json
import traceback
import time
import base64
import errno
import math
#import psutil
import stat
import fnmatch
import sys
from time import sleep
sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from log import *

ZRAM_MAX_DEVS = 32
ATLAS_CFG = "/etc/ilio/atlas.json"

'''
def _debug(*args):
    logging.debug("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))
'''

def usage():
    print('Usage : python %s -s <new_size>' % str(sys.argv[0]))
    print('        -Change the mem_limit size for zram')
    print('Usage : python  %s -c' % str(sys.argv[0]))
    print('        -Check the change range value for the mem_limit')

def find_zram_dev():
    for i in range(ZRAM_MAX_DEVS):
        zram_dev = "zram" + str(i)
        zram_disksize_file = "/sys/block/" + zram_dev + "/disksize"
        f = open(zram_disksize_file, 'r')
        zram_size = int(f.read())
        if zram_size > 0:
            return zram_dev
    debug("ERROR: no zram device.")
    return None

def simple_mem_check():
    f = open(ATLAS_CFG, 'r')
    config = json.load(f)
    f.close
    #SIMPLE_MEMORY check
    if 'SIMPLE_MEMORY' not in \
        config['volumeresources'][0]['raidplans'][0]['volumetype']:
        debug("INFO: Not a Simple Memory Volume, nothing to update")
        return 1
    return 0

def get_current_value(config):
    volume_size =  config['volumeresources'][0]['raidplans'][0]['volumesize']
    for raidbrick in config['volumeresources'][0]['raidplans'][0]['raidbricks']:
        if raidbrick['raidbricktype'].upper() == 'MEMORY':
            mem_limit_size = raidbrick['raidbricksize']
    return volume_size,mem_limit_size

def check_input_value(config, new_size):
    (current_volume_size, current_mem_limit_size) = get_current_value(config)
    if int(new_size) < int(current_mem_limit_size):
        debug('ERROR: Input value is less than mem_limit value %d, exit' % current_mem_limit_size)
        sys.exit(1)
    # hot added memory
    added_memory = memory_hot_add()
    new_added_memory = current_mem_limit_size + added_memory
    if new_added_memory == current_mem_limit_size:
        debug('ERROR: Added memory is 0, exit')
        sys.exit(1)
    # Get the small value from user input and hot added memory, then chage it.
    new_size = min(int(new_size), int(new_added_memory))
    if new_size > current_volume_size:
        new_size = current_volume_size
        debug('WARNING: The max mem_limit should be the volume size: %d' % new_size)
    debug('Set the mem_limit from %d to %d' %(current_mem_limit_size, new_size))
    return new_size

def change_zram_raidbricksize(config, newsize):
    plandetail = json.loads(config['volumeresources'][0]['raidplans'][0]['plandetail'])
    volume_size =  config['volumeresources'][0]['raidplans'][0]['volumesize']
    # We changed the JSON according to the memory type, just error out if it isn't there.
    j = 0
    for subplan in plandetail['subplans']:
        for raidbrick in subplan['raidbricks']:
            for subdevice in raidbrick['subdevices']:
                if subdevice[u'raidbricktype'].upper() == 'MEMORY':
                    subdevice[u'raidbricksize'] = int(newsize)
                    if "ZRAM_SIZE" not in subdevice[u'storageoptions']:
                        subdevice[u'storageoptions'][u'ZRAM_SIZE'] = int(volume_size)
                    j=j+1
    config['volumeresources'][0]['raidplans'][0]['plandetail'] = json.dumps(plandetail)
    if 0 == j:
        debug('ERROR: Can\'t find memory type on JSON, exit')
        sys.exit(1)
        
    for raidbrick in config['volumeresources'][0]['raidplans'][0]['raidbricks']:
        if raidbrick['raidbricktype'].upper() == 'MEMORY':
            raidbrick['raidbricksize'] = int(newsize)

    tmp_fname = '/tmp/new_atlas.json'
    cfgfile = open(tmp_fname, "w")
    json.dump(config, cfgfile, sort_keys=True, indent=4, \
              separators=(',',  ': '))
    cfgfile.close()
    os.rename(tmp_fname, ATLAS_CFG)
    return 0 


def change_zram_max_mem_limit(config, newsize):
    zram_dev = find_zram_dev()
    debug('Changing memory limit') 
    cmd_str = "echo " + str(newsize) + "G" + " > " + "/sys/block/" + \
                zram_dev + "/mem_limit"
    rc = do_system(cmd_str)
    if (rc == 0):
        debug('Updating zram raidbrick size to local configuration')
        rc = change_zram_raidbricksize(config, newsize)
    return rc

def main():
    ##Parse command line args
    rc = 0
    f = open(ATLAS_CFG, 'r')
    config = json.load(f)
    f.close
    if len(sys.argv) < 2:
        usage()
        return errno.EINVAL
    elif sys.argv[1] == '-s':
        if len(sys.argv) < 3:
            print('Please input the new size.')
            return 1
        new_size = sys.argv[2]
        rc = simple_mem_check()
        if (rc == 0):
            zram_dev = find_zram_dev()
            if zram_dev == 'None':
                print('Cannot find the zram device')
                return errno.EINVAL
            new_size = check_input_value(config, new_size)
            rc = change_zram_max_mem_limit(config, new_size)
            if rc == 0:
                print('Memory limit change completed successfully.')
        return rc
    elif sys.argv[1] == '-c':
        (current_volume_size, current_mem_limit_size) = get_current_value(config)
        if current_volume_size == current_mem_limit_size:
            print('mem_limit size has the same size %d GB with volume size, you don\'t need change the value' \
                  % current_volume_size)
        else:
            print('The mem_limit size can be changed from %d(not included) to %d(included)' \
                   %(current_mem_limit_size, current_volume_size))
            print('Please kindly change the "Hot Add" memory first, then run script to change it')
        return rc
    else:
        usage()
        return errno.EINVAL
if __name__ == "__main__":
    main()
