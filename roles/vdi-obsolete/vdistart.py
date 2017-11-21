#!/usr/bin/python
"""
USX VDI role script entry point
"""
from vdiconfig_diskless_free import *
from vdiconfig_diskbacked_free import *
from vdiconfig_diskless import *
from vdiconfig_diskbacked import *

import json

# Configuration files
VDI_CFG = '/etc/ilio/atlas.json'

def load_cfg():
    """
    Process Atlas JSON configuration file
    Return value: dictionary of configuration parameters
    """
    cfg_dict = {}
    if os.access(VDI_CFG, os.R_OK): # /etc/ilio/atlas.json file exists
        fp = open(VDI_CFG)
        jsondata = json.load(fp)
        cfg_dict['role'] = jsondata.get('roles')[0]
        cfg_dict['uuid'] = jsondata.get('uuid')
        cfg_dict['license'] = jsondata.get('license')
        cfg_dict['amcurl'] = jsondata.get('amcurl')
        fp.close()
    else:
        self.debug("ERROR : %s is not found on this ILIO!" % VDI_CFG)
    return cfg_dict

configuration = load_cfg()
if configuration is None or not configuration:
    self.debug("ERROR : Atlas JSON configuration is empty or does not exist!")
    sys.exit(MISSING_ATLAS_CONFIG)

# dictionary of instantiate different role scripts based on role in Atlas JSON
options = {'vdi_diskless_free' : VDIConfigDisklessFree(configuration),
           'vdi_diskless' : VDIConfigDiskless(configuration), # testing. NEED TO change it to VDIConfigDiskless(configuration) 
           'vdi_diskbacked_free' : VDIConfigDiskbackedFree(configuration),
           'vdi_diskbacked' : VDIConfigDiskbacked(configuration),}

myfullrole = configuration['role'].lower()
vdiconfig = options[myfullrole]
vdiconfig.entry()
