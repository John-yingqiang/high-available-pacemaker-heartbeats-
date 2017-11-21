#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
"""
USX 2.0: Virtual Volume role script

Modified: August 12, 2014: incorporate RAID Planner changes

python change JSON key:
  http://stackoverflow.com/questions/11188889/how-can-i-edit-rename-keys-during-json-load-in-python
"""

import httplib
import ConfigParser
import json
#import operator
import os, sys, stat
import logging
import time
import traceback
import urllib2
import ddp_setup
import socket
import math
import uuid
import fnmatch
import os.path

sys.path.insert(0, "/opt/milio/atlas/roles")
from utils import *

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_ibd import *
from atl_md import *
from atl_storage import *
from atl_arbitrator import *
from status_update import *
from cmd import *
from atl_alerts import *

sys.path.insert(0, "/opt/milio/atlas/roles/ha")
import ha_util

CMD_IBDAGENT = "/sbin/ibdagent"
CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_A_UPDATE = CMD_IBDMANAGER + " -r a -u"
CMD_IBDMANAGER_STAT_WD = CMD_IBDMANAGER + " -r a -s get_wd"
CMD_IBDMANAGER_STAT_WU = CMD_IBDMANAGER + " -r a -s get_wu"
CMD_IBDMANAGER_STAT_WUD = CMD_IBDMANAGER + " -r a -s get_wud"
CMD_IBDMANAGER_STAT_UD = CMD_IBDMANAGER + " -r a -s get_ud"
CMD_IBDMANAGER_A_STOP_ONE = CMD_IBDMANAGER + " -r a -d"
CMD_IBDMANAGER_S_STOP_ONE = CMD_IBDMANAGER + " -r s -d"
CMD_IBDMANAGER_S_STOP = CMD_IBDMANAGER + " -r s -S"
CMD_IBDMANAGER_A_STOP = CMD_IBDMANAGER + " -r a -S"
CMD_IBDMANAGER_DROP = CMD_IBDMANAGER + " -r a -d"
CMD_IBDMANAGER_IOERROR = CMD_IBDMANAGER + " -r a -e"

# error code
NO_DEV_TO_STOP = 85 # shared error code between ha_util.py. If you change it here, you need to change it in ha_util.py

#def send_status(*args):
#    return

IBD_AGENT_CONFIG_FILE = '/etc/ilio/ibdagent.conf'
IBD_AGENT_SEC_GLOBAL = "global"
PORT = "port"
LISTENADDR = "listenaddr"
EXPORTNAME = "exportname"
DEV = "dev"
FILESIZE = "filesize"
SIZE = "size"
TYPE="type"
TYPE_DISK = "disk"
TYPE_RAM = "ram"
VV_CFG = "/etc/ilio/atlas.json"
START_SECTOR = 2048
DEFAULT_JOURNAL_SIZE = 400 # Default dedup fs journal size set to 400MB, get from ddp_setup.py.
TIERED_JOURNAL_SIZE = 2048 # Tiered dedup fs journal size set to 2G. TODO: should calculate it.
RETRYNUM = 20 # default retry count when an operation could not succeed.
ISCSI_DAEMON = "/usr/local/sbin/iscsi-scstd"
HA_DISABLE_FILE = '/tmp/ha_disable'
SHARED_STORAGE_DOWN='/var/log/sharedstorage_down'
SHARED_STORAGE_DOWN_RESET='/var/log/sharedstorage_down_reset'
LOCAL_STORAGE_DOWN_RESET='/var/log/localstorage_down_reset'
DEFAULT_LOGSIZE_M=400
HA_LOCKFILE = "/etc/ilio/ha_lockfile"
#array properties
ARRAY_SIZE = "Array-Size"
DEFAULT_SNAPSHOT_SIZE = 5
DEFAULT_SPACE_RATIO = 0.9
UPGREP_VERSION = '/etc/ilio/snapshot-version'
# commands
CMD_PARTED = "/sbin/parted"
CMD_MDADM = "/sbin/mdadm"
CMD_MDASSEMBLE = "/sbin/mdadm --assemble"
CMD_PVCREATE = "/sbin/pvcreate"
CMD_PVREMOVE = "/sbin/pvremove"
CMD_VGCREATE = "/sbin/vgcreate"
CMD_VGEXTEND = "/sbin/vgextend"
CMD_VGACTIVE = "/sbin/vgchange -a y"
CMD_VGDEACTIVE = "/sbin/vgchange -a n"
CMD_ATLASROLE_DIR = "/opt/milio/atlas/roles"
CMD_ADSPOOL = CMD_ATLASROLE_DIR + "/ads/ads-pool.pyc"
CMD_VIRTUALPOOL = CMD_ATLASROLE_DIR + "/pool/cp-load.pyc"
CMD_UMOUNT = "/bin/umount"
CMD_RMVSCALER = "/sbin/dmsetup remove"
CMD_DESTROYVSCALER = "/opt/milio/scripts/vscaler_destroy"
CMD_NFS_STOP = "service nfs-kernel-server stop"
CMD_NFS_STATUS = "service nfs-kernel-server status"

# VDI properties
VDI_DISKBACKED = 'SIMPLE_HYBRID'
VDI_FLASH = 'SIMPLE_FLASH'
VDI_DISKLESS = 'SIMPLE_MEMORY'
VDI_VOLUME_TYPES = [VDI_DISKBACKED, VDI_FLASH, VDI_DISKLESS]

# VDI commands
VDI_DISKLESS_SNAPCLONE = "python /opt/milio/scripts/usx_simplememory_sync.pyc complete_restore"
VDI_DISKLESS_SNAPCLONE_JOB = "/opt/milio/scripts/usx_simplememory_sync_setup.sh add 00 04"  #MM HH

LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'

volume_status_dict = {0:'OK',1:'WARN',2:'CRITICAL',3:'FATAL',4:'UNKNOWN'}
dedupfs_availability_status_file = '/opt/amc/agent/dedupfs_availability_status.prop'
volume_export_availability_status_file = '/opt/amc/agent/volume_export_availability_status.prop'
HA_FAILOVER_STATUS = 'HA_FAILOVER_STATUS'
VOL_STATUS_OK = 0
VOL_STATUS_WARN = 1
VOL_STATUS_FATAL = 3
devopt = []

THINPOOL_METADATA_SIZE = 256 * 1024 #KiB
LV_CHUNKSIZE = 4096 #KiB

def load_conf_from_amc(apiurl, apistr):
    """
    Get configuration information from AMC
     Input: query string
     Return: response data
    """
    try:
        debug("load conf from amc: %s" % apistr)
        protocol = apiurl.split(':')[0]
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        apiaddr = apiurl.split('/')[2]  # like: 10.15.107.2:8443
        debug(apistr)
        debug(apiaddr)
        if use_https == True:
            conn = httplib.HTTPSConnection(apiaddr)
        else:
            conn = httplib.HTTPConnection(apiaddr)
        conn.request("GET", apistr)
        response = conn.getresponse()
        debug(response.status, response.reason)
        if response.status != 200 and response.reason != 'OK':
            return None
        else:
            data = response.read()
    except:
        debug("ERROR : Cannot connect to AMC to query")
        return None

    return data



def save_conf_to_amc(amcurl, apistr, data, putFlag=0):
    """
    Call REST API to update configuration info to AMC
    """
    retVal = 0
    debug("--Save config to AMC")
    conn = urllib2.Request(amcurl + apistr)
    debug(amcurl+apistr)
    conn.add_header('Content-type','application/json')
    if putFlag == 1:
        conn.get_method = lambda: 'PUT'
    debug('**** data to be uploaded to AMC: ', data)
    res = urllib2.urlopen(conn, json.dumps(data))
    debug('Returned response code: ' + str(res.code))
    if res.code != 200:
        retVal = 1
    res.close()
    return retVal



def set_storage_interface_alias(setup_info):
    """
    Create an alias for storage network interface. Set the IP
     address the same as service IP. Don't do anything if
     network is configured as DHCP
    """
    debug("Setting up alias for storage network interface")

    netmask = ""
    cidrmask = ""
    devicename = ""
    serviceip = ""
    config = setup_info['configure']
    nics = config['usx']['nics'] # get network configuration
    for nic in nics:
        if nic['storagenetwork']:
            if nic['mode'].lower() == 'dhcp':
                devicename = nic['devicename'] # device name for setting up alias, always pick the storage NIC configured with DHCP first
                cmd = ('ip addr show | grep "scope global %s"' % devicename)
                for line in os.popen(cmd).readlines():
                    if devicename in line:
                        ip = line.strip().split(' ')[1]
                        cidrmask = ip.split('/')[-1]
            elif nic['mode'].lower() == 'static':
                if not devicename:
                    devicename = nic['devicename']
                netmask = nic['netmask']
    if config['volumeresources']:
        if config['volumeresources'][0].has_key('serviceip'):
            serviceip = config['volumeresources'][0]['serviceip'] # single resource for now

    if not is_vdi_volume(setup_info): # For non-VDI volumes, set service IP.
        if not serviceip:
            debug("WARNING : no service ip specified in atlas.json")
            return 0
        else:
            if not cidrmask and netmask:
                (subret, cidrmask) = ha_util.netmask2cidrmask(netmask)
                if (subret != 0): # convert netmask failed
                    return subret
            #cmd_str = ('ip addr add %s/%s dev %s' % (serviceip, netmask, devicename))
            cmd_str = (("OCF_ROOT=/usr/lib/ocf/ OCF_RESKEY_ip=%s OCF_RESKEY_cidr_netmask=%s " +
                       "/usr/lib/ocf/resource.d/heartbeat/IPaddr2 start") % (serviceip, cidrmask))
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : %s failed!" % cmd_str)
                return rc

    return 0



def vp_initialize_detail(taguuid):
    """
    Initialize vp detail repo; if it has already been initialized,
     skip the process.

    POST /ilio/virtualpool/{taguuid}/detail
     Call it once in the role script to initialize detail repo
    """
    debug("--Initialize VP detail repo...")

    retVal = 0
    api_str = '/usxmanager/usx/virtualpool/' + taguuid + '/detail'
    response = load_conf_from_amc(LOCAL_AGENT, api_str)
    #debug(response)
    if response == None:
        debug("-- **Detail repo not initialized. Initialize it now...")
        api_str = 'usx/virtualpool/' + taguuid +'/detail'
        data = {"taguuid":taguuid}
        rc = save_conf_to_amc(LOCAL_AGENT, api_str, data)
        if rc != 0:
            debug("ERROR : Initialize Detail repo via REST API call failed!")
            retVal = rc

    return retVal



def vp_get_aggexports(apistr, setup_info):
    """
    Load available aggregate base exports from AMC

    """
    def convert_key(obj):
        newkey=''
        #debug("$$$  %s " % obj)
        for key in obj.keys():
            if key == "iliouuid":
                newkey = "ilioid"
                obj[newkey] = obj[key]
                del obj[key]
            if key == "serviceip":
                newkey = "ip"
                obj[newkey] = obj[key]
                del obj[key]
        #debug("==== %s" % obj)
        return obj

    try:
        response = load_conf_from_amc(LOCAL_AGENT, apistr)
        #data = json.loads(response)
        data = json.loads(response, object_hook=convert_key)
        if not data.get('items'): # no aggregate exports infor retrieved
            return 1
        setup_info['imports'] = data.get('items')
    except:
        debug("ERROR : Failed to get aggregate exports from Agent API")
        return 1
    return 0



def vp_load_vv_imports(vp_setup_info):
    """
    Determine virtual volume resource imports to use new aggregate base exports
    or existing from virtual pool, based on virtual pool metrics
    """
    debug("--Get virtual pool imports...")

    rc = 0

    vp_uuid = vp_setup_info['vgname']
    vp_type = vp_setup_info['v_storagetype']
    vv_request_size = int(vp_setup_info['size'])
    adjusted_request_size = vv_request_size * (1024 * 1024 * 1024) # size to be carved out of pool, adjusted according to volume type
    vv_type = vp_setup_info['volumetype']

    apistr = '/usxmanager/usx/virtualpool/' + vp_uuid+ '?fields=tagmetrics'
    vp_metrics = load_conf_from_amc(LOCAL_AGENT, apistr)

    debug("-- %s" % vp_metrics)
    if vp_metrics is not None: # VP info exist, check if satisfy requirement
        result = json.loads(vp_metrics)
        if result['data']['tagmetrics'].has_key('metrics'):
            vp_availablecapacity = result['data']['tagmetrics']['metrics']['availablecapacity']['value']
            debug("--VP available size: %s bytes | request size: %s GB" % (vp_availablecapacity, vv_request_size))

            ## Mempool  = 15% of vv_request_size for hybrid
            if vv_type.upper() == 'HYBRID':
                if vp_type.upper() == 'MEMORY':
                    #debug("-- what is my mem request size %s" % math.ceil(int(adjusted_request_size * 0.15)))
                    adjusted_request_size = max(4294967296, int(math.ceil(int(adjusted_request_size * 0.15)))) # Minimum requirement 4G for mempool for hybrid
                    #debug("--adjuset request size in bytes %s" % adjusted_request_size)


            debug("--VP available size: %s bytes | adjusted request size: %s bytes" % (vp_availablecapacity, long(adjusted_request_size)))
            if long(vp_availablecapacity) < long(adjusted_request_size):
                # Insufficient vp available capacity, get agg exports and add to vp
                debug("Insufficient VP available capacity")

                # Get all unassigned aggregate exports of the same type as the pool
                apistr = ('/usxmanager/usx/inventory/servicevm/exports?' +
                          'query=.%5Bassigned%3D0%20and%20storagetype%3D' +
                          '\'' + vp_type.upper() + '\'%5D')

                rc = vp_get_aggexports(apistr, vp_setup_info)
                if rc != 0:
                    debug("WARNING : Cannot get unassigned aggregate base exports with type: %s!" % vp_type.upper())
                    # Get the aggregate exports from the existing virtual pool
                    apistr = ('/usx/inventory/servicevm/exports?' +
                              'query=.%5Bjxfn%3Ahas(virtualpooluuids%2C%20' +
                              '\'' + vp_uuid + '\')%5D')
                    rc = 0
                    rc = vp_get_aggexports(apistr, vp_setup_info)
                    if rc != 0:
                        debug("ERROR : Pool exists, but cannot get aggregate base exports!")

                vp_setup_info['extendvp'] = True
                configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
                debug("vp_load_vv_imports: aggexport for %s ----------------------- %s" % (vp_uuid, configure_str))


            else:
                # Sufficient vp available capacity, get imports of VP
                #debug("--Virtual pool has sufficient available capacity; return its agg exports")

                # Get the aggregate exports from the existing virtual pool
                apistr = ('/usxmanager/usx/inventory/servicevm/exports?' +
                          'query=.%5Bjxfn%3Ahas(virtualpooluuids%2C%20' +
                          '\'' + vp_uuid + '\')%5D')
                rc = vp_get_aggexports(apistr, vp_setup_info)
                if rc != 0:
                    debug("ERROR : Pool exists, but cannot get aggregate base exports!")

                vp_setup_info['extendvp'] = False

                configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
                debug("vp_load_vv_imports: aggexport for %s ----------------------- %s" % (vp_uuid, configure_str))

        else: # No VP available capacity info, new VP; get aggregate exports to create VP

            configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
            debug("vp_load_vv_imports: no imports %s ----------------------- %s" % (vp_uuid, configure_str))

            # No virtual pool is created yet, get all aggregate exports of the same pool type
            apistr = ('/usxmanager/usx/inventory/servicevm/exports?' +
                       'query=.%5Bassigned%3D0%20and%20storagetype%3D' +
                       '\'' + vp_type.upper() +'\'%5D')
            rc = vp_get_aggexports(apistr, vp_setup_info)
            if rc != 0:
                debug("ERROR : Cannot get aggregate base exports!")

            vp_setup_info['extendvp'] = False
            configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
            debug("vp_load_vv_imports: aggexport for %s ----------------------- %s" % (vp_uuid, configure_str))

    else: # Since VP is seeded, vp_metrics is response from /virtualpool/{taguuid}
          #  that should not be None, even if no metrics contained (new pool)
        debug("ERROR : Retrieve Virtual pool info failed")
        rc = 1
    return rc



def vp_setup_configuration(setup_info, vp_setup_info):
    """
    Setup virtual pool infrastructure and initialize virtual pool
    """
    debug("--Setup virtual pools configurations")

    rc = 0
    configure = setup_info['configure']
    amcurl = configure['usx']['usxmanagerurl']
    #vmmanger = configure['ilio']['vmmanagername']

    poolmgmt_info={}
    configure_json = {}
    vp_config = [] # list of pool configurations
    single_pool_info ={}

    sharedstoragefirst = True

    vvresources = configure['volumeresources']
    for resource in vvresources: # TODO :: Need to support multiple resources later
#         if resource.has_key('sharedstoragefirst'):
#             sharedstoragefirst = True
#         configure_json['sharedstoragefirst'] = resource['sharedstoragefirst']
#         configure_json['amcurl'] = amcurl
#         configure_json['fastsync'] = resource['fastsync']
#         configure_json['raidtype'] = resource['raidtype']
#         configure_json['num_workers'] = resource['numworkers']
#         configure_json['gapratio'] = resource['gapratio']
#         configure_json['size'] = resource['volumesize']
#         configure_json['adsname'] = resource['uuid']

        # Determine Virtual Pool related configuration
        vv_size = resource['volumesize'] * (1024 * 1024 * 1024) # Virtual volume size in GB

        if resource.has_key('uuid'):
            configure_json['uuid'] = resource['uuid']

        if resource.has_key('cappooltaguuid'):
            #debug("Setup virtual pool %s" % resource['cappooltaguuid'])
            configure_json['amcurl'] = amcurl
            configure_json['fastsync'] = resource['fastsync']
            configure_json['raidtype'] = resource['raidtype']
            configure_json['num_workers'] = resource['numworkers']
            configure_json['gapratio'] = resource['gapratio']
            configure_json['size'] = resource['volumesize']
            configure_json['adsname'] = resource['uuid']
            vp_type = 'DISK'

            configure_json['vgname'] = resource['cappooltaguuid']
            configure_json['uuid'] = resource['cappooltaguuid']
            roles = ['CAPACITY_POOL']
            configure_json['roles'] = roles
            configure_json['chunk_size'] = resource['capacitychunksize']
            configure_json['v_storagetype'] = vp_type
            configure_json['volumetype'] = resource['volumetype']

            rc = vp_initialize_detail(resource['cappooltaguuid'])
            if rc != 0:
                debug("ERROR : Cannot initialize virtual pool detail repo")
                #return rc

            rc = vp_load_vv_imports(configure_json)
            if rc != 0:
                debug("ERROR : Cannot load imports for Virtual Pool")
                #return rc

            if resource.has_key('sharedstoragefirst'):
                configure_json['sharedstoragefirst'] = resource['sharedstoragefirst']
            if resource.has_key('sharedstorages'):
                configure_json['sharedstorages'] = resource['sharedstorages']
            if rc != 0 and resource.has_key('sharedstorages'):
                rc = 0

            single_pool_info['configure'] = configure_json
            vp_config.append(single_pool_info) # adding one vp config info
            single_pool_info = {}
            configure_json = {}

        if resource.has_key('mempooltaguuid'):
            #debug("Setup virtual pool %s" % resource['mempooltaguuid'])
            configure_json['amcurl'] = amcurl
            configure_json['fastsync'] = resource['fastsync']
            configure_json['raidtype'] = resource['raidtype']
            configure_json['num_workers'] = resource['numworkers']
            configure_json['gapratio'] = resource['gapratio']
            configure_json['size'] = resource['volumesize']
            configure_json['adsname'] = resource['uuid']
            vp_type = 'MEMORY'

            configure_json['vgname'] = resource['mempooltaguuid']
            configure_json['uuid'] = resource['mempooltaguuid']
            roles = ['MEMORY_POOL']
            configure_json['roles'] = roles
            configure_json['chunk_size'] = resource['memorychunksize']
            configure_json['v_storagetype'] = vp_type
            configure_json['volumetype'] = resource['volumetype']

            rc = vp_initialize_detail(resource['mempooltaguuid'])
            if rc != 0:
                debug("ERROR : Cannot initialize virtual pool detail repo")
                #return rc

            rc = vp_load_vv_imports(configure_json)
            if rc != 0:
                debug("ERROR : Cannot load imports for Virtual Pool")
                #return rc

            single_pool_info['configure'] = configure_json
            vp_config.append(single_pool_info) # adding one vp config info
            single_pool_info = {}
            configure_json = {}

        if resource.has_key('flashpooltaguuid'):
            #debug("Setup virtual pool %s" % resource['flashpooltaguuid'])
            configure_json['amcurl'] = amcurl
            configure_json['fastsync'] = resource['fastsync']
            configure_json['raidtype'] = resource['raidtype']
            configure_json['num_workers'] = resource['numworkers']
            configure_json['gapratio'] = resource['gapratio']
            configure_json['size'] = resource['volumesize']
            configure_json['adsname'] = resource['uuid']
            vp_type = 'FLASH'

            configure_json['vgname'] = resource['flashpooltaguuid']
            configure_json['uuid'] = resource['flashpooltaguuid']
            roles = ['FLASH_POOL']
            configure_json['roles'] = roles
            configure_json['chunk_size'] = resource['flashchunksize']
            configure_json['v_storagetype'] = vp_type
            configure_json['volumetype'] = resource['volumetype']

            rc = vp_initialize_detail(resource['flashpooltaguuid'])
            if rc != 0:
                debug("ERROR : Cannot initialize virtual pool detail repo")
                #return rc

            rc = vp_load_vv_imports(configure_json)
            if rc != 0:
                debug("ERROR : Cannot load imports for Virtual Pool")
                #return rc

            # TODO :: Determine whether flash pool is used as DISK pool (ALL_FLASH, Hybrid where use flash first)
            # OR used as MEMORY pool (Hybrid where memory pool insufficent)

            vp_type = 'DISK'
            configure_json['v_storagetype'] = vp_type
            roles = ['CAPACITY_POOL']
            configure_json['roles'] = roles

            if resource.has_key('sharedstoragefirst'):
                configure_json['sharedstoragefirst'] = resource['sharedstoragefirst']
            if resource.has_key('sharedstorages'):
                configure_json['sharedstorages'] = resource['sharedstorages']
            if rc != 0 and resource.has_key('sharedstorages'):
                rc = 0

            single_pool_info['configure'] = configure_json
            vp_config.append(single_pool_info) # adding one vp config info
            single_pool_info = {}
            configure_json = {}

        vp_setup_info['vps'] = vp_config
        vp_setup_info['adsname'] = resource['uuid']
        vp_setup_info['type'] = resource['volumetype']
        vp_setup_info['journaled'] = resource['directio']
        vp_setup_info['export_type'] = resource['exporttype']

    debug("END vp_setup_configuration: Virtual volume: " + json.dumps(setup_info, indent=4, separators=(',', ': ')))
    debug("END vp_setup_configuration: Virtual pool: " + json.dumps(vp_setup_info, indent=4, separators=(',', ': ')))
    return rc



def load_vp_from_amc(amcurl, pooltaguuid):
    """
    Get Virtual pool configuration from AMC
    """
    try:
        protocol = amcurl.split(':')[0] # like: https
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        amcaddr = amcurl.split('/')[2]  # like: 10.15.107.2:8443
        amcfile = "/usxmanager/usx/virtualpool/" + pooltaguuid + "/attributes/vpconfig"
        if use_https == True:
            conn = httplib.HTTPSConnection(amcaddr)
        else:
            conn = httplib.HTTPConnection(amcaddr)
        debug(amcfile)
        debug(amcaddr)
        conn.request("GET", amcfile)
        r1 = conn.getresponse()
        debug(r1.status, r1.reason)
        data1 = r1.read()
        response = json.loads(data1) # parse vpconfig from response
        data2 = response['vpconfig']
        response = json.loads(data2)
        configure_json = response['configure']
        configure_str = json.dumps(configure_json)
        tmp_fname = '/tmp/' + configure_json['adsname'] + '.json'
        tmp_file = open(tmp_fname, 'w')
        tmp_file.write(configure_str)
        tmp_file.close()
    except:
        debug('Can not connect to AMC for config json.')
        return None
    return configure_str



def vp_get_configuration(setup_info, vp_setup_info):
    """
    Get virtual pool info and infrastructure
    """
    debug("--Get virtual pools configurations")

    rc = 0
    configure = setup_info['configure']
    amcurl = configure['usx']['usxmanagerurl']
    vmmanger = configure['usx']['vmmanagername']

    configure_json = {}
    vp_config = [] # list of pool configurations
    single_pool_info ={}

    sharedstoragefirst = True

    vvresources = configure['volumeresources']
    for resource in vvresources: # TODO :: Need to support multiple resources later

        # Determine Virtual Pool related configuration
        #vv_size = resource['volumesize'] * (1024 * 1024 * 1024) # Virtual volume size in GB
        if resource.has_key('cappooltaguuid'):
            #debug("Setup virtual pool %s" % resource['cappooltaguuid'])
            response_str = load_vp_from_amc(LOCAL_AGENT, resource['cappooltaguuid'])
            configure_json = json.loads(response_str)
            if configure_json is None:
                debug("ERROR : Get virtual pool configuration failed!")
                return 1


            single_pool_info['configure'] = configure_json
            vp_config.append(single_pool_info) # adding one vp config info
            configure_json = {}
            single_pool_info = {}

        if resource.has_key('mempooltaguuid'):
            response_str = load_vp_from_amc(LOCAL_AGENT, resource['mempooltaguuid'])
            configure_json = json.loads(response_str)
            if configure_json is None:
                debug("ERROR : Get virtual pool configuration failed!")
                return 1


            single_pool_info['configure'] = configure_json
            vp_config.append(single_pool_info) # adding one vp config info
            configure_json = {}
            single_pool_info = {}


        if resource.has_key('flashpooltaguuid'):
            #debug("Setup virtual pool %s" % resource['flashpooltaguuid'])
            response_str = load_vp_from_amc(LOCAL_AGENT, resource['flashpooltaguuid'])
            configure_json = json.loads(response_str)
            if configure_json is None:
                debug("ERROR : Get virtual pool configuration failed!")
                return 1


            single_pool_info['configure'] = configure_json
            vp_config.append(single_pool_info) # adding one vp config info
            configure_json = {}
            single_pool_info = {}

        vp_setup_info['vps'] = vp_config
        vp_setup_info['adsname'] = resource['uuid']
        vp_setup_info['type'] = resource['volumetype']
        vp_setup_info['journaled'] = resource['directio']
        vp_setup_info['export_type'] = resource['exporttype']

#         configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#         debug("----------------------- %s" % configure_str)

    return rc



def vv_update_vp_size(pooltaguuid):
    """
    Update pool capacity to AMC
    """
    retVal = 0

    vp_totalcap = 0
    vp_availablecap = 0

    cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vp_get_size ' + '\'' + pooltaguuid + '\''
    ret, msg = runcmd(cmd_str)
    if ret != 0:
        debug("ERROR : Cannot get virtual pool size")
        retVal = ret
    for line in msg.split('\n'):
        info = line.split()
        if info: # make sure vp_get_size returns non-empty string
            if info[0].lower() == 'vp_get_size':
                vp_totalcap = long(info[1])
                vp_availablecap = long(info[2])
                break

    data = {}
    metrics = {}
    totalcapacity = {}
    availablecapacity = {}
    usedcapacity = {}

    totalcapacity['name'] = 'totalcapacity'
    totalcapacity['value'] = vp_totalcap
    totalcapacity['unit'] = 'byte'

    availablecapacity['name'] = 'availablecapacity'
    availablecapacity['value'] = vp_availablecap
    availablecapacity['unit'] = 'byte'

    usedcapacity['name'] = 'usedcapacity'
    usedcapacity['value'] = vp_totalcap - vp_availablecap
    usedcapacity['unit'] = 'byte'

    metrics['totalcapacity'] = totalcapacity
    metrics['availablecapacity'] = availablecapacity
    metrics['usedcapacity'] = usedcapacity

    data['taguuid'] = pooltaguuid
    data['metrics'] = metrics

    vpmetrics_apistr = 'usx/virtualpool/' + pooltaguuid + '/metrics'

    #debug(data)
    #debug(vpmetrics_apistr)
    rc = save_conf_to_amc(LOCAL_AGENT, vpmetrics_apistr, data)
    if rc != 0:
        debug("ERROR : Save pool capactiy metrics REST API call failed ")
        retVal = rc

    return retVal



def vv_update_conf(setup_info):
    """
    After vitual volume is setup, update configurations to AMC
     configurations: imports list of agg exports
                     virtual pool metrics
                     agg exports pool uuids
    """
    retVal = 0

#     configure_str = json.dumps(setup_info, indent=4, separators=(',', ': '))
#     debug("----------------------- %s" % configure_str)

    vvrimports_data = []
    vps = setup_info['vps']
    vvrimports_apistr = 'usx/inventory/volume/resources/' + setup_info['adsname'] + '/imports'

    for pool in vps:
        if pool['configure'].has_key('imports'):
            aggexports = pool['configure']['imports']
            vpimports_apistr = 'usx/inventory/virtualpool/' + pool['configure']['uuid'] + '/imports'
            vpimports_data = [] # input data to update agg exports with vp uuid
            for item in aggexports:
                vpimports_data.append(item['storageuuid'])
                data1 = {} # input data to update virtual volume resouce imports
                data1["servicevmexportuuid"] = item['storageuuid']
                data1["size"] = item['size']
                vvrimports_data.append(data1)

            #debug(vpimports_data)
            #debug(vvrimports_data)
            rc1 = save_conf_to_amc(LOCAL_AGENT, vvrimports_apistr, vvrimports_data, 1)
            if rc1 != 0:
                debug("ERROR : Save imports to Virtual volume resource failed!")
            rc2 = save_conf_to_amc(LOCAL_AGENT, vpimports_apistr, vpimports_data, 1)
            if rc2 != 0:
                debug("ERROR : Save aggregate exports id to Virtual pool failed!")
            rc3 = vv_update_vp_size(pool['configure']['uuid'])
            if rc3 != 0:
                debug("ERROR : Update pool capacity info to AMC failed")

            del vvrimports_data[0:len(vvrimports_data)]
            del vpimports_data[0:len(vpimports_data)]

            retval = rc1 | rc2 | rc3 # return 1 if any one of the REST API updates fails
            if retval != 0:
                debug("ERROR : vv_update_conf failed!")
                break

    return retVal



def vv_get_resource(vvruuid, setup_info):
    """
    Get virtual volume resource info from AMC
    """
    debug("--Get virtual volume resource info for %s..." % vvruuid)

    retVal = 0

    data = {}
    resource_list = []

    api_str = ('/usxmanager/usx/inventory/volume/resources/' + vvruuid)

    response = load_conf_from_amc(LOCAL_AGENT, api_str)
    try:
        if response is not None:
            result = json.loads(response)
            resource_list.append(result['data'])
            setup_info['configure']['volumeresources'] = resource_list
    except ValueError:
        debug("Exception occurred when load conf from amc")
        debug(response)
        response = None

    if response is None:
        debug("ERROR : Failed to get Volume resource from AMC")
        debug("Get Volume resource from pacemaker instead")
        json_str = ha_util.ha_get_conf_from_crm('resource', vvruuid)
        json_dict = json.loads(json_str)
        if json_dict != '':
            setup_info['configure']['volumeresources'] = json_dict['volumeresources']
        else:
            retval = 1

    return retVal



def load_conf(fname, setup_info):
    """
    Retrieve all configuration info from a JSON file
    """
    try:
        cfg_file = open(fname, 'r')
        cfg_str = cfg_file.read()
        cfg_file.close()
        setup_info['configure'] = json.loads(cfg_str)
    except:
        debug("CAUTION: Cannot load the configuration JSON file:", fname)
        return 1
    return 0

def adsname_to_json(adsname):
    json_fname = "/etc/ilio/ads_" + adsname + '.json'
    return json_fname

#
# Download JSON from the AMC server that specified in local config.
#
def get_conf_from_amc(amcurl, adsname_str):
    #
    # retrieve all aggregator info from AMC, a Json file
    #
    #sample: curl -k https://10.15.112.10:8443/amc/model/ilio/ads?query=adsname=test1_wy-ads-mem-31
    amcaddr = amcurl.split('/')[2]    # 10.15.107.3:8080
    protocol = amcurl.split(':')[0]
    if protocol == 'https':
        use_https = True
    else:
        use_https = False
    amcaddr = amcurl.split('/')[2]    # 10.15.107.3:8080
    #amcfile = "/amc/model/ilio/ads/adsresource/" + adsname_str
    amcfile = "/usxmanager/usx/inventory/volume/resources/" + adsname_str
    if use_https == True:
        conn = httplib.HTTPSConnection(amcaddr)
    else:
        conn = httplib.HTTPConnection(amcaddr)
    debug(amcfile)
    debug(amcaddr)
    conn.request("GET", amcfile)
    r1 = conn.getresponse()
    debug(r1.status, r1.reason)
    data1 = r1.read()
    debug(data1)
    json_fname = adsname_to_json(adsname_str)
    tmp_file = open(json_fname, 'w')
    tmp_file.write(data1)
    tmp_file.close()
    return json_fname

def parse_conf_from_amc(fname, setup_info):
    #
    # retrieve all configuration info from a Json file
    #
    try:
        cfg_file = open(fname, 'r')
        cfg_str = cfg_file.read()
        cfg_file.close()
        setup_info['configure'] = json.loads(cfg_str)
        convert_conf(setup_info)
        debug(json.dumps(setup_info, indent=4, separators=(',', ': ')))

    except:
        debug(traceback.format_exc())
        debug("CAUTION: Cannot load configure json file:", fname)
        return 1
    return 0



"""
VP management for Virtaul Volume
"""
def load_pools(vp_setup_info):
    """
    Initialize virtual pool infrastructure and VP
    """
    debug("Enter load_pools ...")
    rc = 0
    adsname_str = vp_setup_info['adsname']
    volume_type = vp_setup_info['type']
    pools_conf = vp_setup_info['vps']
    sharedstoragefirst = True
    if vp_setup_info.has_key('sharedstoragefirst'):
        sharedstoragefirst = vp_setup_info['sharedstoragefirst']

    do_system(CMD_IBDMANAGER_STAT_WU)
    debug(json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    for pool in pools_conf:
        vgname = pool['configure']['vgname']
        size = str(pool['configure']['size'])
        pool_type = pool['configure']['v_storagetype']

        if 'memory' in pool_type.lower() or 'disk' in pool_type.lower():
            # Setup VP infrastructure with agg exports
            # If vp not initialized, create it; otherwise, add aggexports to the vp
            configure_str = json.dumps(pool, indent=4, separators=(',', ': '))
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' virtvol_vp_setup ' + '\'' + configure_str + '\''
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : Cannot create virtual pool infrastructure")
                break
            # Size unit is in GB
            #debug("vgname: %s | size: %s GB | volumetype: %s" % (vgname, size, volume_type))
            if volume_type.lower() == 'hybrid':
                if pool_type.lower() == 'memory':
                    size = str(max(4, int(math.ceil(int(size) * 0.15)))) # min required size for mem pool for hybrid volume is 4G
            #debug("After adjustment: vgname: %s GB | size: %s | volumetype: %s" % (vgname, size, volume_type))
            the_exportname = adsname_str.split('_')[-1] + '_in' + pool_type.lower()
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' ads_vp_init ' + vgname + ' ' + adsname_str + ' ' + the_exportname + ' ' + size
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : Cannot init %s pool" % pool_type)
                break
            pool['pool_loaded'] = True
        else:
            debug("ERROR : Cannot support the pool type: %s!" % pool_type)
            return 1

    do_system(CMD_IBDMANAGER_STAT_WU)
    print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

#     configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#     debug(configure_str)
#     configure_str = json.dumps(vp_setup_info)
#     cfg_file = open('./test.json', 'w')
#     cfg_file.write(configure_str)
#     cfg_file.close()
    return rc

#
# Store ibd devices for ADS stop/status to use.
# /tmp/<adsname>.devlist
#
def store_ibd_list(setup_info):

    config = setup_info['configure']
    adsname_str = config['volumeresources'][0]['uuid'] # TODO::Support multiple resources per container

    ibdlist = ''
    ibd_dev_list = setup_info['ibd_dev_list']
    #configure = setup_info['configure']
    #adsname_str = configure['adsname']

    for the_ibd in ibd_dev_list:
        ibdlist = ibdlist + the_ibd['devname'] + '\n'

    debug("IBD devices for ADS %s:" % adsname_str, '\n' , ibdlist)
    tmp_fname = "/tmp/" + adsname_str + '.devlist'
    tmp_file = open(tmp_fname, 'w')
    tmp_file.write(ibdlist)
    tmp_file.close()
    return

def load_ibd_list(adsname):
    try:
        tmp_fname = "/tmp/" + adsname + '.devlist'
        tmp_file = open(tmp_fname, 'r')
        ibd_str = tmp_file.read()
        tmp_file.close()
        ibd_list = ibd_str.split('\n')
        return ibd_list
    except:
        debug('Cannot load ' + tmp_fname)
        return None
    return None

def remove_ibd_list(adsname):
    try:
        tmp_fname = "/tmp/" + adsname + '.devlist'
        os.remove(tmp_fname)
    except:
        debug('Unable to remove ', tmp_fname)
        pass
    return

def remove_all_ibd_lists():
    #debug('Cleanup *.devlist from previous power-cycle.')
    #do_system('rm /tmp/*.devlist')
    debug('We rely on /etc/default/rcS : TMPTIME=0 to cleanup /tmp/*.devlist on reboot.')

# Use the unique lvname as IBD's uuid
def create_ibd_uuid_syms(ibd_dev_list):
    for ibd_dev in ibd_dev_list:
        devname = ibd_dev['datadev']
        lvname = ibd_dev["lvname"]
        lvpath = '/dev/ibd_' + lvname
        try:
            os.remove(lvpath)
        except:
            pass
        os.symlink(devname, lvpath)
        debug('Created symlink %s for %s' % (lvpath, devname))
        ibd_dev["uuid_path"] = lvpath

        # FIXME: log device doesn't use uuid path anymore.
        if ibd_dev['logdev'] != None:
            devname = ibd_dev['logdev']
            lvname = ibd_dev["lvname"]
            lvpath = '/dev/ibd_' + lvname + '_log'
            try:
                os.remove(lvpath)
            except:
                pass
            os.symlink(devname, lvpath)
            debug('Created symlink %s for %s' % (lvpath, devname))
            ibd_dev["log_uuid_path"] = lvpath
    return

def ads_pick_start_arb_device(setup_info):
    config = setup_info['configure']
    adsname_str = config['volumeresources'][0]['uuid']
    arb_dev_list = []
    local_setup_info = {}

    rc = load_conf(VV_CFG, local_setup_info)
    if rc != 0:
        return rc
    local_configure = local_setup_info['configure']
    if local_configure['usx']['ha'] != True:
        debug('None HA mode, skip arb setup.')
        return 0

    for the_dev in setup_info['ibd_dev_list']:
        dev_name = the_dev['private']
        if not dev_name.startswith('/dev/ibd'):
            # Any compond devices should do fencing inside it.
            # Only use the lowest level ibd device:
            debug('Skip non-ibd device %s for arb device.' % dev_name)
            continue
        arb_dev_list.append(dev_name)

    debug('arb_dev_list: ', arb_dev_list)
    if len(arb_dev_list) == 0:
        debug('Zero valid arb device found, skip arb setup.')
        return 0
    rc = arb_start(adsname_str, arb_dev_list)
    if rc != True:
        debug('ADS Fencing failed!')
        return 1
    return 0

def load_devices(setup_info, init = 0):
    """
    Load devices
    """
    debug("Entering load_devices...")

    adsname_str = setup_info['adsname']
    pools_conf = setup_info['vps']
    volume_type = setup_info['type']

    setup_info['ibd_dev_list'] = []
    ibd_dev_list = setup_info['ibd_dev_list']

    next_ibd_idx = 0
    rc = 0

    device_nr = 0
    cp_exportname = ""
    for pool in pools_conf:
        #the_ip = pool["nbd_ip"] # nbd_ip is the storage network IP of pool node
        the_ip = ""
        pool_type = pool['configure']['v_storagetype']
        vgname = pool["configure"]["vgname"]
        the_exportname = adsname_str.split('_')[-1] + '_in' + pool_type.lower()

        if 'memory' in pool_type.lower() or 'disk' in pool_type.lower():
            debug(pool_type + ' import: ' + vgname + ' ' + the_exportname + the_ip)
            if pool.has_key('pool_loaded') and pool['pool_loaded'] == True:
                debug('Pool already loaded, skip load.')
            else:
                cmd_str = 'python ' + CMD_VIRTUALPOOL + ' ads_vp_start ' + vgname + ' ' + adsname_str + ' ' + the_exportname
                rc = do_system(cmd_str)
                if rc != 0:
                    debug("ERROR : Cannot import %s pool!" % pool_type)
                    return rc
            #Read the generated device name.
            try:
                devname_str = os.readlink('/dev/' + adsname_str + '_' + vgname)
            except:
                debug('Cannot get dev name for %s, Skip.' % adsname_str)
                break
            the_devname = devname_str
        else:
            # TODO: Shared storage pool
            continue

        the_ibd_dev = {"devname":the_devname, "pool_type":pool_type, "lvname":the_exportname}
        the_ibd_dev['private'] = the_devname + 'p1'
        the_ibd_dev['datadev'] = the_devname + 'p2'
        the_ibd_dev['type'] = pool_type
        # Tiered ADS need a journal/log partition in the memory disk.
        # create_ibd_uuid_syms() will need 'logdev'. FIXME: not true any more.
        if volume_type.lower() == "hybrid" and the_ibd_dev['pool_type'].lower() == "memory":
            the_ibd_dev['logdev'] = the_devname + 'p3'
        else:
            the_ibd_dev['logdev'] = None
        ibd_dev_list.append(the_ibd_dev)
        device_nr += 1

    rc = ibd_agent_alive()
    if rc == False:
        cmd_str = CMD_IBDAGENT
    else:
        cmd_str = CMD_IBDMANAGER_A_UPDATE
    rc = do_system(cmd_str)
    # to wait for ibd devices to start
    time.sleep(2);
    cmd_str = CMD_IBDMANAGER_STAT_WUD
    retry = 0
    max_num_retry = 150
    cp_found_flag = False

    if len(cp_exportname) > 0:
        while (retry < max_num_retry):
            if retry != 0:
                time.sleep(2);
            out = ['']
            rc = do_system(cmd_str, out)
            lines = out[0].split('\n')
            for the_line in lines:
                line_parts = the_line.split(' ')
                if len(line_parts) < 2:
                    continue
                the_exportname = line_parts[0]
                if the_exportname == cp_exportname:
                    debug('Started the ibd device in load_devices: %s' % cp_exportname)
                    cp_found_flag = True
                    break
            retry = retry + 1
            if cp_found_flag == True:
                break

        if cp_found_flag == False:
            debug('failed to start the ibd device: %s' % cp_exportname)
            return 1

    time.sleep(2)
    tune_all_ibd(ibd_dev_list)
    create_ibd_uuid_syms(ibd_dev_list)
    store_ibd_list(setup_info)

    if rc != 0:
        debug('Setup device connection failed.')
        return rc

    if init == 1:
        debug('Initializing devices...')
        rc = init_devices(setup_info)
        if rc != 0:
            debug("init_devices failed!")
            return 1
    else:
        rc = reinit_mem_device(setup_info)
        if rc != 0:
            debug("reinit_mem_devices failed!")
            return 1

    rc = ads_pick_start_arb_device(setup_info)
    return rc

#
# Simply check for a valid partition table on memory device.
#
def check_partition(the_dev):
    devname = the_dev['devname']
    out = ['']
    cmd_str = 'parted -m -s -- %s unit s print free' % devname
    rc = do_system(cmd_str, out)
    if rc != 0:
        debug('No partition table on the device, corrupted device?')
        return False
    return True

def check_device(the_dev):
    #lvs --noheadings dedupvg/deduplv -o lv_attr|grep a
    devname = the_dev['datadev_raw']

    out = ['']
    cmd_str = 'lvs --all'
    rc = do_system(cmd_str, out)

    cmd_str = 'lvs --noheadings dedupvg/deduplv -o lv_attr|grep a'
    rc = do_system(cmd_str, out)
    if rc != 0:
        debug('No dedup LV on the device, corrupted device?')
        return False
    return True

def get_disk_attr(the_dev):
    devname = the_dev['devname']
    out = ['']
    cmd_str = 'parted -m -s -- %s unit s print free' % devname
    rc = do_system(cmd_str, out)
    if rc != 0:
        debug('Try to create partition table on the device.')
        cmd_str = 'parted -m -s -- %s mklabel gpt ' % devname
        rc = do_system(cmd_str, out)
        if rc != 0:
            debug('Can not create partition table with parted.')
            return rc
        cmd_str = 'parted -m -s -- %s unit s print free' % devname
        rc = do_system(cmd_str, out)
        if rc != 0:
            debug('Can not run parted to get free space.')
            return rc
    if True:
        msg = out[0]
        print msg.split('\n')
        """
        lines = msg.split('\n')
        for i in range(-1, -len(lines), -1):
            line = lines[i]
            if line.find('free') != -1:
                freeline = line
        if freeline.find('free') == -1:
            debug('Can not get free space from parted')
            return 1
        freesize = freeline.split(':')[-2]
        freesize = freesize.strip('s')
        the_dev['freesize_s'] = long(freesize)
        """
        devline = msg.split('\n')[1]
        the_dev['dev_sector_size'] = devline.split(':')[3]
        devsize = devline.split(':')[1]
        devsize = devsize.strip('s')
        the_dev['devsize_s'] = long(devsize)
    else:
        debug('Can not parse parted free space output.')
        return 1
    debug(the_dev)
    return 0

"""
Setup partitions for a device. The input dictionary contains keys to indicate
if private, and log/journal partitons are required.
	Create private partition, if the corresponding dictionary key is set to
	True
	Create log/journal partition, if the corresponding dictionary key is
	set to True. Log/journal device size is to be sepcified in the input
	dictionary, or else a default value is assumed.
	Create data partition.
@the_dev	dictionary containing at least the name of device to be
	partitioned.
	The names of the newly created partitons are written back to the same
	dictionary
Returns 0; on success
"""
def setup_partitions(the_dev):
    debug('Entering setup_partitions...')
    devname = the_dev.get('devicepath')
    if not devname:
        debug("ERROR: No device name provided for partition")
    	return -1

    # Setup private partition, if required
    if the_dev.get('private_partition'):
    	cmd_str = 'parted -s -- %s mklabel gpt mkpart primary ext2 1024s 2047s' % devname
    	rc = do_system(cmd_str)
    	if rc != 0:
	    debug("ERROR: Failed to create private partition")
            return rc
        the_dev['private_dev'] = devname + '1'

    if the_dev.get("need_log_dev"):
        # Partition creation order is important to keep the partition number!
	logsz_m = the_dev.get("logsize_m") or DEFAULT_LOGSIZE_M
	the_dev["logsize_m"] = logsz_m
	# Setup journal/log partition
        end_sec = logsz_m * (1024 * 1024 / 512) # TODO: Assumed sector sz=512
        cmd_str = 'parted -s -- %s mklabel gpt mkpart primary ext2 2048s %ds' % (devname, 2048 + end_sec - 1)
        rc = do_system(cmd_str)
        if rc != 0:
	    debug("ERROR: Failed to create journal partition")
            return rc
        if the_dev.get('private_partition'):
            the_dev['log_dev'] = devname + '2'
	else:
            the_dev['log_dev'] = devname + '1'

        # Setup data partition *** 'ext3' just to bypass parted bug ***
        cmd_str = 'parted -s -- %s mkpart primary ext3 %ds 100%%' % (devname, 2048 + end_sec)
        rc = do_system(cmd_str)
        if rc != 0:
	    debug("ERROR: Failed to create data partition")
            return rc
        if the_dev.get('private_partition'):
            the_dev['data_dev'] = devname + '3'
	else:
            the_dev['data_dev'] = devname + '2'
    else:
        # Setup data partition
        cmd_str = 'parted -s -- %s mkpart primary ext3 2048s 100%%' % devname
        rc = do_system(cmd_str)
        if rc != 0:
	    debug("ERROR: Failed to create data partition")
            return rc
        if the_dev.get('private_partition'):
            the_dev['data_dev'] = devname + '2'
	else:
            the_dev['data_dev'] = devname + '1'

    # Log the result partition table.
    cmd_str = 'parted -s -- %s unit B print' % devname
    rc = do_system(cmd_str)
    return rc


def is_dedupvg_existed():
    """
    Use the vgdisplay command to check the dedupvs is or not existed.
    """
    return do_system('vgdisplay dedupvg') == 0

def fix_lvm_config(config):
    # Fix LVM config file
    cmd_str = "sed -i -e 's/md_chunk_alignment = 1/md_chunk_alignment = 0/'" + \
        " -e 's/data_alignment_detection = 1/data_alignment_detection = 0/'" + \
        " -e 's/data_alignment_offset_detection = 1/data_alignment_offset_detection = 0/'" + \
        " /etc/lvm/lvm.conf"
    rc = do_system(cmd_str)
    if rc != 0:
        debug("ERROR: Cannot fix lvm.conf!")
        return rc

    # Simple hybrid volume must skip /dev/sdb.
    if config['volumeresources'][0]['volumetype'].upper() in [VDI_DISKLESS]:
        # Simple memory volume's zram device doesn't support direct-IO, thin_check
        # will always fail, we must disable thin_check in this case.
        cmd_str = "sed -i -e 's/thin_check_executable.*/thin_check_executable = \"\"/'" + \
                " /etc/lvm/lvm.conf"
        rc = do_system(cmd_str)
        if rc != 0:
            debug("ERROR: Cannot fix lvm.conf for simple memory volume!")
            return rc
        # Simple hybrid volume must skip /dev/sdb to avoid the os find dedupvg before we
        # start ibd/vscaler.
        cmd_str = "sed -i -e 's/.*\/dev\/cdrom.*/\tfilter = \[ \"r|\/dev\/sdb|\" \]/'" + \
                " /etc/lvm/lvm.conf"
        rc = do_system(cmd_str)
        if rc != 0:
            debug("ERROR: Cannot fix lvm.conf for simple memory volume!")
            return rc
    return 0


def setup_lv(config, the_dev, extent_size_fixed = None):
    devname = the_dev['devname']
    rc = 0

    vgname = 'dedupvg'
    lvname = 'deduplv'
    #pvcreate /dev/md3
    #vgcreate -s 1M dedupvg /dev/md3
    #vgs -o vg_free_count --noheadings dedupvg
    #lvcreate -l 154623 --contiguous y --zero n dedupvg -n deduplv

    #Backup first 1MB of underlying device
    cmd_str = 'dd if=%s of=/etc/ilio/gpt.backup bs=1M count=1' % devname
    rc = do_system(cmd_str)
    if rc != 0:
        return rc

    # Cleanup any GPT leftover
    cmd_str = 'dd if=/dev/zero of=%s bs=1M count=1' % devname
    rc = do_system(cmd_str)
    if rc != 0:
        return rc
    if config['volumeresources'][0]['volumetype'].upper() in [VDI_DISKBACKED]:
        return 0
    rc = fix_lvm_config(config)
    if rc != 0:
        return rc

    # Setup VG/LV
    # Make sure the lv start at 1MB of the underlying PV for backwork compatibility
    cmd_str = 'pvcreate -ff -y -v --dataalignment 512 --dataalignmentoffset 512 %s' % devname
    rc = do_system(cmd_str)
    if rc != 0:
        return rc
    cmd_str = 'pvs --units k -o +pv_all,pvseg_all %s' % devname
    rc = do_system(cmd_str)
    if rc != 0:
        return rc

    if extent_size_fixed != None:
        cmd_str = 'vgcreate -s %dk %s %s' % (extent_size_fixed, vgname, devname)
    else:
        cmd_str = 'vgcreate %s %s' % (vgname, devname)
    rc = do_system(cmd_str)
    if rc != 0:
        return rc

    out = ['']
    cmd_str = 'vgs -o vg_free_count --noheadings %s' % vgname
    rc = do_system(cmd_str, out)
    if rc != 0:
        return rc
    free_extents = int(out[0].split(' ')[-1])
    cmd_str = 'vgs -o vg_extent_size --noheadings --units=k --nosuffix %s' % vgname
    rc = do_system(cmd_str, out)
    if rc != 0:
        return rc
    extent_size = int(float(out[0].split(' ')[-1]))

    # Create the primary LV for dedupfs.
    if extent_size_fixed != None:
        # Upgrade from old non-LVM disk layout, need to keep the contiguous LV for upgrade.
        #cmd_str = 'lvcreate -l 100%FREE --contiguous y --zero n -n %s %s' % (lvname, vgname)
        cmd_str = 'lvcreate -l %d --contiguous y --zero n -n %s %s' % (free_extents, lvname, vgname)
    elif is_snapshot_enabled(config) != True:
        if is_infra_volume(config):
            # Reserve 80% space for infrastructure volume golden image,
            # 20% for the snapshot.
            free_extents = free_extents * 0.8
        cmd_str = 'lvcreate -l %d --contiguous y --zero n -n %s %s' % (free_extents, lvname, vgname)
    else:
        # Reserve for thinpool metadata
        metadata_size = int(free_extents * 0.001) * extent_size #KiB
        if metadata_size < THINPOOL_METADATA_SIZE:
            metadata_size = THINPOOL_METADATA_SIZE

        # FIXME: '--zero n' will cause the snapshots first 4k got zeroed latter, lvm bug?
        free_extents = free_extents - metadata_size/extent_size
        lvsize = free_extents * extent_size

        if is_infra_volume(config):
            # Since the SVM OS images are highly dedupable, Infrastructure volume try to provide a larger volume size
            # than the actual available storage.
            lvsize = lvsize * 2
        else:
            # Reserve disk space for snapshot
            lvsize = lvsize - int(milio_config.snapshot_space) * 1024 * 1024
            debug('original volume size is {size}G'.format(size=milio_config.original_volumesize))
            debug('created volume size is {size}G'.format(size=lvsize/1024/1024))
        cmd_str = 'lvcreate -V %dk -l %d --poolmetadatasize %dk --chunksize %dk -n %s --thinpool %s/%s' \
                % (lvsize, free_extents, metadata_size, LV_CHUNKSIZE, lvname, vgname, vgname + 'pool')

    rc = do_system(cmd_str)
    if rc != 0:
        return rc

    if is_snapshot_enabled(config):
        # Disable zeroing of thinpool, double the performance!
        cmd_str = 'lvchange -Z n dedupvg/dedupvgpool'
        rc = do_system(cmd_str)

    # Log the result partition table.
    cmd_str = 'lvs -a -o +seg_start_pe,seg_pe_ranges'
    rc = do_system(cmd_str)
    return rc

def restore_golden_image(setup_info):
    # Infrastructure volume
    config = setup_info['configure']

    golden_snap_id = 'dedup_golden_image_lv'

    cmd_str = '/opt/milio/bin/e2fsck -f -y -Z 0 /dev/dedupvg/deduplv'
    rc = do_system(cmd_str)
    if rc == 0:
        debug('dedupfs verified, skip restore from golden image.')
        return 0
    elif rc == 1:
        debug('dedupfs errors fixed, skip restore from golden image.')
        return 0

    debug('dedupfs corrupted, restore from golden image.')

    cmd_str = 'lvs|grep %s' % golden_snap_id
    rc = do_system(cmd_str)
    if rc != 0:
        debug('dedupfs golden image not found, skip restore.')
        return 0

    cmd_str = 'lvremove -f dedupvg/deduplv'
    rc = do_system(cmd_str)
    if rc != 0:
        debug('Failed to remove corrupted LV.')
        return rc

    if is_snapshot_enabled(config):
        cmd_str = 'lvcreate -s -l 100%%FREE -n deduplv dedupvg/%s' % golden_snap_id
    else:
        cmd_str = 'lvcreate -s -n deduplv dedupvg/%s' % golden_snap_id
    rc = do_system(cmd_str)
    return rc

def start_lv(setup_info):
    rc = 0
    config = setup_info['configure']
    if VDI_DISKBACKED in config['volumeresources'][0]['volumetype'].upper() and (not os.path.exists(UPGREP_VERSION) and is_new_simple_hybrid()):
        debug('is new simple hybrid skip start lv.')
        return 0

    if config['volumeresources'][0]['volumetype'].upper() in [VDI_DISKLESS, VDI_DISKBACKED]:

        # Simple memory volume's zram device doesn't support direct-IO, thin_check
        # will always fail, we must disable thin_check in this case.
        cmd_str = "sed -i -e 's/thin_check_executable.*/thin_check_executable = \"\"/'" + \
                " /etc/lvm/lvm.conf"
        rc = do_system(cmd_str)
        if rc != 0:
            debug("ERROR: Cannot fix lvm.conf for simple memory volume!")
            return rc

    # Run udevadm first.
    udev_trigger()

    rc = vgchange_active_sync('dedupvg')
    # cmd_str = 'vgchange -ay dedupvg'
    # for i in range(3):
    #     try:
    #         rc = do_system_timeout(cmd_str, 10)
    #     except timeout_error, e:
    #         rc = 1
    #     if rc == 0:
    #         break
    #     time.sleep(5)
    if rc != 0:
        return rc

    if is_infra_volume(config):
        rc = restore_golden_image(setup_info)

    if config['volumeresources'][0]['volumetype'].upper() == "SIMPLE_HYBRID":
        def find_next_dev(mapper,ma,mi):
            #dmsetup table | grep dedupvg-dedupvgpool-tpool | awk '// {print $5;} /thin-pool/ {print $6;}'
            d_n = mapper[mi]
            cmd="dmsetup table | grep %s | awk  '// {print $5;} /thin-pool/ {print $6;}' " % d_n
            (rc, msg) = runcmd(cmd)
            lines=msg.strip("\n").split("\n")
            (l_ma,l_mi) = lines[0].split(":")
            if l_ma == ma:
               dev = mapper[l_mi]
               if "tmeta" in dev and len(lines) == 2:
                   (l_ma,l_mi) = lines[1].split(":")
            return (d_n, l_ma,l_mi)


        deduplv_dev_name = None
        dedupvgpool_dev_name = None
        vscaler_name = None
        (dlv_ma,dlv_mi) = (0,0)
        (vgpool_ma,vgpool_mi) = (0,0)

        # Check major:minor of the deduplv
        cmd="dmsetup table | grep dedupvg-deduplv | awk  '{print $5;}'"
        (rc, msg) = runcmd(cmd)
        if msg == "":
            debug("WARNING: can't find deduplv! Possibly there is no lvm on volume")
            return rc
        else:
            (dlv_ma,dlv_mi) = msg.strip("\n").split(":")
            #cmd="awk 'BEGIN{flag=0; FS=" ";} { if ($1 ~/Block/) {flag++;} if( flag && $1 == " + ma + ") {print $2} }' < /proc/devices"
            cmd="awk 'BEGIN{flag=0;} {if ($1 ~/Block/) {flag++;} if(flag && $1 == %s) {print $2}}' < /proc/devices" % dlv_ma
            (rc, msg) = runcmd(cmd)
            deduplv_dev_name = msg.strip("\n")
            debug("deduplv starts on device: %s (%s:%s)" % (deduplv_dev_name, dlv_ma, dlv_mi) )
            if(deduplv_dev_name != "device-mapper"):
                debug("ERROR: deduplv is not on device-mapper")
                return 1

        # Find vscaler name
        cmd="dmsetup table | grep vscaler | awk  -F: '{print $1;}'"
        (rc, msg) = runcmd(cmd)
        if msg == "":
            debug("WARNING: vscaler not found")
            return 1
        else:
            vscaler_name=msg.strip("\n")
            debug("vscaler: %s" % vscaler_name)


        # Build mapped devices table
        cmd="ls -l /dev/mapper | grep dm- | awk '{print $11, $9;}' | sort -V | cut -c 7-"
        (rc, msg) = runcmd(cmd)
        mapper=msg.strip("\n").split("\n")
        dict = {}
        for s in mapper:
           (num, vl) = s.split(" ")
           dict[num] = vl
        mapper = dict

        str = "deduplv"
        vl = mapper[dlv_mi]

        (dev_name, a,i) = find_next_dev(mapper,dlv_ma,dlv_mi)
        str += " -> "+dev_name
        if is_snapshot_enabled( config ):
            if not ("dedupvgpool" in dev_name):
                debug("ERROR: deduplv is not on dedupvgpool")
                rc = 1

        counter = 0
        while a == dlv_ma:
            (dev_name, a,i) = find_next_dev(mapper,a,i)
            str += " -> "+dev_name
            counter += 1
            if counter > 10:
                break

        if dev_name != vscaler_name:
            debug("ERROR: deduplv is not on top of vscaler")

        if a.isdigit():
            cmd="awk 'BEGIN{flag=0;} {if ($1 ~/Block/) {flag++;} if(flag && $1 == %s) {print $2}}' < /proc/devices" % a
            (rc, msg) = runcmd(cmd)
            dev_name = msg.strip("\n")
            str += " -> "+dev_name

        debug("Device sequence: " + str)
        debug("Mapped device list:\n" + msg)

    return rc

def stop_lv():
    rc = 0
    cmd_str = 'vgchange -an dedupvg'
    rc = do_system(cmd_str)
    if rc != 0:
        return rc
    return rc


def init_devices(setup_info):
    ibd_dev_list = setup_info['ibd_dev_list']
    config = setup_info['configure']
    volume_type = config['volumeresources'][0]['volumetype']

    debug("During init_devices:" + json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    for the_dev in ibd_dev_list:
        # Tiered ADS need a journal/log partition in the memory disk.
        # We can only get disk size after we started ibd
        if volume_type.lower() == "hybrid_deprecate" and the_dev['storagetype'].lower() == "memory":
            #if get_disk_attr(the_ibd_dev) != 0:
            #    return 1
            logsize_m = TIERED_JOURNAL_SIZE # In MB, log size 2G.
            the_dev['logsize_m'] = logsize_m
        else:
            the_dev['logsize_m'] = DEFAULT_JOURNAL_SIZE

        rc = setup_lv(config, the_dev)
        if rc != 0:
            return rc
    setup_info['need_reinit'] = False
    return 0

def is_old_layout(setup_info):
    config = setup_info['configure']
    if config['usx']['agentversion'] < '2.3':
        debug('Old layout detected!')
        return True
    if 'attributes' in config['usx']:
        if 'upgradedfrom' in config['usx']['attributes']:
            if config['usx']['attributes']['upgradedfrom'] < '2.3':
                debug('Upgrade old layout detected!')
                return True
    #TODO: Add check of the actual device for "deduplv"
    debug('Latest layout detected!')
    return False

def is_vdi_volume(setup_info):
    try:
        volume_type = setup_info['configure']['volumeresources'][0]['volumetype']
        if volume_type.upper() in VDI_VOLUME_TYPES:
            return True
    except:
        pass
    return False


def layout_upgrade(setup_info):
    ibd_dev_list = setup_info['ibd_dev_list']
    config = setup_info['configure']
    volume_type = config['volumeresources'][0]['volumetype']

    if is_vdi_volume(setup_info):
            debug('Skip layout upgrade for VDI volumes.')
            return 0

    for the_dev in ibd_dev_list:
        if check_partition(the_dev) == True:
            # FIXME: We should add more check before try to perform upgrade.
            # Old layout detected, upgrade
            debug('Old layout detected, perform upgrade...')

            # We are using 16k extent size to overcome alignment issue during layout upgrade.
            # The GPT table in old layout has a 34 sector(17K) backup at the end of disk.
            # The new LV can use that space to make sure it include the whole dedupfs partition.
            rc = setup_lv(config, the_dev, 16)
            if rc == 0:
                debug('Layout upgrade done...')
            else:
                debug('Layout upgrade error!')
                return rc
        else:
            debug('Already the latest layout, skip upgrade...')
            continue
    return 0

def reinit_mem_device(setup_info):
    ibd_dev_list = setup_info['ibd_dev_list']
    config = setup_info['configure']
    volume_type = config['volumeresources'][0]['volumetype']
    setup_info['need_reinit'] = False

    if is_vdi_volume(setup_info): # skip re-partition vscaler device for VDI volumes
            debug('Skip reinit mem device for VDI volumes.')
            return 0

    for the_dev in ibd_dev_list:
        # Tiered ADS need a journal/log partition in the memory disk.
        # We can only get disk size after we started ibd
        if volume_type.lower() == "hybrid_deprecate" and the_dev['storagetype'].lower() == "memory":
            #if get_disk_attr(the_ibd_dev) != 0:
            #    return 1
            logsize_m = TIERED_JOURNAL_SIZE # In MB, log size 2G.
            the_dev['logsize_m'] = logsize_m
        else:
            the_dev['logsize_m'] = DEFAULT_JOURNAL_SIZE

        if volume_type.upper() not in ['MEMORY']:
            debug("Persistent device, skip validation.")
            continue

        if check_device(the_dev) == True:
            # Skip reinitialize if the memory device still have valid data.
            debug('Memory device validated, skip reinitialize.')
            continue

        debug('Memory device validate failed, data lost, perform reinitialize.')
        rc = setup_lv(config, the_dev)
        if rc != 0:
            debug('ERROR: Memory device reinitialize failed!')
            return rc
        debug('Memory device reinitialize done.')
        setup_info['need_reinit'] = True

        # May need reinit dedup fs.
    return 0


#
# Call pool node to remove LV for this ADS.
#
def destroy_local_devices(setup_info):
    #
    # retrieve all configuration info from AMC, a Json file
    #

#     configure_str = json.dumps(setup_info, indent=4, separators=(',', ': '))
#     debug("In destroy_local_devices: ----------------------- %s" % configure_str)

    adsname_str = setup_info['adsname']
    volume_type = setup_info['type']
    pools_conf = setup_info['vps']
    export_type = setup_info['export_type']

    if volume_type.lower() != 'hybrid':
        # for non-hybrid node, there is nothing to clear locally yet.
        debug('not hybrid ads node, no need to do local cleanup.')
        return 0
    else: #volume_type.lower() == 'hybrid'
        [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev] = ddp_setup.readmnttab(ddp_setup.mnttab)
        if vscaler_dev is None:
            debug('Vscaler should already been removed, could not find the vscaler cache device from ' + ddp_setup.mnttab)
            return 0

        if export_type.lower() == 'iscsi':

            # sometimes it takes a while to die
            cnt = 0
            iscsi_running = True
            while cnt < RETRYNUM:

                out = ['']
                cmd_str = 'pkill -TERM -f ' + ISCSI_DAEMON
                rc = do_system(cmd_str, out)
                if rc == 0:
                    iscsi_running = False
                    break
                else: # fail to stop the server
                    sub_cmd_str = 'pidof ' + ISCSI_DAEMON
                    sub_rc = do_system(sub_cmd_str)
                    if sub_rc == 1:
                        # the server process has been killed
                        iscsi_running = False
                        break

                time.sleep(1)
                cnt += 1

            if cnt >= RETRYNUM:
                debug('Retry kill iscsi server %d times, all failed' %cnt)
                return 4
        else:
            # Try to stop NFS export service
            rc = do_system(CMD_NFS_STOP)
            if rc != 0:
                rc = do_system(CMD_NFS_STATUS)
                if rc == 0:
                    debug('Cannot stop NFS service.')
                    return rc
                else:
                    debug('NFS already stopped.')
            # NFS already stopped, we can continue delete.

            # endof stop iscsi server for hybrid

        out = ['']
        cmd_str = CMD_UMOUNT + ' ' + mnt_point
        rc = do_system(cmd_str, out)
        if rc != 0:
            # if it is already umounted, ignore the error
            # Example: root@SuUbuntu12Ilio:/mnt/sdb/source/product/milio/atlas/roles/ads# umount /mnt/abc/
            #          umount: /mnt/abc/: not mounted
            msg = out[0]
            msgindex = msg.find('not mounted')
            if msgindex >= 0:
                # the device has been remove, ignore the error
                debug(ddp_setup.VSCALER_NAME + 'has been umounted, msg=' + msg)
            else:
                # some other error
                return rc

        time.sleep(5)

        out = ['']
        cmd_str = CMD_RMVSCALER + ' ' + ddp_setup.VSCALER_NAME
        rc = do_system(cmd_str, out)
        if rc != 0:
            # Ignore the case that vscaler has been removed
            # Example: su-ads-opt-31 testfile # dmsetup remove vmdata_cache2
            #          device-mapper: remove ioctl failed: No such device or address
            #          Command failed
            #        su-ads-opt-31 testfile # echo $?
            #          1

            msg = out[0]
            msgindex = msg.find('No such device or address')
            if msgindex >= 0:
                # the device has been remove, ignore the error
                debug(ddp_setup.VSCALER_NAME + 'has been removed, msg=' + msg)
            else:
                # some other error
                return rc

        out = ['']
        # use force option to destroy the device when it has dirty blocks
        cmd_str = CMD_DESTROYVSCALER + ' -f ' + vscaler_dev
        rc = do_system(cmd_str, out)
        if rc != 0:
            #Example1: a successful run
            #su-ads-opt-31 testfile # /opt/milio/scripts/vscaler_destroy /dev/nbd_su-ads-tier-37_importsmemory
            #/opt/milio/scripts/vscaler_destroy: Destroying VScaler found on /dev/nbd_su-ads-tier-37_importsmemory. Any data will be lost !!
            #su-ads-opt-31 testfile # echo $?
            #0
            #Example2: the vscaler has been destroyed
            #su-ads-opt-31 testfile # /opt/milio/scripts/vscaler_destroy /dev/nbd_su-ads-tier-37_importsmemory
            #/opt/milio/scripts/vscaler_destroy: No valid VScaler found on /dev/nbd_su-ads-tier-37_importsmemory
            #su-ads-opt-31 testfile # echo $?
            #1
            #Example2: no such device
            #su-ads-opt-31 testfile # /opt/milio/scripts/vscaler_destroy /dev/nbd_su-ads-tier-37_importsmemory1
            #Failed to open /dev/nbd_su-ads-tier-37_importsmemory1
            #su-ads-opt-31 testfile # echo $?
            #1
            msg = out[0]
            msgindex1 = msg.find('No valid VScaler found')
            msgindex2 = msg.find('Failed to open')
            if msgindex1 >= 0:
                # the device has been remove, ignore the error
                debug(vscaler_dev + 'has been destroyed, msg=' + msg)
            elif msgindex2 >= 0:
                # some other error
                debug('Vscaler memory device is disconnected, msg=' + msg)
            else:
                return rc
        return 0

        # end of vscaler destroy for hybrid ads node

#
# Call pool node to remove LV for this ADS.
#
def destroy_remote_devices(setup_info):
    #
    # retrieve all configuration info from AMC, a Json file
    #

    adsname_str = setup_info['adsname']
    pools_conf = setup_info['vps']
    volume_type = setup_info['type']

    ret = 0
    for pool in pools_conf:
        vgname = pool['configure']['vgname']
        size = str(pool['configure']['size'])
        pool_type = pool['configure']['v_storagetype']

        if 'memory' in pool_type.lower() or 'disk' in pool_type.lower():
            debug("Delete " + pool_type + " Import: " + vgname + " " + adsname_str)
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' ads_vp_destroy ' + vgname + ' ' + adsname_str
            ret = do_system(cmd_str)
            if ret != 0:
                debug('Cannot delete ' + pool_type + ' pool!')
                break
            continue

        # TODO: shared storage pool

    # If there is failure, ret is the last one
    return ret

def create_raid1(setup_info):

    config = setup_info['configure']
    volume_type = config['volumeresources'][0]['volumetype']

    #configure = setup_info['configure']
    #adstype = configure['type']
    #if adstype != "mem_persistent":
    #    return 0

    if volume_type.lower() != "mem_persistent":
        return 0

    ibd_dev_list = setup_info['ibd_dev_list']
    the_idx = md_next_available_idx(0)
    md_dev = "/dev/md" + str(the_idx)
    md_name = "atlas-" + md_dev
    cmd_str = CMD_MDADM + " --create --assume-clean --run --force --metadata=1.2 " + md_dev + " -N "\
        + md_name + " --level=raid1 --bitmap=internal --raid-devices=2 " + \
        ibd_dev_list[0]["datadev"] + ' ' + ibd_dev_list[1]["datadev"]
    rc = do_system(cmd_str)
    if rc != 0:
        debug("Could not create md.")
        return 1
    cmd_str = CMD_MDADM + " --zero-superblock " + md_dev
    rc = do_system(cmd_str)
    return 0

def raid1_start(setup_info):
    debug('Enter raid1_start ...')
    ibd_dev_list = setup_info['ibd_dev_list']
    the_idx = md_next_available_idx(0)
    md_dev = "/dev/md" + str(the_idx)

    mem_devname = "unknown"
    disk_devname = "unknown"
    for the_ibd in ibd_dev_list:
        if the_ibd['type'] == 'memory':
            mem_devname = the_ibd['datadev']
        else:
            disk_devname = the_ibd['datadev']
    debug('mem_devname:' + mem_devname + ' ' + 'disk_devname:' + disk_devname)

    assemble_with_mem = False
    cmd_str = CMD_MDADM + ' --examine ' + mem_devname
    rc = do_system(cmd_str)
    if rc == 0:
        assemble_with_mem = True

    cmd_str = CMD_MDASSEMBLE + ' --force ' + md_dev + ' ' + disk_devname
    if assemble_with_mem == True:
        cmd_str = cmd_str + ' ' + mem_devname
    rc = do_system(cmd_str)
    if rc == 0:
        if assemble_with_mem == False:
            rc = md_re_add(md_dev, mem_devname)
            #if rc != 0:
                #TODO

    #else:
        # TODO
    return 0

def vv_up(setup_info):

    volume_type = setup_info['type']
    rc = 0
    if volume_type.lower() == "mem_persistent":
        rc = raid1_start(setup_info)
    return rc

def vv_export(setup_info, first, ha):
    config = setup_info['configure']
    adsname_str = setup_info['adsname']
    pools_conf = setup_info['vps']
    volume_type = setup_info['type']
    journaled = setup_info['journaled']

    ibd_dev_list = setup_info['ibd_dev_list']
    #configure = setup_info['configure']
    need_reinit = setup_info['need_reinit']
    #adstype = configure['type']
    #journaled = configure['journaled']
    log_dev = None
    log_size = DEFAULT_JOURNAL_SIZE

    debug(json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': ')))
    if volume_type.lower() == "hybrid":
        if len(ibd_dev_list) != 2:
            debug('ERROR: ibd_dev_list for hybrid ADS: %s ' % str(ibd_dev_list))
            return 1
        if ibd_dev_list[0]["pool_type"].lower() == "memory": #or ibd_dev_list[0]["pool_type"].lower() == "memory_pool":
            vscaler_dev = ibd_dev_list[0]["uuid_path"]
            #log_dev = ibd_dev_list[0]["log_uuid_path"]
            log_dev = ibd_dev_list[0]["logdev"]
            if ibd_dev_list[0].has_key('logsize_m'):
                log_size = ibd_dev_list[0]["logsize_m"]
            ddp_dev = ibd_dev_list[1]["uuid_path"]
            ddp_dev_type = ibd_dev_list[1]["pool_type"]
        else:
            vscaler_dev = ibd_dev_list[1]["uuid_path"]
            #log_dev = ibd_dev_list[1]["log_uuid_path"]
            log_dev = ibd_dev_list[1]["logdev"]
            if ibd_dev_list[1].has_key('logsize_m'):
                log_size = ibd_dev_list[1]["logsize_m"]
            ddp_dev = ibd_dev_list[0]["uuid_path"]
            ddp_dev_type = ibd_dev_list[0]["pool_type"]

        debug("========vscaler: %s | log_Dev: %s | ddp_dev: %s | ddp_dev_type: %s" % (vscaler_dev, log_dev, ddp_dev, ddp_dev_type))

    else:
        vscaler_dev = None
        ddp_dev = ibd_dev_list[0]["uuid_path"]
        ddp_dev_type = ibd_dev_list[0]["pool_type"]

    if volume_type.lower() == "mem_persistent":
        ibd_dev_list = setup_info['ibd_dev_list']
        ibd_dev = ibd_dev_list[0]["datadev"]
        cmd_str = "mdadm --examine " + ibd_dev + '|grep \"Array UUID\"'
        out_stream = os.popen(cmd_str, 'r', 1)
        uuid_list = out_stream.read().split(' ')
        md_devname = uuid_list[len(uuid_list) - 1]
        md_devname = "/dev/disk/by-id/md-uuid-" + md_devname.split('\n')[0]
        print md_devname
        debug("md device: ", ibd_to_md(ibd_dev))
        ddp_dev = md_devname
        ddp_dev_type = ibd_dev_list[0]["pool_type"]
        #need to wait before call ddp...
        while (True):
            if (os.path.exists(md_devname)):
                break
            debug("Waiting md...", md_devname)
            time.sleep(0.1)

    #ddp_setup may change our stdout /stderr...
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    ddp_setup.ddp_prepare_update()

    mnt_point = "/exports/" + adsname_str.split('_')[-1] # construct mount point for dedup fs
#     debug("YJ:vv_export: mnt_poiont : %s" % mnt_point)

    if need_reinit == True and (volume_type.lower() == 'hybrid' or volume_type.lower() == 'memory'):
        is_hybrid = False
        if volume_type.lower() == "hybrid": # FIXME: Should use pool_type here?
            is_hybrid = True
        debug("Calling ddp_setup.reset_ddp with ddp_dev:",  ddp_dev, ' log_dev:', log_dev)
        rc = ddp_setup.reset_ddp(ddp_dev, vscaler_dev, log_dev, log_size, is_hybrid)

    if first == True:
        debug("Calling ddp_setup.config_ddp with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        rc = ddp_setup.config_ddp(mnt_point, ddp_dev, vscaler_dev, log_dev, log_size, ha)
    else:
        debug("Calling ddp_setup.ddp_update_device_list with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        rc = ddp_setup.ddp_update_device_list(mnt_point, ddp_dev, vscaler_dev, log_dev)

    if ha == 0:
        debug("None HA, Start DDP by calling ddp_setup.init_ddp with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        rc = ddp_setup.init_ddp(mnt_point, ddp_dev, vscaler_dev, log_dev, ha)
    if rc == None:    #Handle 'None' return value
        rc = 0
    if rc != 0:
        debug("ddp_setup initialization failed with: ", rc)

    #Restore original stdout / stderr
    sys.stdout = old_stdout
    sys.stderr = old_stderr
    return rc

def vv_stop_vp(adsname):
    rc = 0
    vv_setup_info = {}
    vp_setup_info = {}

    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    rc = vv_get_resource(adsname, vv_setup_info)
    if rc != 0:
        return rc

    rc = vp_get_configuration(vv_setup_info, vp_setup_info)
    if rc != 0:
        debug("get vp configuration failed!")
        return 1

    adsname_str = vp_setup_info['adsname']
    pools_conf = vp_setup_info['vps']

#     configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#     debug("vv_stop_vp: vp_get_configuration: vp_setup_info----------------------- %s" % configure_str)

    for pool in pools_conf:
        vgname = pool['configure']['vgname']
        size = str(pool['configure']['size'])
        pool_type = pool['configure']['v_storagetype']
        if 'memory' in pool_type.lower() or 'disk' in pool_type.lower():
            debug(pool_type + ' import: ' + vgname + ' ' + adsname_str)
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' ads_vp_stop ' + vgname + ' ' + adsname
            rc = do_system(cmd_str)
            if rc != 0:
                debug('Cannot stop ' + pool_type + ' pool!')
                break
    return rc

#
# Stop all IBD devices in "/tmp/<adsname>.devlist
# Also any affected MDs.
#
def vv_stop(arg_list):
    adsname = arg_list[2]
    ads_setup_info = {}

    debug("Enter vv_stop: stoping %s ..." % adsname)

    # TODO: need to get export type first to decide stop either ISCSI or NFS
    cnt = 0
    while cnt < RETRYNUM:
        out = ['']
        cmd_str = 'pkill -TERM -f ' + ISCSI_DAEMON
        rc = do_system(cmd_str, out)
        if rc == 0:
            break
        else: # fail to stop the server
            sub_cmd_str = 'pidof ' + ISCSI_DAEMON
            sub_rc = do_system(sub_cmd_str)
            if sub_rc == 1:
                # the server process has been killed
                break
        time.sleep(1)
        cnt += 1
    if cnt >= RETRYNUM:
        debug('Retry kill iscsi server %d times, all failed' %cnt)

    # stop NFS service
    cmd_str = "service nfs-kernel-server stop"
    rc = do_system(cmd_str)
    if rc != 0:
        debug("stop nfs service failed!")
    time.sleep(5)

    [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev] = ddp_setup.readmnttab(ddp_setup.mnttab)
    # RV Fixme: mnttab is not in sync when another resource failover to this volume (assuming this volume already
    # failed over to the standby node and now it is a standby node)
    #  MOUNTPOINT=$(echo $OCF_RESKEY_directory | sed 's/\/*$//')/

    my_list = [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev]
    debug(my_list)
    cnt = 0
    if mnt_point != None and os.path.exists(mnt_point):
        while cnt < RETRYNUM and os.path.exists(mnt_point):
            cmd_str = CMD_UMOUNT + " " + mnt_point
            out = ['']
            rc = do_system(cmd_str, out)
            if rc == 0:
                break
            elif "not mounted" in out[0]:
                debug("WARNING: %s not mounted" %(mnt_point))
                break

            if cnt == 0:
                debug("WARNING: umount failed: %s" % cmd_str)
            time.sleep(30)
            cnt += 1
    if cnt >= RETRYNUM:
        debug("ERROR: retry umount failed %d times, all failed" %cnt)
        return 1

    if cache_name and cache_name.lower() == ddp_setup.VSCALER_NAME.lower(): # vscaler cache exist, this is a hybrid volume
        out = ['']
        cmd_str = CMD_RMVSCALER + ' ' + ddp_setup.VSCALER_NAME
        rc = do_system(cmd_str, out)
        if rc != 0:
            # Ignore the case that vscaler has been removed
            # Example: su-ads-opt-31 testfile # dmsetup remove vmdata_cache2
            #          device-mapper: remove ioctl failed: No such device or address
            #          Command failed
            #        su-ads-opt-31 testfile # echo $?
            #          1

            msg = out[0]
            msgindex = msg.find('No such device or address')
            if msgindex >= 0:
                # the device has been remove, ignore the error
                debug(ddp_setup.VSCALER_NAME + 'has been removed, msg=' + msg)
            else:
                # some other error
                return rc
        time.sleep(5)

    ibd_list = load_ibd_list(adsname)
    if ibd_list == None:
        # Give a warning message instead of return NO_DEV_TO_STOP
        debug("WARNING: no devlist for %s." % adsname)

    # Must stop arbitrator before disconnect ibd.
    arb_stop(adsname)
    rc = vv_stop_vp(adsname)
    if rc != 0:
        return rc
    remove_ibd_list(adsname)
    debug("========vv_stop=======: completed.")
    return 0

def get_node_dict_from_AMC(uuid):
    amcfile = "/usxmanager/usx/inventory/volume/containers/" + uuid + '?composite=true'
    conn = httplib.HTTPConnection("127.0.0.1:8080")
    conn.request("GET", amcfile)
    res = conn.getresponse()
    data = res.read()
    node_dict = json.loads(data)
    if node_dict is None:
        debug('Error getting Node dictionary information from AMC')
        return None
    #debug('corosync_config node dict JSON: ', json.dumps(node_dict, sort_keys=True, indent=4, separators=(',', ': ')))
    node_dict = node_dict.get('data')
    if node_dict is None:
        debug('Error getting Ilio data from AMC')
        return None
    return node_dict

def runcmd_nonblock(cmd, print_ret=False):
    if print_ret:
        debug('Running: %s' % cmd)
    try:
        tmpfile = tempfile.TemporaryFile()
        p = Popen(
            [cmd],
            stdin=PIPE,
            stdout=tmpfile,
            stderr=STDOUT,
            shell=True,
            close_fds=True,
            )
        time.sleep(5)
        if p.poll() is None:
            return 1
	return 0
    except OSError:
        debug('Exception with Popen')
        return -1

def local_device_is_accessible(dev):
    #cmd = 'dd if=' + dev + ' of=/dev/null bs=512 count=1 iflag=direct,nonblock'
    ioping_cmd = '/usr/bin/ioping'
    if os.path.exists('/usr/bin/ioping'):
        ioping_cmd = '/usr/bin/ioping'
    elif os.path.exists('/usr/local/bin/ioping'):
        ioping_cmd = '/usr/local/bin/ioping'
    else:
        debug('ERROR: cannot find ioping')
    cmd = ioping_cmd + ' -A -D -c 1 -s 512 ' + dev
    ret = runcmd_nonblock(cmd, print_ret=True)
    check_file = '/tmp/local_storage_check_error'
    if ret != 0:
        if not os.path.isfile(check_file):
            ret = runcmd_nonblock(cmd, print_ret=True)
            if ret != 0:
                runcmd('touch ' + check_file, print_ret=True)
                # return True now to give this check one more try
                return True
        else:
            return False
    if os.path.isfile(check_file):
        runcmd('rm -f ' + check_file,print_ret=True)
    return True

def shared_device_is_accessible(dev):
    #cmd = 'dd if=' + dev + ' of=/dev/null bs=512 count=1 iflag=direct,nonblock'
    ioping_cmd = '/usr/bin/ioping'
    if os.path.exists('/usr/bin/ioping'):
        ioping_cmd = '/usr/bin/ioping'
    elif os.path.exists('/usr/local/bin/ioping'):
        ioping_cmd = '/usr/local/bin/ioping'
    else:
        debug('ERROR: cannot find ioping')

    cmd = ioping_cmd + ' -A -D -c 1 -s 512 ' + dev
    ret = runcmd_nonblock(cmd, print_ret=True)
    check_file = '/tmp/shared_storage_check_error'
    if ret != 0:
        time.sleep(15)
        ret = runcmd_nonblock(cmd, print_ret=True)
        if ret == 0:
            return True
        elif not os.path.isfile(check_file):
            runcmd('touch ' + check_file, print_ret=True)
            # return True now to give this check one more try
            return True
        else:
            return False
    if os.path.isfile(check_file):
        runcmd('rm -f ' + check_file,print_ret=True)
    return True

def get_shared_storage_status(node_dict):
    virtualvols = node_dict.get('volumeresources')
    if virtualvols is None:
        debug('Error getting Virtual Volume Resources')
        return 1
    for vv in virtualvols:
        for raidplan in vv.get("raidplans"):
            for sharedstorage in raidplan.get("sharedstorages"):
                if sharedstorage.get("storagetype") == "DISK" or sharedstorage.get("storagetype") == "FLASH":
                    dev = scsi_to_device(sharedstorage.get("scsibus"))
                    #if dev is None or not shared_device_is_accessible(dev):
                    # NOT check shared_device_is_accessible(dev) to reduce false alarm, and then reduce reboot
                    if dev is None:
                        return 1
    return 0

def get_local_storage_status():
    cmd = 'df -P / | tail -n 1 | awk \'/.*/ { print $1 }\''
    (ret,msg) = runcmd(cmd,print_ret=True,lines=True)
    for dev in msg:
        if not local_device_is_accessible(dev):
            return 1
    return 0

#
# Check Virtual volume health status
# Check IBDs, and if exist, check MDs too.
# Return none-zero on any error.
#
def vv_status(arg_list):

    # disable check local storage status to avoid ioping overhead, see TISILIO-7467
    #rc = get_local_storage_status()
    #if rc != 0:
    #    # Can we still write to local disk??
    #    debug("Local storage inaccessible")
    #reset_vm('root_disk_reset')

    adsname = arg_list[2]

    ibd_list = load_ibd_list(adsname)
    if ibd_list == None:
        debug('Volume resource %s is not started on this node.' % adsname)
        return 1

    # Always return success if the vol resource has been started on this node.
    # Skip checking the ibd status.
    debug('Volume resource %s is already started on this node.' % adsname)
    return 0

    bad_ibds = check_all_ibd(ibd_list)
    if bad_ibds != None:
        debug("Failed IBDs:",bad_ibds)
        return 1

    # check shared storage status
    cfgfile = open(VV_CFG, 'r')
    s = cfgfile.read()
    cfgfile.close()
    node_dict = json.loads(s)
    if node_dict is None:
        debug('Error getting Node data from local atlas json')
        return 1
    '''
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        debug('Error getting Ilio data from local atlas json')
        return 1
    uuid = ilio_dict.get('uuid')
    node_dict = get_node_dict_from_AMC(uuid)
    if node_dict is None:
	return 1
    '''

    volres = node_dict.get('volumeresources')
    resuuid = None
    if volres != None and len(volres) > 0:
        resuuid = volres[0].get('uuid')
    # skip checking shared storage status if the existing atlas.json has not been updated yet
    if resuuid == None or resuuid != adsname:
        debug('vv_status: volume uuid did not match, skip')
        return 0

    rc = get_shared_storage_status(node_dict)
    if rc != 0:
        debug("Shared storage inaccessible")
        if not os.path.isfile(SHARED_STORAGE_DOWN):
            fd = open(SHARED_STORAGE_DOWN, 'a')
	    fd.flush()
	    os.fsync(fd)
	    fd.close()
	    reset_vm('shared_storage_down_reset')
        else:
            return rc
    else:
        if os.path.isfile(SHARED_STORAGE_DOWN):
            (ret, msg) = runcmd('rm -f ' + SHARED_STORAGE_DOWN, print_ret=False)

    # FIXME: this check are currently totally skipped.
    rc = ha_util.ha_check_ibd_status()
    if rc != 0:
        debug('ERROR ibd status is wrong')
        return 1

    return 0


def usx_status(arg_list):
    debug("Enter usx_status %s ..." % arg_list[2])
    # disable check local storage status to avoid ioping overhead, see TISILIO-7467
    #rc = get_local_storage_status()
    #if rc != 0:
    #    # Can we still write to local disk??
    #    debug("Local storage inaccessible")
    #    fd = open(LOCAL_STORAGE_DOWN_RESET, 'a')
    #    fd.flush()
    #    os.fsync(fd)
    #    fd.close()
    #    (ret, msg) = runcmd('echo c > /proc/sysrq-trigger', print_ret=False)

    adsname = arg_list[2]

    ibd_list = load_ibd_list(adsname)
    if ibd_list == None:
        return 1
    bad_ibds = check_all_ibd(ibd_list)
    if bad_ibds != None:
        debug("Failed IBDs:",bad_ibds)
        return 1

    # check shared storage status
    cfgfile = open(VV_CFG, 'r')
    s = cfgfile.read()
    cfgfile.close()
    node_dict = json.loads(s)
    if node_dict is None:
        debug('Error getting Node data from local atlas json')
        return 1
    '''
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        debug('Error getting Ilio data from local atlas json')
        return 1
    uuid = ilio_dict.get('uuid')
    node_dict = get_node_dict_from_AMC(uuid)
    if node_dict is None:
	return 1
    '''

    volres = node_dict.get('volumeresources')
    resuuid = None
    if volres != None and len(volres) > 0:
        resuuid = volres[0].get('uuid')
    # skip checking shared storage status if the existing atlas.json has not been updated yet
    if resuuid == None or resuuid != adsname:
        debug('vv_status: volume uuid did not match, skip')
        return 0

    rc = get_shared_storage_status(node_dict)
    if rc != 0:
        debug("Shared storage inaccessible")
        if not os.path.isfile(SHARED_STORAGE_DOWN):
            fd = open(SHARED_STORAGE_DOWN, 'a')
	    fd.flush()
	    os.fsync(fd)
	    fd.close()
	    reset_vm('shared_storage_down_reset')
        else:
            return rc
    else:
        if os.path.isfile(SHARED_STORAGE_DOWN):
            (ret, msg) = runcmd('rm -f ' + SHARED_STORAGE_DOWN, print_ret=False)

    rc = ha_util.ha_check_ibd_status()
    if rc != 0:
        debug('ERROR ibd status is wrong')
        return 1

    rc = ha_util.ha_check_status();
    if rc != 0:
        debug('ERROR during ha_check_status')
        return 1

    debug("INFO: usx_status: successful")
    return 0


def vv_init(arg_list):
    """
    Initialize virtual volume; for both HA & Non-HA
    """
    debug("Enter vv_init ...")
    vv_setup_info = {}
    vp_setup_info = {}
    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    configure = vv_setup_info['configure']
    if not configure['volumeresources']: # no resources; by definition it is an HA standby node
        debug("HA standby node; do nothing...")
        return 0

    ha = 0
    if configure['usx']['ha'] == True:
        ha = 1

    # remove ibd agent configuration file for ads_init
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass

    rc = vp_setup_configuration(vv_setup_info, vp_setup_info)
    if rc != 0:
        debug("vp_setup_configuration failed!")
        return 1

    rc = load_pools(vp_setup_info)
    if rc != 0:
        debug("load_pools failed!")
        return 1
#     configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#     debug("vp_setup_info after setup pool: ********************** %s" % configure_str)

    rc = load_devices(vp_setup_info, init=1)
    if rc != 0:
        debug("load_devices failed!")
        return 1
#     configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#     debug("----------------------- %s" % configure_str)

    create_raid1(vp_setup_info)
    rc = vv_export(vp_setup_info, True, ha)    # non-ha


    #rc = 0 # DELETE AFTER TEST!!!!!!!

    if rc == 0 : # vv_export successful
    # Now calculate vp metrics and update tag metircs, update agg export virtual pool uuid list
        #configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
        #debug("After Export vp_setup_info----------------------- %s" % configure_str)
        #cfg_file = open('./vv_init.json', 'w')
        #cfg_file.write(configure_str)
        #cfg_file.close()

        #vp_setup_info={}
        #cfg_file = open('./vv_init.json', 'r')
        #cfg_str = cfg_file.read()
        #cfg_file.close()
        #vp_setup_info = json.loads(cfg_str)


        # post imports to virtual volume resources
#         configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#         debug("YJ AFTER INIT ----------------------- %s" % configure_str)
        rc1 = vv_update_conf(vp_setup_info)
        if rc1 != 0:
            debug("ERROR : Update configurations to AMC failed!")
            return rc1

    return rc
'''
def _send_alert_ha(adsname):
	cmd = 'date +%s'
	(ret, epoch_time) = runcmd(cmd, print_ret=True)
	epoch_time = epoch_time.rstrip('\n')
	cfgfile = open(VV_CFG, 'r')
	s = cfgfile.read()
	cfgfile.close()
	node_dict = json.loads(s)
	usx = node_dict.get('usx')
	usx_displayname = usx.get('displayname')

	ad = {
		"uuid"			:"",
		"checkId"		:"",
		"usxuuid"		:"",
		"value"			:0.0,
		"target"		:"",
		"warn"			:0.0,
		"error"			:0.0,
		"oldStatus"		:"OK",
		"status"		:"OK",
		"description"		:"Start HA Failover",
		"service"		:"HA",
		"alertTimestamp"	:"",
		"iliotype"		:"VOLUME"
	}

	ad["uuid"] = adsname + '-ha-alert-' + str(epoch_time)
	ad["checkId"] = adsname + '-ha'
	ad["usxuuid"] = adsname
	ad["displayname"] = usx_displayname
	ad["target"] = "servers." + adsname + ".ha"
	ad["alertTimestamp"] = epoch_time

	data = json.dumps(ad)
	cmd = 'curl -X POST -H "Content-type:application/json" ' + LOCAL_AGENT + 'alerts/ -d \'' + data + '\''
	(ret, out) = runcmd(cmd, print_ret=True, block=False)
	(ret, out) = runcmd('touch /run/start_ha_alert', print_ret=True, block=False)
'''

#
# Move the shared storage to HA node.
#
def move_shared_storage(vv_uuid, ha_uuid, amcurl):
    # generate the input json for moving shared storage
    input_json = {}
    input_json["volumeresourceuuid"] = vv_uuid
    input_json["hailiouuid"] = ha_uuid
    debug("move_shared_storage: " + json.dumps(input_json, sort_keys=True, indent=4, separators=(',', ': ')))

    # move shared storage via USX 2.0 REST API
    ss_url = '/usx/deploy/move/disk?api_key=' + ha_uuid
    conn = urllib2.Request(amcurl + ss_url)
    debug("move_shared_storage: " + amcurl + ss_url)
    conn.add_header('Content-type','application/json')
    res = urllib2.urlopen(conn, json.dumps(input_json))
    debug('POST returned response code: ' + str(res.code))
    res.close()

    if str(res.code) == "200":
        debug("INFO: completed moving shared storage.")
        return 0
    else:
        debug("ERROR: failed to move shared storage.")
        return 1

def send_ha_failover_status(adsname_str, status):
    stats = {}
    stats['HA_FAILOVER_STATUS'] = long(status)
    return ha_util.send_volume_availability_status(adsname_str, stats, "VOLUME_RESOURCE")


def send_volume_status(adsname_str, status):
    stats = {}
    stats['VOLUME_SERVICE_STATUS'] = long(status)
    return ha_util.send_volume_availability_status(adsname_str, stats, "VOLUME_RESOURCE")


def vv_ha_start(arg_list):
    debug("Enter vv_ha_start ...")
    ha_setup_info = {}
    vp_setup_info = {}
    adsname_str = arg_list[2]
    #set_res_loc_preference(adsname_str)

    # TODO: a workaround for TISILIO-3933: a timing issue for rw upgrade failure.
    time.sleep(20)

    # remvoe ibd agent configuration file
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass

    jobid_file_exists = does_jobid_file_exist(True)
    does_jobid_file_need_deletion = not jobid_file_exists
    send_status("HA",  0, 0, "VOLUME HA", "Starting Volume HA takeover", False)

    rc = load_conf(VV_CFG, ha_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "VOLUME HA", "Failed to load conf", does_jobid_file_need_deletion)
        return rc
    haconfigure = ha_setup_info['configure']

    #
    # retrieve all aggregator info from AMC, a Json file
    #
    # Get json from localhost agent.
    #json_fname = get_conf_from_amc('http://127.0.0.1:8080/amc', adsname_str)
    #rc = parse_conf_from_amc(json_fname, ads_setup_info)

    rc = vv_get_resource(adsname_str, ha_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "VOLUME HA", "Failed to get resource from AMC", does_jobid_file_need_deletion)
        return 1

    rc = vp_get_configuration(ha_setup_info, vp_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "VOLUME HA", "Failed to get vp conf from AMC", does_jobid_file_need_deletion)
        return 1

    configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
    debug("After get vpconfig:  vp_setup_info----------------------- %s" % configure_str)
    debug("ha_setup_info: " + json.dumps(ha_setup_info, indent=4, separators=(',', ': ')))

    ss_flag = False
    if ha_setup_info['configure'].has_key('volumeresources'):
        for the_resource in ha_setup_info['configure']['volumeresources']:
            if the_resource.has_key('sharedstorages'):
                for the_ss in the_resource['sharedstorages']:
                    if the_ss.has_key('scsibus'):
                        ss_flag = True
                        break

    if ss_flag == True:
        configure = ha_setup_info['configure']
        vv_uuid = adsname_str
        ha_uuid =  configure['usx']['uuid']
        amcurl = configure['usx']['usxmanagerurl']
        ret = move_shared_storage(vv_uuid, ha_uuid, amcurl)
        if ret != 0:
            return 1

    #load_pools(ads_setup_info)
    #
    # start the vg based on json_fname
    #
    send_status("HA",  25, 0, "VOLUME HA", "Loading devices...", False)
    rc = load_devices(vp_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "VOLUME HA", "Failed to load devices", does_jobid_file_need_deletion)
        debug("load_devices failed!")
        return 1

    send_status("HA",  50, 0, "VOLUME HA", "Setting up Volume...", False)
    rc = vv_up(vp_setup_info)
    send_status("HA",  75, 0, "VOLUME HA", "Exporting Volume...", False)
    rc = vv_export(vp_setup_info, False, 1)    # non-ha
    send_status("HA",  100, 0, "VOLUME HA", "Volume completed failover", does_jobid_file_need_deletion)
    return 0


def vv_local_destroy(arg_list):
    debug("Enter vv_local_destroy ...")
    vv_setup_info = {}
    vp_setup_info = {}
    adsname_str = arg_list[2]

    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    rc = vv_get_resource(adsname_str, vv_setup_info)
    if rc != 0:
        return rc

    rc = vp_get_configuration(vv_setup_info, vp_setup_info)
    if rc != 0:
        debug("get vp configuration failed!")
        return rc

    rc = destroy_local_devices(vp_setup_info)
    if rc != 0:
        debug("destroy_local_devices failed!")
        return rc

    return 0


def vv_remote_destroy(arg_list):
    debug("Enter vv_remote_destroy ...")
    vv_setup_info = {}
    vp_setup_info = {}
    adsname_str = arg_list[2]

    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    rc = vv_get_resource(adsname_str, vv_setup_info)
    if rc != 0:
        return rc

    rc = vp_get_configuration(vv_setup_info, vp_setup_info)
    if rc != 0:
        debug("get vp configuration failed!")
        return rc

    rc = destroy_remote_devices(vp_setup_info)
    if rc != 0:
        debug("destroy_remote_devices failed!")
        return rc

    return 0


def vv_start(arg_list):
    """
    Virtual volume start sequence for configured virtual volume
    """
    debug("Enter vv_start ...")
    vv_setup_info = {}
    vp_setup_info = {}

    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    configure = vv_setup_info['configure']

    # remvoe ibd agent configuration file
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass
    #
    # if i'm a ha node, do nothing here, let ha start it
    #
    if configure['usx']['ha'] == True:
        debug("vv_start: this is an ha node, let ha start this virtual volume")
        return 0

    rc = vp_get_configuration(vv_setup_info, vp_setup_info)
    if rc != 0:
        debug("get vp configuration failed!")
        return 1

#     configure_str = json.dumps(vp_setup_info, indent=4, separators=(',', ': '))
#     debug("AFTER GET_VP_Conf----------------------- %s" % configure_str)

    rc = load_devices(vp_setup_info)
    if rc != 0:
        debug("load_devices failed!")
        return 1
    rc = vv_up(vp_setup_info)
    rc = vv_export(vp_setup_info, False, 0) # non-ha

    return rc
#
# Create sym link
#
def create_link(devpath, linkname):
    SYMLINK_DIR = '/dev/usx/'
    linkpath = SYMLINK_DIR + linkname
    if linkpath == devpath:
        debug("INFO: devicepath equals to link path, skip symlink.")
        return
    if not os.path.isdir(SYMLINK_DIR):
        do_system("/bin/mkdir " + SYMLINK_DIR)
    if os.path.islink(linkpath):
        do_system("rm " + linkpath)
    cmd_str = "/bin/ln -s " + devpath + " " + linkpath
    do_system(cmd_str)
    return

"""
Creates external Journal partition from the data device
Saves the resulting journal partition's name at deviceoptions list
This method is called for Simple Hybrid Volume Types
@raidbricks	List from /etc/ilio/atlas.json under
		[volumeresources][0][raidplans][0][plandetail][subplans]
		This list can be either raidbricks or sharedstorages
"""
def vdi_carve_ext_jbd_subdev(raidbricks):
    debug('Entering vdi_carve_ext_jbd_subdev...')
    global devopt
    for raidbrick in raidbricks:
        subdevices = raidbrick['subdevices']
        for subdevice in subdevices:
            if subdevice['uuid'] == raidbrick['cachedevuuid']:
                continue
            the_dev = {}
            the_dev['need_log_dev'] = True
            the_dev['logsize_m'] = DEFAULT_JOURNAL_SIZE
            the_dev['devicepath'] = scsi_to_device(subdevice['scsibus'])
            if setup_partitions(the_dev) != 0:
                debug("setup_partitions returned error")
                return 1
            logdev_dict = {}
            logdev_dict['logdev_name'] = the_dev['log_dev']
            logdev_dict['logdev_size'] = the_dev['logsize_m']
            subdevice['devicepath'] = the_dev['data_dev']
            create_link(subdevice['devicepath'], subdevice['uuid'])
            datadev_name = os.readlink('/dev/usx/' + subdevice['uuid'])
	    for key, value in logdev_dict.items():
                newopt = key+'='+str(value)
                if newopt not in raidbrick['deviceoptions']:
                    raidbrick['deviceoptions'].append(newopt)
            devopt = raidbrick['deviceoptions']
    return 0

"""
USX 2.0 RAID Planner enhancement
  volume init/start/stop
"""

def vol_load_devices(setup_info, init = 0, ha_uuid = None, amcurl = None):
    """
    Set up devices for volume:
     init = 1 : call cp-load vv_init
     init = 0 : call cp-load vv_start
    """
    debug("Entering vol_load_devices ...")

    need_ext_jbd = False
    config = setup_info['configure']

    if config['volumeresources'][0].has_key('need_ext_jbd'):
        need_ext_jbd = config['volumeresources'][0]['need_ext_jbd']
    vr_uuid = config['volumeresources'][0]['uuid'] # TODO::Support multiple resources per container
    volume_type = config['volumeresources'][0]['volumetype']
    raid_plans = config['volumeresources'][0]['raidplans']
    #TODO This uuid generation should move to AMC, keeping sync with rest
    #of UUID creation design. Once that is done, we can remove all this
    #code specific to VDI_DISKBACKED and also remove import for uuid module
    if volume_type.upper() == VDI_DISKBACKED and init == 1 and need_ext_jbd:
        config['volumeresources'][0]['logdev_uuid'] = str(uuid.uuid4())
        for raidplan in raid_plans:
            plandetail = json.loads(raidplan['plandetail'])
            for subplan in plandetail['subplans']:
                if subplan.has_key('raidbricks'):
                    if vdi_carve_ext_jbd_subdev(subplan['raidbricks']):
                        return 1
                if subplan.has_key('sharedstorages'):
                    if vdi_carve_ext_jbd_subdev(subplan['sharedstorages']):
                        return 1
            raidplan['plandetail'] = json.dumps(plandetail)
        data = json.dumps(config, sort_keys=True, indent=4, separators=(',', ': '))
        try:
            cfg_file = open(VV_CFG, 'w', 0)
            cfg_file.write(data)
            cfg_file.close()
        except:
            debug("Could not write to /etc/ilio/atlas.json after assigning \
            UUID for the log device")
            debug(traceback.format_exc())
            return 1

    setup_info['ibd_dev_list'] = []
    dev_list = setup_info['ibd_dev_list']

    devname_str = ''
    next_ibd_idx = 0
    rc = 0

    device_nr = 0

    if init == 1:
        # Check if it s a VDI volume
        if is_vdi_volume(setup_info):
            debug("Configure for VDI")
            #TODO: call compound device script
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vdi_init'
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : cp-load.py vdi_init failed!")
                return rc
        else: # USX volumes
            # call cp-load vv_init to setup devices based on RAID planner
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vv_init'
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : cp-load.py vv_init failed!")
                return rc
    else: # subsequent start, call cp-load vv_start
        # Check if it is a VDI volume
        if is_vdi_volume(setup_info):
            debug("Start for VDI")
            #TODO: call compound device script
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vdi_start'
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : cp-load.py vdi_start failed!")
                return rc
        else: # USX volumes
            cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vv_start ' + vr_uuid
            if ha_uuid != None and amcurl != None:
                cmd_str = cmd_str + " " + ha_uuid + " " + amcurl
            rc = do_system(cmd_str)
            if rc != 0:
                debug("ERROR : cp-load.py vv_start failed!")
                return rc

    if milio_config.is_fastfailover:
        debug('Fastfailover mode, skip the lvm setup')
        return 0

    # get all plantypes in RAID plans; Modified Aug, 29 for compound device, only one raid plan
    for raid_plan in raid_plans:
#         plantype = raid_plan['plantype']
        pdetail_str = raid_plan['plandetail']
        the_plandetail = json.loads(pdetail_str)

        storagetype = volume_type
        for the_subplan in the_plandetail["subplans"]:
            if "exportname" in the_subplan and the_subplan["exportname"] != None:
                planuuid = the_subplan["exportname"]
            if the_subplan.has_key('raidbricks'):
                for raidbrick in the_subplan['raidbricks']:
                    #derive logdev_name for the case of reboot in simple hybrid volume
                    if volume_type.upper() in [VDI_DISKBACKED]:
                        for opt in raidbrick['deviceoptions']:
                            if "logdev_name" in opt.lower():
                                logdev_name = opt.lower().split("=")[1]
                    euuid = raidbrick["euuid"]
            if the_subplan.has_key('sharedstorages'):
                for sharedstorage in the_subplan['sharedstorages']:
                    #derive logdev_name for the case of reboot in simple hybrid volume
                    if volume_type.upper() in [VDI_DISKBACKED]:
                        for opt in sharedstorage['deviceoptions']:
                            if "logdev_name" in opt.lower():
                                logdev_name = opt.lower().split("=")[1]
                    euuid = sharedstorage["euuid"]
#         if plantype.lower() == 'capacity':
#             the_exportname = vr_uuid.split('_')[-1] + '_indisk'
#         else:
#             the_exportname = vr_uuid.split('_')[-1] + '_in' + plantype.lower()
        try:
            if is_vdi_volume(setup_info):
                devname_str = os.readlink('/dev/usx/' + euuid)
            else:
                devname_str = os.readlink('/dev/usx-' + planuuid)
        except:
            debug("ERROR : Cannot get the device name for %s. Skip..." % planuuid)
            return 1

        #the_dev = {'devname':devname_str, 'storagetype':'plain', 'lvname':the_exportname }
        the_dev = {'devname':devname_str, 'storagetype':storagetype, 'lvname':vr_uuid.split('_')[-1]}
        the_dev['private'] = devname_str + 'p1'
        if init == 0 and is_old_layout(setup_info) and \
           is_vdi_volume(setup_info):
            # use whole device without LVM layer for old VDI layout
            the_dev['datadev_raw'] = devname_str
            the_dev['datadev'] = devname_str
        else:
            the_dev['datadev_raw'] = devname_str + 'p2'
            the_dev['datadev'] = "/dev/dedupvg/deduplv"
            if VDI_DISKBACKED in volume_type.upper() and (not os.path.exists(UPGREP_VERSION) and is_new_simple_hybrid()):
                the_dev['datadev'] = the_dev['devname']
        #the_dev['type'] = plantype
        the_dev['type'] = 'plain'

        # Tiered ADS need a journal/log partition in the memory disk.
        # create_ibd_uuid_syms() will need 'logdev'. FIXME: not true any more.
        if volume_type.lower() == "hybrid_deprecate" and the_dev['storagetype'].lower() == "memory":
            the_dev['logdev'] = devname_str + 'p3'
        elif volume_type.upper() in [VDI_DISKBACKED] and need_ext_jbd:
            the_dev['logdev'] = logdev_name
        else:
            the_dev['logdev'] = None
        dev_list.append(the_dev)
        device_nr += 1

    time.sleep(2)
    #tune_all_ibd(ibd_dev_list)
    create_ibd_uuid_syms(dev_list)
    if not milio_config.is_ha:
        store_ibd_list(setup_info)

    if rc != 0:
        debug('Setup device connection failed.')
        return rc

    if init == 1:
        debug('Initializing devices...')
        rc = init_devices(setup_info)
        if rc != 0:
            debug("init_devices failed!")
            return 1
    else:
        clone_enabled = setup_info['configure']['volumeresources'][0].get('snapcloneenabled')
        volume_type = setup_info['configure']['volumeresources'][0]['volumetype'].upper() #== VDI_DISKLESS
        debug(clone_enabled)

        # For backward compatibility
        # if the snapcloneenabled is not set. the snapclone must enable
        # to avoid to reinit LVM and dedupfs when upgraded from low version.
        if clone_enabled == None:
            clone_enabled = True
        if volume_type == VDI_DISKLESS and not clone_enabled:
            debug('Simple in memory : Initializing devices...')
            rc = init_devices(setup_info)
            if rc != 0:
                debug("init_devices failed!")
                return 1
        else:
            if is_dedupvg_existed():
                # USX-59510 if the dedupvg is existed, and then we can do start
                # the dedup lvm if the dudupvg is not existed. maybe this is old
                # version or it need to reinit memory devices.
                debug('dedupvg is existed, can start LV')
                rc = start_lv(setup_info)
                if rc != 0:
                    debug("start LV failed!")
                    return 1
            rc = layout_upgrade(setup_info)
            rc = reinit_mem_device(setup_info)
            if rc != 0:
                debug("reinit_mem_devices failed!")
                return 1

    rc = ads_pick_start_arb_device(setup_info)
    return rc


def get_export_ibd_dev(export_name = None):
    try:
        if not export_name:
            export_name = UsxConfig().volume_uuid
        # (ret, ibd_dev) = IBDAgent().get_ibd_devname_by_uuid(export_name)
        # if ret != 0:
        #     debug('get ibd device by uuid %s failed.' % export_name)
        #     return None
        # debug('got the ibd device %s' % ibd_dev)
        # return ibd_dev
        ibd_dev = '/dev/usx-%s' % export_name
        with open('/tmp/%s.devlist' % export_name, 'w') as fd:
            fd.write(ibd_dev)
            fd.write('\n')
        debug('got the ibd device %s' % ibd_dev)
        return ibd_dev
    except Exception, e:
        debug('ERROR: got exception when export ibd device %s' % e)
        raise e


def vol_up(setup_info):
    """
    Call raid1_start if volume type is "mem_persistent"
    """
    config = setup_info['configure']
    volume_type = config['volumeresources'][0]['volumetype']
    rc = 0
    if volume_type.lower() == 'mem_persistent':
        rc = raid1_start(setup_info)
    return rc

def flush_vscaler(config):
    out=['']
    rc = do_system("sysctl -a|grep 'vscaler.*do_sync'", out)
    if rc != 0:
        debug('No vscaler to flush, skip.')
        return 0
    #dev.vscaler.zram0+zram1.do_sync = 0
    do_sync_list = out[0].split('\n')
    for do_sync in do_sync_list:
        if len(do_sync) == 0:
            continue
        cmd_str = 'sysctl ' + do_sync.split()[0] + '=1'
        do_system(cmd_str)
    return 0

def vol_export(setup_info, first, ha):
    """
    Create device export, mount point
    """
    config = setup_info['configure']
    adsname_str = config['volumeresources'][0]['uuid']
    volume_type = config['volumeresources'][0]['volumetype']

    ibd_dev_list = setup_info['ibd_dev_list']
    need_reinit = setup_info['need_reinit']
    log_dev = None
    log_size = DEFAULT_JOURNAL_SIZE

    debug(json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': ')))
    if volume_type.lower() == "hybrid_deprecate":
        if len(ibd_dev_list) != 2:
            debug('ERROR: ibd_dev_list for hybrid ADS: %s ' % str(ibd_dev_list))
            return 1
        if ibd_dev_list[0]["storagetype"].lower() == "memory": #or ibd_dev_list[0]["pool_type"].lower() == "memory_pool":
            vscaler_dev = ibd_dev_list[0]["uuid_path"]
            #log_dev = ibd_dev_list[0]["log_uuid_path"]
            log_dev = ibd_dev_list[0]["logdev"]
            if ibd_dev_list[0].has_key('logsize_m'):
                log_size = ibd_dev_list[0]["logsize_m"]
            ddp_dev = ibd_dev_list[1]["uuid_path"]
            ddp_dev_type = ibd_dev_list[1]["storagetype"]
        else:
            vscaler_dev = ibd_dev_list[1]["uuid_path"]
            #log_dev = ibd_dev_list[1]["log_uuid_path"]
            log_dev = ibd_dev_list[1]["logdev"]
            if ibd_dev_list[1].has_key('logsize_m'):
                log_size = ibd_dev_list[1]["logsize_m"]
            ddp_dev = ibd_dev_list[0]["uuid_path"]
            ddp_dev_type = ibd_dev_list[0]["storagetype"]

        debug("========vscaler: %s | log_Dev: %s | ddp_dev: %s | ddp_dev_type: %s" % (vscaler_dev, log_dev, ddp_dev, ddp_dev_type))
    else:
        if len(ibd_dev_list) == 0:
            debug("ERROR: Underlying device not available")
            return 1
        vscaler_dev = None
        ddp_dev = ibd_dev_list[0]["uuid_path"]
        ddp_dev_type = ibd_dev_list[0]["storagetype"]
        if volume_type.lower() == "simple_hybrid":
            log_dev = ibd_dev_list[0]["logdev"]

    if volume_type.lower() == "mem_persistent":
        ibd_dev_list = setup_info['ibd_dev_list']
        ibd_dev = ibd_dev_list[0]["datadev"]
        cmd_str = "mdadm --examine " + ibd_dev + '|grep \"Array UUID\"'
        out_stream = os.popen(cmd_str, 'r', 1)
        uuid_list = out_stream.read().split(' ')
        md_devname = uuid_list[len(uuid_list) - 1]
        md_devname = "/dev/disk/by-id/md-uuid-" + md_devname.split('\n')[0]
        print md_devname
        debug("md device: ", ibd_to_md(ibd_dev))
        ddp_dev = md_devname
        ddp_dev_type = ibd_dev_list[0]["storagetype"]
        #need to wait before call ddp...
        while (True):
            if (os.path.exists(md_devname)):
                break
            debug("Waiting md...", md_devname)
            time.sleep(0.1)

    #ddp_setup may change our stdout /stderr...
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    if is_snapshot_enabled( config ):
        snapshot_supported_flag = open( '/var/run/snapshot_supported', 'a' )
        #os.utime( '/etc/ilio/snapshot_supported', None )
        snapshot_supported_flag.close()

    ddp_setup.ddp_prepare_update()

    if config['volumeresources'][0].has_key('dedupfsmountpoint'):
        mnt_point = config['volumeresources'][0]['dedupfsmountpoint']
    else:
        #get vm name out of uuid: yj-vc_yj-hyb-1-1413398521118. vm name: yj-hyb-1
        mnt_point = '/exports/' + adsname_str.split('_')[-1].rsplit('-',1)[0]

    fs_dev = FsManager().get_dev(milio_settings.export_fs_mode, ddp_dev, vscaler_dev=vscaler_dev, log_dev=log_dev, log_size=log_size)

    if need_reinit == True and (volume_type.lower() == 'hybrid' or volume_type.lower() == 'memory'):
        is_hybrid = False
        if volume_type.lower() == "hybrid_deprecate": # FIXME: Should use pool_type here?
            is_hybrid = True
        debug("Calling ddp_setup.reset_ddp with ddp_dev:",  ddp_dev, ' log_dev:', log_dev)
        # rc = ddp_setup.reset_ddp(config, ddp_dev, vscaler_dev, log_dev, log_size, is_hybrid)
        rc = fs_dev.reset_fs()

    clone_enabled = setup_info['configure']['volumeresources'][0].get('snapcloneenabled')

    # For backward compatibility
    # if the snapcloneenabled is not set. the snapclone must enable
    # to avoid to reinit LVM and dedupfs when upgraded from low version.
    if clone_enabled == None:
        clone_enabled = True
    if volume_type == VDI_DISKLESS and not clone_enabled:
        first = True

    # We need to reinit disk if snapclone for SIMPLE_MEMORY volume is disabled
    rc = None
    if first:
        rc = fs_dev.init_with_mount(mnt_point)
        # debug("Calling ddp_setup.config_ddp with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        # rc = ddp_setup.config_ddp(config, mnt_point, ddp_dev, vscaler_dev, log_dev, log_size, ha)
        flush_vscaler(config)
    else:
        # debug("Calling ddp_setup.ddp_update_device_list with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        # rc = ddp_setup.ddp_update_device_list(config, mnt_point, ddp_dev, vscaler_dev, log_dev)
        fs_dev.setup_mnttable_for_ha(mount_point=mnt_point)
        if ha == 0:
            rc = fs_dev.start_with_mount(mnt_point)
        # debug("None HA, Start DDP by calling ddp_setup.init_ddp with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        # rc = ddp_setup.init_ddp(config, mnt_point, ddp_dev, vscaler_dev, log_dev, ha)
    if rc == None:    #Handle 'None' return value
        rc = 0
    if rc != 0:
        debug("ddp_setup initialization failed with: ", rc)

    #Restore original stdout / stderr
    sys.stdout = old_stdout
    sys.stderr = old_stderr
    return rc



def vol_init(arg_list):
    """
    Initialize volume; for both HA & Non-HA
    """
    debug("Entering vol_init ...")
    vv_setup_info = {}

    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    configure = vv_setup_info['configure']
    if not configure['volumeresources']: # no resources; by definition it is an HA standby node
        debug("HA standby node; do nothing...")
        return 0
    adsname_str = configure['volumeresources'][0]['uuid']
    ha_util.set_curr_volume(adsname_str)

    ha = 0
    if configure['usx']['ha'] == True:
        ha = 1

    # Create an alias for storage network interface, set it to service IP
    # Don't set service IP if volume is simple-hybrid or simple-memory
    rc = set_storage_interface_alias(vv_setup_info)
    if rc != 0:
        debug("ERROR : set_storage_interface_alias failed!")
        return 1

    # remove ibd agent configuration file for ads_init
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass

    # Update /etc/lvm/lvm.conf fileters for Simple Hybrid volume
    # if configure['volumeresources'][0]['volumetype'].upper() == "SIMPLE_HYBRID":
    #     if not os.path.isfile("/etc/lvm/lvm.conf.orig"):
    #         debug("vol_init: Setting up filter in /etc/lvm.lvm.conf")
    #         runcmd("cp /etc/lvm/lvm.conf /etc/lvm/lvm.conf.orig")
    #         #Updating /etc/lvm/lvm.conf
    #         runcmd("sed 's!^\s*filter.*!    filter = [\"a|/dev/sda|\", \"a|/dev/xvda|\", \"a|/dev/mapper|\", \"r/.*/\" ]!g' /etc/lvm/lvm.conf.orig >/etc/lvm/lvm.conf")
    #         #Update initramfs
    #         runcmd("update-initramfs -u -k `uname -r`")
    #         debug("vol_init: initramfs updated")
    #     else:
    #         # Make sure we have correct filter
    #         (ret, msg) = runcmd('grep \'filter = \["a|/dev/sda|", \"a|/dev/xvda|\", "a|/dev/mapper|", "r/.*/" \]\' /etc/lvm/lvm.conf ')
    #         if msg == "":
    #             debug("vol_init: filter in /etc/lvm.lvm.conf incorrect. Changing...")
    #             runcmd("cp /etc/lvm/lvm.conf /etc/lvm/lvm.conf.orig")
    #             #Updating /etc/lvm/lvm.conf
    #             runcmd("sed 's!^\s*filter.*!    filter = [\"a|/dev/sda|\", \"a|/dev/xvda|\", \"a|/dev/mapper|\", \"r/.*/\" ]!' /etc/lvm/lvm.conf.orig >/etc/lvm/lvm.conf")
    #             #Update initramfs
    #             runcmd("update-initramfs -u -k `uname -r`")
    #             debug("vol_init: initramfs updated")



    rc = vol_load_devices(vv_setup_info, init=1)
    if rc != 0:
        debug("ERROR : vol_load_devices failed!")
        return 1
    configure_str = json.dumps(vv_setup_info, indent=4, separators=(',', ': '))
    debug("After load_devices: -------- %s" % configure_str)

    if milio_config.is_fastfailover:
        debug('Fastfailover mode export init...')
        ibd_dev = get_export_ibd_dev()
        print ibd_dev
        if not ibd_dev:
            return 1

        mount_point = configure['volumeresources'][0]['dedupfsmountpoint']
        if mount_point is None:
            debug('Cannot find mount point.')
            return 1

        # Create direcory for mount_point
        if not os.path.exists(mount_point):
            cmd_str = 'mkdir -p %s' % mount_point
            rc = do_system(cmd_str)
            if rc != 0:
                debug('Create mount point failed.')
                return 1

        vscaler_dev = None
        log_dev = None
        # debug('Update /etc/ilio/mnttab')
        # ddp_setup.ddp_prepare_update()
        # ddp_setup.ddp_update_device_list(configure, mount_point, ibd_dev, vscaler_dev, log_dev)
        if ha == 0:
            debug('None HA, start init.')
            fs_dev = FsManager().get_dev(milio_settings.export_fs_mode, ibd_dev)
            rc = fs_dev.init_with_mount(mount_point)
            if rc != 0:
                debug('Initialize failed.')
                return rc
            fs_dev.setup_mnttable_for_ha()
            # Start NFS/iSCSI.
            UsxServiceManager.start_service(mount_point)

        rc = 0
    else:
        create_raid1(vv_setup_info) # for mem_persistent volume
        rc = vol_export(vv_setup_info, True, ha) # Non-HA
        if rc != 0: # vol_export is unsuccessful
            debug("ERROR : vol_export failed!")
            return 1

    # Start md monitor for non simple volumes.
    if milio_config.volume_type not in ['SIMPLE_HYBRID', 'SIMPLE_MEMORY', 'SIMPLE_FLASH']:
        debug('Start md monitor for: %s' % milio_config.volume_type)
        vv_start_md_monitor()

    #configure_str = json.dumps(vv_setup_info, indent=4, separators=(',', ': '))
    #debug("After Export vv_setup_info----------------------- %s" % configure_str)
    #cfg_file = open('./vv_init.json', 'w')
    #cfg_file.write(configure_str)
    #cfg_file.close()

    #vv_setup_info={}
    #cfg_file = open('./vv_init.json', 'r')
    #cfg_str = cfg_file.read()
    #cfg_file.close()
    #vv_setup_info = json.loads(cfg_str)

    if configure['volumeresources'][0]['volumetype'].upper() == VDI_DISKLESS:
        # Enable snapclone job only if 'snapcloneenabled' flag is 1 or non exist (for backward compatibility)
        clone_enabled = configure['volumeresources'][0].get('snapcloneenabled')
        clone_activated = configure['volumeresources'][0].get('snapcloneactivated')
        if clone_enabled == None or clone_enabled:
            if clone_activated == None or clone_activated:
                debug("Setup Diskless VDI volume backup/restore job ...")
                retVal = do_system(VDI_DISKLESS_SNAPCLONE_JOB)
                if retVal != 0:
                    debug('ERROR : online snapclone setup script failed!')
                    return retVal
            debug("Run Diskless VDI volume backup/restore script ...")
            retVal = do_system(VDI_DISKLESS_SNAPCLONE)
            if retVal != 0:
                debug("WARNING : online snapclone script failed to run!")
    return rc



def vol_start(arg_list):
    """
    Volume start for configured volume, used for subsequent reboot after init
    """
    debug("Enter vol_start ...")
    vv_setup_info = {}

    rc = load_conf(VV_CFG, vv_setup_info)
    if rc != 0:
        return rc

    config = vv_setup_info['configure']

    # remvoe ibd agent configuration file
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass
    #
    # if i'm a ha node, do nothing here, let ha start it
    #
    if config['usx']['ha'] == True:
        runcmd_nonblock("rm -rf /etc/ilio/mnttab")
        # Empty volumeresource in local atlas.json
        try:
            cfgfile = open(VV_CFG, 'w')
            config['volumeresources'] = []
            json.dump(config, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
            cfgfile.flush()
            cfgfile.close()
        except:
            debug("vol_start: failed to empty the volume resources in local atlas.json")

        debug("vol_start: this is an ha node, let ha start this virtual volume")
        return 0

    adsname_str = config['volumeresources'][0]['uuid']
    ha_util.set_curr_volume(adsname_str)
    # Create an alias for storage network interface, set it to service IP
    rc = set_storage_interface_alias(vv_setup_info)
    if rc != 0:
        debug("ERROR : set_storage_interface_alias failed!")
        return 1

    rc = vol_load_devices(vv_setup_info)
    if rc != 0:
        debug("ERROR : vol_load_devices failed!")
        return 1

    if milio_config.is_fastfailover:
        debug('Fastfailover mode export start...')
        ibd_dev = get_export_ibd_dev()
        if not ibd_dev:
            return 1

        mount_point = config['volumeresources'][0]['dedupfsmountpoint']
        if mount_point is None:
            debug('Cannot find mount mount.')
            return 1

        vscaler_dev = None
        log_dev = None
        # debug('Update /etc/ilio/mnttab')
        # ddp_setup.ddp_prepare_update()
        # ddp_setup.ddp_update_device_list(config, mount_point, ibd_dev, vscaler_dev, log_dev)

        fs_dev = FsManager().get_dev(milio_settings.export_fs_mode, ibd_dev)
        rc = fs_dev.start_with_mount(mount_point)
        if rc != 0:
            return rc
        fs_dev.setup_mnttable_for_ha()
        # Start NFS/iSCSI.
        UsxServiceManager.start_service(mount_point)

    else:
        rc = vol_up(vv_setup_info)
        rc = vol_export(vv_setup_info, False, 0) # Non-HA

    # Start md monitor for non simple volumes.
    if milio_config.volume_type not in ['SIMPLE_HYBRID', 'SIMPLE_MEMORY', 'SIMPLE_FLASH']:
        debug('Start md monitor for: %s' % milio_config.volume_type)
        vv_start_md_monitor()

    # mount snapshots
    debug("INFO : begin to mount snapshots")
    if config['volumeresources'][0]['volumetype'].upper() == "SIMPLE_HYBRID" and not config['volumeresources'][0].get('snapshotenabled'):
        debug("Snapshot for Symple Hybrid is not enabled.")
    else:
        cmd = 'python /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc add_all_exports -u ' + adsname_str
        (ret, msg) = ha_util.ha_retry_cmd(cmd, 3, 2)
        if ret != 0:
            debug("ERROR : mount snapshots failed!")
            rc = 1

    if config['volumeresources'][0]['volumetype'].upper() == VDI_DISKLESS:
        debug("Run Diskless VDI volume backup/restore script ...")
        retVal = do_system(VDI_DISKLESS_SNAPCLONE)
        if retVal != 0:
            debug("WARNING : online snapclone script failed to run!")
        debug("Run diskless VDI snapshot sync script ...")
        amcurl = config['usx']['usxmanagerurl']
        apistr = '/usx/dataservice/volume/resource/snapshots/synchronize/' + adsname_str + '?api_key=' + adsname_str
        cmd ='curl -k -X PUT -H "Content-Type:application/json" ' + amcurl + apistr
        retVal = do_system(cmd)
        if retVal != 0:
            debug("WARNING : diskless VDI snapshot sync script failed to run!")

    return rc

def vol_stop(arg_list):
    """
    Stop all IBD devices in "/tmp/<adsname_str>.devlist
     Also any affected MDs.
    """
    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_stop_begin'
    (ret, msg) = runcmd(cmd, print_ret=True)

    adsname_str = arg_list[2]
    vv_setup_info = {}

    debug("Enter vol_stop: stopping %s ..." % adsname_str)

    if 0 == send_volume_status(adsname_str, VOL_STATUS_FATAL):
        debug("INFO: update volume %s status to FATAL (offline) " % adsname_str)

    # TODO: need to get export type first to decide stop either ISCSI or NFS
    cnt = 0
    while cnt < RETRYNUM:
        out = ['']
        cmd_str = 'pkill -TERM -f ' + ISCSI_DAEMON
        rc = do_system(cmd_str, out)
        if rc == 0:
            break
        else: # fail to stop the server
            sub_cmd_str = 'pidof ' + ISCSI_DAEMON
            sub_rc = do_system(sub_cmd_str)
            if sub_rc == 1:
                # the server process has been killed
                break
        time.sleep(1)
        cnt += 1
    if cnt >= RETRYNUM:
        debug('Retry kill iscsi server %d times, all failed' %cnt)

    # stop NFS service
    cmd_str = "service nfs-kernel-server stop"
    rc = do_system(cmd_str)
    if rc != 0:
        debug("stop nfs service failed!")
    time.sleep(5)

    [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev] = ddp_setup.readmnttab(ddp_setup.mnttab)
    # RV Fixme: mnttab is not in sync when another resource failover to this volume (assuming this volume already
    # failed over to the standby node and now it is a standby node)
    #  MOUNTPOINT=$(echo $OCF_RESKEY_directory | sed 's/\/*$//')/

    my_list = [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev]
    debug(my_list)
    cnt = 0
    if mnt_point != None and os.path.exists(mnt_point):
        while cnt < RETRYNUM and os.path.exists(mnt_point):
            cmd_str = CMD_UMOUNT + " " + mnt_point
            out = ['']
            rc = do_system(cmd_str, out)
            if rc == 0:
                break
            elif "not mounted" in out[0]:
                debug("WARNING: %s not mounted" %(mnt_point))
                break

            if cnt == 0:
                debug("WARNING: umount failed: %s" % cmd_str)
            time.sleep(30)
            cnt += 1
    if cnt >= RETRYNUM:
        debug("ERROR: retry umount failed %d times, all failed" %cnt)
        return 1

    if cache_name and cache_name.lower() == ddp_setup.VSCALER_NAME.lower(): # vscaler cache exist, this is a hybrid volume
        out = ['']
        cmd_str = CMD_RMVSCALER + ' ' + ddp_setup.VSCALER_NAME
        rc = do_system(cmd_str, out)
        if rc != 0:
            # Ignore the case that vscaler has been removed
            # Example: su-ads-opt-31 testfile # dmsetup remove vmdata_cache2
            #          device-mapper: remove ioctl failed: No such device or address
            #          Command failed
            #        su-ads-opt-31 testfile # echo $?
            #          1

            msg = out[0]
            msgindex = msg.find('No such device or address')
            if msgindex >= 0:
                # the device has been remove, ignore the error
                debug(ddp_setup.VSCALER_NAME + 'has been removed, msg=' + msg)
            else:
                # some other error
                return rc
        time.sleep(5)

    ibd_list = load_ibd_list(adsname_str)
    if ibd_list == None:
        # Give a warning message instead of return NO_DEV_TO_STOP
        debug("WARNING: no devlist for %s." % adsname_str)

    # Must stop arbitrator before disconnect ibd.
    arb_stop(adsname_str)

    stop_lv()

    # CALL cp-load.py to stop md devices
    cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vv_stop ' + adsname_str
    rc = do_system(cmd_str)
    if rc != 0:
        debug("ERROR : cp-load.py vv_stop failed!")
        return rc

    remove_ibd_list(adsname_str)
    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_stop_end'
    (ret, msg) = runcmd(cmd, print_ret=True)
    debug("==== vol_stop: successful")
    return 0



def usx_stop(arg_list):
    """
    Stop all IBD devices in "/tmp/<adsname_str>.devlist
     Also any affected MDs.
    """
    debug("Enter usx_stop: stopping %s ..." % arg_list[2])

    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_stop_begin'
    (ret, msg) = runcmd(cmd, print_ret=True)

    adsname_str = arg_list[2]
    vv_setup_info = {}

    if 0 == send_volume_status(adsname_str, VOL_STATUS_FATAL):
        debug("INFO: update volume %s status to FATAL (offline) " % adsname_str)

    # stop IPaddr2
    ha_util.ha_stop_service_ip()

    # TODO: need to get export type first to decide stop either ISCSI or NFS
    cnt = 0
    while cnt < RETRYNUM:
        out = ['']
        cmd_str = 'pkill -TERM -f ' + ISCSI_DAEMON
        rc = do_system(cmd_str, out)
        if rc == 0:
            break
        else: # fail to stop the server
            sub_cmd_str = 'pidof ' + ISCSI_DAEMON
            sub_rc = do_system(sub_cmd_str)
            if sub_rc == 1:
                # the server process has been killed
                break
        time.sleep(1)
        cnt += 1
    if cnt >= RETRYNUM:
        debug('Retry kill iscsi server %d times, all failed' %cnt)

    # stop NFS service
    cmd_str = "service nfs-kernel-server stop"
    rc = do_system(cmd_str)
    if rc != 0:
        debug("stop nfs service failed!")
    time.sleep(5)

    # unmount dedupFS
    ha_util.ha_umount_dedupFS()
    [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev] = ddp_setup.readmnttab(ddp_setup.mnttab)
    # RV Fixme: mnttab is not in sync when another resource failover to this volume (assuming this volume already
    # failed over to the standby node and now it is a standby node)
    #  MOUNTPOINT=$(echo $OCF_RESKEY_directory | sed 's/\/*$//')/

    my_list = [ddp_dev, vscaler_dev, cache_name, mnt_point, mnt_opts, jdev]
    debug(my_list)
    cnt = 0
    if mnt_point != None and os.path.exists(mnt_point):
        while cnt < RETRYNUM and os.path.exists(mnt_point):
            cmd_str = CMD_UMOUNT + " " + mnt_point
            out = ['']
            rc = do_system(cmd_str, out)
            if rc == 0:
                break
            elif "not mounted" in out[0]:
                debug("WARNING: %s not mounted" %(mnt_point))
                break

            if cnt == 0:
                debug("WARNING: umount failed: %s" % cmd_str)
            time.sleep(30)
            cnt += 1
    if cnt >= RETRYNUM:
        debug("ERROR: retry umount failed %d times, all failed" %cnt)
        return 1

    if cache_name and cache_name.lower() == ddp_setup.VSCALER_NAME.lower(): # vscaler cache exist, this is a hybrid volume
        out = ['']
        cmd_str = CMD_RMVSCALER + ' ' + ddp_setup.VSCALER_NAME
        rc = do_system(cmd_str, out)
        if rc != 0:
            # Ignore the case that vscaler has been removed
            # Example: su-ads-opt-31 testfile # dmsetup remove vmdata_cache2
            #          device-mapper: remove ioctl failed: No such device or address
            #          Command failed
            #        su-ads-opt-31 testfile # echo $?
            #          1

            msg = out[0]
            msgindex = msg.find('No such device or address')
            if msgindex >= 0:
                # the device has been remove, ignore the error
                debug(ddp_setup.VSCALER_NAME + 'has been removed, msg=' + msg)
            else:
                # some other error
                return rc
        time.sleep(5)

    ibd_list = load_ibd_list(adsname_str)
    if ibd_list == None:
        # Give a warning message instead of return NO_DEV_TO_STOP
        debug("WARNING: no devlist for %s." % adsname_str)

    # Must stop arbitrator before disconnect ibd.
    arb_stop(adsname_str)

    stop_lv()

    # CALL cp-load.py to stop md devices
    cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vv_stop ' + adsname_str
    rc = do_system(cmd_str)
    if rc != 0:
        debug("ERROR : cp-load.py vv_stop failed!")
        return rc

    remove_ibd_list(adsname_str)
    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_stop_end'
    (ret, msg) = runcmd(cmd, print_ret=True)
    debug("INFO: usx_stop successful")
    return 0


def vol_ha_start(arg_list):
    """
    Start volume resource when HA is enabled
    """
    debug("Enter vol_ha_start ...")
    debug('From AMC: ' + ' '.join(arg_list))

    ha_lock_fd = None
    usxmanager_alive_flag = True
    ha_lock_fd = node_trylock(HA_LOCKFILE)
    if ha_lock_fd == None:
        return 1

    ha_setup_info = {}
    adsname_str = arg_list[2]

    # Let usx_daemon we are starting.
    ha_util.set_curr_volume(adsname_str)
    ha_util.ha_set_volume_starting_flag()

    # Check USX Manager is alive or not
    usxm_flag = ha_util.is_usxmanager_alive()

    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_start_begin ' + arg_list[2]
    (ret, msg) = runcmd(cmd, print_ret=True, block=False)

    ret = ha_util.ha_handle_multiple_start_volume(adsname_str)
    if ret == 1:
        node_unlock(ha_lock_fd)
        return 0
    elif ret >= 3:
        node_unlock(ha_lock_fd)
        if ret == 5:
            ha_util.ha_stop_cluster()
        ha_util.unset_curr_volume()
        return 1

    stretchcluster_or_robo_flag = ha_util.is_stretchcluster_or_robo()
    if stretchcluster_or_robo_flag:
        (stretchcluster_flag, availability_flag, tiebreakerip) = ha_util.ha_stretchcluster_config()
        tiebreakerip = ha_util.ha_get_tiebreakerip()
        scl_timeout = ha_util.ha_get_scl_timeout()
        nodename = ha_util.ha_get_local_node_name()
        # If we lost quorum before finish start the resource, usx_daemon will
        # force start resource.
        while True:
            # Acquire a 60 seconds lock for tiebreaker
            # 0: successfully acquired lock
            # 1: other node already owned the lock
            # 255: timeout
            result = ha_util.ha_acquire_stretchcluster_lock(tiebreakerip, adsname_str, nodename, 60)
            # If it is Robo cluster, we have 2 tiebreaker, the result will be a list for both tiebreaker
            if result in [[0],[0,0],[0,255],[255,0]]:
                # We successfully get the lock on at least 1 tiebreaker
                break
            elif result in [[255],[255,255]] and availability_flag == True:
                # handle the case for preferavailability == true
                debug("INFO: cannot acquire stretchcluster lock with preferavailability = true, skip")
                break
            elif result in [[255],[255,255]] and ha_util.ha_has_quorum() and not ha_util.ha_check_storage_network_status(1):
                debug('HA has quorum and this node could reach at least half Service VMs, acquire stretchcluster lock timeout, continue to pick up resource.')
                break
            else:
                ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
                ha_util.ha_set_no_quorum_policy('freeze', True)
                node_unlock(ha_lock_fd)
                ha_util.unset_curr_volume()
                return 1

    if usxm_flag:
        if 0 == send_ha_failover_status(adsname_str, VOL_STATUS_WARN):
            with open(dedupfs_availability_status_file, 'w') as fd:
                fd.write('UNMOUNTED')
            with open(volume_export_availability_status_file, 'w') as fd:
                fd.write('UNKNOWN')
        send_alert_ha(adsname_str)

    # stop dedupvg
    cmd_str = 'vgchange -an dedupvg'
    try:
        rc = do_system_timeout(cmd_str)
    except Exception, e:
        debug('WARNING: stop dedupvg failed.[err={}]'.format(e))

    # stop MD devices
    cmd_str = "/sbin/mdadm --stop --scan"
    rc = do_system(cmd_str)
    if rc != 0:
        if stretchcluster_or_robo_flag:
            ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
        node_unlock(ha_lock_fd)
        ha_util.reset_vm("vol_ha_start_md_reset")
    debug("INFO: Stop MD devices before HA failover")

    # remove flag files
    cmd_str = "rm /tmp/dev-uuid-*"
    do_system(cmd_str)
    debug("INFO: Remove dev-uuid files")
    cmd_str = "rm /tmp/*.devlist"
    do_system(cmd_str)
    debug("INFO: Remove dev-list files")
    cmd_str = "rm /etc/ilio/mnttab"
    do_system(cmd_str)
    cmd_str = "rm /etc/ilio/md_stat_local_*"
    do_system(cmd_str)
    debug("INFO: Remove devlist files")
    cmd_str = "rm /etc/ilio/pool_lockfile"
    do_system(cmd_str)
    debug("INFO: Remove cp-load flag file")

    # stop ibd service
    cmd_str = CMD_IBDMANAGER_A_STOP
    do_system(cmd_str)
    debug("INFO: Stop IBD agent before HA failover")

    # stop ibd service
    cmd_str = CMD_IBDMANAGER_S_STOP
    do_system(cmd_str)
    debug("INFO: Stop IBD server before HA failover")

    # remvoe ibd agent configuration file
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass

    # remove HA_DISABLE_FILE
    try:
        os.remove(HA_DISABLE_FILE)
    except:
        pass

    # remove old ibd devices
    cmd_str = 'cmd="rm -rf /dev/ibd[^0]*"; /bin/bash -c "$cmd"'
    do_system(cmd_str)
    debug("INFO: Remove ibd device")

    if usxm_flag:
        ret = delete_jobid_file(True)
        jobid_file_exists = False
        does_jobid_file_need_deletion = not jobid_file_exists
        send_status("HA",  0, 0, "HA Failover", "HA VM becoming active VM...", False, False, adsname_str)
        debug("INFO: Send status update before HA failover")

    rc = load_conf(VV_CFG, ha_setup_info)
    if rc != 0:
        if usxm_flag:
            send_status("HA",  100, 1, "HA Failover", "Failed to load conf", does_jobid_file_need_deletion, False, adsname_str)
        if stretchcluster_or_robo_flag:
            ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
        node_unlock(ha_lock_fd)
        ha_util.unset_curr_volume()
        return rc

    milio_config.ha_reload(adsname_str)
    milio_settings.ha_reset_fs_mode()

    haconfigure = ha_setup_info['configure']

    rc = vv_get_resource(adsname_str, ha_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "HA Failover", "Failed to get resource from AMC", does_jobid_file_need_deletion, False, adsname_str)
        if stretchcluster_or_robo_flag:
            ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
        node_unlock(ha_lock_fd)
        ha_util.unset_curr_volume()
        return 1
    debug("INFO: Get volume resource info from AMC before HA failover")

    #debug("INFO: try to move shared storage if needed, and update volume location")
    configure = ha_setup_info['configure']
    vv_uuid = adsname_str
    ha_uuid =  configure['usx']['uuid']
    amcurl = configure['usx']['usxmanagerurl']

    if configure['volumeresources'][0]['raidplans'][0].has_key('sharedstorages'):
        if len(configure['volumeresources'][0]['raidplans'][0]['sharedstorages']) == 0:
            amcurl = None

    #ret = move_shared_storage(vv_uuid, ha_uuid, amcurl)
    #if ret != 0:
    #    return 1

    if ha_util.ha_storage_network_status(configure) == 1:
        if usxm_flag:
            send_status("HA",  25, 1, "HA Failover", "Failed to get storage network status", does_jobid_file_need_deletion, False, adsname_str)
        debug("vol_storage_network_check failed!")
        if stretchcluster_or_robo_flag:
            ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
        node_unlock(ha_lock_fd)
        ha_util.unset_curr_volume()
        return 1

    location_set = ha_util.ha_set_location(adsname_str)
    debug("INFO: set resource location")

    if usxm_flag:
        send_status("HA",  25, 0, "HA Failover", "Loading devices...", False, False, adsname_str)
    rc = vol_load_devices(ha_setup_info, 0, ha_uuid, amcurl)
    if rc != 0:
        if usxm_flag:
            send_status("HA",  100, 1, "VOLUME HA", "Failed to load devices", does_jobid_file_need_deletion, False, adsname_str)
        debug("vol_load_devices failed!")
        if stretchcluster_or_robo_flag:
            ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
        node_unlock(ha_lock_fd)
        ha_util.unset_curr_volume()
        return 1
    debug("INFO: Load devices during HA failover")

    if usxm_flag:
        send_status("HA",  50, 0, "HA Failover", "Setting up Volume...", False, False, adsname_str)
    rc = vol_up(ha_setup_info)
    if rc != 0:
        if usxm_flag:
            send_status("HA",  100, 1, "VOLUME HA", "Failed to set up Volume", does_jobid_file_need_deletion, False, adsname_str)
        debug("vol_up failed!")
        if stretchcluster_or_robo_flag:
            ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
        node_unlock(ha_lock_fd)
        ha_util.unset_curr_volume()
        return 1
    debug("INFO: Set up Volume during HA failover")

    if usxm_flag:
        send_status("HA",  75, 0, "HA Failover", "Exporting Volume...", False, False, adsname_str)
    ibd_dev = get_export_ibd_dev()
    if not milio_config.is_fastfailover:
        rc = vol_export(ha_setup_info, False, 1)
        if rc != 0:
            if usxm_flag:
                send_status("HA",  100, 1, "VOLUME HA", "Failed to export Volume", does_jobid_file_need_deletion, False, adsname_str)
            debug("vol_export failed!")
            if stretchcluster_or_robo_flag:
                ha_util.ha_release_stretchcluster_lock(tiebreakerip, adsname_str, nodename)
            node_unlock(ha_lock_fd)
            ha_util.unset_curr_volume()
            return 1
    else:
        debug('Update /etc/ilio/mnttab')
        mount_point = ha_setup_info['configure']['volumeresources'][0]['dedupfsmountpoint']
        vscaler_dev = None
        log_dev = None
        ddp_setup.ddp_prepare_update()
        ddp_setup.ddp_update_device_list(ha_setup_info['configure'], mount_point, ibd_dev, vscaler_dev, log_dev)
        debug("INFO: Volume up and export during HA failover")

    # Once resource has started, update the local ATLAS JSON as well
    atlas_file = open(VV_CFG, 'r')
    atlas_str = atlas_file.read()
    atlas_file.close()
    vol_setup_info = json.loads(atlas_str)

    this_resource = []
    if ha_setup_info.has_key('configure'):
        if ha_setup_info['configure'].has_key('volumeresources'):
            for resource in ha_setup_info['configure']['volumeresources']:
                this_resource.append(resource)
            vol_setup_info['volumeresources'] = this_resource

    tmp_fname = '/tmp/new_atlas_conf.json'
    cfgfile = open(tmp_fname, "w")
    json.dump(vol_setup_info, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
    cfgfile.flush()
    cfgfile.close()
    os.rename(tmp_fname, VV_CFG)
    debug("INFO: Save volume %s configuration file after HA failover" % adsname_str)

    # Start md monitor after updating local atlas.json
    vv_start_md_monitor()
    debug('INFO: Start md monitor for: %s' % milio_config.volume_type)

    # Leave a flag file, usx_deamon will help to mount after USX Manager is online
    ha_util.set_skip_mount_snapshot_flag()
    debug("INFO: Leave flag to mount Snapshot after HA failover")

    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_start_end'
    ha_util.runcmd_nonblock(cmd, False, 0.1)

    debug("INFO: Done with HA failover for volume %s" % adsname_str)

    # send ha failover status OK after failover
    if usxm_flag:
        send_status("HA",  100, 0, "HA Failover", "Volume completed failover", does_jobid_file_need_deletion, False, adsname_str)
        send_ha_failover_status(adsname_str, VOL_STATUS_OK)
    debug("INFO: usx_start successful, done HA failover for volume %s" % adsname_str)
    ha_util.runcmd_nonblock('sync&', False, 0.1)

    ha_util.ha_set_volume_running_status(adsname_str)

    # Set location again if it is not successfully set at beginning
    if location_set:
        ha_util.ha_set_location(adsname_str)

    # Let usx_deamon know the failover is finished successfully
    ha_util.ha_remove_volume_starting_flag()
    if stretchcluster_or_robo_flag:
        # Acquire a 20 seconds lock to avoid other HA nodes grab the same lock and try to start the same volume resource again
        ha_util.ha_acquire_stretchcluster_lock(tiebreakerip, adsname_str, nodename, 20)
    node_unlock(ha_lock_fd)

    return 0


def usx_start(arg_list):
    """
    Start volume resource when HA is enabled
    """
    debug("Enter usx_start %s ..." % arg_list[2])
    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_start_begin ' + arg_list[2]
    (ret, msg) = runcmd(cmd, print_ret=True, block=False)

    ha_setup_info = {}
    adsname_str = arg_list[2]
    ha_util.ha_handle_multiple_start_volume(adsname_str)
    if 0 == send_ha_failover_status(adsname_str, VOL_STATUS_WARN):
        with open(dedupfs_availability_status_file, 'w') as fd:
            fd.write('UNMOUNTED')
        with open(volume_export_availability_status_file, 'w') as fd:
            fd.write('UNKNOWN')
    send_alert_ha(adsname_str)

    # stop IPaddr2
    ha_util.ha_stop_service_ip()
    # stop MD devices
    cmd_str = "/sbin/mdadm --stop --scan"
    do_system(cmd_str)
    debug("INFO: Stop MD devices before HA failover")
    cmd_str = "rm /tmp/dev-uuid-*"
    do_system(cmd_str)
    debug("INFO: Remove dev-uuid files")
    cmd_str = "rm /tmp/*.devlist"
    do_system(cmd_str)
    debug("INFO: Remove devlist files")

    # stop ibd service
    cmd_str = CMD_IBDMANAGER_A_STOP
    do_system(cmd_str)
    debug("INFO: Stop IBD agent before HA failover")

    # remvoe ibd agent configuration file
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass

    # remove HA_DISABLE_FILE
    try:
        os.remove(HA_DISABLE_FILE)
    except:
        pass

    ret = delete_jobid_file()
    jobid_file_exists = False
    does_jobid_file_need_deletion = not jobid_file_exists
    send_status("HA",  0, 0, "HA Failover", "Starting Volume HA takeover", False, False, adsname_str)
    debug("INFO: Send status update before HA failover")

    rc = load_conf(VV_CFG, ha_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "HA Failover", "Failed to load conf", does_jobid_file_need_deletion, False, adsname_str)
        return rc
    haconfigure = ha_setup_info['configure']

    rc = vv_get_resource(adsname_str, ha_setup_info)
    if rc != 0:
        send_status("HA",  100, 1, "HA Failover", "Failed to get resource from AMC", does_jobid_file_need_deletion, False, adsname_str)
        return 1
    debug("INFO: Get volume resource info from AMC before HA failover")

    #debug("INFO: try to move shared storage if needed, and update volume location")
    configure = ha_setup_info['configure']
    vv_uuid = adsname_str
    ha_uuid =  configure['usx']['uuid']
    amcurl = configure['usx']['usxmanagerurl']

    if configure['volumeresources'][0]['raidplans'][0].has_key('sharedstorages'):
        if len(configure['volumeresources'][0]['raidplans'][0]['sharedstorages']) == 0:
            amcurl = None

    #ret = move_shared_storage(vv_uuid, ha_uuid, amcurl)
    #if ret != 0:
    #    return 1

    send_status("HA",  25, 0, "HA Failover", "Loading devices...", False, False, adsname_str)
    rc = vol_load_devices(ha_setup_info, 0, ha_uuid, amcurl)
    if rc != 0:
        send_status("HA",  100, 1, "VOLUME HA", "Failed to load devices", does_jobid_file_need_deletion, False, adsname_str)
        debug("vol_load_devices failed!")
        return 1
    debug("INFO: Load devices during HA failover")

    send_status("HA",  50, 0, "HA Failover", "Setting up Volume...", False, False, adsname_str)
    rc = vol_up(ha_setup_info)
    send_status("HA",  75, 0, "HA Failover", "Exporting Volume...", False, False, adsname_str)
    rc = vol_export(ha_setup_info, False, 0)    # start all resources
    send_status("HA",  100, 0, "HA Failover", "Volume completed failover", does_jobid_file_need_deletion, False, adsname_str)
    debug("INFO: Volume up and export during HA failover")


    # Once resource has started, update the local ATLAS JSON as well
    #configure_str = json.dumps(ha_setup_info, indent=4, separators=(',', ': '))
    #cfg_file = open('/root/vol_ha_start.json', 'w')
    #cfg_file.write(configure_str)
    #cfg_file.close()

    atlas_file = open(VV_CFG, 'r')
    atlas_str = atlas_file.read()
    atlas_file.close()
    vol_setup_info = json.loads(atlas_str)

    this_resource = []
    if ha_setup_info.has_key('configure'):
        if ha_setup_info['configure'].has_key('volumeresources'):
            for resource in ha_setup_info['configure']['volumeresources']:
                this_resource.append(resource)
            vol_setup_info['volumeresources'] = this_resource

    tmp_fname = '/tmp/new_atlas_conf.json'
    cfgfile = open(tmp_fname, "w")
    json.dump(vol_setup_info, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
    cfgfile.close()
    os.rename(tmp_fname, VV_CFG)
    debug("INFO: Save volume %s configuration file after HA failover" % adsname_str)
    # start IPaddr2
    set_storage_interface_alias(ha_setup_info)

    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc ha_start_end'
    (ret, msg) = runcmd(cmd, print_ret=True)

    debug("INFO: usx_start successful, done HA failover for volume %s" % adsname_str)
    return 0


debug("===== BEGIN VOLUME NODE OPERATION =====")
cmd_options =  {
    "init"          : vol_init,
    "start"         : vol_start,
    "usx_start"     : usx_start,
    "ha"            : vol_ha_start,
    "stop"          : vol_stop,
	"usx_stop"      : usx_stop,
    "status"        : vv_status,
    "usx_status"    : usx_status,
}

debug("Entering vv-load:", sys.argv)
if len(sys.argv) < 2:
    debug("ERROR : Incorrect number of arguments!")
    debug("Usage: " + sys.argv[0] + " init|start|ha|stop|status")
    exit(1)

cmd_type = sys.argv[1]

if cmd_type in cmd_options:
    try:
        rc = cmd_options[cmd_type](sys.argv)
    except:
        debug(traceback.format_exc())
        debug("Exception exit...")
        rc = 1
    if rc != 0:
        debug("%s Failed with: %s" % (sys.argv, rc))
        exit(rc)
    else:
        debug("===== END VOLUME NODE OPERATION: SUCCESSFUL! =====")
        exit(0)
else:
    debug("ERROR : Incorrect argument '%s'" % cmd_type)
    debug("Usage: " + sys.argv[0] + " init|start|stop|status")
    exit(1)
