#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import httplib
import ConfigParser
import json
import operator
import os, sys
import string
import time
import copy
import urllib2
import traceback
import socket
import math
import base64
import errno
import datetime
import signal
import shutil
from multiprocessing import Pool

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_ibd import *
from atl_md import *
from atl_storage import *
from atl_arbitrator import *
from status_update import does_jobid_file_exist
from status_update import send_status
from cmd import *
from atl_alerts import *

sys.path.insert(0, '/opt/milio/atlas/roles')
from utils import *

sys.path.insert(0, '/opt/milio/atlas/roles/virtvol')
import ddp_setup

sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
from ha_util import is_vg_used
from ha_util import ha_get_availabe_nodes
from ha_util import reset_vm
from ha_util import ha_manage_one_resouce
from ha_util import ha_unmange_one_resouce
from ha_util import ha_retry_cmd
from ha_util import is_stretchcluster_or_robo
from ha_util import ha_has_quorum
from ha_util import ha_get_tiebreakerip
from ha_util import ha_get_scl_timeout
from ha_util import ha_get_local_node_name
from ha_util import ha_stretchcluster_config
from ha_util import ha_reset_node_fake
from ha_util import ha_check_stretchcluster_lock
from ha_util import ha_acquire_stretchcluster_lock
from ha_util import ha_disable_wr_hook
from ha_util import ha_set_no_quorum_policy
from ha_util import ha_stop_volume
from ha_util import ha_get_local_remote_Service_VM_power_status
from ha_util import ha_get_conf_from_crm
from ha_util import ha_check_enabled
from ha_util import is_vmmanager_reachable

POOL_LOCKFILE = "/etc/ilio/pool_lockfile"
CP_CFG = "/etc/ilio/atlas.json"
ALERT_LOG = "/var/log/alert-ha.log"

VV_READD = "vv_readd"
VV_START = "vv_start"

POOL_TYPE_RAID0 = 'RAID_0'
POOL_TYPE_RAID5 = 'RAID_5'

REGULAR_POOL = 0
VIRTUAL_POOL = 1
VP_TIMEOUT = 1
VP_LOCKTIME = 600
VP_SLEEPTIME = 5

AGGCREATE_RETRY_CNT = 5
AGGCREATE_RETRY_INTERVAL = 30

IBD_AGENT_CONFIG_FILE = '/etc/ilio/ibdagent.conf'
IBD_AGENT_SEC_GLOBAL = 'global'
RAID_IO_ERROR_LOCK = "/tmp/raid_io_lock"
IBD_AGENT_STOP_FILE = "/tmp/ibdagent_stop"

IBD_IO_STATUS = '/var/log/usx-set-io-error.log'

RAID_raid5 = "RAID_5"
RAID_raid0 = "RAID_0"
RAID_raid1 = "RAID_1"

AMC_volresources = "volumeresources"
AMC_raidplans = "raidplans"
AMC_planuuid = "uuid"
AMC_plantype = "plantype"  # "CAPACITY" | "MEMORY"
AMC_plantype_memory = "MEMORY"
AMC_raidtype = "raidtype"
AMC_raidbricks = "raidbricks"
AMC_plandetail = "plandetail"
AMC_subplans = "subplans"
AMC_hypervisoruuid = "hypervisoruuid"
AMC_sharedstorages = "sharedstorages"
AMC_euuid = "euuid"
AMC_serviceip = "serviceip"
AMC_bricksize = "raidbricksize"
AMC_sizeunit = "storageunit"
AMC_pairnumber = "pairnumber"
AMC_exportname = "exportname"
AMC_chunksize = "chunksize"
AMC_bitmap = "bitmap"
AMC_volumetype = "volumetype"
AMC_hybrid = "HYBRID"

#
# Device: raid0|raid5|raid1|ibd
#
DEV_devname = "devname"  # raid0|raid5|raid1|ibd
DEV_detail = "detail"
DEV_planid = "planid"
DEV_uuid = "uuid"  # raid0|raid5|raid1|ibd
DEV_exportname = "exportname"
DEV_index = "index"  # raid0|raid5|raid1|ibd
DEV_ip = "ip"  # ibd
DEV_storagetype = "storagetyep"  # ibd
DEV_raidtype = "raidtype"  # raid0|raid5
DEV_children = "children"  # raid0|raid5|raid1
DEV_state = "state"  # raid0|raid5|raid1|ibd
DEV_chunksize = "chunksize"  # raid0|raid5
DEV_bitmap = "bitmap"  # raid0|raid5|raid1
DEV_working = "working"
DEV_valid = "valid"
DEV_subvalid_counter = "subvalid_counter"

#
# an ibd conatins key:
#
IBD_ip = DEV_ip  # ip of service vm
IBD_uuid = DEV_uuid  # ibd exportname
IBD_devname = DEV_devname
IBD_index = DEV_index
IBD_state = DEV_state
IBD_size = "size"
IBD_raid1number = "raid1number"
IBD_storagetype = "storagetype"
IBD_detail = "detail"

RAID1_device1 = "device_1"
RAID1_device2 = "device_2"

#
# a shared storage conatins key:
#
SS_scsibus = "scsibus"
SS_uuid = DEV_uuid
SS_devname = DEV_devname
SS_state = "state"
SS_detail = "info"

#
# a plan conatins key:
#
PLAN_uuid = "uuid"
PLAN_ibdlist = "ibd_dev_list"
PLAN_sharedlist = "shared_dev_list"
PLAN_pairdict = "raid1_pair_dict"
PLAN_type = "type"  # "CAPACITY" | "MEMORY"
PLAN_capacity = "CAPACITY"
PLAN_memory = "MEMORY"
PLAN_raidtype = "raidtype"  # "RAID_5" | "RAID_0"
PLAN_chunksize = "chunksize"
PLAN_raid1list = "raid1_list"
PLAN_devname = "plan_storage"
PLAN_exportname = "exportname"
PLAN_bitmap = "bitmap"

#
# infrastructure keys
#
INFR_raidtype = "raidtype"
INFR_raid5 = "RAID_5"
INFR_raid0 = "RAID_0"
INFR_memory = "memory"
INFR_disk = "disk"

# commands
CMD_LINK = "/bin/ln"
CMD_PARTED = "/sbin/parted"
CMD_MDADM = "/sbin/mdadm"
CMD_MDSTOP = "/sbin/mdadm --stop"
CMD_MDASSEMBLE = "/sbin/mdadm --assemble --run"
CMD_MDCREATE = "/sbin/mdadm --create --assume-clean --run --force --metadata=1.2"
CMD_MDMANAGE = "/sbin/mdadm --manage"
CMD_MDEXAMINE = "/sbin/mdadm --examine"
# CMD_MDMONITOR = "/sbin/mdadm --monitor"
CMD_MDVERSION = "/sbin/mdadm --version"
CMD_DETAIL = "/sbin/xmdadm --detail"
CMD_PVCREATE = "/sbin/pvcreate"
CMD_PVREMOVE = "/sbin/pvremove"
CMD_VGCREATE = "/sbin/vgcreate"
CMD_LVCREATE = "/sbin/lvcreate"
CMD_LVREMOVE = "/sbin/lvremove"
CMD_LVDEACTIVE = "/sbin/lvchange -a n"
CMD_VGEXTEND = "/sbin/vgextend"
CMD_VGACTIVE = "/sbin/vgchange -a y"
CMD_VGDEACTIVE = "/sbin/vgchange -a n"
CMD_VGREMOVE = "/sbin/vgremove"
CMD_ATLASROLE_DIR = "/opt/milio/atlas/roles"
CMD_ADSPOOL = "python " + CMD_ATLASROLE_DIR + "/ads/ads-pool.pyc"
CMD_CPSTOP = "python " + CMD_ATLASROLE_DIR + "/pool/cp-stop.pyc"
CMD_CPLOAD = "python " + CMD_ATLASROLE_DIR + "/pool/cp-load.pyc"
CMD_AGGSIZE = CMD_ATLASROLE_DIR + "/aggregate/agexport.pyc" + " -s "
CMD_AGGCREATE = CMD_ATLASROLE_DIR + "/aggregate/agexport.pyc" + " -c  "
CMD_AGGSTART = CMD_ATLASROLE_DIR + "/aggregate/agexport.pyc" + " -e "
CMD_AGGDESTROY = CMD_ATLASROLE_DIR + "/aggregate/agexport.pyc" + " -d  "
CMD_CPREADD = CMD_CPLOAD + ' readd'
CMD_VVREADD = CMD_CPLOAD + ' vv_readd'
CMD_VVPRESTARTCHECKING = CMD_CPLOAD + ' vv_pre_start_checking'
CMD_CPSETIOERROR = CMD_CPLOAD + ' raid_device_set_io_error'
CMD_CPUNSETIOERROR = CMD_CPLOAD + ' raid_device_unset_io_error'
CMD_IBDAGENT = "/sbin/ibdagent"
CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_A_UPDATE = CMD_IBDMANAGER + " -r a -u"
CMD_IBDMANAGER_A_UPGRADE = CMD_IBDMANAGER + " -r a -U"
CMD_IBDMANAGER_A_FORCE_UPGRADE = CMD_IBDMANAGER + " -r a -F"
CMD_IBDMANAGER_STAT_WD = CMD_IBDMANAGER + " -r a -s get_wd"
CMD_IBDMANAGER_STAT_WU = CMD_IBDMANAGER + " -r a -s get_wu"
CMD_IBDMANAGER_STAT_WUD = CMD_IBDMANAGER + " -r a -s get_wud"
CMD_IBDMANAGER_STAT_RWWD = CMD_IBDMANAGER + " -r a -s get_rwwd"
CMD_IBDMANAGER_STAT_RWWU = CMD_IBDMANAGER + " -r a -s get_rwwu"
CMD_IBDMANAGER_STAT_RWWUD = CMD_IBDMANAGER + " -r a -s get_rwwud"
CMD_IBDMANAGER_STAT_UD = CMD_IBDMANAGER + " -r a -s get_ud"
CMD_IBDMANAGER_STAT = CMD_IBDMANAGER + " -r a -s get"
CMD_IBDMANAGER_A_STOP_ONE = CMD_IBDMANAGER + " -r a -d"
CMD_IBDMANAGER_S_STOP_ONE = CMD_IBDMANAGER + " -r s -d"
CMD_IBDMANAGER_S_STOP = CMD_IBDMANAGER + " -r a -S"
CMD_IBDMANAGER_A_STOP = CMD_IBDMANAGER + " -r a -S"
CMD_IBDMANAGER_DROP = CMD_IBDMANAGER + " -r a -d"
CMD_IBDMANAGER_IOERROR = CMD_IBDMANAGER + " -r a -e"
# CMD_PVS = "/sbin/pvs"
CMD_CAT = "/bin/cat"
CMD_PS = "/bin/ps"
GAP_RATIO_MAX = 1073741824  # 1024*1024*1024, just a big number
GB_SIZE = 1073741824  # 1024*1024*1024
START_SECTOR = 2048
INTERNAL_LV_NAME = 'atlas_internal_lv'
CMD_SET_RAID_SPEED_LIMIT_MIN = "/proc/sys/dev/raid/speed_limit_min"

MD_DEFAULT_BITMAP_STR = " --bitmap=internal "
MD_BITMAP_4K_CHUNK_STR = " --bitmap-chunk=64M "

# USX Status Update
VOLUME_STORAGE_STATUS = 'VOLUME_STORAGE_STATUS'
VOL_RESOURCE_TYPE = 'VOLUME_RESOURCE'
VOL_CONTAINER_TYPE = 'VOLUME_CONTAINER'
CMD_STATUS_DIR = "/opt/milio/atlas/system"
CMD_STATUS = "python " + CMD_STATUS_DIR + "/status_check.pyc"

DEV_STATE_DISCONNECT = 'disconnect'  # not exist
DEV_STATE_OFFLINE = 'offline'  # not in array, but exist/connected
DEV_STATE_ONLINE = 'online'  # in array, normal
DEV_STATE_FAILED = 'failed'  # in array, but faulty
DEV_STATE_SPARE = 'spare'  # in arry, apare
DEV_STATE_UNKNOWN = 'unknown'

STOR_TYPE_MEMORY = "memory"
STOR_TYPE_DISK = "disk"
STOR_TYPE_FLASH = "flash"
STOR_TYPE_UNKNOWN = "unknown"

MEMORY_INFRASTRUCTURE = "memory"
DISK_INFRASTRUCTURE = "disk"
SS_INFRASTRUCTURE = "sharedstorage"

STOR_SS_NONE = 0
STOR_SS_ONLY = 1
STOR_SS_MIX = 2

LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager'

MDADM_VERSION = ""
MDADM_VERSION_3_3 = "v3.3"
RAID1_DATA_OFFSET = ""

# lvm
DEFAULT_SNAPSHOT_SIZE = 5
DEFAULT_SPACE_RATIO = 0.9
THINPOOL_METADATA_SIZE = 256 * 1024  # KiB
LV_CHUNKSIZE = 4096  # KiB


#
# -------------------------------- NEW VV SECTION START ------------------------------------
#


def vv_load_conf(fname, setup_info):
    debug('Enter vv_load_conf ...')
    #
    # retrieve all configuration info from a Json file
    #
    rc = 0
    try:
        cfg_file = open(fname, 'r')
        cfg_str = cfg_file.read()
        cfg_file.close()
        setup_info['configure'] = json.loads(cfg_str)
        configure_str = json.dumps(setup_info['configure'], indent=4, separators=(',', ': '))
        cfg_file = open('/tmp/atlas.json', 'w')
        cfg_file.write(configure_str)
        cfg_file.close()
    except:
        debug("CAUTION: Cannot load configure json file:", fname)
        rc = 1
    if rc == 0:
        configure = setup_info['configure']

        setup_info['gap_ratio'] = 0
        if configure.has_key('gapratio'):
            setup_info['gap_ratio'] = configure['gapratio']
        if setup_info['gap_ratio'] == 0:
            setup_info['gap_ratio'] = GAP_RATIO_MAX

        if configure.has_key('sharedstoragefirst'):
            setup_info['sharedstoragefirst'] = configure['sharedstoragefirst']
        else:
            setup_info['sharedstoragefirst'] = True  # always put sharedstorage first by default

        if configure.has_key('chunk_size'):
            setup_info['chunk_size'] = configure['chunk_size']
        else:
            setup_info['chunk_size'] = 512  # 512K by default
        if setup_info['chunk_size'] <= 0:
            setup_info['chunk_size'] = 512

        setup_info['storagetype'] = STOR_TYPE_UNKNOWN
        if configure.has_key('roles'):
            for the_role in configure['roles']:
                if the_role == 'CAPACITY_POOL':
                    setup_info['storagetype'] = STOR_TYPE_DISK
                elif the_role == 'MEMORY_POOL':
                    setup_info['storagetype'] = STOR_TYPE_MEMORY
        elif configure.has_key('storagetype'):
            if configure['storagetype'] in ALL_DISK_STORAGE_TYPES:
                setup_info['storagetype'] = STOR_TYPE_DISK
            elif configure['storagetype'] in ALL_DISK_STORAGE_TYPES:
                setup_info['storagetype'] = STOR_TYPE_MEMORY
    return rc


def vv_dev_has_children(dev):
    if (dev.has_key(DEV_children) == True) and (len(dev[DEV_children]) > 0):
        return True
    return False


def vv_create_vol_link(md_dev, exportname):
    debug("Enter vv_create_vol_link (%s %s) ..." % (md_dev, exportname))
    cmd_str = CMD_LINK + " -f -s " + md_dev + " " + "/dev/usx-" + exportname
    rc = do_system(cmd_str)
    return rc


def vv_infrastructure_substitue_raid1_uuid(setup_info, old_uuid, new_uuid):
    infrastructure_dict = setup_info['infrastructure']
    for the_key in infrastructure_dict:
        for the_raid in infrastructure_dict[the_key]:
            for the_raid1 in the_raid[DEV_children]:
                if vv_dev_has_children(the_raid1) == False:
                    continue  # ignore shared storage
                if the_raid1[DEV_uuid] == old_uuid:
                    the_raid1[DEV_uuid] = new_uuid
                    return 0
    return 1


def vv_infrastructure_substitue_raid_uuid(setup_info, old_uuid, new_uuid):
    infrastructure_dict = setup_info['infrastructure']
    for the_key in infrastructure_dict:
        for the_raid in infrastructure_dict[the_key]:
            if the_raid[DEV_uuid] == old_uuid:
                the_raid[DEV_uuid] = new_uuid
                return 0
    return 1


#
# action: 0 for GET, 1 for POST
#         2 get form local file, 3 save to local file
#
def vv_access_detail(setup_info, action):
    debug("Enter vv_access_detail, action:%d" % (action))
    vv_uuid = setup_info['vv_uuid']
    local_filename = "/tmp/detail-" + vv_uuid
    detail_url = "/usxmanager/usx/inventory/volume/resources/" + vv_uuid + "/detail"  # USX 2.0 REST API
    conn = urllib2.Request("http://127.0.0.1:8080" + detail_url)
    conn.add_header('Content-type', 'application/json')

    if action == 0:
        try:
            debug('vv_access_detail: Try to load detail from USX Manager')
            res = urllib2.urlopen(conn)
            res_data = json.load(res)
            data = res_data['detail']
            res.close()
            debug('vv detail response: ',
                  json.dumps(json.loads(data), sort_keys=True, indent=4, separators=(',', ': ')))
            if len(data) > 0:
                detail_dict = json.loads(data)
                setup_info['infrastructure'] = detail_dict
                return 0
        except:
            debug('vv_access_detail: Can not load detail from USX Manager')

        if ha_check_enabled():
            debug('vv_access_detail: Try to load detail from Pacemaker')
            json_str = ha_get_conf_from_crm('raid', vv_uuid)
            if json_str != '':
                setup_info['infrastructure'] = json.loads(json_str)
                return 0
            else:
                debug("vv_access_detail: Can not load detail from Pacemaker")

        debug('vv_access_detail: Try to load detail from local file')
        fname = '/etc/ilio/pool_infrastructure_' + vv_uuid + '.json'
        if os.path.exists(fname) == True:
            cfg_file = open(fname, 'r')
            s = cfg_file.read()
            setup_info['infrastructure'] = json.loads(s)
            cfg_file.close()
            return 0
        debug("vv_access_detail: Can not load detail from local file")

        setup_info['infrastructure'] = {}
        return 1

    elif action == 1:
        infrastructure = setup_info['infrastructure']
        detail_str = json.dumps(infrastructure)
        debug('VV detail: ', detail_str)
        data = {
            'detail': detail_str
        }
        try:
            res = urllib2.urlopen(conn, json.dumps(data))
            debug('POST returned response code: ' + str(res.code))
            res.close()
        except:
            debug('ERROR: failed to save infrastructure to USX Manager')

    elif action == 2:
        f = open(local_filename, 'r')
        data = f.read()
        detail_dict = json.loads(data)
        setup_info['infrastructure'] = detail_dict

    elif action == 3:
        infrastructure = setup_info['infrastructure']
        detail_str = json.dumps(infrastructure, indent=4, separators=(',', ': '))
        f = open(local_filename, 'w')
        f.write(detail_str)
        f.close()

    elif action == 4:
        debug('vv_access_detail: Try to load detail from local file')
        fname = '/etc/ilio/pool_infrastructure_' + vv_uuid + '.json'
        if os.path.exists(fname) == True:
            cfg_file = open(fname, 'r')
            s = cfg_file.read()
            setup_info['infrastructure'] = json.loads(s)
            cfg_file.close()
            return 0
        debug("vv_access_detail: Can not load detail from local file")

        setup_info['infrastructure'] = {}
        return 1

    return 0


def vv_save_infrastructure(setup_info):
    debug("Enter vv_save_infrastructure ...")
    rc = vv_access_detail(setup_info, 1)
    # rc = vv_access_detail(setup_info, 3)
    if rc != 0:
        debug('vv_save_infrastructure: Can not save detail to USX Manager')

    # Save to local file
    vv_uuid = setup_info['vv_uuid']
    infrastructure_dict = setup_info['infrastructure']
    infrastructure_str = json.dumps(infrastructure_dict, indent=4, separators=(',', ': '))
    fname = '/etc/ilio/pool_infrastructure_' + vv_uuid + '.json'
    cfg_file = open(fname, 'w')
    cfg_file.write(infrastructure_str)
    cfg_file.close()
    debug('vv_save_infrastructure: Save detail to local file')


def vv_save_c_infrastructure(setup_info):
    debug("Enter vv_save_c_infrastructure ...")
    try:
        vv_uuid = setup_info['vv_uuid']
        c_infrastructure_dict = setup_info['c_infrastructure']
        c_infrastructure_str = json.dumps(c_infrastructure_dict, indent=4, separators=(',', ': '))
    except:
        debug("save_c_infrastructure cannot get data!")
        return

    fname = '/etc/ilio/c_pool_infrastructure_' + vv_uuid + '.json'
    cfg_file = open(fname, 'w')
    cfg_file.write(c_infrastructure_str)
    cfg_file.close()


#
# create a volume from the given device list
# dev_name_list: list of devices like ["/dev/md6", "/dev/md7"]
#
def vv_create_volume_group(vg_dev_list, vgname_str):
    debug("vv_create_volume_group: start ...")
    if len(vg_dev_list) == 0:
        return 1

    devices = ' '.join(vg_dev_list)
    # Force to create the PVs, erase any previous data!
    pvremove_cmd_str = CMD_PVREMOVE + ' -ff -y ' + ' ' + devices
    rc = do_system(pvremove_cmd_str)
    pvcreate_cmd_str = CMD_PVCREATE + ' -ff -y ' + ' ' + devices
    rc = do_system(pvcreate_cmd_str);
    if rc != 0:
        return rc

    vgcreate_cmd_str = CMD_VGCREATE + ' ' + vgname_str + ' ' + devices
    rc = do_system(vgcreate_cmd_str);
    if rc != 0:
        return rc

    vgactive_cmd_str = CMD_VGACTIVE + ' ' + vgname_str
    rc = do_system(vgactive_cmd_str)
    return rc


#
def vv_set_ibd_list_size(ibd_dev_list):
    debug("Enter vv_set_ibd_list_size ...")
    for the_ibd in ibd_dev_list:
        the_idx = the_ibd[IBD_index]
        sizefilename = '/sys/class/block/ibd' + str(the_idx) + '/size'
        try:
            sizefile = open(sizefilename, 'r')
            size_str = sizefile.read()
            sizefile.close()
        except:
            debug("Cannot get ibd size from " + sizefilename)
            the_ibd[IBD_size] = 0
        else:
            the_ibd[IBD_size] = int(size_str) * 512
    return


def vv_parted_ibd_devices(vv_setup_info):
    debug('Enter vv_parted_ibd_devices...')
    ibd_dev_list = vv_setup_info['ibd_dev_list']
    for the_ibd in ibd_dev_list:
        cmd_str = CMD_MDADM + " --zero-superblock " + the_ibd[IBD_devname]
        do_system_timeout(cmd_str, 10)
        rc = parted_device(the_ibd[IBD_devname], START_SECTOR, -1, 'p1')
        if rc != 0:
            return rc
    return 0


def vv_parted_raid1_devices(vv_setup_info):
    debug('Enter vv_parted_raid1_devices...')
    plan_list = vv_setup_info['plan_list']

    for the_plan in plan_list:
        the_raid1_list = the_plan[PLAN_raid1list]
        raid_type = the_plan[PLAN_raidtype]
        for the_raid1 in the_raid1_list:
            if raid_type == RAID_raid1 and the_plan[PLAN_exportname] is None:
                continue
            if the_raid1['iscache']:
                continue
            the_devname = the_raid1[DEV_devname]
            cmd_str = CMD_MDADM + " --zero-superblock " + the_devname
            do_system_timeout(cmd_str, 10)
            rc = parted_device(the_devname, START_SECTOR, -1, 'p1')
            if rc != 0:
                return rc
    return 0


def is_cache_raid_brick(raidbrick_dict):
    cache_dev = raidbrick_dict.get('cachedev')
    if cache_dev and int(cache_dev) == 1:
        return True
    return False


def vv_zero_cachedev_header(wc_dev, rc_dev=None):
    debug('WARNING: start to zero the cache device header...')
    for dev in [wc_dev, rc_dev]:
        if dev is None:
            continue
        out = ['']
        cmd_str = 'blockdev --getsz %s' % dev
        do_system(cmd_str, out)
        count_num = int(out[0].strip()) / 2 / 1024
        cmd_str = 'dd if=/dev/zero of=%s bs=1M count=%s oflag=direct conv=notrunc' % (dev, count_num)
        try:
            # Set the timeout as 300s.
            do_system_timeout(cmd_str, 300)
            debug('Zero on cache device finished.')
        except:
            # Ignore the timeout error.
            debug('WARNNING: cannot zero the header of %s' % dev)


def vv_create_ibd_lvm(target_dev, wc_dev):
    debug('Entering vv_create_ibd_lvm:')
    ibd_target_vg = 'ibd-target-vg'
    ibd_target_lv = 'ibd-target-lv'
    ibd_wc_vg = 'ibd-wc-vg'
    ibd_wc_lv = 'ibd-wc-lv'

    lvm_dev = [(target_dev, ibd_target_vg, ibd_target_lv), (wc_dev, ibd_wc_vg, ibd_wc_lv)]
    for lvm in lvm_dev:
        ret = vv_create_lvm(lvm[0], lvm[1], lvm[2])
        if ret != 0:
            debug('ERROR: failed to create lvm for ibdserver!')
            return ret
    ibd_target_dev = '/dev/%s/%s' % (ibd_target_vg, ibd_target_lv)
    ibd_wc_dev = '/dev/%s/%s' % (ibd_wc_vg, ibd_wc_lv)
    vv_create_vol_link(ibd_target_dev, 'lv-target')
    vv_create_vol_link(ibd_wc_dev, 'lv-wc')
    return 0


def vv_start_ibd_lvm(target_dev, wc_dev):
    debug('Entering vv_start_ibd_lvm:')
    ibd_target_vg = 'ibd-target-vg'
    ibd_target_lv = 'ibd-target-lv'
    ibd_wc_vg = 'ibd-wc-vg'
    ibd_wc_lv = 'ibd-wc-lv'

    # Run udevadm first.
    udev_trigger()

    lvm_dev = [(target_dev, ibd_target_vg, ibd_target_lv), (wc_dev, ibd_wc_vg, ibd_wc_lv)]
    for lvm in lvm_dev:
        ret = vv_start_lvm(lvm[0], lvm[1], lvm[2])
        if ret != 0:
            debug('ERROR: failed to create lvm for ibdserver!')
            return ret
    ibd_target_dev = '/dev/%s/%s' % (ibd_target_vg, ibd_target_lv)
    ibd_wc_dev = '/dev/%s/%s' % (ibd_wc_vg, ibd_wc_lv)
    vv_create_vol_link(ibd_target_dev, 'lv-target')
    vv_create_vol_link(ibd_wc_dev, 'lv-wc')
    return 0


def vv_create_lvm(devname, vgname, lvname):
    # Backup first 1MB of underlying device
    cmd_str = 'dd if=%s of=/etc/ilio/gpt.backup bs=1M count=1' % devname
    rc = do_system(cmd_str)
    if rc != 0:
        return rc

    # Cleanup any GPT leftover
    cmd_str = 'dd if=/dev/zero of=%s bs=1M count=1' % devname
    rc = do_system(cmd_str)
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
    if is_snapshot_enabled(milio_config.atltis_conf) != True or (
                milio_settings.enable_new_ibdserver and lvname in ['ibd-wc-lv']):
        cmd_str = 'lvcreate -l %d --contiguous y --zero n -n %s %s' % (free_extents, lvname, vgname)
    else:
        # Reserve for thinpool metadata
        metadata_size = int(free_extents * 0.001) * extent_size  # KiB
        if metadata_size < THINPOOL_METADATA_SIZE:
            metadata_size = THINPOOL_METADATA_SIZE

        # FIXME: '--zero n' will cause the snapshots first 4k got zeroed latter, lvm bug?
        free_extents = free_extents - metadata_size / extent_size
        lvsize = free_extents * extent_size

        # Reserve disk space for snapshot
        lvsize = lvsize - int(milio_config.snapshot_space) * 1024 * 1024
        debug('original volume size is {size}G'.format(size=milio_config.original_volumesize))
        debug('created volume size is {size}G'.format(size=lvsize / 1024 / 1024))

        if lvname in ['ibd-target-lv']:
            if int(lvsize / 1024 / 1024) < 1:
                errormsg('lvsize less than 1G')
                return 1

        # For write cache internal logical volume, if snapshot enabled, raid planner will assign double space.
        # The logical volume should use 50% space, remaining should be for snapshot.
        if lvname in ['ibd-wc-lv']:
            lvsize = milio_config.wc_size / 2 * 1024 * 1024
        cmd_str = 'lvcreate -V %dk -l %d --poolmetadatasize %dk --chunksize %dk -n %s --thinpool %s/%s' \
                  % (lvsize, free_extents, metadata_size, LV_CHUNKSIZE, lvname, vgname, vgname + 'pool')

    rc = do_system(cmd_str)
    if rc != 0:
        return rc

    if is_snapshot_enabled(milio_config.atltis_conf) and lvname not in ['ibd-wc-lv']:
        # Disable zeroing of thinpool, double the performance!
        cmd_str = 'lvchange -Z n %s/%spool' % (vgname, vgname)
        rc = do_system(cmd_str)

    # Log the result partition table.
    cmd_str = 'lvs -a -o +seg_start_pe,seg_pe_ranges'
    rc = do_system(cmd_str)
    return rc


def vv_start_lvm(devname, vgname, lvname):
    # cmd_str = 'vgchange -ay %s' % vgname
    # for i in range(5):
    #     try:
    #         rc = do_system_timeout(cmd_str, 10)
    #     except timeout_error, e:
    #         rc = 1
    #     if rc == 0:
    #         break
    #     time.sleep(5)
    # if rc != 0:
    #     debug('ERROR: failed to active lvm %s.' % vgname)
    rc = vgchange_active_sync(vgname)
    return rc


@singleton
class StorageMakerMgr(object):
    """The manager of storage makers. Could get the storage maker by type.
    """

    def __init__(self):
        self._sm_map = {
            'lvm_dedup': 'LvmDedupStorageMaker',
            'blk_zvol_ext4': 'BlkZvolExt4StorageMaker',
        }

    def get(self, type, target_dev, cache_dev, **kw):
        if type.lower() in self._sm_map:
            return globals()[self._sm_map[type.lower()]](target_dev, cache_dev, **kw)
        return None


class StorageError(Exception):
    """the base exception for StorageMaker

     Attributes:
         value (Exception): parent class
     """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class StorageMaker(object):
    """
    The parent class for exporting storage.If inherit this class
    these methods called by _make() may be implemented.
    """
    NEW_IBDMANAGER = '/usr/local/bin/ibdmanager.new'
    OLD_IBDMANAGER = '/usr/local/bin/ibdmanager.org'
    IBDMANAGER = '/usr/local/bin/ibdmanager'
    NEW_IBDSERVER = '/usr/local/bin/ibdserver.new'
    OLD_IBDSERVER = '/usr/local/bin/ibdserver.org'
    IBDSERVER = '/usr/local/bin/ibdserver'
    IBDSERVERCONFIGFILE_DEF = '/etc/ilio/ibdserver.conf'
    IBDSERVERCONFIGFILE_TMP = '/etc/ilio/ibdserver.conf.tmp'
    IBDSERVERCONFIGFILE_UP = '/etc/ilio/ibdserver.conf.upgrade'

    def __init__(self, target_dev, cache_dev, **kw):
        """Initialize the storage maker by devices.

        Args:
            target_dev (Type): target device for ibdserver
            cache_dev (Type): cache device for ibdserver
            **kw (Type): reserved for more supporting.
        """
        self._target_dev = target_dev
        self._cache_dev = cache_dev
        self._export_name = milio_config.volume_uuid
        self._tg_dev = None
        self._wc_dev = None
        self._rc_dev = None
        self._exp_dev = None
        self._zvol_dev = None
        self._is_memory_cache = kw.get('is_memory_cache', False)
        self._is_need_reconstruct = False

    def create(self):
        try:
            self._is_create = True
            self._make()
            return 0
        except Exception, e:
            debug('StorageMaker.create:%s' % e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            debug(repr(traceback.format_exception(exc_type, exc_value,
                                                  exc_traceback)))
            return 1

    def start(self):
        try:
            self._is_create = False
            self._make()
            return 0
        except Exception, e:
            debug('StorageMaker.start:%s' % e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            debug(repr(traceback.format_exception(exc_type, exc_value,
                                                  exc_traceback)))
            return 1

    def _make(self):
        """
            The main logic of making storage.
                +------------+
                |make cache  |
                |dev on raid1|
                |            |
                +-----+------+
                      |
                      |
                +-----v------+        +------------+
                |zero cache  |        |make target |
                |dev when    +-------->dev on raid5|
                |creating    |        |            |
                +---+--------+        +------+-----+
                    |                        |
                    |                        |
                    |                        |
                    |     +-------------+    |
                    |     |configure ibd|    |
                    +-----+server and   | <--+
                          |start it     |
                          +------+------+
                                 |
                                 |
                          +------v------+
                          |configure ibd|
                          |agent and    |
                          |start it     |
                          +------+------+
                                 |
                                 |
                          +------v------+
                          |make storage |
                          |on ibd       |
                          |             |
                          +------+------+
                                 |
                                 |
                          +------v------+
                          |create fixed |
                          |link for     |
                          |exporting    |
                          +-------------+

        Returns:
            None exception be catched means success.
        """
        debug('INFO: Entering _mk_cache')
        self._mk_cache()
        debug('INFO: Entering _zero_cache')
        self._zero_cache()
        debug('INFO: Entering _mk_target')
        self._mk_target()
        debug('INFO: Entering _mk_ibdserver')
        self._mk_ibdserver()
        debug('INFO: Entering _mk_ibdagent')
        self._mk_ibdagent()
        debug('INFO: Entering _mk_storage')
        self._mk_storage()
        debug('INFO: Entering _mk_storage_exp_link')
        self._mk_storage_exp_link()

    def _mk_cache(self):
        """Make the cache device for ibd server.
        In default workflow, do nothing.if you want change the usage of the
        raid1 device. you should override this method.

        Note:
            Should call the _mk_wc_link()/ _mk_rc_link() to create cache device
            links for starting ibdserver.

        Returns:
            None exception be catched means success.
        """
        pass

    def _mk_target(self):
        """Make the target device for ibd server.
        In default workflow, do nothing.if you want change the usage of the
        raid5 device. you should override this method.

        Note:
            Should call the  _mk_tg_link() to target device
            links for starting ibdserver.

        Returns:
            None exception be catched means success.
        """
        pass

    def _mk_ibdserver(self):
        """Configure the ibdserver and then start it.you could override this
        method to start ibd server by your way.

        Returns:
            None exception be catched means success.

        Raises:
            StorageError: Failed to add channel to ibserver.
        """
        if not milio_settings.enable_manual_ibdserver_config:
            reset_ibdserver_config()
            config_support_volume()
        self._can_open_devices([self._tg_dev, self._wc_dev, self._rc_dev])
        if not self._is_need_reconstruct:
            self._ibd_precheck()
        ret = apply_new_drw_channel(self._export_name, self._tg_dev, self._wc_dev, self._rc_dev)
        if ret != 0:
            raise StorageError('ERROR: failed to apply new channel to ibdserver.')

    def _mk_ibdagent(self):
        """Configure the local ibd server to ibd agent..you could override this
        method to start ibd agent by your way.

        Returns:
            None exception be catched means success.

        Raises:
            StorageError: Failed to add ibd server to ibd agent.
        """
        ibd_idx = next_available_ibd_idx(1)
        ibd_conf = {}
        ibd_dev_name = '/dev/ibd%s' % ibd_idx
        ibd_conf['devuuid'] = self._export_name
        ibd_conf['cacheip'] = '127.0.0.1'
        ibd_conf['devexport'] = ibd_dev_name
        ibd_conf['minornum'] = ibd_idx * 16
        ret = add_ibd_channel(ibd_conf)
        if ret != 0:
            raise StorageError('ERROR: add ibdserver failed.')
        if ibd_agent_alive() is False:
            cmd_str = CMD_IBDAGENT
        else:
            cmd_str = CMD_IBDMANAGER_A_UPDATE
        ret = do_system(cmd_str)
        if ret != 0:
            raise StorageError('ERROR: update ibdagent failed.')
        # wait until the ibd working
        while not IBDManager.is_ibd_working(self._export_name):
            time.sleep(1)

        self._ibd_dev = ibd_dev_name

    def _mk_storage(self):
        """In default workflow, do nothing. you must override this method at
        the sub class.

        Note:
            Should set the property _zvol_dev  to appoint the export device.

        Returns:
            None exception be catched means success.
        """
        pass

    def _mk_storage_exp_link(self):
        """Create the export link for the exporting device. If you want to
        change the exporting link, you could override this method.

        Returns:
            None exception be catched means success.
        """
        vv_create_vol_link(self._exp_dev, self._export_name)

    def _zero_cache(self, is_force=False):
        if not self._is_create and not is_force:
            debug('active cahce device, skip zero_cache')
            return
        cache_dev = [self._wc_dev, self._rc_dev]
        for dev in cache_dev:
            if dev is None:
                continue
            out = ['']
            cmd_str = 'blockdev --getsz %s' % dev
            do_system(cmd_str, out)
            # dd zero on wc header and tail
            # header:size_in_byte/4096*32 + 10M
            # tail: last 40M.
            dd_tail_size = 40
            count_num = int(out[0].strip()) / 2 / (1024 * 1024) * 8 + 10
            cmd_str = 'dd if=/dev/zero of=%s bs=1M count=%s oflag=sync conv=notrunc' % (dev, count_num)
            try:
                # Set the timeout as 300s.
                do_system_timeout(cmd_str, 300)
                debug('Zero on cache device header finished.')
            except:
                # Ignore the timeout error.
                debug('WARNNING: cannot zero the header of %s' % dev)
            seek_num = int(out[0].strip()) / 2 / 1024 - int(dd_tail_size)
            cmd_str = 'dd if=/dev/zero of=%s bs=1M seek=%s count=%s oflag=sync conv=notrunc' % (
                dev, seek_num, dd_tail_size)
            try:
                # Set the timeout as 300s
                do_system_timeout(cmd_str, 300)
                debug('Zero on cache device tail finished.')
            except:
                # Ignore the timeout error.
                debug('WARNNING: cannot zero the tail of %s' % dev)

    def _mk_wc_link(self, wc_dev):
        vv_create_vol_link(wc_dev, 'default-wc')
        self._wc_dev = '/dev/usx-default-wc'

    def _mk_rc_link(self, rc_dev):
        vv_create_vol_link(rc_dev, 'default-rc')
        self._rc_dev = '/dev/usx-default-rc'

    def _mk_tg_link(self, tg_dev):
        vv_create_vol_link(tg_dev, 'default-tg')
        self._tg_dev = '/dev/usx-default-tg'

    def _mk_zvol(self, target_dev, log_dev=None):
        link_dev = '/dev/usx-default-zvol'
        if self._is_create:
            fs_dev = FsManager().get_dev('zfs', target_dev)
            ret = fs_dev.init_block_dev(log_dev)
            if ret != 0:
                raise StorageError('ERROR: init zvol failed.')
            ret = fs_dev.export_block_dev(link_dev)
            if ret != 0:
                raise StorageError('ERROR: create link for zvol failed.')
        else:
            try:
                os.mkdir('/dev/usx/')
            except:
                pass
            cmd = 'ln -f -s %s /dev/usx/usx-zvol-dev' % target_dev
            do_system(cmd)
            # raid device will be parted by zvol.
            dev_part1 = target_dev + '-part1'
            dev_part9 = target_dev + '-part9'
            if os.path.exists(dev_part1):
                cmd = 'ln -f -s %s /dev/usx/usx-zvol-dev-part1' % dev_part1
                do_system(cmd)
            if os.path.exists(dev_part9):
                cmd = 'ln -f -s %s /dev/usx/usx-zvol-dev-part9' % dev_part9
                do_system(cmd)

            if log_dev is not None:
                cmd = 'ln -f -s %s /dev/usx/usx-zvol-log-dev' % log_dev
                do_system(cmd)
            target_dev = '/dev/usx/usx-zvol-dev'
            fs_dev = FsManager().get_dev('zfs', target_dev)
            ret = fs_dev.start_block_dev()
            if ret != 0:
                raise StorageError('ERROR: start zvol failed.')
            ret = fs_dev.export_block_dev(link_dev)
            if ret != 0:
                raise StorageError('ERROR: create link for zvol failed.')
        self._zvol_dev = link_dev

    def _can_open_devices(self, dev_list):
        """check if the device can be opened.

        Args:
            dev_list (list): the devices which need to checked.

        Returns:
            None exception be catched means success.
        """
        for dev in dev_list:
            if dev is None:
                continue
            # wait 30 seconds.
            deadtime = time.time() + 30
            can_open = False
            while not can_open and time.time() < deadtime:
                try:
                    with open(dev, 'r+'):
                        debug('DEBUG: can open device %s' % dev)
                        can_open = True
                except IOError as e:
                    debug('ERROR: IO error, cannot open %s [%s]' % (dev, e))
                except Exception as e:
                    debug('ERROR: Unkown, cannnot open %s [%s]' % (dev, e))
                time.sleep(1)

            if can_open is False:
                raise StorageError('ERROR: cannot open device.')

    def _ibd_precheck(self):
        """
        Summary:
            use bwc_ver_cmp tool to check whather need to upgrade.
        Returns:
            Type: Description
        """
        if self._is_create:
            return
        (can_flush, can_zero) = IBDManager.ibd_layout_check(self._wc_dev)
        if not can_flush and not can_zero:
            return
        self._pre_ibd_upgrade()
        if can_flush :
            # need to flush data.
            debug('WARN: layout is not consistent, it need to flush write cache data.')
            self._use_old_ibd()
            ret = apply_new_drw_channel(self._export_name, self._tg_dev, self._wc_dev, self._rc_dev)
            if ret != 0:
                raise StorageError('ERROR: failed to apply new channel to ibdserver.')
            ret = IBDManager.flush_ibd_write_cache_sync()
            if ret != 0:
                raise StorageError('ERROR: flush write cache data failed.')
            stop_ibdserver()
        if can_zero:
            # need to zero device.
            debug('WARN: layout was be destroyed, it need to zero write cache header.')
            self._zero_cache(is_force=True)
        self._use_new_ibd()
        self._after_ibd_upgrade()

    def _pre_ibd_upgrade(self):
        """
        Summary:
            check whether the ibd components are enough or not.
        Returns:
            Type: Description
        """

        for elem in [self.NEW_IBDMANAGER, self.OLD_IBDMANAGER, self.NEW_IBDSERVER, self.OLD_IBDSERVER]:
            if not os.path.exists(elem):
                raise StorageError('ERROR: cannot find {} for upgrade ibd'.format(elem))

        if os.path.exists(self.IBDSERVERCONFIGFILE_DEF) and not os.path.exists(self.IBDSERVERCONFIGFILE_UP):
            shutil.copyfile(self.IBDSERVERCONFIGFILE_DEF, self.IBDSERVERCONFIGFILE_UP)

    def _after_ibd_upgrade(self):
        """
        Summary:
            Cleanup the ibd binaries to capture unexpected upgrading operation.
        Note:
            HA node could not remove ibd binaries.
        Returns:
            Type: Description
        """
        # TODO
        #
        if os.path.exists(self.IBDSERVERCONFIGFILE_UP):
            os.remove(self.IBDSERVERCONFIGFILE_UP)
        else:
            debug('WARN: miss the back up operation for ibdserver configuration file.')

    def _use_old_ibd(self):
        """
        Summary:
            In upgrading, if need to flush wc data, must use the former binaries.
        Returns:
            Type: Description
        """
        debug('INFO: use the former ibd components as default.')
        shutil.copyfile(self.OLD_IBDSERVER, self.IBDSERVER)
        shutil.copyfile(self.OLD_IBDMANAGER, self.IBDMANAGER)

    def _use_new_ibd(self):
        """
        Summary:
            after upgraded, must use the latest binaries.
        Returns:
            Type: Description
        """
        debug('INFO: use the latest ibd components as default.')
        shutil.copyfile(self.NEW_IBDSERVER, self.IBDSERVER)
        shutil.copyfile(self.NEW_IBDMANAGER, self.IBDMANAGER)



class LvmDedupStorageMaker(StorageMaker):
    """
                      +------+
                      |DEDUP |
                      |      |
                      +--^---+
                         |
                      +--+---+
                wc    | IBD  |  target
               +----> |      | <----+
               |      +------+      |
               |                    |
               |                 +--+---+
               |                 | LVM  |
               |                 |      |
               |                 +---^--+
               |                     |
            +--+---+             +---+--+
            | LVM  |             | RAID5|
       +--> |      |       +---> |      | <---+
       |    +------+       |     +---^--+     |
       |                   |         |        |
       |                   |         |        |
    +--+---+           +---+--+  +---+--+  +--+---+
    | RAID1|           | RAID1|  | RAID1|  | RAID1|
    |      |           |      |  |      |  |      |
    +-^---^+           +---^--+  +---^--+  +--^---+
      |   |                |         |        |
+-----++  +------+     +---+--+  +---+--+  +--+---+
| IBD  |  | IBD  |     | IBD  |  | IBD  |  | IBD  |
|      |  |      |     |      |  |      |  |      |
+------+  +------+     +------+  +------+  +------+

    """
    _ibd_target_vg = 'ibd-target-vg'
    _ibd_target_lv = 'ibd-target-lv'
    _ibd_wc_vg = 'ibd-wc-vg'
    _ibd_wc_lv = 'ibd-wc-lv'

    def _mk_cache(self):
        ret = do_system('/sbin/vgdisplay {}'.format(self._ibd_wc_vg))
        self._is_need_reconstruct = (ret != 0 and self._is_memory_cache)
        if self._is_need_reconstruct:
            debug('WARN: memory cache will be reconstructed.')
        if self._is_create or self._is_need_reconstruct:
            ret = vv_create_lvm(self._cache_dev, self._ibd_wc_vg, self._ibd_wc_lv)
        else:
            ret = vv_start_lvm(self._cache_dev, self._ibd_wc_vg, self._ibd_wc_lv)
        if ret != 0:
            raise StorageError('ERROR: LVM failed when making cache device.')

        self._mk_wc_link('/dev/%s/%s' % (self._ibd_wc_vg, self._ibd_wc_lv))

    def _mk_target(self):
        if self._is_create:
            ret = vv_create_lvm(self._target_dev, self._ibd_target_vg, self._ibd_target_lv)
        else:
            ret = vv_start_lvm(self._target_dev, self._ibd_target_vg, self._ibd_target_lv)
        if ret != 0:
            raise StorageError('ERROR: LVM failed when making target device.')

        self._mk_tg_link('/dev/%s/%s' % (self._ibd_target_vg, self._ibd_target_lv))

    def _mk_storage(self):
        self._exp_dev = self._ibd_dev


class BlkZvolExt4StorageMaker(StorageMaker):
    """
                              +------+
                              | EXT4 |
                              |      |
                              +--^---+
                                 |
                              +--+---+
                   WC         | IBD  |  TARGET
               +------------> |      | <----+
               |              +------+      |
               |                            |
               |                         +--+---+
               |                         | ZVOL |
               |                         |      |
               |                         +---^--+
               |                             |
               |                         +---+--+
               |                         | RAID5|
               |                   +---> |      | <---+
               |                   |     +---^--+     |
               |                   |         |        |
               |                   |         |        |
            +--+---+           +---+--+  +---+--+  +--+---+
            | RAID1|           | RAID1|  | RAID1|  | RAID1|
            |      |           |      |  |      |  |      |
            +-^---^+           +---^--+  +---^--+  +--^---+
              |   |                |         |        |
        +-----++  +------+     +---+--+  +---+--+  +--+---+
        | IBD  |  | IBD  |     | IBD  |  | IBD  |  | IBD  |
        |      |  |      |     |      |  |      |  |      |
        +------+  +------+     +------+  +------+  +------+

    """

    def _mk_cache(self):
        self._mk_wc_link(self._cache_dev)

    def _mk_target(self):
        property_dict = {}
        # TODO: about robo mode, the target device will be parted (/dev/md0p1)
        raid_detail(self._target_dev, property_dict)
        target_dev = '/dev/disk/by-id/md-uuid-%s' % property_dict[RAID_UUID]
        # None log deivce just now.
        log_dev = None
        self._mk_zvol(target_dev, log_dev)
        self._mk_tg_link(self._zvol_dev)

    def _mk_storage(self):
        self._exp_dev = self._ibd_dev


def vv_create_export_storage(vv_setup_info):
    debug('Entering vv_create_export_storage...')
    if not milio_config.is_fastfailover:
        debug('Old version export mode! Skip....')
        return 0

    plan_list = vv_setup_info["plan_list"]
    is_memory_cache = False
    for the_plan in plan_list:
        if the_plan.get('iscache') is True:
            cache_dev = the_plan[PLAN_devname]
            is_memory_cache = the_plan[PLAN_type].upper() == 'MEMORY'
        elif the_plan.get('exportname') is not None:
            target_dev = the_plan[PLAN_devname]

    ret = StorageMakerMgr().get(milio_settings.storage_mode, target_dev, cache_dev, is_memory_cache=is_memory_cache).create()
    if ret != 0:
        return ret
    return 0


def vv_start_export_storage(vv_setup_info):
    debug('Entering vv_start_export_storage...')
    if not milio_config.is_fastfailover:
        debug('Old version export mode! Skip....')
        return 0
    c_infrastructure = vv_setup_info['c_infrastructure']
    the_plan = None
    for raid_plan in c_infrastructure['disk']:
        if 'iscache' in raid_plan and raid_plan['iscache']:
            the_plan = raid_plan
            cache_dev = vv_get_md_by_uuid(raid_plan['uuid'])
            is_memory_cache = False
        elif raid_plan.get('exportname') is not None:
            target_dev = vv_get_md_by_uuid(raid_plan['uuid'])
            if raid_plan['raidtype'] == 'RAID_1':
                target_dev = target_dev + 'p1'
    if 'memory' in c_infrastructure:
        for raid_plan in c_infrastructure['memory']:
            if 'iscache' in raid_plan and raid_plan['iscache']:
                the_plan = raid_plan
                cache_dev = vv_get_md_by_uuid(raid_plan['uuid'])
                is_memory_cache = True

    if the_plan is None:
        debug('Old version export mode! skip start....')
        return 1

    ret = StorageMakerMgr().get(milio_settings.storage_mode, target_dev, cache_dev, is_memory_cache=is_memory_cache).start()
    if ret != 0:
        return ret
    return 0


def vv_get_md_by_uuid(uid):
    deadtime = time.time() + 30
    while time.time() < deadtime:
        md_dev = get_md_by_uuid(uid)
        if md_dev:
            return md_dev
        time.sleep(0.25)
    return None


def setup_lodev(file_path):
    cmd_str = 'losetup -f'
    out = ['']
    do_system(cmd_str, out)
    lodevname = out[0].strip()
    cmd_str = 'losetup ' + lodevname + ' ' + file_path
    rc = do_system(cmd_str, out)
    if rc != 0:
        debug("ERROR: can not setup lo device for %s" % file_path)
        return None
    return lodevname


def vv_create_cache_storage(vv_setup_info):
    debug('Entering vv_create_cache_storage...')
    plan_list = vv_setup_info['plan_list']
    dev_name = None
    dev_size = None
    is_support_cache = False
    for the_plan in plan_list:
        the_raid1_list = the_plan[PLAN_raid1list]
        for the_raid1 in the_raid1_list:
            if the_raid1['iscache']:
                dev_name = the_raid1[DEV_devname]
                is_support_cache = True
                break
        for the_shared in the_plan[PLAN_sharedlist]:
            if the_shared['iscache']:
                dev_name = the_plan[PLAN_devname]
                is_support_cache = True
                break
        if is_support_cache:
            break

    if not is_support_cache:
        debug('Not support, skip the creation of cache storage.')
        return 0

    # ret = vv_parted_cache_device(vv_setup_info, dev_name, dev_size)
    # if ret != 0:
    #     return ret

    # rc = ddp_setup.simple_mount("/cachefs", dev_name+"p3")
    # if rc != 0:
    #     debug('ERROR: Cannot mount cache dedup filesystem.')
    #     return 1

    # # The wc_size and rc_size units are G bytes.
    # wc_size = vv_get_wc_size_from_dedupfs()
    # if wc_size == 0:
    #     debug('ERROR: invalid size for write cache.')
    #     return 1
    # ret = do_system('truncate --size %dG /cachefs/usx-default-wc' % wc_size)
    # if ret != 0:
    #     debug('ERROR: cannot truncate on cachefs.')
    # # vv_create_vol_link('/cachefs/usx-default-wc', 'default-wc')
    ret = vv_create_vol_link('%s' % dev_name, 'default-wc')
    return ret


# Handles different cache device methods for ALL_FLASH and HYBRID volumes.
def vv_start_cache_device(vv_setup_info, dev_name):
    # TODO: vv_start code path do not have 'configure'
    # volume_type = vv_setup_info['configure']['volumeresources'][0]['volumetype']
    if False:
        # create link for ibdserver configuration.
        # the first partition of raid1 is for read cache.
        # the second partition of raid1 is for write cache.
        vv_create_vol_link('%sp1' % dev_name, 'default-wc')
        # vv_create_vol_link('%sp2' % dev_name, 'default-rc')
        return 0
    else:
        # NOTICE: For HYBRID volume only.
        # Only LTS HYBRID volume use dedupfs as cache device.
        # (part_one_sz, part_two_sz) = vv_divide_raid1_size(milio_config.wc_size)

        # rc = ddp_setup.simple_mount("/cachefs", dev_name+"p3")
        # if rc != 0:
        #     debug('ERROR: Cannot mount cache dedup filesystem.')
        #     return 1

        # The wc_size and rc_size units are G bytes.

        # lodevname = setup_lodev(dev_name+'p2')
        # if lodevname == None:
        #     debug("ERROR: can not setup lo device for cache dev.")
        #     return 1

        vv_create_vol_link('%s' % dev_name, 'default-wc')
        # vv_create_vol_link('/cachefs/usx-default-wc', 'default-wc')
        # vv_create_vol_link(lodevname, 'default-loop')
        return 0


# Handles different cache device methods for ALL_FLASH and HYBRID volumes.
def vv_parted_cache_device(vv_setup_info, dev_name, dev_size):
    debug('Entering vv_parted_cache_device...')
    # setup superblock
    cmd_str = CMD_MDADM + " --zero-superblock " + dev_name
    do_system_timeout(cmd_str, 10)
    # part two partitions
    # 1024*1024*2=2097152
    wc_start_sect = 2048
    wc_end_sect = -1
    # wc_end_sect = milio_config.wc_size * 2097152 + wc_start_sect
    # rc_start_sect = wc_start_sect + wc_end_sect + 1
    # rc_end_sect = -1

    # volume_type = vv_setup_info['configure']['volumeresources'][0]['volumetype']
    if False:
        ret = vv_parted_primary_device(dev_name, wc_start_sect, wc_end_sect)
        if ret != 0:
            return ret
        # ret = vv_parted_primary_device(dev_name, rc_start_sect, rc_end_sect, 2)
        return ret
    else:
        # NOTICE: For HYBRID volume only.
        # Only LTS HYBRID volume use dedupfs as cache device.
        # Setup dedupfs on the cache device
        # ret = vv_parted_primary_device(dev_name, wc_start_sect, wc_end_sect)
        # if ret != 0:
        #     return ret
        # ret = ddp_setup.config_ddp(vv_setup_info['configure'], "/cachefs", dev_name+"p1", None, None, 400, 0, is_small_mode=True)
        # if ret != 0:
        #     debug("ERROR: can not setup dedupfs for cache device!")

        (part_one_sz, part_two_sz, part_three_sz) = vv_divide_raid1_size(milio_config.wc_size)
        first_start_sect = 2048
        first_end_sect = part_one_sz * 2097152 + first_start_sect
        second_start_sect = first_end_sect + 1
        second_end_sect = second_start_sect + part_two_sz * 2097152
        third_start_sect = second_end_sect + 1
        third_end_sect = -1

        ret = vv_parted_primary_device(dev_name, first_start_sect, first_end_sect)
        if ret != 0:
            return ret
        ret = vv_parted_primary_device(dev_name, second_start_sect, second_end_sect, 2)
        if ret != 0:
            return ret
        ret = vv_parted_primary_device(dev_name, third_start_sect, third_end_sect, 3)
        if ret != 0:
            return ret

        # use the third partition as write cache device.
        ret = ddp_setup.config_ddp(vv_setup_info['configure'], "/cachefs", dev_name + "p3", None, None, 400, 0,
                                   is_small_mode=True)
        if ret != 0:
            debug("ERROR: can not setup dedupfs for cache device!")
        return ret


def vv_parted_primary_device(dev_name, start_sect, end_sect, partition_num=1):
    start_str = str(start_sect) + 's'
    end_str = str(end_sect)
    if end_sect >= 0:
        end_str = end_str + 's'
    if partition_num == 1:
        cmd_str = '/sbin/parted -s -- %s mklabel gpt mkpart primary %s %s set 1 raid on' % (
            dev_name, start_str, end_str)
    elif partition_num > 1:
        cmd_str = '/sbin/parted -s -- %s mkpart primary %s %s set %s raid on' % (
            dev_name, start_str, end_str, partition_num)

    ret = do_system_timeout(cmd_str, 30)
    if ret != 0:
        debug('ERROR: Can not parted the device [%s]' % dev_name)
    return ret


def vv_divide_raid1_size(dev_size):
    """ [divide the raid device size for zvol log device
         and write cache device]

    [
        1.the raid1 device will be pareted as three partitions
        2.the 1st partition will be used as log device and own 10 percent of the space but the max size is 4GiB,
        3.the 2nd partition will be used as write cache device and own 10 percent of the space
          and the min size is 2G, the max size is 8G
        4.the 3rd partition will be used as dedupfs and reserved.
    ]

    Arguments:
        dev_size {[int]} -- [the raid1 device size]
    """
    part_one_sz = dev_size * 10 / 100
    if part_one_sz > 4:
        part_one_sz = 4
    elif part_one_sz < 1:
        part_one_sz = 1

    part_two_sz = dev_size * 10 / 100
    if part_two_sz > 8:
        part_two_sz = 8
    elif part_two_sz < 2:
        part_two_sz = 2

    part_three_sz = dev_size - part_one_sz - part_two_sz

    return (part_one_sz, part_two_sz, part_three_sz)


def vv_get_wc_size_from_dedupfs():
    cmd = 'df -B 1m | grep /cachefs | grep -v grep'
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) > 0:
        # get the avail size of dedupfs for write cache
        wc_size = int(msg[0].split()[3]) / 1024 * 80 / 100
        if wc_size < 1:
            debug('WARNING: set the min size, the real size is not enough.')
            wc_size = 1
        return wc_size
    return 0


def vv_init_shared_devices(vv_setup_info):
    debug('Enter vv_init_shared_devices...')
    shared_dev_list = vv_setup_info['shared_dev_list']

    for the_shared in shared_dev_list:
        the_devname = the_shared[SS_devname]
        cmd_str = CMD_MDADM + " --zero-superblock " + the_devname
        do_system_timeout(cmd_str, 10)
    return 0


#
# Generate:
#   1> plan_list (PLAN_ibdlist, PLAN_sharedlist)
#   2> ibd_dev_list
#   3> shared_dev_list
#
def vv_generate_resource_info(vv_setup_info):
    debug("Enter vv_generate_resource_info: ...")
    configure = vv_setup_info['configure']
    volresources = configure[AMC_volresources]
    the_volresource = volresources[0]  # there should be only one resource
    the_raidplans = the_volresource[AMC_raidplans]
    vv_setup_info['plan_list'] = []
    plan_list = vv_setup_info['plan_list']
    vv_setup_info['ibd_dev_list'] = []
    ibd_dev_list = vv_setup_info['ibd_dev_list']
    vv_setup_info['shared_dev_list'] = []
    shared_dev_list = vv_setup_info['shared_dev_list']

    vv_setup_info['volumetype'] = the_volresource[AMC_volumetype]
    vv_setup_info['vv_uuid'] = the_volresource["uuid"]
    for the_raidplan in the_raidplans:
        the_plandetail_str = the_raidplan[AMC_plandetail]
        the_plandetail = json.loads(the_plandetail_str)
        the_plandetail_str = json.dumps(the_plandetail, indent=4, separators=(',', ': '))
        print the_plandetail_str
        for the_subplan in the_plandetail[AMC_subplans]:
            the_plan = {}
            the_plan[PLAN_uuid] = the_subplan[AMC_planuuid]
            the_plan[PLAN_type] = the_subplan[AMC_plantype]
            plan_list.append(the_plan)

            the_plan[PLAN_exportname] = None
            if the_subplan.has_key(AMC_exportname):
                the_plan[PLAN_exportname] = the_subplan[AMC_exportname]

            the_plan[PLAN_bitmap] = True
            if the_subplan.has_key(AMC_bitmap) and the_subplan[AMC_bitmap] == False:
                the_plan[PLAN_bitmap] = False

            the_plan[PLAN_chunksize] = 512
            if the_subplan.has_key(AMC_chunksize):
                the_plan[PLAN_chunksize] = the_subplan[AMC_chunksize]

            if the_subplan["raidtype"] == "RAID_5":
                the_plan[PLAN_raidtype] = RAID_raid5
            elif the_subplan["raidtype"] == "RAID_1":
                the_plan[PLAN_raidtype] = RAID_raid1
            else:
                the_plan[PLAN_raidtype] = RAID_raid0

            the_plan['iscache'] = False

            if the_subplan.has_key(AMC_raidbricks):
                all_raidbricks = the_raidplan[AMC_raidbricks]
                the_plan[PLAN_ibdlist] = []
                the_dev_list = the_plan[PLAN_ibdlist]
                for the_raidbrick in the_subplan[AMC_raidbricks]:
                    the_ibd = {}
                    for this_raidbrick in all_raidbricks:
                        if this_raidbrick[AMC_hypervisoruuid] == the_raidbrick[AMC_hypervisoruuid]:
                            the_ibd[IBD_ip] = this_raidbrick[AMC_serviceip]
                            break
                    the_ibd[IBD_uuid] = the_raidbrick[AMC_euuid]
                    the_ibd[IBD_devname] = None
                    the_ibd[IBD_raid1number] = the_raidbrick[AMC_pairnumber]
                    if the_plan[PLAN_type] == "CAPACITY":
                        the_ibd[IBD_storagetype] = "DISK"
                    elif the_plan[PLAN_type] == "MEMORY":
                        the_ibd[IBD_storagetype] = "MEMORY"
                    else:
                        the_ibd[IBD_storagetype] = "DISK"
                    the_ibd[IBD_detail] = the_raidbrick
                    the_ibd['iscache'] = the_raidbrick.get('cachedev') == 1
                    if the_ibd['iscache']:
                        the_plan['iscache'] = True
                    the_dev_list.append(the_ibd)
                    ibd_dev_list.append(the_ibd)

            the_plan[PLAN_sharedlist] = []
            the_dev_list = the_plan[PLAN_sharedlist]
            if the_subplan.has_key(AMC_sharedstorages):
                sharedstorages = the_subplan[AMC_sharedstorages]
                for the_sharedstorage in sharedstorages:
                    the_shared = {}
                    the_shared[SS_devname] = "/dev/usx/" + the_sharedstorage[AMC_euuid]
                    the_shared[SS_detail] = the_sharedstorage
                    the_shared['iscache'] = the_sharedstorage.get('cachedev') == 1
                    if the_shared['iscache']:
                        the_plan['iscache'] = True
                    # the_json_str = json.dumps(the_sharedstorage)
                    # cmd_str = python + CMD_AGGCREATE + " \'" + the_json_str + "\'"
                    # print cmd_str
                    # do_system(cmd_str)
                    the_dev_list.append(the_shared)
                    shared_dev_list.append(the_shared)

        plan_str = json.dumps(vv_setup_info['plan_list'], indent=4, separators=(',', ': '))
        print plan_str


def vv_create_ibd_resource(vv_setup_info):
    debug("Enter vv_create_ibd_resource ...")
    ibd_dev_list = vv_setup_info['ibd_dev_list']
    rc = 0

    for the_ibd in ibd_dev_list:
        the_raidbrick = the_ibd[IBD_detail]
        the_json_str = json.dumps(the_raidbrick)
        the_json_str_base64 = base64.urlsafe_b64encode(the_json_str)
        args_str = CMD_AGGCREATE + " " + the_json_str_base64
        print args_str
        for i in range(AGGCREATE_RETRY_CNT):
            (rc, out, err) = remote_exec(the_ibd[IBD_ip], 'python ', args_str)
            if rc == errno.EEXIST:
                debug("Space already exist on SVM, assume it's for us.")
                rc = 0
            if rc == 0:
                break
            time.sleep(AGGCREATE_RETRY_INTERVAL)
        if rc != 0:
            debug("Failed to allocate space for %s" % str(the_ibd))
            break
    return rc


def vv_setup_dev_list(vv_setup_info):
    debug("Enter vv_setup_dev_list: ...")
    vv_setup_info['ibd_dev_list'] = []
    ibd_dev_list = vv_setup_info['ibd_dev_list']
    vv_setup_info['shared_dev_list'] = []
    shared_dev_list = vv_setup_info['shared_dev_list']
    infrastructure_dict = vv_setup_info['infrastructure']

    for the_key in infrastructure_dict:
        for the_raid in infrastructure_dict[the_key]:
            for the_raid1 in the_raid[DEV_children]:
                if vv_dev_has_children(the_raid1) == True:
                    for the_ibd in the_raid1[DEV_children]:
                        if the_key == INFR_disk:
                            the_ibd[IBD_storagetype] = "DISK"
                        elif the_key == INFR_memory:
                            the_ibd[IBD_storagetype] = "MEMORY"

                        ibd_dev_list.append(the_ibd)
                else:
                    shared_dev_list.append(the_raid1)


#
# setup uuid_dev_mappping
#
def vv_find_ibd_mapping(setup_info):
    debug("Enter vv_find_ibd_mapping...")
    setup_info['wud_mapping'] = {}
    wud_mapping = setup_info['wud_mapping']
    cmd_str = CMD_IBDMANAGER_STAT_WUD
    out = ['']
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    for the_line in lines:
        line_parts = the_line.split(' ')
        if len(line_parts) < 2:
            continue
        exportname = line_parts[0]
        devname = line_parts[1]
        wud_mapping[exportname] = devname

    # Find any readonly ibd connections and try to upgrade them to readwrite.
    (readonly_ibd_list, readonly_uuid_list) = vv_find_working_ibd("alevel:readonly")
    for devname in readonly_ibd_list:
        debug("Trying to upgrade readonly IBD: %s" % str(devname))
        cmd_str = CMD_IBDMANAGER_A_UPGRADE + " " + devname
        rc = do_system(cmd_str)
        if rc == errno.EBUSY:
            # retry upgrade
            for i in range(5):
                time.sleep(5)
                rc = do_system(cmd_str)
                if rc == 0:
                    break
        if rc != 0:
            debug("WARNING: upgrade for IBD %s failed!" % devname)

    setup_info['rwwud_mapping'] = {}
    rwwud_mapping = setup_info['rwwud_mapping']
    cmd_str = CMD_IBDMANAGER_STAT_RWWUD
    out = ['']
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    for the_line in lines:
        line_parts = the_line.split(' ')
        if len(line_parts) < 2:
            continue
        exportname = line_parts[0]
        devname = line_parts[1]
        rwwud_mapping[exportname] = devname

    setup_info['ud_mapping'] = {}
    ud_mapping = setup_info['ud_mapping']
    cmd_str = CMD_IBDMANAGER_STAT_UD
    out = ['']
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    for the_line in lines:
        line_parts = the_line.split(' ')
        if len(line_parts) < 2:
            continue
        exportname = line_parts[0]
        devname = line_parts[1]
        ud_mapping[exportname] = devname

    return


#
# setup c_infrastructure: vv_find_ibd_mapping must be called before
#
def vv_setup_current_infrastructure(vv_setup_info):
    debug("Enter vv_setup_current_infrastructure ...")
    infrastructure_dict = vv_setup_info['infrastructure']
    vv_setup_info['c_infrastructure'] = copy.deepcopy(infrastructure_dict)
    c_infrastructure = vv_setup_info['c_infrastructure']
    wud_mapping = vv_setup_info['wud_mapping']
    rwwud_mapping = vv_setup_info['rwwud_mapping']
    ud_mapping = vv_setup_info['ud_mapping']
    update_raid1_uuid = []
    # setup devname, set its state
    for the_key in c_infrastructure:
        for the_raid in c_infrastructure[the_key]:  # raid0|raid5
            #
            # setup the_raid state:
            #   DISCONNECT | ONLINE
            #
            the_raid[DEV_state] = DEV_STATE_DISCONNECT
            the_raid[DEV_devname] = get_md_by_uuid(the_raid[DEV_uuid])
            if the_raid[DEV_devname] == None:
                the_raid[DEV_devname] = load_dev_uuid(the_raid[DEV_uuid])
            the_raid_devname = the_raid[DEV_devname]
            if the_raid_devname != None:
                the_raid[DEV_state] = DEV_STATE_ONLINE
            the_raid_type = the_raid['raidtype']
            for the_raid1 in the_raid[DEV_children]:  # raid1
                # To skip the check for shared storage
                if (the_raid1.has_key(DEV_children) == False) or (len(the_raid1[DEV_children]) == 0):
                    the_raid1[DEV_state] = DEV_STATE_ONLINE
                    the_raid1[DEV_working] = True
                    continue
                if the_raid_type == 'RAID_5':
                    update_raid1_uuid.append(the_raid1[DEV_uuid])
                the_raid1[DEV_state] = DEV_STATE_DISCONNECT
                the_raid1[DEV_working] = False
                the_raid1[DEV_devname] = get_md_by_uuid(the_raid1[DEV_uuid])
                if the_raid1[DEV_devname] == None:
                    the_raid1[DEV_devname] = load_dev_uuid(the_raid1[DEV_uuid])
                the_raid1_devname = the_raid1[DEV_devname]
                if the_raid1_devname != None:
                    the_raid1[DEV_state] = DEV_STATE_OFFLINE

                #
                # setup ibd state:
                #   DISCONNECT | OFFLINE | FAILED | ONLINE
                #
                for the_ibd in the_raid1[DEV_children]:  # ibd
                    the_ibd[IBD_state] = DEV_STATE_DISCONNECT
                    the_ibd[IBD_devname] = None
                    the_ibd_uuid = the_ibd[IBD_uuid]
                    if rwwud_mapping.has_key(the_ibd_uuid):
                        the_ibd[IBD_devname] = rwwud_mapping[the_ibd_uuid]
                        the_ibd[IBD_state] = DEV_STATE_OFFLINE
                        debug("vv_setup_current_infrastructure: ibd %s offline" % (the_ibd[IBD_devname]))
                    elif ud_mapping.has_key(the_ibd_uuid):
                        the_ibd[IBD_devname] = ud_mapping[the_ibd_uuid]
                        the_ibd[IBD_state] = DEV_STATE_DISCONNECT
                        debug("vv_setup_current_infrastructure: ibd %s disconnected " % (the_ibd[IBD_devname]))
                    else:
                        debug("vv_setup_current_infrastructure: ibd %s disconnected " % (the_ibd_uuid))

                if the_raid1[DEV_state] != DEV_STATE_DISCONNECT:
                    property_dict = {}
                    raid_detail_nohung(the_raid1_devname, property_dict)
                    debug(property_dict)
                    for the_ibd in the_raid1[DEV_children]:
                        if the_ibd[IBD_state] == DEV_STATE_DISCONNECT:
                            continue

                        the_ibd_devname = the_ibd[IBD_devname]
                        if property_dict.has_key(the_ibd_devname) == True:
                            if property_dict[the_ibd_devname] == 'active':
                                if the_ibd[IBD_state] != DEV_STATE_DISCONNECT:
                                    the_raid1[DEV_working] = True
                                the_ibd[IBD_state] = DEV_STATE_ONLINE
                            elif property_dict[the_ibd_devname] == 'rebuilding':
                                if the_ibd[IBD_state] != DEV_STATE_DISCONNECT:
                                    the_raid1[DEV_working] = True
                                the_ibd[IBD_state] = DEV_STATE_ONLINE
                            else:
                                the_ibd[IBD_state] = DEV_STATE_FAILED
                        else:
                            debug(the_ibd_devname + " is not in detail of " + the_raid1_devname)

            #
            # setup raid1 state
            #   DISCONNECT | OFFLINE | FAILED | ONLINE
            #
            if the_raid[DEV_state] != DEV_STATE_DISCONNECT:
                property_dict = {}
                raid_detail_nohung(the_raid_devname, property_dict)

                if property_dict != None:
                    for the_raid1 in the_raid[DEV_children]:  # raid1
                        if vv_dev_has_children(the_raid1) == False:
                            debug(the_raid1)
                        the_raid1_devname = the_raid1[DEV_devname]
                        if property_dict.has_key(the_raid1_devname) == True:
                            if property_dict[the_raid1_devname] == 'active':
                                the_raid1[DEV_state] = DEV_STATE_ONLINE
                            elif property_dict[the_raid1_devname] == 'rebuilding':
                                the_raid1[DEV_state] = DEV_STATE_ONLINE
                            else:
                                the_raid1[DEV_state] = DEV_STATE_FAILED
                        elif property_dict['Raid-Level'] == 'raid1':
                            v_nu = 0
                            for key_raid in property_dict.values():
                                if key_raid == 'active':
                                    v_nu = v_nu + 1
                                if v_nu == 2:
                                    the_raid1[DEV_state] = DEV_STATE_ONLINE
                else:
                    the_raid1[DEV_state] = DEV_STATE_FAILED
    #MdStatMgr.update_devuuid(update_raid1_uuid)
    debug("vv_setup_current_infrastructure:")
    # debug(json.dumps(c_infrastructure, sort_keys=True, indent=4, separators=(',', ': ')))
    vv_save_c_infrastructure(vv_setup_info)
    return 0


def vv_find_working_ibd(level):
    debug('Enter vv_find_working_ibd')

    cmd = CMD_IBDMANAGER_STAT
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    debug("check the ibd working state: " + str(msg))

    the_ibd = None
    the_uuid = None
    the_state = False
    ibd_found = False
    working_ibd_list = []
    working_uuid_list = []
    for line in msg:
        if line.find("Service Agent Channel") >= 0:
            the_ibd = None
            the_uuid = None
            the_state = False
            ibd_found = True
        elif line.find("uuid") >= 0:
            tmp = line.split(":")
            the_uuid = tmp[1]
        elif line.find("devname") >= 0:
            tmp = line.split(":")
            the_ibd = tmp[1]
        elif line.find("state:working") >= 0:
            the_state = True
        elif line.find(level) >= 0:
            if the_ibd != None and the_uuid != None and the_state == True and ibd_found == True:
                working_ibd_list.append(the_ibd)
                working_uuid_list.append(the_uuid)
                the_ibd = None
                ibd_found = False

    debug("working_ibd_list: " + str(working_ibd_list))
    debug("working_uuid_list: " + str(working_uuid_list))
    return (working_ibd_list, working_uuid_list)


def vv_find_all_working_ibds():
    debug('Enter vv_find_all_working_ibds')
    cmd = CMD_IBDMANAGER_STAT_WD
    (ret, msg) = runcmd(cmd, print_ret=True)
    ibd_dev_list = []
    if ret == 0:
        ibd_dev_list = msg.split()
    return ibd_dev_list


def vv_set_ibd_access():
    f = open(IBD_AGENT_CONFIG_FILE, "r")
    lines = f.readlines()
    f.close()

    f = open(IBD_AGENT_CONFIG_FILE, "w")
    for the_line in lines:
        # set to be default: readwrite
        if the_line.find("access = r") < 0:
            f.write(the_line)
    f.close()


#
# Move the shared storage to HA node.
#
def vv_move_shared_storage(vv_uuid, ha_uuid):
    # generate the input json for moving shared storage
    input_json = {}
    input_json["volumeresourceuuid"] = vv_uuid
    input_json["hailiouuid"] = ha_uuid
    debug("move_shared_storage: " + json.dumps(input_json, sort_keys=True, indent=4, separators=(',', ': ')))

    retry_num = 12 * 60
    cnt = 0
    rc = 0
    while cnt < retry_num:
        # move shared storage via USX 2.0 REST API with local agent
        amcurl = LOCAL_AGENT
        ss_url = '/usxds/move/disk'
        conn = urllib2.Request(amcurl + ss_url)
        debug("move_shared_storage: " + amcurl + ss_url)
        conn.add_header('Content-type', 'application/json')
        try:
            res = urllib2.urlopen(conn, json.dumps(input_json), timeout=30)
        except:
            debug(traceback.format_exc())
            cnt += 1
            debug('Exception caught on move_shared_storage, retry move_shared_storage: %d' % cnt)
            rc = 1
            if cnt % 200 == 0:
                cmd = '/opt/amc/agent/bin/amc_agent_stop.sh '
                (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                time.sleep(15)
                cmd = '/opt/amc/agent/bin/amc_agent_start.sh '
                (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                time.sleep(15)
            else:
                time.sleep(5)
            continue
        debug('POST returned response code: ' + str(res.code))

        flag_file = '/var/log/failed_movedisk_' + vv_uuid
        rc = 0
        if str(res.code) == "200":
            read_data = res.read()
            res_data = json.loads(read_data)
            debug('move_shared_storage response: ' + json.dumps(res_data, sort_keys=True, indent=4,
                                                                separators=(',', ': ')))
            if res_data.has_key('status'):
                the_status = res_data['status']
                if the_status in ['MOVE_DISK_FAILED', 'VALIDATION_FAILED']:
                    debug("ERROR: status MOVE_DISK_FAILED.")
                    rc = 1
                    if cnt >= 4 * 60 and os.path.exists(flag_file) != True and ha_get_availabe_nodes() > 1:
                        cmd = 'touch ' + flag_file
                        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                        cmd = 'reboot'
                        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                else:
                    debug("INFO: completed moving shared storage.")
                    rc = 0
                    if os.path.exists(flag_file):
                        os.unlink(flag_file)
                    res.close()
                    break
        else:
            debug("ERROR: failed to move shared storage.")
            rc = 1

        res.close()
        time.sleep(5)
        cnt += 1
        debug('retry move_shared_storage: %d' % cnt)

    return rc


def vv_start_ibd_clients(vv_setup_info, next_idx, timeout):
    debug('Enter vv_start_ibd_clients')

    # We updated ibd hook, just clear the old one before configuring.
    debug('Reset ibdagent conf before configuring')
    reset_ibdagent_config()

    ha_enabled = check_ha_enabled()
    ibd_dev_list = vv_setup_info['ibd_dev_list']
    vv_uuid = vv_setup_info['vv_uuid']
    device_nr = 0
    for the_ibd in ibd_dev_list:
        the_idx = next_available_ibd_idx(next_idx)
        next_idx = the_idx + 1
        the_exportname = the_ibd[IBD_uuid]
        the_ip = the_ibd[IBD_ip]
        the_ibd[IBD_index] = the_idx
        the_ibd[IBD_devname] = "/dev/ibd" + str(the_ibd[IBD_index])
        the_devname = the_ibd[IBD_devname]
        the_minor = the_idx * 16
        ibdagent_conf = {}
        ibdagent_conf['devuuid'] = the_exportname
        ibdagent_conf['cacheip'] = the_ip
        ibdagent_conf['devexport'] = the_devname
        ibdagent_conf['minornum'] = the_minor
        add_ibd_channel(ibdagent_conf, vv_uuid, milio_config.is_ha)

    device_nr = len(ibd_dev_list)
    if device_nr == 0:
        return next_idx

    rc = ibd_agent_alive()
    if rc == False:
        cmd_str = CMD_IBDAGENT
    else:
        cmd_str = CMD_IBDMANAGER_A_UPDATE
    rc = do_system(cmd_str)

    max_num_ibds = len(ibd_dev_list)
    if milio_config.is_mirror_volume:
        if milio_config.is_raid1_volume:
            max_num_disconnect = max_num_ibds / 2
        else:
            max_num_disconnect = max_num_ibds / 2 + 1
    else:
        if milio_config.is_fastfailover:
            max_num_disconnect = 2
        else:
            max_num_disconnect = 1

    # During volume init phase, we should wait for all ibds connected.
    if is_init_volume():
        max_num_disconnect = 0

    num_ibds_pass = max_num_ibds - max_num_disconnect
    debug("Total ibd num is " + str(max_num_ibds))
    debug("Total ibd disconnectable num is " + str(max_num_disconnect))
    force_upgrade = True
    shutdown_case = False
    if ha_enabled:
        # set the ibd access to default: readwrite
        time.sleep(1)
        vv_set_ibd_access()
        # Use arbitrator to handle the case where the device has been accessed by another volume
        # find the working ibd
        working_ibd_list = []
        if len(ibd_dev_list) > 0:
            wait_nr = 0
            while (wait_nr <= 30):
                wait_nr += 1
                working_ibd_list = []
                (working_ibd_list, working_uuid_list) = vv_find_working_ibd("alevel:readonly")
                debug("working_ibd_list: " + str(working_ibd_list))
                if len(working_ibd_list) >= num_ibds_pass:
                    break
                time.sleep(1)

        rc = arb_read_pill(working_ibd_list, SHUTDOWN_PREP_PILL)
        debug("perform arb_read_pill of SHUTDOWN_PREP_PILL, and return %d." % rc)
        if rc:
            shutdown_case = True
            force_upgrade = False
            debug("perform arb_write_pill for shutdown case: %s " % str(working_ibd_list))
            arb_write_pill(working_ibd_list)
            wait_nr = 0
            # Wait for up to 1 minute to get SHUTDOWN_PILL
            while (wait_nr <= 1 * 60):
                wait_nr += 1
                (working_ibd_list, working_uuid_list) = vv_find_working_ibd("alevel:readonly")
                rc = arb_read_pill(working_ibd_list, SHUTDOWN_PILL)
                if rc:
                    # force_upgrade = True
                    break
                time.sleep(1)

        arb_dev_list = []
        rw_ibd_list = []
        rw_uuid_list = []
        max_retry_time = 10
        if force_upgrade == True:
            force_upgrade_ibd_dev_list = []
            use_poison_pill = False
            one_by_one_flag = True

            debug("ibd_dev_list: " + str(ibd_dev_list))
            for the_ibd in ibd_dev_list:
                working_flag = False
                for the_working in working_ibd_list:
                    if the_working == the_ibd[IBD_devname]:
                        if one_by_one_flag:
                            retry_time = 1
                            cmd_str = CMD_IBDMANAGER_A_FORCE_UPGRADE + " " + the_ibd[IBD_devname]
                            while (retry_time <= max_retry_time):
                                debug("ibd force upgrade: " + cmd_str)
                                out = ['']
                                rc = do_system_timeout(cmd_str, 10)
                                if the_ibd[IBD_devname] not in IBDManager.find_write_read_working_ibd():
                                    debug('ibd %s force upgrade failed %s time' % (the_ibd[IBD_devname], retry_time))
                                    if retry_time == max_retry_time:
                                        use_poison_pill = True
                                        arb_dev_list.append(the_ibd[IBD_devname])
                                    else:
                                        time.sleep(1)
                                else:
                                    break
                                retry_time += 1
                        else:
                            force_upgrade_ibd_dev_list.append(the_ibd[IBD_devname])
                        break

            if not one_by_one_flag:
                cmd_list = [CMD_IBDMANAGER_A_FORCE_UPGRADE + " " + the_ibd for the_ibd in force_upgrade_ibd_dev_list]
                pool = Pool(len(force_upgrade_ibd_dev_list))
                pool_result = pool.map(runcmd, cmd_list)
                debug(pool_result)
                for (ret, msg) in pool_result:
                    if len(msg) > 0:
                        if 'failed' in msg:
                            debug('ibd %s force upgrade failed, need retry.' % msg.split()[-2])
                            time.sleep(0.1)
                            out = ['']
                            rc = do_system_timeout(CMD_IBDMANAGER_A_FORCE_UPGRADE + " " + msg.split()[-2], 10)
                            if rc != 0:
                                debug('retry ibd %s force upgrade failed.' % msg.split()[-2])
                                use_poison_pill = True
                                arb_dev_list.append(msg.split()[-2])

            wait_nr = 0
            upgrade_flag = False
            while (wait_nr <= 10):
                if wait_nr != 0:
                    time.sleep(1)
                wait_nr += 1
                (rw_ibd_list, rw_uuid_list) = vv_find_working_ibd("alevel:read write")
                if len(rw_uuid_list) > 0:
                    upgrade_flag = True
                    break
        else:
            # for not force upgrade case
            upgrade_flag = False
            debug("ibd_dev_list: " + str(ibd_dev_list))
            for the_ibd in ibd_dev_list:
                for the_working in working_ibd_list:
                    if the_working == the_ibd[IBD_devname]:
                        retry_time = 1
                        cmd_str = CMD_IBDMANAGER_A_UPGRADE + " " + the_ibd[IBD_devname]
                        while (retry_time <= max_retry_time):
                            debug("ibd upgrade: " + cmd_str)
                            out = ['']
                            rc = do_system(cmd_str, out)
                            if the_ibd[IBD_devname] not in IBDManager.find_write_read_working_ibd():
                                debug('ibd %s upgrade failed %s time' % (the_ibd[IBD_devname], retry_time))
                                if retry_time != max_retry_time:
                                    time.sleep(1)
                            else:
                                break
                            retry_time += 1

        debug("arb_dev_list is: %s " % str(arb_dev_list))
        debug("working_ibd_list: %s " % str(working_ibd_list))
        debug("rw_ibd_list: %s " % str(rw_ibd_list))
        debug("rw_uuid_list: %s " % str(rw_uuid_list))
        if upgrade_flag == False and shutdown_case == False:
            pill_ibd_list = vv_find_all_working_ibds()
            debug("start arb_write_pill for non shutdown case: %s " % str(pill_ibd_list))
            arb_write_pill(pill_ibd_list)

    connecting_ibds = list(ibd_dev_list)
    device_len = 0
    wait_nr = 0
    max_num_ibds = len(connecting_ibds)
    rw_ibd_list = []
    rw_uuid_list = []
    while (len(connecting_ibds) != 0 and wait_nr <= timeout):
        if wait_nr != 0:
            time.sleep(1)
        wait_nr += 1
        (rw_ibd_list, rw_uuid_list) = vv_find_working_ibd("alevel:read write")
        debug("rw_uuid_list: " + str(rw_uuid_list))
        for the_ibd in connecting_ibds:
            the_exportname = the_ibd["uuid"]
            for the_rw in rw_uuid_list:
                if the_rw == the_exportname:
                    debug("Found " + the_exportname)
                    connecting_ibds.remove(the_ibd)
                    break

        # reduce HA failover time, and move on
        if len(connecting_ibds) <= max_num_disconnect and (is_init_volume() or MdStatMgr.is_primary_enough()):
            break

        # TISILIO-5359: to wait forever if no ibd is connected
        if wait_nr >= timeout and (
                        len(connecting_ibds) > max_num_disconnect or not (
                    is_init_volume() or MdStatMgr.is_primary_enough())):
            debug('No enough ibd is connected in rw mode, try more times ...... ')
            wait_nr = 0
            timeout = 60

        if ha_enabled:
            if wait_nr % 5 == 0:
                # do upgrade again
                (readonly_ibd_list, readonly_uuid_list) = vv_find_working_ibd("alevel:readonly")
                for the_ibd_dev in readonly_ibd_list:
                    if force_upgrade == True:
                        cmd_str = CMD_IBDMANAGER_A_FORCE_UPGRADE + " " + the_ibd_dev
                    else:
                        cmd_str = CMD_IBDMANAGER_A_UPGRADE + " " + the_ibd_dev
                    debug("ibd upgrade: " + cmd_str)
                    out = ['']
                    rc = do_system(cmd_str, out)
                    time.sleep(1)

    # if ha_enabled:
    #   pill_ibd_list = vv_find_all_working_ibds()
    #   debug("start arbitrator for: %s" % str(pill_ibd_list))
    #   arb_start(vv_uuid, pill_ibd_list)

    # cmd_str = CMD_IBDMANAGER_STAT_WU
    # out = ['']
    # rc = do_system(cmd_str, out)
    # active_dev = out[0].split('\n')
    active_dev = rw_uuid_list
    debug("All IBDs:", ibd_dev_list)
    debug("Active_list:", active_dev)
    ibd_drop_list = []
    for the_ibd in ibd_dev_list:
        got_it = False
        for the_uuid in active_dev:
            if the_uuid == the_ibd[IBD_uuid]:
                got_it = True
                the_ibd[IBD_state] = DEV_STATE_OFFLINE
                debug("GOT_IT: the_uuid:%s IBD_uuid:%s" % (the_uuid, the_ibd[IBD_uuid]))
                break
        if got_it == False:
            the_ibd[IBD_state] = DEV_STATE_DISCONNECT
            ibd_drop_list.append({'ibd': the_ibd, 'reason': 'failed'})
            debug("not connected:" + the_ibd[IBD_devname])

    debug(ibd_dev_list)
    debug(ibd_drop_list)
    tune_all_ibd(ibd_dev_list)
    vv_set_ibd_list_size(ibd_dev_list)
    return next_idx


def vv_create_shared_device(vv_setup_info):
    debug("Enter vv_create_shared_device ...")
    shared_dev_list = vv_setup_info['shared_dev_list']
    for the_shared in shared_dev_list:
        the_sharedstorage = the_shared[SS_detail]
        the_json_str = json.dumps(the_sharedstorage)
        the_json_str_base64 = base64.urlsafe_b64encode(the_json_str)
        cmd_str = "python " + CMD_AGGCREATE + " " + the_json_str_base64
        print cmd_str
        rc = do_system(cmd_str)
        if rc != 0:
            return rc
    return 0


def vv_start_shared_storage(vv_setup_info):
    debug("Enter vv_start_shared_storage: ...")
    shared_dev_list = vv_setup_info['shared_dev_list']
    for the_shared in shared_dev_list:
        the_sharedstorage = the_shared[DEV_detail]
        the_json_str = json.dumps(the_sharedstorage)
        the_json_str_base64 = base64.urlsafe_b64encode(the_json_str)
        cmd_str = "python " + CMD_AGGSTART + " " + the_json_str_base64
        print cmd_str
        rc = do_system(cmd_str)
        if rc != 0:
            return rc
    '''
    shared_dev_list = vv_setup_info['shared_dev_list']
    if do_hotscan == True:
        scsi_hotscan()
    cmd = 'lsscsi'
    (ret, all_scsi_list) = runcmd(cmd, print_ret=True, lines=True)

    for the_shared in shared_dev_list:
        debug("vv_start_shared_storage: scan %s" % (the_shared[SS_scsibus]))
        the_shared[SS_state] = DEV_STATE_DISCONNECT
        for line in all_scsi_list:
            line = line.split()
            the_scsi = line[0][1:][:-1] # the_scsi = host:channel:target:lun
            tmp = the_scsi.split(':')   # ["0", "0", "2", "0"]
            scsibus = tmp[0] + ':' + tmp[2] # "0:2"
            if scsibus == the_shared[SS_scsibus]:
                the_shared[SS_devname] = line[-1]
                the_shared[SS_state] = DEV_STATE_OFFLINE
    debug(shared_dev_list)
    '''
    return 0


def vv_setup_raid1_storage(vv_setup_info):
    debug("Enter vv_setup_raid1_storage ...")
    plan_list = vv_setup_info['plan_list']

    for the_plan in plan_list:
        if the_plan.has_key(PLAN_ibdlist) == False:
            continue
        debug("pairing ", the_plan[PLAN_ibdlist])
        the_plan[PLAN_pairdict] = {}
        raid1_pair_dict = the_plan[PLAN_pairdict]
        for the_ibd in the_plan[PLAN_ibdlist]:
            the_raid1number = the_ibd[IBD_raid1number]
            if raid1_pair_dict.has_key(the_raid1number) == False:
                raid1_pair_dict[the_raid1number] = []
            the_pair_list = raid1_pair_dict[the_raid1number]
            the_pair_list.append(the_ibd)

        for the_raid1number in raid1_pair_dict:
            if len(raid1_pair_dict[the_raid1number]) < 2:
                the_missing = {}
                the_missing[IBD_devname] = "missing"
                the_pair_list = raid1_pair_dict[the_raid1number]
                the_pair_list.append(the_missing)


def vv_create_raid1_storage(vv_setup_info, next_md_idx):
    debug("Enter vv_create_raid1_storage ...")
    plan_list = vv_setup_info['plan_list']

    for the_plan in plan_list:
        use_bitmap = the_plan[PLAN_bitmap]
        the_plan[PLAN_raid1list] = []
        if the_plan.has_key(PLAN_ibdlist) == False:
            continue
        the_raid1_list = the_plan[PLAN_raid1list]
        the_pair_dict = the_plan[PLAN_pairdict]
        for the_raid1number in the_pair_dict:
            the_raid1 = {}
            the_pair_list = the_pair_dict[the_raid1number]
            the_ibd1 = the_pair_list[0]
            the_ibd2 = the_pair_list[1]
            the_devname1 = the_ibd1[IBD_devname] + 'p1'
            the_devname2 = the_ibd2[IBD_devname] + 'p1'
            if the_ibd2[IBD_devname] == "missing":
                the_devname2 = the_ibd2[IBD_devname]
            curr_md_idx = md_next_available_idx(next_md_idx)
            next_md_idx = curr_md_idx + 1
            md_dev = "/dev/md" + str(curr_md_idx)
            the_raid1[DEV_devname] = md_dev
            the_raid1[DEV_index] = curr_md_idx
            the_raid1[RAID1_device1] = the_ibd1
            the_raid1[RAID1_device2] = the_ibd2
            md_name = "atlas-md-" + str(curr_md_idx)
            if use_bitmap == True:
                md_bitmap_str = MD_DEFAULT_BITMAP_STR
                if the_devname2 != "missing":
                    md_bitmap_str += MD_BITMAP_4K_CHUNK_STR
            else:
                md_bitmap_str = ""
            cmd_str = CMD_MDCREATE + ' ' + md_dev + " -N " + md_name + \
                      md_bitmap_str + " --level=raid1 --raid-devices=2 " + \
                      the_devname1 + ' ' + the_devname2
            the_plan[PLAN_devname] = md_dev
            do_system_timeout(cmd_str, 60)
            the_raid1['iscache'] = False
            if is_cache_raid_brick(the_ibd1[IBD_detail]['subdevices'][0]):
                the_raid1['iscache'] = True
                debug('cache: %s' % md_dev)
            the_raid1_list.append(the_raid1)
    return next_md_idx


def vv_create_plan_storage(vv_setup_info, next_md_idx, the_plan, exportname):
    debug("Enter vv_create_plan_storage ...")
    use_bitmap = the_plan[PLAN_bitmap]
    the_chunk_size = the_plan[PLAN_chunksize]
    the_device_nr = 0
    the_device_list = []
    the_device_str = ""
    if the_plan.has_key(PLAN_raid1list) == True:
        the_raid1_list = the_plan[PLAN_raid1list]
        for the_raid1 in the_raid1_list:
            if the_raid1['iscache']:
                debug('raid1 is cache, skip vv_create_plan_storage...')
                return 0
            the_device_list.append(the_raid1[DEV_devname])
            the_device_str = the_raid1[DEV_devname] + "p1 " + the_device_str
            if the_raid1[DEV_devname] != "missing":
                the_device_nr += 1

    if the_plan.has_key(PLAN_sharedlist) == True:
        the_shared_list = the_plan[PLAN_sharedlist]
        for the_ss in the_shared_list:
            if the_ss[SS_devname] != None:
                the_device_list.append(the_ss[SS_devname])
                the_device_str = the_ss[SS_devname] + " " + the_device_str
                the_device_nr += 1
            else:
                the_device_list.append(the_ss[SS_devname])
                the_device_str = "None" + " " + the_device_str

    curr_md_idx = md_next_available_idx(next_md_idx)
    next_md_idx = curr_md_idx + 1
    md_dev = "/dev/md" + str(curr_md_idx)
    if the_plan[PLAN_raidtype] == RAID_raid5:
        if use_bitmap == True:
            md_bitmap_str = MD_DEFAULT_BITMAP_STR + MD_BITMAP_4K_CHUNK_STR
        else:
            md_bitmap_str = " "

        cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                  " --level=5" + " --chunk=" + str(the_chunk_size) + \
                  " --raid-devices=" + str(the_device_nr) + " " + \
                  md_bitmap_str + the_device_str

    elif the_plan[PLAN_raidtype] == RAID_raid1:
        debug('vv_create_plan_storage %s' % the_plan[PLAN_raidtype])
        next_md_idx = curr_md_idx
    else:

        cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                  " --level=stripe" + " --chunk=" + str(the_chunk_size) + \
                  " --raid-devices=" + str(the_device_nr) + " " + \
                  the_device_str
    if the_plan[PLAN_raidtype] != RAID_raid1:
        do_system_timeout(cmd_str, 60)
        tune_raid5(md_dev)
        the_plan[PLAN_devname] = md_dev
    if exportname != None:
        if the_plan[PLAN_raidtype] == RAID_raid1:
            md_link = the_plan[PLAN_devname] + 'p1'
            debug('plan_start_ %s' % the_plan[PLAN_devname])
            debug('link name %s' % md_link)
            the_plan[PLAN_devname] = md_link
            vv_create_vol_link(md_link, exportname)
        else:
            vv_create_vol_link(the_plan[PLAN_devname], exportname)
    else:
        if the_plan[PLAN_raidtype] == RAID_raid1:
            if the_plan[PLAN_exportname] == None:
                the_plan[PLAN_devname] = the_plan[PLAN_devname]
            else:
                md_link = the_plan[PLAN_devname] + 'p1'
                the_plan[PLAN_devname] = md_link
    return next_md_idx


def vv_create_memory_storage(vv_setup_info, next_md_idx):
    debug("vv_create_memory_storage: start ...")
    plan_list = vv_setup_info["plan_list"]
    for the_plan in plan_list:
        if the_plan[PLAN_type] != PLAN_memory:
            continue
        #
        # we got a memory type plan
        #
        next_md_idx = vv_create_plan_storage(vv_setup_info, next_md_idx, the_plan, None)
    return next_md_idx


def vv_create_nonmemory_storage(vv_setup_info, next_md_idx):
    debug("Enter vv_create_nonmemory_storage: ...")
    plan_list = vv_setup_info["plan_list"]

    for the_plan in plan_list:
        if the_plan[PLAN_type] == PLAN_memory:
            continue
        exportname = the_plan[PLAN_exportname]
        next_md_idx = vv_create_plan_storage(vv_setup_info, next_md_idx, the_plan, exportname)
    return next_md_idx


def vv_create_memory_vg(devname, vgname):
    debug("Enter vv_create_memory_vg: ...")
    cmd_str = CMD_MDADM + " --zero-superblock " + devname
    do_system_timeout(cmd_str, 10)
    rc = parted_device(devname, START_SECTOR, -1, 'p1')
    if rc != 0:
        return rc
    the_parted_devname = devname + "p1"
    vg_dev_list = []
    vg_dev_list.append(the_parted_devname)
    return vv_create_volume_group(vg_dev_list, vgname)


def vv_create_memory_export(vv_setup_info):
    debug("Enter vv_create_memory_export: ...")
    vv_uuid = vv_setup_info["vv_uuid"]
    plan_list = vv_setup_info["plan_list"]
    got_it = False
    rc = 0

    for the_plan in plan_list:
        if the_plan[PLAN_type] != PLAN_memory:
            continue
        if the_plan.get('iscache') is True:
            continue
        #
        # we got a memory type plan
        #
        if the_plan[PLAN_exportname] != None:
            the_devname = the_plan[PLAN_devname]
            the_exportname = the_plan[PLAN_exportname]
            vv_create_vol_link(the_devname, the_exportname)
            rc = 0
        else:
            rc = vv_create_memory_vg(the_plan[PLAN_devname], the_plan[PLAN_uuid])
            if rc != 0:
                break;
    return rc


def vv_start_memory_export(vv_setup_info):
    debug("Enter vv_start_memory_export: ...")
    c_infrastructure = vv_setup_info["c_infrastructure"]

    rc = 0
    for the_key in c_infrastructure:
        if the_key == INFR_memory:
            the_storagetype = STOR_TYPE_MEMORY
        else:
            continue
        raid_list = c_infrastructure[the_key]
        for the_raid in raid_list:
            if the_raid[DEV_state] != DEV_STATE_ONLINE:
                continue
            if the_raid.get('iscache') is True:
                continue

            raid_type = the_raid[DEV_raidtype]
            if the_raid[DEV_exportname] != None:
                the_exportname = the_raid[DEV_exportname]
                if raid_type == RAID_raid1:
                    link_name = the_raid[DEV_devname] + 'p1'
                else:
                    link_name = the_raid[DEV_devname]
                vv_create_vol_link(link_name, the_exportname)
                continue

            if milio_config.volume_type not in ['HYBRID']:
                # Only Internal devices needs partition & VG
                rc = device_check_partition(the_raid)
                if rc == 0:
                    debug("Memory device validated, skip create VG.")
                    return 0
            else:
                # check wether VG exists.
                if do_system('/sbin/vgdisplay {}'.format(the_raid[DEV_planid])) == 0:
                    return 0
            vv_create_memory_vg(the_raid[DEV_devname], the_raid[DEV_planid])
    return rc


#
# must be called after all storage created
#
def vv_generate_infrastructure(vv_setup_info):
    debug("Enter vv_generate_infrastructure ...")
    plan_list = vv_setup_info['plan_list']
    vv_setup_info["infrastructure"] = {}
    infrastructure_dict = vv_setup_info["infrastructure"]

    for the_plan in plan_list:
        the_storage = the_plan[PLAN_devname]
        if the_plan[PLAN_type] == PLAN_capacity:
            # kai
            if infrastructure_dict.has_key(INFR_disk) == False:
                infrastructure_dict[INFR_disk] = []
            the_storage_list = infrastructure_dict[INFR_disk]
        else:
            # kai
            if infrastructure_dict.has_key(INFR_memory) == False:
                infrastructure_dict[INFR_memory] = []
            the_storage_list = infrastructure_dict[INFR_memory]
        the_top_dev = {}
        property_dict = {}
        raid_detail(the_plan[PLAN_devname], property_dict)
        the_top_dev[DEV_planid] = the_plan[PLAN_uuid]
        the_top_dev[DEV_uuid] = property_dict[RAID_UUID]
        the_top_dev[DEV_exportname] = the_plan[PLAN_exportname]
        the_top_dev[DEV_raidtype] = the_plan[PLAN_raidtype]
        the_top_dev[DEV_chunksize] = the_plan[PLAN_chunksize]
        the_top_dev[DEV_bitmap] = the_plan[PLAN_bitmap]
        the_top_dev[DEV_children] = []
        the_top_children = the_top_dev[DEV_children]
        the_storage_list.append(the_top_dev)
        for the_raid1 in the_plan[PLAN_raid1list]:
            the_raid1_dev = {}
            property_dict = {}
            the_raid1_dev['iscache'] = the_raid1['iscache']
            the_top_dev['iscache'] = the_raid1_dev['iscache']
            raid_detail(the_raid1[DEV_devname], property_dict)
            the_raid1_dev[DEV_uuid] = property_dict[RAID_UUID]
            the_raid1_dev[DEV_children] = []
            the_raid1_children = the_raid1_dev[DEV_children]
            the_top_children.append(the_raid1_dev)

            the_ibd = the_raid1[RAID1_device1]
            the_ibd_dev = {}
            the_ibd_dev[DEV_uuid] = the_ibd[IBD_uuid]
            the_ibd_dev[DEV_ip] = the_ibd[IBD_ip]
            the_ibd_dev[DEV_detail] = the_ibd[IBD_detail]
            the_raid1_children.append(the_ibd_dev)

            the_ibd = the_raid1[RAID1_device2]
            if the_ibd[IBD_devname] != "missing":
                the_ibd_dev = {}
                the_ibd_dev[DEV_uuid] = the_ibd[IBD_uuid]
                the_ibd_dev[DEV_ip] = the_ibd[IBD_ip]
                the_ibd_dev[DEV_detail] = the_ibd[IBD_detail]
                the_raid1_children.append(the_ibd_dev)

        for the_shared in the_plan[PLAN_sharedlist]:
            the_shared_dev = {}
            the_shared_dev['iscache'] = the_shared['iscache']
            the_top_dev['iscache'] = the_shared_dev['iscache']
            the_shared_dev[DEV_detail] = the_shared[SS_detail]
            the_shared_dev[DEV_devname] = the_shared[SS_devname]
            the_shared_dev[DEV_children] = []
            the_top_children.append(the_shared_dev)


def vv_save_dev_uuid(uuid, parent_devname):
    debug("vv_save_dev_uuid " + parent_devname + " " + uuid)
    uuid_dev_file = "/tmp/dev-uuid-" + uuid
    f = open(uuid_dev_file, 'w')
    f.write(parent_devname)
    f.close()


def vv_remove_dev_uuid(uuid):
    debug("vv_remove_dev_uuid " + uuid)
    uuid_dev_file = "/tmp/dev-uuid-" + uuid
    if os.path.exists(uuid_dev_file):
        os.unlink(uuid_dev_file)


def vv_check_all_ibd(setup_info, the_raid):
    debug("vv_check_all_ibd: start ...")
    for the_raid1 in the_raid[DEV_children]:
        the_raid1[DEV_subvalid_counter] = 0
        if vv_dev_has_children(the_raid1) == False:
            continue
        for the_ibd in the_raid1[DEV_children]:
            if the_ibd[DEV_state] == DEV_STATE_DISCONNECT:
                the_ibd[DEV_valid] = False
                continue
            rc = ibd_check_and_wait_array_partition(the_ibd)
            if rc == 0:
                the_ibd[DEV_valid] = True
                the_raid1[DEV_subvalid_counter] += 1
            else:
                the_ibd[DEV_valid] = False


def vv_drop_raid1(setup_info, the_raid, the_raid1):
    debug("vv_drop_raid1: the_raid:%s the_raid1:%s" % (the_raid[DEV_devname], the_raid1[DEV_devname]))
    if the_raid[DEV_state] == DEV_STATE_ONLINE:
        md_fail(the_raid[DEV_devname], the_raid1[DEV_devname] + 'p1')
        md_remove(the_raid[DEV_devname], the_raid1[DEV_devname] + 'p1')
    rc = md_stop(the_raid1[DEV_devname])
    if rc == 0:
        vv_remove_dev_uuid(the_raid1[DEV_uuid])
        the_raid1[DEV_state] = DEV_STATE_DISCONNECT
        for the_ibd in the_raid1[DEV_children]:
            if the_ibd[DEV_state] != DEV_STATE_DISCONNECT:
                the_ibd[IBD_state] = DEV_STATE_OFFLINE
                # else: TODO. But why failed


#
# Only called by raid_assemble
#
def vv_raid_assemble_core(setup_info, the_raid, storagetype, status):
    log_header = "vv_raid_assemble_core"
    debug('"Enter %s: (%s) ...' % (log_header, storagetype))
    rejoin_raid1 = True
    rejoin_raid5 = True
    if setup_info.get('ibd_uuid'):
        rejoin_raid1 = MdStatMgr.can_rejoin_raid1(setup_info['ibd_uuid'])
        rejoin_raid5 = MdStatMgr.can_rejoin_raid5(setup_info['ibd_uuid'])
    vv_uuid = setup_info['vv_uuid']
    chunk_size = the_raid[DEV_chunksize]
    use_bitmap = the_raid[DEV_bitmap]
    raid_type = the_raid[DEV_raidtype]
    c_infrastructure = setup_info['c_infrastructure']
    next_md_idx = 0
    curr_md_idx = 0
    infrastructure_modified = False
    top_array_rebuilt = False

    vv_check_all_ibd(setup_info, the_raid)
    to_assemble_ss_str = ''
    for the_raid1 in the_raid[DEV_children]:  # raid1 | shared
        if vv_dev_has_children(the_raid1) == False:
            to_assemble_ss_str = to_assemble_ss_str + ' ' + the_raid1['devname']
    to_create_ss_str = to_assemble_ss_str

    #
    # First of all, assemble all ibd of the_raid
    #
    new_create_raid1_list = []
    new_create_raid1_str = ''
    to_assemble_raid1_list = []
    to_assemble_raid1_str = ''
    to_create_raid1_list = []
    to_create_raid1_str = ''
    if rejoin_raid1:
        debug('can running rejoin_raid1')
        for the_raid1 in the_raid[DEV_children]:  # raid1
            if vv_dev_has_children(the_raid1) == False:
                continue  # don't worry about shared device
            parent_devname = the_raid1[DEV_devname]
            if the_raid1[DEV_state] == DEV_STATE_DISCONNECT:
                #
                # the raid1 is not there at all, need assemble all ibd in this raid1
                #
                debug("%s: raid1 %s does not exist" % (log_header, parent_devname))
                curr_md_idx = md_next_available_idx(next_md_idx)
                next_md_idx = curr_md_idx + 1
                parent_devname = '/dev/md' + str(curr_md_idx)
                to_assemble_ibd_str = ''
                to_assemble_ibd_list = []
                to_add_ibd_str = ''
                to_add_ibd_list = []
                to_create_ibd_str = ''
                to_create_ibd_list = []
                debug("%s: checking raid1(%s) children" % (log_header, parent_devname))
                for the_ibd in the_raid1[DEV_children]:
                    debug("%s: raid1(%s) child %s" % (log_header, parent_devname, the_ibd[DEV_devname]))
                    if the_ibd[DEV_state] != DEV_STATE_DISCONNECT:
                        debug("%s: %s is connected" % (log_header, the_ibd[DEV_devname]))
                        part_ibd_devname = the_ibd[DEV_devname] + 'p1'
                        # rc = ibd_check_and_wait_array_partition(the_ibd)
                        # if rc == 0:
                        if the_ibd[DEV_valid] == True:
                            #
                            # this ibd device has valid content, ok for assemble
                            #
                            to_assemble_ibd_str = part_ibd_devname + ' ' + to_assemble_ibd_str
                            to_assemble_ibd_list.append(the_ibd)
                        # elif storagetype == STOR_TYPE_MEMORY:
                        elif (storagetype == STOR_TYPE_MEMORY) \
                                or (storagetype == STOR_TYPE_DISK and is_new_disk(the_ibd[DEV_devname])):
                            #
                            # the ibd got destroyed. parted it as a new device
                            #

                            rc = parted_device(the_ibd[DEV_devname], START_SECTOR, -1, 'p1')
                            if rc == 0:
                                #
                                # this ibd can join the raid1 as a new device.
                                #
                                to_add_ibd_str = part_ibd_devname + ' ' + to_add_ibd_str
                                to_add_ibd_list.append(the_ibd)
                                if storagetype == STOR_TYPE_DISK and is_new_disk(the_ibd[DEV_devname]):
                                    clear_new_disk(the_ibd[DEV_devname])
                            else:
                                debug("failed to parted %s" % the_ibd[DEV_devname])
                                part_ibd_devname = None
                        else:
                            debug("vv_raid_assemble_core: invalid %s" % part_ibd_devname)
                            part_ibd_devname = None
                        # TODO

                        if part_ibd_devname != None:
                            #
                            # this is a valid ibd. ok to create a new raid1
                            #
                            to_create_ibd_str = part_ibd_devname + ' ' + to_create_ibd_str
                            to_create_ibd_list.append(the_ibd)

                    else:
                        debug("%s: %s is disconnected" % (log_header, the_ibd[DEV_devname]))
                cmd_str = CMD_MDASSEMBLE + ' ' + parent_devname + ' ' + to_assemble_ibd_str
                rc = do_system_timeout(cmd_str, 60)
                #
                # For USX-59386 debug, in that issue
                # mdadm: failed to RUN_ARRAY /dev/md0: Input/output error
                # mdadm: Not enough devices to start the array.
                #
                if rc == 1:
                    runcmd('ls /dev/ibd*', print_ret=True)
                    runcmd('ls /dev/md*', print_ret=True)
                    rc = do_system_timeout(cmd_str, 10)
                if rc == 0:
                    vv_save_dev_uuid(the_raid1[DEV_uuid], parent_devname)
                    # the_raid1[DEV_state] = DEV_STATE_ONLINE
                    the_raid1[DEV_state] = DEV_STATE_OFFLINE
                    the_raid1[DEV_working] = True
                    the_raid1[DEV_devname] = parent_devname

                    #
                    # some devices can not joined the array when do assemble due to lower events
                    # add them to join the array. It's harmless even if they are already in the array
                    #
                    for the_ibd in to_assemble_ibd_list:
                        part_ibd_devname = the_ibd[DEV_devname] + 'p1'
                        cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + part_ibd_devname
                        do_system_timeout(cmd_str, 60)

                    for the_ibd in to_assemble_ibd_list:
                        #
                        # We assume all ibd in to_assemble_ibd_list have alrady joined the the_raid1.
                        # Wrong? Any thoughts?
                        #
                        the_ibd[DEV_state] = DEV_STATE_ONLINE

                    for the_ibd in to_add_ibd_list:
                        part_ibd_devname = the_ibd[DEV_devname] + 'p1'
                        cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + part_ibd_devname
                        rc = do_system_timeout(cmd_str, 60)
                        if rc == 0:
                            the_ibd[DEV_state] = DEV_STATE_ONLINE
                        else:
                            debug("%s cannot be re-added" % part_ibd_devname)
                            # TODO

                    if raid_type == RAID_raid1:
                        rc = raid1_check_and_wait_array_partition_robo(the_raid1)
                    else:
                        rc = raid1_check_and_wait_array_partition(the_raid1)
                    if rc == 0:
                        #
                        # this raid1 is valid, ok to assemble
                        #
                        to_assemble_raid1_list.append(the_raid1)
                        to_assemble_raid1_str = parent_devname + 'p1' + ' ' + to_assemble_raid1_str
                        # USX-37670: ready to re-create raid5|raid0
                        to_create_raid1_list.append(the_raid1)
                        to_create_raid1_str = parent_devname + 'p1 ' + to_create_raid1_str
                    else:
                        if the_raid1.get('iscache') is not True:
                            if (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                                rc = parted_device(the_raid1[DEV_devname], START_SECTOR, -1, 'p1')
                                if rc == 0:
                                    #
                                    # use this raid1 as a new created raid1 device
                                    #
                                    # USX-37670:
                                    device_zero_header(the_raid1[DEV_devname] + 'p1', 4)  # zero the first 4M
                                    new_create_raid1_list.append(the_raid1)
                                    new_create_raid1_str = parent_devname + 'p1 ' + new_create_raid1_str
                                    # USX-37670: ready to re-create raid5|raid0
                                    to_create_raid1_list.append(the_raid1)
                                    to_create_raid1_str = parent_devname + 'p1 ' + to_create_raid1_str

                # USX-37670: to re-create a raid1, all of it's ibd(s) must be in valid status
                elif ((storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK)) and \
                        (len(to_create_ibd_list) == len(the_raid1[DEV_children])):
                    debug("assemble failed, try to re-create the raid1(%s)" % to_create_ibd_str),
                    md_stop(parent_devname)
                    curr_md_idx = md_next_available_idx(curr_md_idx)  # curr_md_idx may be released after stop
                    next_md_idx = curr_md_idx + 1
                    md_name = "atlas-md-" + str(curr_md_idx)
                    num_devices = len(to_create_ibd_list)
                    if len(to_create_ibd_list) == 1:
                        to_create_ibd_str = to_create_ibd_str + ' missing'
                        num_devices += 1
                    if use_bitmap == True:
                        md_bitmap_str = MD_DEFAULT_BITMAP_STR
                        if len(to_create_ibd_list) == 2:
                            md_bitmap_str += MD_BITMAP_4K_CHUNK_STR
                    else:
                        md_bitmap_str = ""

                    cmd_str = CMD_MDCREATE + \
                              " --chunk=" + str(chunk_size) + ' ' + \
                              parent_devname + " -N " + md_name + \
                              md_bitmap_str + \
                              RAID1_DATA_OFFSET + \
                              " --level=raid1 --raid-devices=" + str(num_devices) + ' ' + to_create_ibd_str
                    for i in range(2):
                        try:
                            rc = do_system_timeout(cmd_str, 60)
                            if rc != 0:
                                debug("raid1 re-create failed!, parted all ibd and re-create again")
                                for the_ibd in to_create_ibd_list:
                                    parted_device(the_ibd['devname'], START_SECTOR, -1, 'p1')
                                continue
                        except timeout_error, e:
                            debug(
                                're-create timeout error, try to wait that creating raid1 has done sucessfully in 30 seconds')
                            cmd_check_raid1 = '%s %s' % (CMD_DETAIL, md_name)
                            for j in range(6):
                                if do_system_timeout(cmd_check_raid1, 10) == 0:
                                    break
                                time.sleep(5)
                            else:
                                raise e

                        # re-create succeed
                        property_dict = {}
                        raid_detail(parent_devname, property_dict)
                        old_uuid = the_raid1[DEV_uuid]
                        new_uuid = property_dict[RAID_UUID]
                        vv_remove_dev_uuid(old_uuid)
                        vv_save_dev_uuid(new_uuid, parent_devname)
                        the_raid1[DEV_uuid] = new_uuid
                        the_raid1[DEV_working] = True
                        infrastructure_modified = True

                        #
                        # the_raid1['uuid'] should be replaced by it's new uuid: property_dict[RAID_UUID]
                        #
                        rc = vv_infrastructure_substitue_raid1_uuid(setup_info, old_uuid, new_uuid)
                        debug("vv_infrastructure_substitue_raid1_uuid: old:%s new:%s rc:%d" % (old_uuid, new_uuid, rc))

                        for the_ibd in to_create_ibd_list:
                            the_ibd[IBD_state] = DEV_STATE_ONLINE
                        the_raid1[DEV_state] = DEV_STATE_OFFLINE
                        the_raid1[DEV_uuid] = new_uuid
                        the_raid1[DEV_devname] = parent_devname

                        if raid_type == RAID_raid1 and the_raid[DEV_exportname] == None:
                            infrastructure_modified = True
                            the_raid[DEV_uuid] = new_uuid
                            rc = vv_infrastructure_substitue_raid_uuid(setup_info, old_uuid, new_uuid)
                            debug(
                                "vv_infrastructure_substitue_raid_uuid: old:%s new:%s rc:%d" % (old_uuid, new_uuid, rc))
                            break
                        else:
                            rc = parted_device(parent_devname, START_SECTOR, -1, 'p1')
                            if rc == 0:
                                if raid_type == RAID_raid1:
                                    infrastructure_modified = True
                                    the_raid[DEV_uuid] = new_uuid
                                    rc = vv_infrastructure_substitue_raid_uuid(setup_info, old_uuid, new_uuid)
                                    debug("vv_infrastructure_substitue_raid_uuid: old:%s new:%s rc:%d" % (
                                        old_uuid, new_uuid, rc))
                                # USX-37670:
                                device_zero_header(the_raid1[DEV_devname] + 'p1', 4)  # zero the first 4M
                                new_create_raid1_list.append(the_raid1)
                                new_create_raid1_str = parent_devname + 'p1 ' + new_create_raid1_str
                                # USX-37670: ready to re-create raid5|raid0
                                to_create_raid1_list.append(the_raid1)
                                to_create_raid1_str = parent_devname + 'p1 ' + to_create_raid1_str
                            else:
                                debug("failed to parted %s" % parent_devname)
                            # TODO
                            break
                else:
                    md_stop(parent_devname)
                    debug("assemble failed, give up")

                    if storagetype == STOR_TYPE_DISK:
                        for the_ibd in to_add_ibd_list:
                            set_new_disk(the_ibd[DEV_devname])

            else:
                #
                # the raid1 has already been there, just need add not-online ibd
                #
                debug("vv_raid_assemble_core: raid1 %s exists" % (parent_devname))
                if the_raid1[DEV_working] == False:
                    continue
                if the_raid1.has_key(DEV_children) == False:
                    continue

                if the_raid1[DEV_subvalid_counter] == 0:
                    if raid_type == RAID_raid1 and status == VV_READD:
                        reset_vm('reset_with_raid1_subvalid_0')
                    vv_drop_raid1(setup_info, the_raid, the_raid1)
                    if infrastructure_modified == True:
                        vv_save_infrastructure(setup_info)
                        vv_setup_current_infrastructure(setup_info)
                    debug("vv_raid_assemble_core: DEV_subvalid_counter is 0")
                    return 2

                for the_ibd in the_raid1[DEV_children]:
                    if the_ibd[IBD_state] != DEV_STATE_DISCONNECT:  # and the_ibd['state'] != DEV_STATE_ONLINE:
                        part_ibd_devname = the_ibd[IBD_devname] + 'p1'
                        # rc = ibd_check_and_wait_array_partition(the_ibd)
                        # if rc != 0:
                        if the_ibd[DEV_valid] == False:
                            # if storagetype == STOR_TYPE_MEMORY:
                            if (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                                rc = md_fail(parent_devname, part_ibd_devname)
                                if rc == 0:
                                    rc = md_remove(parent_devname, part_ibd_devname)
                                elif rc == RC_DEVICVE_NOTEXIST:
                                    rc = 0

                                if rc == 0:
                                    the_ibd[IBD_state] = DEV_STATE_OFFLINE
                                    rc = parted_device(the_ibd[IBD_devname], START_SECTOR, -1, 'p1')
                                    if rc != 0:
                                        continue  # not ready, skip it
                                else:
                                    vv_drop_raid1(setup_info, the_raid, the_raid1)
                                    if infrastructure_modified == True:
                                        vv_save_infrastructure(setup_info)
                                        vv_setup_current_infrastructure(setup_info)
                                    debug("vv_raid_assemble_core: remove failed")
                                    return 2
                            else:
                                # TODO: now we just skip this one
                                debug("%s does not exist" % part_ibd_devname)
                                continue

                        elif the_ibd[IBD_state] == DEV_STATE_ONLINE:
                            # nothing to do it has valid partition and online
                            continue

                        if the_ibd[IBD_state] != DEV_STATE_OFFLINE:
                            # remove a Faulty (any other state?) device
                            md_remove(parent_devname, part_ibd_devname)
                            the_ibd[IBD_state] = DEV_STATE_OFFLINE

                        rc = md_re_add(parent_devname, part_ibd_devname)
                        if rc == 0:
                            time.sleep(1)
                            r1_detail = {}
                            debug("Check %s status after add:" % the_ibd[IBD_devname])
                            raid_detail_nohung(parent_devname, r1_detail)
                            if (not r1_detail.has_key(the_ibd[IBD_devname]) or
                                    (r1_detail.has_key(the_ibd[IBD_devname]) and r1_detail[
                                        the_ibd[IBD_devname]] == 'faulty')):
                                # Try to add again
                                debug("%s still is not active, try to add again" % the_ibd[IBD_devname])
                                if (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                                    rc = md_fail(parent_devname, part_ibd_devname)
                                    if rc == 0:
                                        rc = md_remove(parent_devname, part_ibd_devname)
                                else:
                                    debug("%s does not exist" % part_ibd_devname)
                                    continue
                                rc = md_re_add(parent_devname, part_ibd_devname)

                            the_ibd[IBD_state] = DEV_STATE_ONLINE
                        else:
                            vv_drop_raid1(setup_info, the_raid, the_raid1)
                            if infrastructure_modified == True:
                                vv_save_infrastructure(setup_info)
                                vv_setup_current_infrastructure(setup_info)
                            debug("vv_raid_assemble_core: readd failed")
                            return 2
                if raid_type == RAID_raid1:
                    rc = raid1_check_and_wait_array_partition_robo(the_raid1)
                else:
                    rc = raid1_check_and_wait_array_partition(the_raid1)
                if rc != 0:
                    if the_raid1.get('iscache') is not True:
                        if (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                            parted_device(the_raid1[DEV_devname], START_SECTOR, -1, 'p1')
                else:
                    to_assemble_raid1_list.append(the_raid1)
                    to_assemble_raid1_str = parent_devname + 'p1' + ' ' + to_assemble_raid1_str

                # USX-37670: ready to re-create raid5|raid0
                to_create_raid1_list.append(the_raid1)
                to_create_raid1_str = parent_devname + 'p1 ' + to_create_raid1_str
    else:
        debug('cannot running rejoin_raid1')
        return 1
    #
    # make sure all raid1 are ready
    #


    debug("vv_raid_assemble_core: to_assemble_raid1_list %s" % (to_assemble_raid1_str))
    if raid_type == RAID_raid1:
        rc = raid1_check_and_wait_array_partition_list_robo(to_assemble_raid1_list)
    else:
        rc = raid1_check_and_wait_array_partition_list(to_assemble_raid1_list)
    if rc != 0:
        debug("vv_raid_assemble_core: return 1 at step 1")
        if infrastructure_modified == True:
            vv_save_infrastructure(setup_info)
            vv_setup_current_infrastructure(setup_info)
        return 1
    if raid_type == RAID_raid1:
        rc = raid1_check_and_wait_array_partition_list_robo(new_create_raid1_list)
    else:
        rc = raid1_check_and_wait_partition_list(new_create_raid1_list)
    if rc != 0:
        debug("vv_raid_assemble_core: return 1 at step 2")
        if infrastructure_modified == True:
            vv_save_infrastructure(setup_info)
            vv_setup_current_infrastructure(setup_info)
        return 1

    '''
    if the_raid[DEV_state] != DEV_STATE_DISCONNECT:
        parent_devname = the_raid[DEV_devname]
        raid1_not_online_counter = 0
        for the_raid1 in the_raid[DEV_children]:
            if the_raid1[DEV_state] != DEV_STATE_ONLINE:
                raid1_not_online_counter += 1
        if (raid1_not_online_counter > 0 and raid_type == RAID_raid0) or \
           (raid1_not_online_counter > 1 and raid_type == RAID_raid5):
            md_stop(parent_devname)
            the_raid[DEV_state] = DEV_STATE_DISCONNECT
    '''

    if raid_type == RAID_raid1:
        for this_raid1 in the_raid['children']:
            if this_raid1['state'] != DEV_STATE_DISCONNECT:
                this_raid1['state'] = DEV_STATE_ONLINE
        the_raid[DEV_state] = DEV_STATE_ONLINE
        the_raid[DEV_devname] = parent_devname

        if the_raid[DEV_exportname] != None:
            if storagetype != STOR_TYPE_MEMORY and not milio_config.is_fastfailover:
                the_exportname = the_raid[DEV_exportname]
                debug('the exportname %s' % the_exportname)
                link_name = parent_devname + 'p1'
                vv_create_vol_link(link_name, the_exportname)

            if infrastructure_modified == True:
                vv_save_infrastructure(setup_info)
                vv_setup_current_infrastructure(setup_info)
        else:
            if infrastructure_modified == True:
                vv_save_infrastructure(setup_info)
                vv_setup_current_infrastructure(setup_info)
        return 0
    #
    # Now assemble all raid1 of this raid
    #
    if rejoin_raid5:
        debug('can running rejoin_raid5')
        if the_raid[DEV_state] == DEV_STATE_DISCONNECT:
            debug("vv_raid_assemble_core: assemble raid1 for disconnected raid")
            curr_md_idx = md_next_available_idx(next_md_idx)
            next_md_idx = curr_md_idx + 1
            parent_devname = '/dev/md' + str(curr_md_idx)

            cmd_str1 = CMD_MDASSEMBLE + ' ' + parent_devname + ' ' + to_assemble_ss_str + ' ' + to_assemble_raid1_str
            cmd_str2 = CMD_MDASSEMBLE + ' --force ' + parent_devname + ' ' + to_assemble_ss_str + ' ' + to_assemble_raid1_str
            cmd_str_list = [cmd_str1, cmd_str2]
            for cmd in cmd_str_list:
                try:
                    rc = do_system_timeout(cmd, 60)
                    if rc == 0:
                        break
                except timeout_error:
                    rc = 1
            if rc == 0:
                vv_save_dev_uuid(the_raid[DEV_uuid], parent_devname)

                tune_raid5(parent_devname)

                for the_raid1 in to_assemble_raid1_list:
                    part_raid1_devname = the_raid1[DEV_devname] + 'p1'
                    cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + part_raid1_devname
                    do_system_timeout(cmd_str, 60)

                # Add share storage as well.
                if to_assemble_ss_str != '':
                    to_assemble_ss_list = []
                    to_assemble_ss_list.append(to_assemble_ss_str)
                    for the_ss in to_assemble_ss_list:
                        cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + the_ss
                        do_system_timeout(cmd_str, 60)

                for this_raid1 in the_raid['children']:
                    if this_raid1['state'] != DEV_STATE_DISCONNECT:
                        this_raid1['state'] = DEV_STATE_ONLINE
                the_raid[DEV_state] = DEV_STATE_ONLINE
                the_raid[DEV_devname] = parent_devname
                for the_raid1 in new_create_raid1_list:
                    cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + the_raid1['devname'] + 'p1'
                    rc = do_system_timeout(cmd_str, 60)
                    if rc == 0:
                        the_raid1['state'] = DEV_STATE_ONLINE

                if the_raid[DEV_exportname] != None:
                    the_exportname = the_raid[DEV_exportname]
                    vv_create_vol_link(parent_devname, the_exportname)

            # elif storagetype == STOR_TYPE_MEMORY:
            elif (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                #
                # we cannot recover raid stroage, re-create it
                #
                debug("cannot recovery the_raid for memory pool ...")
                md_stop(parent_devname)

                #
                # re-create the_raid: all children of the_raid must be ready
                #
                if status == VV_START:
                    if len(the_raid[DEV_children]) > len(to_create_raid1_list):
                        #
                        # some children of the_raid are not ready, need retry
                        #
                        debug(
                            "vv_raid_assemble_core: waiting for all raid1 children to come up (%s)" % to_create_raid1_str)
                        time.sleep(2);  # waiting for new ibd to come
                        vv_save_infrastructure(setup_info)
                        vv_find_ibd_mapping(setup_info)  # checking all ibd status
                        vv_setup_current_infrastructure(setup_info)
                        return 2;
                else:
                    debug("Cannot recovery the_raid at status: %s!" % status)
                    time.sleep(2)
                    return 1;

                num_devices = len(to_create_raid1_list)
                if len(the_raid[DEV_children]) == num_devices:
                    debug("re-create the_raid %s" % (the_raid[DEV_devname]))
                    curr_md_idx = md_next_available_idx(curr_md_idx)  # curr_md_idx may be released after stop
                    next_md_idx = curr_md_idx + 1
                    parent_devname = "/dev/md" + str(curr_md_idx)
                    md_name = "atlas-md-" + str(curr_md_idx)

                    if use_bitmap == True:
                        md_bitmap_str = MD_DEFAULT_BITMAP_STR + MD_BITMAP_4K_CHUNK_STR
                    else:
                        md_bitmap_str = ""

                    if raid_type == RAID_raid5:
                        if status != VV_START:
                            reset_vm("vv_raid_assemble_core");

                        cmd_str = CMD_MDCREATE + \
                                  " --chunk=" + str(chunk_size) + ' ' + \
                                  parent_devname + " -N " + md_name + \
                                  md_bitmap_str + " --level=raid5 " + \
                                  "--raid-devices=" + str(num_devices) + \
                                  ' ' + to_create_ss_str + to_create_raid1_str
                    elif raid_type == RAID_raid0:
                        if status != VV_START:
                            reset_vm("vv_raid_assemble_core");

                        cmd_str = CMD_MDCREATE + \
                                  " --chunk=" + str(chunk_size) + ' ' + \
                                  parent_devname + " -N " + md_name + \
                                  " --level=stripe " + \
                                  "--raid-devices=" + str(num_devices) + \
                                  ' ' + to_create_ss_str + to_create_raid1_str
                    elif raid_type == RAID_raid1:
                        if status != VV_START:
                            reset_vm("vv_raid_assemble_core");
                        cmd_str = CMD_MDCREATE + \
                                  " --chunk=" + str(chunk_size) + ' ' + \
                                  parent_devname + " -N " + md_name + \
                                  " --level=raid1 " + \
                                  "--raid-devices=" + str(num_devices) + \
                                  ' ' + to_create_ss_str + to_create_raid1_str

                    rc = do_system_timeout(cmd_str, 60)
                    if rc == 0:
                        # re-create succeed
                        # USX-37670:
                        device_zero_header(parent_devname, 4)  # zero the first 4M
                        tune_raid5(parent_devname)
                        top_array_rebuilt = True
                        property_dict = {}
                        raid_detail(parent_devname, property_dict)
                        old_uuid = the_raid[DEV_uuid]
                        new_uuid = property_dict[RAID_UUID]
                        vv_remove_dev_uuid(old_uuid)
                        vv_save_dev_uuid(new_uuid, parent_devname)
                        the_raid[DEV_uuid] = new_uuid
                        the_raid[DEV_devname] = parent_devname
                        infrastructure_modified = True

                        #
                        # the_raid['uuid'] should be replaced by it's new uuid: property_dict[RAID_UUID]
                        #
                        rc = vv_infrastructure_substitue_raid_uuid(setup_info, old_uuid, new_uuid)
                        debug("vv_infrastructure_substitue_raid_uuid: old:%s new:%s rc:%d" % (old_uuid, new_uuid, rc))
                        the_raid[DEV_state] = DEV_STATE_ONLINE
                        if the_raid[DEV_exportname] != None:
                            the_exportname = the_raid[DEV_exportname]
                            vv_create_vol_link(parent_devname, the_exportname)

            else:
                debug("cannot recovery raid for non memory pool, stuck here for failover or ...");
                md_stop(parent_devname)
                # time.sleep(10000000);

        else:
            debug("vv_raid_assemble_core: assemble raid1 for connected raid")
            for this_raid1 in the_raid['children']:
                if vv_dev_has_children(this_raid1) == False:
                    continue  # don't worry about shared device
                if this_raid1[DEV_working] == False:
                    debug("%s is not in working status" % (this_raid1['devname']))
                    continue
                if this_raid1['state'] != DEV_STATE_DISCONNECT and this_raid1['state'] != DEV_STATE_ONLINE:
                    debug("%s is neither disconnected nor online" % (this_raid1['devname']))
                    child_devname = this_raid1['devname'] + 'p1'
                    if this_raid1['state'] != DEV_STATE_OFFLINE:
                        debug("%s is not offline" % (this_raid1['devname']))
                        #
                        # sometime, a failed md just cannot be removed, try 3 times
                        #
                        for i in range(3):
                            rc = md_fail(the_raid[DEV_devname], child_devname)
                            if rc == 0:
                                rc = md_remove(the_raid[DEV_devname], child_devname)
                                if rc == 0:
                                    this_raid1['state'] = DEV_STATE_OFFLINE
                                    break
                            time.sleep(0.5)
                        if rc != 0:
                            continue
                    if this_raid1['state'] != DEV_STATE_OFFLINE:
                        continue
                    debug("%s is offline" % (this_raid1['devname']))
                    # rc = md_stop(this_raid1['devname'])
                    # Add retry for the md_stop.
                    # if rc != 0:
                    for i in range(30):
                        try:
                            rc = md_stop(this_raid1['devname'])
                            if rc == 0:
                                break
                        except timeout_error as e:
                            debug('md_stop timout error')
                        time.sleep(1)
                    vv_remove_dev_uuid(this_raid1[DEV_uuid])
                    this_raid1[DEV_state] = DEV_STATE_DISCONNECT
                    to_assemble_ibd_list = []
                    to_assemble_ibd_str = ''
                    for the_ibd in this_raid1['children']:
                        if the_ibd['state'] != DEV_STATE_DISCONNECT:
                            to_assemble_ibd_list.append(the_ibd)
                            to_assemble_ibd_str = the_ibd['devname'] + 'p1' + ' ' + to_assemble_ibd_str
                    cmd_str1 = CMD_MDASSEMBLE + ' ' + this_raid1['devname'] + ' ' + to_assemble_ibd_str
                    cmd_str2 = CMD_MDASSEMBLE + ' --force ' + this_raid1['devname'] + ' ' + to_assemble_ibd_str
                    rc = do_system_timeout(cmd_str1, 60)
                    if rc != 0:
                        rc = do_system_timeout(cmd_str2, 60)

                    if rc == 0:
                        vv_save_dev_uuid(this_raid1['uuid'], this_raid1['devname'])
                        for the_ibd in to_assemble_ibd_list:
                            cmd_str = CMD_MDMANAGE + ' ' + this_raid1['devname'] + ' --add ' + the_ibd['devname'] + 'p1'
                            do_system_timeout(cmd_str, 60)

                        rc = md_re_add(the_raid[DEV_devname], child_devname)
                        if rc == 0:
                            time.sleep(1)
                            r5_detail = {}
                            debug("Check5 %s status after add:" % child_devname)
                            raid_detail_nohung(the_raid[DEV_devname], r5_detail)
                            r1_devname = child_devname.replace('p1', '')
                            if (not r5_detail.has_key(r1_devname) or
                                    (r5_detail.has_key(r1_devname) and r5_detail[r1_devname] == 'faulty')):
                                # Try to add again
                                debug("%s still is not active, try to add again" % child_devname)
                                if (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                                    rc = md_fail(the_raid[DEV_devname], child_devname)
                                    if rc == 0:
                                        rc = md_remove(the_raid[DEV_devname], child_devname)
                                else:
                                    debug("%s does not exist" % part_ibd_devname)
                                    continue
                                rc = md_re_add(the_raid[DEV_devname], child_devname)

                            this_raid1['state'] = DEV_STATE_ONLINE
                        else:
                            reset_vm("vv_raid_assemble_core-md_re_add-failed")
                    # elif storagetype == STOR_TYPE_MEMORY:
                    elif (storagetype == STOR_TYPE_MEMORY) or (storagetype == STOR_TYPE_DISK):
                        md_stop(this_raid1['devname'])
                        for the_ibd in to_assemble_ibd_list:
                            parted_device(the_ibd['devname'], START_SECTOR, -1, 'p1')

                        if infrastructure_modified == True:
                            vv_save_infrastructure(setup_info)
                            vv_setup_current_infrastructure(setup_info)

                        if storagetype == STOR_TYPE_DISK:
                            for the_ibd in to_assemble_ibd_list:
                                set_new_disk(the_ibd['devname'])

                        debug("vv_raid_assemble_core: assemble failed")
                        return 2
                    else:
                        md_stop(this_raid1['devname'])
    else:
        debug('cannot running rejoin_raid5')
        return 1

    if infrastructure_modified == True:
        debug("vv_raid_assemble_core: save_ads_infrastructure ...")
        vv_save_infrastructure(setup_info)
        vv_setup_current_infrastructure(setup_info)

    '''
    if top_array_rebuilt == True:
        reset_vm('array_rebuild_reset')
    '''

    rc = 0
    if the_raid['state'] != DEV_STATE_ONLINE:
        rc = 1
    debug('vv_raid_assemble_core return %d' % rc)
    return rc


#
# The caller must already setup c_infrastructure
#
def vv_raid_assemble(vv_setup_info, status):
    debug("Enter vv_raid_assemble ...")
    c_infrastructure = vv_setup_info["c_infrastructure"]

    rc = 0
    for the_key in c_infrastructure:
        if the_key == INFR_memory:
            the_storagetype = STOR_TYPE_MEMORY
        else:
            the_storagetype = STOR_TYPE_DISK
        raid_list = c_infrastructure[the_key]
        for the_raid in raid_list:
            rc = 2
            while rc == 2:
                rc = vv_raid_assemble_core(vv_setup_info, the_raid, the_storagetype, status)
                debug('vv_raid_assemble return %d' % rc)
    return rc


#
# The caller must already setup c_infrastructure
#
def vv_memory_raid_assemble(vv_setup_info, status):
    debug("Enter vv_memory_raid_assemble ...")

    rc = 2
    while rc == 2:
        rc = 0
        c_infrastructure = vv_setup_info["c_infrastructure"]
        for the_key in c_infrastructure:
            if the_key == INFR_memory:
                the_storagetype = STOR_TYPE_MEMORY
            else:
                continue

            raid_list = c_infrastructure[the_key]
            for the_raid in raid_list:
                rc = vv_raid_assemble_core(vv_setup_info, the_raid, the_storagetype, status)
                debug('vv_memory_raid_assemble return %d' % rc)
                if rc == 2:
                    break
            if rc == 2:
                break
    return rc


#
# The caller must already setup c_infrastructure
#
def vv_nonmemory_raid_assemble(vv_setup_info, status):
    debug("Enter vv_nonmemory_raid_assemble...")

    rc = 2
    while rc == 2:
        rc = 0
        c_infrastructure = vv_setup_info["c_infrastructure"]
        for the_key in c_infrastructure:
            if the_key == INFR_disk:
                the_storagetype = STOR_TYPE_DISK
            else:
                continue

            raid_list = c_infrastructure[the_key]
            for the_raid in raid_list:
                rc = vv_raid_assemble_core(vv_setup_info, the_raid, the_storagetype, status)
                debug('vv_nonmemory_raid_assemble return %d' % rc)
                if rc == 2:
                    break
            if rc == 2:
                break
    return rc


def vv_get_mdadm_version():
    global MDADM_VERSION

    cmd_str = CMD_MDVERSION
    out = ['']
    rc = do_system(cmd_str, out)
    if rc != 0:
        return 1
    out_data = out[0].split(' - ')
    MDADM_VERSION = out_data[1]


def vv_init(arg_list):
    debug("Enter vv_init ...")

    vv_setup_info = {}
    rc = vv_load_conf(CP_CFG, vv_setup_info)
    if rc != 0:
        return rc

    #
    # generate info for all ibd, shared resources
    #
    vv_generate_resource_info(vv_setup_info)

    #
    # get all ibd storage ready (create, start, parted, raid1)
    #
    rc = vv_create_ibd_resource(vv_setup_info)
    if rc != 0:
        debug("ERROR: Failed to allocate resource from SVMs.")
        return rc
    next_idx = 1
    next_idx = vv_start_ibd_clients(vv_setup_info, next_idx, 10)
    vv_parted_ibd_devices(vv_setup_info)
    vv_setup_raid1_storage(vv_setup_info)
    next_md_idx = 0
    next_md_idx = vv_create_raid1_storage(vv_setup_info, next_md_idx)
    vv_parted_raid1_devices(vv_setup_info)

    #
    # always create memory storage first
    #
    next_md_idx = vv_create_memory_storage(vv_setup_info, next_md_idx)
    vv_create_memory_export(vv_setup_info)

    #
    # since shared device may use memory storage as vscaler,
    # so always create shared device after create memory storage
    #
    rc = vv_create_shared_device(vv_setup_info)
    if rc != 0:
        debug("ERROR: Failed to create shared storage.")
        return rc
    vv_init_shared_devices(vv_setup_info)

    #
    # Now to create all non-memory storage
    #
    next_md_idx = vv_create_nonmemory_storage(vv_setup_info, next_md_idx)

    # rc = vv_create_cache_storage(vv_setup_info)
    # if rc != 0:
    #     debug('ERROR: Failed to create cache device')
    #     return rc
    rc = vv_create_export_storage(vv_setup_info)
    if rc != 0:
        debug('ERROR: vv_create_export_storage failed')
        return rc
    #
    # generate infrastructure
    #
    vv_generate_infrastructure(vv_setup_info)
    vv_save_infrastructure(vv_setup_info)

    #
    # generate c_infrastructure
    # Notice: we need to re-establish shared_dev_list since vv_generate_infrastructure
    # destroy it
    #
    vv_setup_dev_list(vv_setup_info)
    vv_find_ibd_mapping(vv_setup_info)
    vv_setup_current_infrastructure(vv_setup_info)
    # vv_save_c_infrastructure(vv_setup_info)

    MdStatMgr.create_stat()
    return 0


def vv_start(arg_list):
    debug("Enter vv_start...")
    vv_uuid = arg_list[2]
    ha_uuid = None
    amcurl = None
    if len(arg_list) == 5:
        ha_uuid = arg_list[3]
        amcurl = arg_list[4]

    vv_setup_info = {}
    vv_setup_info['vv_uuid'] = vv_uuid

    if milio_config.is_ha:
        milio_config.ha_reload(vv_uuid)
        milio_settings.ha_reset_fs_mode()

    rc = vv_access_detail(vv_setup_info, 0)
    # rc = vv_access_detail(vv_setup_info, 2)
    if rc != 0:
        debug('vv_start: Can not load detail')
        return rc

    #
    # allocate all ibd resources
    #
    vv_setup_dev_list(vv_setup_info)
    next_idx = 1
    next_idx = vv_start_ibd_clients(vv_setup_info, next_idx, 60)

    #
    # generate c_infrastructure
    #
    vv_find_ibd_mapping(vv_setup_info)
    vv_setup_current_infrastructure(vv_setup_info)

    #
    # start memory storage before shared storage
    #
    rc = vv_memory_raid_assemble(vv_setup_info, VV_START)
    if rc != 0:
        reset_vm("vv_start-vv_memory_raid_assemble-return-error")
    rc = vv_start_memory_export(vv_setup_info)

    #
    # start shared storage
    #
    if ha_uuid != None and amcurl != None:
        debug("INFO: try to move shared storage if needed, and update volume location")
        ret = ha_unmange_one_resouce(vv_uuid)
        rc = vv_move_shared_storage(vv_uuid, ha_uuid)
        cmd = 'touch /var/log/HASM_MOVEDISK_' + vv_uuid
        (ret, msg) = ha_retry_cmd(cmd, 5, 2)
        ret = ha_manage_one_resouce(vv_uuid)
        if rc != 0:
            return rc
    rc = vv_start_shared_storage(vv_setup_info)
    if rc != 0:
        debug("ERROR: Failed to start shared storage.")
        return rc

    #
    # start arbitrator
    #
    ha_enabled = check_ha_enabled()
    if ha_enabled:
        (pill_ibd_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
        debug("start arbitrator for: %s" % str(pill_ibd_list))
        if len(pill_ibd_list) > 0:
            arb_start(vv_uuid, pill_ibd_list)

    #
    # start nonmemory storage
    #
    rc = vv_nonmemory_raid_assemble(vv_setup_info, VV_START)
    vv_setup_current_infrastructure(vv_setup_info)
    # vv_save_c_infrastructure(vv_setup_info)
    vv_save_infrastructure(vv_setup_info)

    if rc != 0:
        debug('can not assemble nonmemory storage')
        return 1
    #
    # after vv_raid_assemble, c_infrastructure could be changed
    #
    rc = vv_start_export_storage(vv_setup_info)
    if rc != 0:
        debug('can not start export storage for fastfailover.')
        return 1
    # vv_start_md_monitor()
    return rc


def vv_readd(arg_list):
    debug("Enter vv_readd ...")
    vv_uuid = arg_list[2]
    exportname = arg_list[3]
    vv_setup_info = {}
    vv_setup_info['vv_uuid'] = vv_uuid
    vv_setup_info['ibd_uuid'] = exportname
    if milio_config.is_ha:
        milio_config.ha_reload(vv_uuid)
        milio_settings.ha_reset_fs_mode()

    vv_get_mdadm_version()

    rc = vv_access_detail(vv_setup_info, 4)
    # rc = vv_access_detail(vv_setup_info, 2)
    if rc != 0:
        debug('vv_readd: Can not load detail')
        return rc

    #
    # generate c_infrastructure
    #
    vv_setup_dev_list(vv_setup_info)
    # vv_start_shared_storage(vv_setup_info, True)
    vv_find_ibd_mapping(vv_setup_info)
    vv_setup_current_infrastructure(vv_setup_info)
    rc = vv_raid_assemble(vv_setup_info, VV_READD)

    #
    # after vv_raid_assemble, c_infrastructure could be changed
    #
    vv_setup_current_infrastructure(vv_setup_info)
    # vv_save_c_infrastructure(vv_setup_info)
    # return rc
    if rc != 0:
        debug("vv_readd: Failed with %d" % rc)
    return 0


def vv_stop(arg_list):
    debug("Enter vv_stop ...")
    vv_uuid = arg_list[2]
    cmd_str = "python /opt/milio/atlas/roles/aggregate/agexport.pyc -S"
    do_system(cmd_str)
    cmd_str = CMD_MDSTOP + " --scan"
    rc = do_system(cmd_str)
    if rc != 0:
        reset_vm("vv_stop_md_reset")

    cmd_str = "rm /tmp/dev-uuid-*"
    do_system(cmd_str)
    cmd_str = CMD_IBDMANAGER_A_STOP
    do_system(cmd_str)
    return 0


"""
USX 2.0 VDI related methods
"""


def vdi_get_resource_info(vv_setup_info):
    debug("Enter vdi_get_resource_info ...")
    configure = vv_setup_info['configure']

    raidplans = configure['volumeresources'][0]['raidplans']
    vv_setup_info['ibd_dev_list'] = []
    ibd_dev_list = vv_setup_info['ibd_dev_list']

    for raidplan in raidplans:
        plandetail = json.loads(raidplan['plandetail'])
        for subplan in plandetail['subplans']:
            if subplan.has_key('raidbricks'):
                for raidbrick in subplan['raidbricks']:
                    the_ibd = {}
                    the_ibd[IBD_uuid] = raidbrick['euuid']
                    the_ibd[IBD_devname] = None
                    the_ibd[IBD_raid1number] = raidbrick['pairnumber']
                    the_ibd[IBD_detail] = raidbrick
                    ibd_dev_list.append(the_ibd)
            if subplan.has_key('sharedstorages'):
                for sharedstorage in subplan['sharedstorages']:
                    the_shared = {}
                    the_shared[IBD_devname] = "/dev/usx/" + sharedstorage['euuid']
                    the_shared[IBD_detail] = sharedstorage
                    ibd_dev_list.append(the_shared)

    debug(ibd_dev_list)


def vdi_create_compound_device(vv_setup_info):
    debug("Enter vdi_create_compound_device ...")
    ibd_dev_list = vv_setup_info['ibd_dev_list']

    for the_ibd in ibd_dev_list:
        the_raidbrick = the_ibd[IBD_detail]

        volume_type = vv_setup_info['configure']['volumeresources'][0]['volumetype']
        debug("volume_type is %s" % (volume_type))
        if volume_type.upper() == "SIMPLE_HYBRID":
            debug("SIMPLE_HYBRID - create big buffer file")
            # code to create the file
            big_buffer_flag = open('/etc/ilio/big_buffer', 'a')
            os.utime('/etc/ilio/big_buffer', None)
            big_buffer_flag.close()
            the_raidbrick["deviceoptions"].append("BIGBUFFER")

        clone_enabled = vv_setup_info['configure']['volumeresources'][0].get('snapcloneenabled')
        if clone_enabled == None:
            clone_enabled = True

        # Enable snapclone UNLY if volume type is Simple-InMemory
        if clone_enabled == True and volume_type.upper() == "SIMPLE_MEMORY":
            the_raidbrick['p_snapcloneenabled'] = True
        else:
            the_raidbrick['p_snapcloneenabled'] = False

        the_json_str = json.dumps(the_raidbrick)
        the_json_str_base64 = base64.urlsafe_b64encode(the_json_str)
        cmd_str = 'python ' + CMD_AGGCREATE + " " + the_json_str_base64
        rc = do_system(cmd_str)
        if rc != 0:
            debug("ERROR : %s failed" % cmd_str)
            return rc
    return 0


def vdi_start_compound_device(vv_setup_info):
    debug("Enter vdi_start_compound_device ...")
    ibd_dev_list = vv_setup_info['ibd_dev_list']

    for the_ibd in ibd_dev_list:
        the_raidbrick = the_ibd[IBD_detail]
        volume_type = vv_setup_info['configure']['volumeresources'][0]['volumetype']
        debug("volume_type is %s" % (volume_type))
        if volume_type.upper() == "SIMPLE_HYBRID":
            debug("SIMPLE_HYBRID - check for big buffer file")
            # code to check if the file exists, ie
            if os.path.isfile('/etc/ilio/big_buffer'):
                the_raidbrick["deviceoptions"].append("BIGBUFFER")

        clone_enabled = vv_setup_info['configure']['volumeresources'][0].get('snapcloneenabled')
        if clone_enabled == None:
            clone_enabled = True

        # Enable snapclone UNLY if volume type is Simple-InMemory
        if clone_enabled == True and volume_type.upper() == "SIMPLE_MEMORY":
            the_raidbrick['p_snapcloneenabled'] = True
        else:
            the_raidbrick['p_snapcloneenabled'] = False

        the_json_str = json.dumps(the_raidbrick)
        the_json_str_base64 = base64.urlsafe_b64encode(the_json_str)
        is_need_init = False
        if not clone_enabled and volume_type.upper() == "SIMPLE_MEMORY":
            is_need_init = True

        if not is_need_init:
            cmd_str = 'python ' + CMD_AGGSTART + " " + the_json_str_base64
        else:
            # If snapclone is disabled we need to create device each time we start VM
            cmd_str = 'python ' + CMD_AGGCREATE + " " + the_json_str_base64
        rc = do_system(cmd_str)
        if rc != 0:
            debug("ERROR : %s failed" % cmd_str)
            return rc
    return 0


def vdi_init(arg_list):
    debug("Enter vdi_init ...")

    vv_setup_info = {}
    rc = vv_load_conf(CP_CFG, vv_setup_info)
    if rc != 0:
        return rc

    # get compund device info ready for vdi
    vdi_get_resource_info(vv_setup_info)

    # create compound device for VDI volume
    rc = vdi_create_compound_device(vv_setup_info)

    return rc


def vdi_start(arg_list):
    debug("Enter vdi_start...")

    vv_setup_info = {}
    rc = vv_load_conf(CP_CFG, vv_setup_info)
    if rc != 0:
        return rc

    # get compund device info ready for vdi
    vdi_get_resource_info(vv_setup_info)

    # start compound device for VDI volume
    rc = vdi_start_compound_device(vv_setup_info)

    return rc


"""
End VDI related methods
"""


#
# --------------------------- NEW VV SECTION END ------------------------------
#


def ads_vp_lock(pv_name):
    debug("Enter ads_vp_lock ...")
    # curl -s -k -X GET -H "Accept: application/json"  -H "Content-Type:application/json" http://127.0.0.1:8080/amc/grids/lock?lockName=VC13809_Mem-Neil-143&timeout=20&locktime=80
    cmd_str = 'curl -s -k -X GET -H "Accept: application/json"  -H "Content-Type:application/json" ' + \
              LOCAL_AGENT + '/grid/lock?lockName=' + pv_name + '&timeout=' + str(VP_TIMEOUT) + \
              '&locktime=' + str(VP_LOCKTIME)

    while True:
        lock_flag = 0
        rc = 0
        out = ['']
        rc = do_system(cmd_str, out)

        if rc == 0:
            if out[0] == 'true':
                lock_flag = 1
                break
            else:
                lock_flag = 0
                time.sleep(VP_SLEEPTIME)
        else:
            break

    return rc


def ads_vp_unlock(pv_name):
    debug("Enter ads_vp_unlock ...")
    # https://10.21.138.1:8443/amc/grids/lock?lockName=VC13809_Mem-Neil-143
    cmd_str = 'curl -s -k -X POST -H "Accept: application/json"  -H "Content-Type:application/json" ' + \
              LOCAL_AGENT + '/grid/lock?lockName=' + pv_name

    lock_flag = 0
    rc = 0
    out = ['']
    rc = do_system(cmd_str, out)

    if rc == 0:
        if out[0] == 'true':
            lock_flag = 1
        else:
            lock_flag = 0
            rc = 1

    return rc


def remove_config_sections(config_file_name, section_list):
    ini_config = ConfigParser.ConfigParser()
    try:
        ini_config.read(config_file_name)
    except:
        debug('Cannot read %s.' % IBD_AGENT_CONFIG_FILE)
        return 1

    debug('configure section list is %s: ' % str(section_list))
    debug('configure sections before remove in %s: %s' % (config_file_name, str(ini_config.sections())))
    for the_section in section_list:
        ini_config.remove_section(the_section)
    debug('configure sections after remove in %s: %s' % (config_file_name, str(ini_config.sections())))

    cfgfile = open(config_file_name, 'w')
    ini_config.write(cfgfile)
    cfgfile.close()
    return 0


#
# return 0: ok
#
def device_check_partition_old(device):
    devname = device['devname']
    cmd_str = CMD_PARTED + ' -m -s -- %s unit s print free' % devname
    rc = do_system_timeout(cmd_str, 10)
    if rc != 0:
        debug("device_check_partition error for %s" % devname)
        return 1
    return 0


#
# return 0: ok
#
def device_check_partition(device):
    debug('Enter device_check_partition ' + str(device))
    devname = device['devname']
    tmp = devname.split('/')
    filename = '/tmp/part_' + tmp[-1]
    try:
        os.remove(filename)
    except OSError:
        pass
    # dd if=/dev/ibd1 of=header bs=4k count=5
    cmd_str = 'dd if=' + devname + ' of=' + filename + ' bs=4k count=5 oflag=direct iflag=direct '
    rc = do_system_timeout(cmd_str, 20)
    if rc != 0:
        debug("cmd %s failed " % cmd_str)
        return 1

    # od -x  header | grep "0001000 4645 2049 4150 5452"
    cmd_str = 'od -x ' + filename + '  | grep "0001000 4645 2049 4150 5452"'
    rc = do_system_timeout(cmd_str, 10)
    if rc != 0:
        debug("device_check_partition error for %s" % devname)
        return 1

    return 0


#
# return 0: ok
#
def ibd_check_partition(device):
    rc = device_check_partition(device)
    return rc


def ibd_check_and_wait_array_partition(device):
    global MDADM_VERSION
    global RAID1_DATA_OFFSET
    debug('get ibd status for debug')
    (ret, msg) = runcmd(CMD_IBDMANAGER_STAT, print_ret=True)
    debug("check the ibd working state: " + str(msg))
    rc = ibd_check_partition(device)
    if rc != 0:
        return rc

    dev = device['devname'] + 'p1'
    property = {}
    rc = device_examine(dev, property)
    if rc == 0 and MDADM_VERSION >= MDADM_VERSION_3_3:
        RAID1_DATA_OFFSET = "--data-offset=" + property[DATA_OFFSET] + "s"
        debug("The data offset of %s: %s" % (dev, RAID1_DATA_OFFSET))

    return rc


#
# return 0:ok
#
def device_check_partition_oob(ibd_uuid, ip):
    filename = "/tmp/" + ibd_uuid + "-20K-header"
    cmd_str = CMD_IBDMANAGER + ' -r s -a ' + ip + \
              ' read ' + ' ' + ibd_uuid + ' ' + str(0) + ' ' + str(20480) + ' ' + filename
    rc = do_system_timeout(cmd_str, 30)
    if rc != 0:
        debug("device_check_partition_oob: failed to fetch header for %s from %s" % (ibd_uuid, ip))
        return 1

    cmd_str = 'od -x ' + filename + '  | grep "0001000 4645 2049 4150 5452"'
    rc = do_system_timeout(cmd_str, 5)
    if rc != 0:
        debug("device_check_partition_oob: error for %s" % ibd_uuid)
        return 2
    return 0


#
# return 0: ok
#
def raid1_check_partition(device):
    rc = device_check_partition(device)
    return rc


def raid1_check_and_wait_partition(device):
    rc = raid1_check_partition(device)
    if rc != 0:
        return rc

    part_devname = device['devname'] + 'p1'
    while os.path.exists(part_devname) != True:
        time.sleep(1)
    return 0


def raid1_check_and_wait_array_partition(device):
    rc = raid1_check_partition(device)
    if rc != 0:
        return rc

    part_devname = device['devname'] + 'p1'
    cmd_str = CMD_MDEXAMINE + ' ' + part_devname
    rc = do_system_timeout(cmd_str, 10)
    if rc != 0:
        time.sleep(1)
        rc = do_system_timeout(cmd_str, 10)
    return rc


def raid1_check_and_wait_array_partition_robo(device):
    rc = raid1_check_partition(device)
    if rc != 0:
        return rc
    part_devname = device['devname']
    cmd_str = CMD_DETAIL + ' ' + part_devname
    rc = do_system_timeout(cmd_str, 10)
    if rc != 0:
        time.sleep(1)
        rc = do_system_timeout(cmd_str, 10)
    return rc


def raid1_check_and_wait_partition_list(dev_list):
    for the_raid1 in dev_list:
        rc = raid1_check_and_wait_partition(the_raid1)
        if rc != 0:
            return rc
    return 0


def raid1_check_and_wait_array_partition_list_robo(dev_list):
    for the_raid1 in dev_list:
        rc = raid1_check_and_wait_array_partition_robo(the_raid1)
        if rc != 0:
            return rc
    return 0


def raid1_check_and_wait_array_partition_list(dev_list):
    for the_raid1 in dev_list:
        rc = raid1_check_and_wait_array_partition(the_raid1)
        if rc != 0:
            return rc
    return 0


def create_ads_link(md_dev, adsname, vgname):
    cmd_str = CMD_LINK + " -f -s " + md_dev + " " + "/dev/" + adsname + "_" + vgname
    rc = do_system(cmd_str)
    return rc


def ibd_get_export(devname, export, wud_mapping):
    for the_key in wud_mapping:
        if wud_mapping[the_key] == devname:
            export['exportname'] = the_key
            return


def set_atlas_disk(disk_dict):
    cmd_str = "./diskmgr.py isatl " + disk_dict["devname"]
    out_stream = os.popen(cmd_str, 'r', 1)
    out_data = out_stream.read().split('\n')
    if out_data == '1':
        disk_dict["isatl"] = "yes"
    else:
        disk_dict["isatl"] = "no"


def set_ibd_list_size(ibd_dev_list):
    for the_dev in ibd_dev_list:
        the_idx = the_dev['idx']
        sizefilename = '/sys/class/block/ibd' + str(the_idx) + '/size'
        try:
            sizefile = open(sizefilename, 'r')
            size_str = sizefile.read()
            sizefile.close()
        except:
            debug("Cannot get ibd size from " + sizefilename)
            the_dev['size'] = 0
        else:
            the_dev['size'] = int(size_str) * 512
    return


#
# setup uuid_dev_mappping
#
def find_ibd_online_info(setup_info):
    print 'enter find_ibd_online_info ...'
    setup_info['wud_mapping'] = {}
    wud_mapping = setup_info['wud_mapping']
    cmd_str = CMD_IBDMANAGER_STAT_WUD
    out = ['']
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    for the_line in lines:
        line_parts = the_line.split(' ')
        if len(line_parts) < 2:
            continue
        exportname = line_parts[0]
        devname = line_parts[1]
        wud_mapping[exportname] = devname

    setup_info['ud_mapping'] = {}
    ud_mapping = setup_info['ud_mapping']
    cmd_str = CMD_IBDMANAGER_STAT_UD
    out = ['']
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    for the_line in lines:
        line_parts = the_line.split(' ')
        if len(line_parts) < 2:
            continue
        exportname = line_parts[0]
        devname = line_parts[1]
        ud_mapping[exportname] = devname
    return


#
# setup c_infrastructure:
#
def vg_setup_current_infrastructure_core(vg_setup_info):
    configure = vg_setup_info['configure']
    vgname_str = configure['vgname']
    infrastructure = vg_setup_info['infrastructure']
    c_infrastructure = copy.deepcopy(infrastructure)
    vg_setup_info['c_infrastructure'] = c_infrastructure
    c_sub_infrastructure = vg_setup_info['c_infrastructure'][configure['pool_infrastructure_type']]

    print "vg_setup_current_infrastructure_core_step_1"
    print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # setup uuid_dev_mappping to get the latest state of ibds
    find_ibd_online_info(vg_setup_info)
    wud_mapping = vg_setup_info['wud_mapping']
    ud_mapping = vg_setup_info['ud_mapping']

    debug("vg_setup_current_infrastructure_core_step_2")
    debug(json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    # setup devname, set its state
    for the_raid in c_sub_infrastructure:  # raid0|raid5
        #
        # setup the_raid state:
        #   DISCONNECT | ONLINE
        #
        the_raid['state'] = DEV_STATE_DISCONNECT
        the_raid['devname'] = get_md_by_uuid(the_raid['uuid'])
        the_raid_devname = the_raid['devname']
        if the_raid_devname != None:
            the_raid['state'] = DEV_STATE_ONLINE

        for the_raid1 in the_raid['children']:  # raid1
            # To skip the check for shared storage
            if the_raid1.has_key('children') == False:
                the_raid1['state'] = DEV_STATE_ONLINE
                the_raid1['working'] = True
                continue

            the_raid1['state'] = DEV_STATE_DISCONNECT
            the_raid1['working'] = False
            the_raid1['devname'] = get_md_by_uuid(the_raid1['uuid'])
            if the_raid1['devname'] == None:
                the_raid1['devname'] = load_dev_uuid(the_raid1['uuid'])
            the_raid1_devname = the_raid1['devname']
            if the_raid1_devname != None:
                the_raid1['state'] = DEV_STATE_OFFLINE

            #
            # setup ibd state:
            #   DISCONNECT | OFFLINE | FAILED | ONLINE
            #
            for the_ibd in the_raid1['children']:  # ibd
                the_ibd['state'] = DEV_STATE_DISCONNECT
                the_ibd['devname'] = None
                the_ibd_uuid = the_ibd['uuid']
                if wud_mapping.has_key(the_ibd_uuid):
                    the_ibd['devname'] = wud_mapping[the_ibd_uuid]
                    the_ibd['state'] = DEV_STATE_OFFLINE
                elif ud_mapping.has_key(the_ibd_uuid):
                    the_ibd['devname'] = ud_mapping[the_ibd_uuid]
                    the_ibd['state'] = DEV_STATE_DISCONNECT

            if the_raid1['state'] != DEV_STATE_DISCONNECT:
                property_dict = {}
                raid_detail_nohung(the_raid1_devname, property_dict)
                debug("property_dict of " + the_raid1_devname)
                debug(property_dict)
                for the_ibd in the_raid1['children']:
                    if the_ibd['state'] == DEV_STATE_DISCONNECT:
                        continue

                    the_ibd_devname = the_ibd['devname']
                    if property_dict.has_key(the_ibd_devname) == True:
                        if property_dict[the_ibd_devname] == 'active':
                            if the_ibd['state'] != DEV_STATE_DISCONNECT:
                                the_raid1['working'] = True
                            the_ibd['state'] = DEV_STATE_ONLINE
                        else:
                            the_ibd['state'] = DEV_STATE_FAILED
                    else:
                        debug(the_ibd_devname + " is not in detail of " + the_raid1_devname)

        #
        # setup raid1 state
        #   DISCONNECT | OFFLINE | FAILED | ONLINE
        #
        if the_raid['state'] != DEV_STATE_DISCONNECT:
            property_dict = {}
            raid_detail_nohung(the_raid_devname, property_dict)
            for the_raid1 in the_raid['children']:  # raid1
                the_raid1_devname = the_raid1['devname']
                if property_dict.has_key(the_raid1_devname) == True:
                    if property_dict[the_raid1_devname] == 'active':
                        the_raid1['state'] = DEV_STATE_ONLINE
                    else:
                        the_raid1['state'] = DEV_STATE_FAILED

    debug("vg_setup_current_infrastructure_core_step_3")
    debug(json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    return 0


def vg_setup_current_infrastructure(vg_setup_info):
    configure = vg_setup_info['configure']

    if configure.has_key('virtualpool') == False:
        vg_fetch_infrastructure(vg_setup_info)

    find_ibd_online_info(vg_setup_info)
    vg_setup_current_infrastructure_core(vg_setup_info)
    save_c_infrastructure(vg_setup_info)
    return


def ads_vp_setup_current_infrastructure(vg_setup_info):
    find_ibd_online_info(vg_setup_info)
    vg_setup_current_infrastructure_core(vg_setup_info)
    save_c_infrastructure(vg_setup_info)
    return


#
# Find a vg related information:
#   first_level_raid_list: all ibd devices
#   second_level_raid_list: all vg's devices
#   has_missing_list: all RAIDs (raid1) which has missing device
#
def find_vg_info(pool_info):
    debug('Enter find_vg_info')
    configure = pool_info['configure']
    vgname_str = configure['vgname']
    pool_info['first_level_raid_list'] = []
    first_level_raid_list = pool_info['first_level_raid_list']
    pool_info['second_level_raid_list'] = []
    second_level_raid_list = pool_info['second_level_raid_list']
    pool_info['has_missing_list'] = []
    has_missing_list = pool_info['has_missing_list']
    pool_info['ibd_dev_list'] = []
    ibd_dev_list = pool_info['ibd_dev_list']

    vg_setup_current_infrastructure(pool_info)
    wud_mapping = pool_info['wud_mapping']

    # get the dev info directly from c_infrastructure instead of /proc/mdstat
    c_infrastructure = pool_info['c_infrastructure']
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        second_level_raid_list.append({'devname': the_raid['devname'], 'uuid': the_raid['uuid']})
        for the_raid1 in the_raid['children']:  # raid1
            first_level_raid_list.append({'devname': the_raid1['devname'], 'uuid': the_raid1['uuid']})
            if the_raid1.has_key('children') == False:
                continue

            for the_ibd in the_raid1['children']:
                the_dev = the_ibd
                the_devname = the_dev['devname']
                if the_devname == None:
                    debug("IBD %s not connected, skip." % str(the_ibd))
                    continue
                the_dev['idx'] = the_devname.split('ibd')[1]
                # the_dev['md'] = the_raid1
                the_export = {}
                ibd_get_export(the_dev['devname'], the_export, wud_mapping)
                the_ibd['export'] = the_export
                ibd_dev_list.append(the_dev)

    set_ibd_list_size(ibd_dev_list)

    debug('find_vg_info first_level_raid_list:')
    debug(first_level_raid_list)

    debug('find_vg_info second_level_raid_list:')
    debug(second_level_raid_list)

    debug('find_vg_info ibd_dev_list:')
    debug(ibd_dev_list)

    return 0


def start_vg(vg_name):
    cmd_str = CMD_VGACTIVE + ' ' + vg_name
    rc = do_system(cmd_str)
    return rc


def start_ibd_clients(setup_info, next_idx, timeout):
    debug('Enter start_ibd_clients')

    configure = setup_info['configure']
    all_ibd_imports = configure['imports']
    ibd_dev_list = setup_info['ibd_dev_list']
    ibd_drop_list = setup_info['ibd_drop_list']
    vgname_str = configure['vgname']
    ini_config = ConfigParser.ConfigParser()
    try:
        ini_config.read(IBD_AGENT_CONFIG_FILE)
    except:
        debug('Cannot read %s.' % IBD_AGENT_CONFIG_FILE)
        return 1

    debug('ibd agent sections before start_ibd_clients update: %s' % str(ini_config.sections()))
    try:
        ini_config.add_section(IBD_AGENT_SEC_GLOBAL)
    except:
        pass

    device_nr = 0
    for the_device in all_ibd_imports:
        the_exportname = the_device["uuid"]
        the_ip = the_device["ip"]
        the_idx = next_available_ibd_idx(next_idx)
        next_idx = the_idx + 1
        the_devname = "/dev/ibd" + str(the_idx)
        the_minor = the_idx * 16
        the_iw_hook = CMD_CPREADD + ' ' + vgname_str + ' ' + the_exportname
        the_wr_hook = CMD_CPSETIOERROR + ' ' + vgname_str + ' ' + the_exportname + ' ' + the_devname
        the_rw_hook = CMD_CPUNSETIOERROR + ' ' + vgname_str + ' ' + the_exportname + '; ' + \
                      CMD_CPREADD + ' ' + vgname_str + ' ' + the_exportname
        if configure.has_key('adsname') == True:
            the_iw_hook += ' ' + configure['adsname']
            the_rw_hook += ' ' + configure['adsname']

        try:
            debug(the_devname)
            ini_config.add_section(the_exportname)
            ini_config.set(the_exportname, "devname", the_devname)
            ini_config.set(the_exportname, "minor", the_minor)
            ini_config.set(the_exportname, "ip", the_ip)
            ini_config.set(the_exportname, "iw_hook", the_iw_hook)
            ini_config.set(the_exportname, "wr_hook", the_wr_hook)
            ini_config.set(the_exportname, "rw_hook", the_rw_hook)
            debug('ibd agent current sections during start_ibd_clients update: %s' % str(ini_config.sections()))
        except:
            debug('duplicate ibd agent section during start_ibd_clients update: %s' % the_exportname)
            pass
        device_nr += 1
        the_device['idx'] = the_idx
        the_device['devname'] = the_devname
        debug("Configing ibd device: %s" % str(the_device))

    if device_nr == 0:
        return

    cfgfile = open(IBD_AGENT_CONFIG_FILE, 'w')
    ini_config.write(cfgfile)
    debug('ibd agent sections after start_ibd_clients update: %s' % str(ini_config.sections()))
    cfgfile.close()
    rc = ibd_agent_alive()
    if rc == False:
        cmd_str = CMD_IBDAGENT
    else:
        cmd_str = CMD_IBDMANAGER_A_UPDATE
    rc = do_system(cmd_str)

    cmd_str = CMD_IBDMANAGER_STAT_WU
    device_len = 0
    wait_nr = 0
    debug("device_len:" + str(device_len) + ", device_nr:" + str(device_nr))
    connecting_ibds = list(all_ibd_imports)
    while (len(connecting_ibds) != 0 and wait_nr < timeout):
        if wait_nr != 0:
            time.sleep(1)
        wait_nr += 1
        out = ['']
        rc = do_system(cmd_str, out)
        for the_device in connecting_ibds:
            the_exportname = the_device["uuid"]
            if re.search(r'\b' + re.escape(the_exportname) + r'\b', out[0]):
                debug("Found " + the_exportname)
                connecting_ibds.remove(the_device)

    active_dev = out[0].split('\n')
    debug("All IBDs:", all_ibd_imports)
    for the_device in all_ibd_imports:
        the_exportname = the_device["uuid"]
        got_it = False
        for the_uuid in active_dev:
            if the_uuid == the_exportname:
                got_it = True
                break

        the_devname = the_device['devname']
        the_idx = the_device['idx']
        del the_device['devname']
        del the_device['idx']

        the_ibd = {'devname': the_devname, 'idx': the_idx, 'import': the_device}
        debug(the_ibd)

        if got_it == True:
            ibd_dev_list.append(the_ibd)
            debug("connected:" + the_ibd["devname"])
        else:
            ibd_drop_list.append({'ibd': the_ibd, 'reason': 'failed'})
            debug("not connected:" + the_ibd["devname"])

    debug(ibd_dev_list)
    debug(ibd_drop_list)

    time.sleep(2);
    tune_all_ibd(ibd_dev_list)
    set_ibd_list_size(ibd_dev_list)
    return next_idx


def check_ibd_list(setup_info, ini_config, ibd_check_list, next_idx, timeout, reserved_size):
    debug('Enter check_ibd_list')
    configure = setup_info['configure']
    ibd_dev_list = setup_info['ibd_dev_list']
    ibd_drop_list = setup_info['ibd_drop_list']
    vgname_str = configure['vgname']
    adsname_str = configure['adsname']
    ibd_usable_list = []

    rc = 0
    time_end = time.time() + timeout
    for the_device in ibd_check_list:
        if the_device["devname"] == "missing":
            ibd_usable_list.append({'devname': 'missing', 'idx': '-1', 'size': 0})
            continue

        # create the exportname
        the_exportname = the_device["uuid"] + "_" + adsname_str
        the_ip = the_device["ip"]
        the_size = reserved_size / (1024 * 1024)
        the_idx = next_available_ibd_idx(next_idx)
        next_idx = the_idx + 1
        the_devname = "/dev/ibd" + str(the_idx)
        the_minor = the_idx * 16
        the_iw_hook = CMD_CPREADD + ' ' + vgname_str + ' ' + the_exportname
        the_wr_hook = CMD_CPSETIOERROR + ' ' + vgname_str + ' ' + the_exportname + ' ' + the_devname
        the_rw_hook = CMD_CPUNSETIOERROR + ' ' + vgname_str + ' ' + the_exportname + '; ' + \
                      CMD_CPREADD + ' ' + vgname_str + ' ' + the_exportname
        if configure.has_key('adsname') == True:
            the_iw_hook += ' ' + configure['adsname']
            the_rw_hook += ' ' + configure['adsname']

        new_device = {}
        new_device['devname'] = the_devname
        new_device['uuid'] = the_exportname
        new_device['ip'] = the_device['ip']
        new_device['free'] = 0
        new_device['size'] = reserved_size
        the_ibd = {'devname': new_device['devname'], 'idx': the_idx, 'size': new_device['size'], 'import': new_device}

        # create a new ibd client
        args_str = CMD_AGGCREATE + the_device["uuid"] + " " + the_exportname + " " + str(the_size)

        (rc, out, err) = remote_exec(the_ip, 'python ', args_str)

        if rc != 0:
            debug('create a new export %s with size %d from %s failed, errcode=%d, errmsg=%s' % (
                the_exportname, the_size, the_ip, rc, err))
            return rc

        # ibd connection
        try:
            ini_config.add_section(the_exportname)
            ini_config.set(the_exportname, "devname", the_devname)
            ini_config.set(the_exportname, "minor", the_minor)
            ini_config.set(the_exportname, "ip", the_ip)
            ini_config.set(the_exportname, "iw_hook", the_iw_hook)
            ini_config.set(the_exportname, "wr_hook", the_wr_hook)
            ini_config.set(the_exportname, "rw_hook", the_rw_hook)
            debug('ibd agent current sections during check_ibd_list update: %s' % str(ini_config.sections()))
        except:
            debug('duplicate ibd agent section during check_ibd_list update: %s' % the_exportname)
            pass

        # add this device into ibd_usable_list
        ibd_usable_list.append(the_ibd)

    for the_device in ibd_usable_list:
        ibd_dev_list.append(the_device)

    return 0


def parted_ibd_devices(setup_info):
    debug('Enter parted_ibd_devices...')
    device_list = setup_info['ibd_dev_list']
    for the_dev in device_list:
        if the_dev['devname'] == "missing":
            continue
        cmd_str = CMD_MDADM + " --zero-superblock " + the_dev['devname']
        do_system_timeout(cmd_str, 10)
        rc = parted_device(the_dev['devname'], START_SECTOR, -1, 'p1')
        if rc != 0:
            return rc
    return 0


def parted_first_level_devices(setup_info):
    debug('Enter parted_first_level_devices...')
    first_level_raid_list = setup_info['first_level_raid_list']
    for the_group in first_level_raid_list:
        for the_dev in the_group:
            rc = parted_device(the_dev['devname'], START_SECTOR, -1, 'p1')
            if rc != 0:
                return rc
    return 0


def parted_second_level_devices(setup_info):
    debug('Enter parted_second_level_devices...')
    device_list = setup_info['second_level_raid_list']
    for the_dev in device_list:
        rc = parted_device(the_dev['devname'], START_SECTOR, -1, 'p1')
        if rc != 0:
            return rc
    return 0


def parted_shared_devices(setup_info):
    debug('Enter parted_shared_devices...')
    device_list = setup_info['shared_dev_list']
    for the_dev in device_list:
        rc = parted_device(the_dev['devname'], START_SECTOR, -1, '1')
        if rc != 0:
            debug('ERROR: Failed to partition %s ' % the_dev['devname'])
            return rc
    return 0


#
#
#
def start_shared_storage(setup_info):
    disk_list = setup_info['sharedstorages']
    setup_info['shared_dev_list'] = []
    shared_dev_list = setup_info['shared_dev_list']

    #
    # bring up all shared devices
    #
    scsi_hotscan()

    #
    # find out all shared devices
    #
    cmd = 'lsscsi'
    (ret, all_scsi_list) = runcmd(cmd, print_ret=True, lines=True)
    for the_disk in disk_list:  # the_disk sample: {"scsibus":"0:2","xxx.vmdk"}
        if the_disk.has_key('scsibus') == True:
            for line in all_scsi_list:
                line = line.split()
                the_scsi = line[0][1:][:-1]  # the_scsi = host:channel:target:lun
                tmp = the_scsi.split(':')  # ["0", "0", "2", "0"]
                scsibus = tmp[0] + ':' + tmp[2]  # "0:2"
                if scsibus == the_disk["scsibus"]:
                    shared_dev_list.append({"devname": line[-1], "uuid": scsibus})
    return


#
# Generate:
# 1> high_gap_group:
# 2> ibd_drop_list
#
# Note:
#  For raid0, there could be only one or two ibds
#  For raid5, there should be at least 3 ibds
#  For both types, empty ibd_dev_list should be acceptable
#
def grouping_ibd_list(setup_info):
    configure = setup_info['configure']
    if configure.has_key('raidtype'):
        if configure['raidtype'] == 'RAID_0':
            raid_type = POOL_TYPE_RAID0
        elif configure['raidtype'] == 'RAID_5':
            raid_type = POOL_TYPE_RAID5
        else:
            return -1
    else:
        pool_setup_info = {}
        rc = load_conf(CP_CFG, pool_setup_info)
        raid_type = pool_setup_info['configure']['raidtype']
        setup_info['configure']['raidtype'] = raid_type

    high_gap_ratio = setup_info['gap_ratio']
    if high_gap_ratio == 0:
        high_gap_ratio = GAP_RATIO_MAX
    ibd_list = setup_info['ibd_dev_list']
    ibd_list.sort(key=operator.itemgetter("size"), reverse=True)

    #
    # generate high_gap_group_list
    #
    setup_info['high_gap_group_list'] = []
    high_gap_group_list = setup_info['high_gap_group_list']
    the_group = []
    for the_ibd in ibd_list:
        if len(the_group) == 0:
            the_group.append(the_ibd)
            the_size = the_ibd['size']
        else:
            if (the_size - the_ibd['size']) * 100 / the_size <= high_gap_ratio:
                the_group.append(the_ibd)
            else:
                high_gap_group_list.append(the_group)
                the_group = []
                the_group.append(the_ibd)
                the_size = the_ibd['size']
    if len(the_group) > 0:
        high_gap_group_list.append(the_group)

    #
    #
    #
    for i in range(len(high_gap_group_list)):
        the_high_gap_group = high_gap_group_list.pop()
        the_group = []
        the_sub_group = []
        counter = 0
        for the_ibd in the_high_gap_group:
            counter = counter + 1
            the_sub_group.append(the_ibd)
            if counter == 6:
                counter = 0
                the_group.append(the_sub_group)
                the_sub_group = []
        if len(the_sub_group) > 0:
            the_group.append(the_sub_group)
        if len(the_group) > 0:
            high_gap_group_list.insert(0, the_group)

    setup_info['drop_group'] = []
    drop_group = setup_info['drop_group']
    for i in range(len(high_gap_group_list)):
        the_group = high_gap_group_list.pop()
        for j in range(len(the_group)):
            the_sub_group = the_group.pop()
            if raid_type == POOL_TYPE_RAID0 or len(the_sub_group) >= 3:
                the_group.insert(0, the_sub_group)
            else:
                drop_group.append(the_sub_group)
        if len(the_group) > 0:
            high_gap_group_list.insert(0, the_group)

    #
    # insert missing dev if necessary
    #
    for the_group in high_gap_group_list:
        for the_sub_group in the_group:
            if len(the_sub_group) == 1:
                the_sub_group.insert(1, {'devname': 'missing', 'idx': '-1', 'size': 0})
            elif len(the_sub_group) == 2:
                the_sub_group.insert(1, {'devname': 'missing', 'idx': '-1', 'size': 0})
                the_sub_group.insert(3, {'devname': 'missing', 'idx': '-1', 'size': 0})
            elif len(the_sub_group) == 3:
                the_sub_group.insert(1, {'devname': 'missing', 'idx': '-1', 'size': 0})
                the_sub_group.insert(3, {'devname': 'missing', 'idx': '-1', 'size': 0})
                the_sub_group.insert(5, {'devname': 'missing', 'idx': '-1', 'size': 0})
            elif len(the_sub_group) == 4:
                the_sub_group.insert(3, {'devname': 'missing', 'idx': '-1', 'size': 0})
                the_sub_group.insert(5, {'devname': 'missing', 'idx': '-1', 'size': 0})
            elif len(the_sub_group) == 5:
                the_sub_group.insert(5, {'devname': 'missing', 'idx': '-1', 'size': 0})

    #
    # re-generate ibd_dev_list which does not contains dropped ibd dev but contains missing dev
    #
    del setup_info['ibd_dev_list'][:]
    ibd_list = setup_info['ibd_dev_list']
    for the_group in high_gap_group_list:
        for the_sub_group in the_group:
            for the_ibd in the_sub_group:
                ibd_list.append(the_ibd)

    #
    # add ibd in drop group to ibd drop list
    #
    ibd_drop_list = setup_info['ibd_drop_list']
    for the_group in drop_group:
        for the_ibd in the_group:
            ibd_drop_list.append({'ibd': the_ibd, 'reason': 'failed-matching'})

    return


#
# ibd_section_list: list of (ibd_section_name, size, ibd_idx)
# raid1_list: list of (raid1-dev-path, size-in-byte), like (/dev/md0, 8589934592)
# md_curr_idx: md index to start
#
def create_first_level_raid(setup_info, next_md_idx):
    chunk_size = setup_info['chunk_size']
    fastsync = setup_info['fastsync']
    high_gap_group_list = setup_info['high_gap_group_list']
    setup_info['first_level_raid_list'] = []
    first_level_raid_list = setup_info['first_level_raid_list']
    for the_group in high_gap_group_list:
        for the_sub_group in the_group:
            the_raid1_group = []
            num_mirror = len(the_sub_group) / 2
            for i in range(num_mirror):
                the_ibd1 = the_sub_group[2 * i]
                the_ibd2 = the_sub_group[2 * i + 1]
                the_devname1 = the_ibd1['devname'] + 'p1'
                the_devname2 = the_ibd2['devname'] + 'p1'
                if the_sub_group[2 * i + 1]['devname'] == 'missing':
                    the_devname2 = the_sub_group[2 * i + 1]['devname']
                curr_md_idx = md_next_available_idx(next_md_idx)
                next_md_idx = curr_md_idx + 1
                md_dev = "/dev/md" + str(curr_md_idx)
                md_name = "atlas-md-" + str(curr_md_idx)
                if fastsync == True:
                    md_bitmap_str = MD_DEFAULT_BITMAP_STR
                else:
                    md_bitmap_str = ""
                cmd_str = CMD_MDCREATE + ' ' + md_dev + " -N " + md_name + \
                          " --chunk=" + str(chunk_size) + ' ' + \
                          md_bitmap_str + \
                          " --level=raid1 --raid-devices=2 " + the_devname1 + ' ' + the_devname2
                rc = do_system_timeout(cmd_str, 10)
                if rc == 0:
                    property_dict = {}
                    raid_detail(md_dev, property_dict)
                    save_dev_uuid(property_dict[RAID_UUID], md_dev)
                    cmd_str = CMD_MDADM + " --zero-superblock " + md_dev
                    rc = do_system_timeout(cmd_str, 10)
                    the_raid1_group.append(
                        {"devname": md_dev, "size": property_dict[RAID_SIZE], "children": [the_ibd1, the_ibd2]})
                else:
                    debug('Raid Creation failed, Skip!')
                    # FIXME: handle error.
            first_level_raid_list.append(the_raid1_group)

    parted_first_level_devices(setup_info)
    return next_md_idx


#
# device_list: list of (dev_pathname, size), sorted by size
# md_idx: md index to start
#
def create_second_level_raid(setup_info, next_md_idx):
    configure = setup_info['configure']
    if configure['raidtype'] == 'RAID_0':
        raid_type = POOL_TYPE_RAID0
    elif configure['raidtype'] == 'RAID_5':
        raid_type = POOL_TYPE_RAID5
    else:
        return -1
    chunk_size = setup_info['chunk_size']
    fastsync = setup_info['fastsync']
    first_level_raid_list = setup_info['first_level_raid_list']
    setup_info['second_level_raid_list'] = []
    second_level_raid_list = setup_info['second_level_raid_list']
    for the_raid1_group in first_level_raid_list:
        the_dev1 = the_raid1_group[0]
        the_devname1 = the_dev1['devname'] + 'p1'
        device_num = 1
        the_dev2 = ''
        the_devname2 = ''
        if len(the_raid1_group) >= 2:
            the_dev2 = the_raid1_group[1]
            the_devname2 = the_dev2['devname'] + 'p1'
            device_num = 2
        the_dev3 = ''
        the_devname3 = ''
        if len(the_raid1_group) >= 3:
            the_dev3 = the_raid1_group[2]
            the_devname3 = the_dev3['devname'] + 'p1'
            device_num = 3
        curr_md_idx = md_next_available_idx(next_md_idx)
        next_md_idx = curr_md_idx + 1
        md_dev = "/dev/md" + str(curr_md_idx)
        if raid_type == POOL_TYPE_RAID5:
            if fastsync == True:
                md_bitmap_str = MD_DEFAULT_BITMAP_STR
            else:
                md_bitmap_str = ""
            cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                      " --level=5" + " --chunk=" + str(chunk_size) + " --raid-devices=3 " + \
                      md_bitmap_str + \
                      the_devname1 + ' ' + the_devname2 + ' ' + the_devname3
        # " --level=5 --bitmap=internal --raid-devices=3 " + \
        elif raid_type == POOL_TYPE_RAID0:
            cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                      " --level=stripe" + " --chunk=" + str(chunk_size) + " --raid-devices=" + str(device_num) + " " + \
                      the_devname1 + ' ' + the_devname2 + ' ' + the_devname3

        rc = do_system_timeout(cmd_str, 10)
        if rc == 0:
            property_dict = {}
            raid_detail(md_dev, property_dict)
            save_dev_uuid(property_dict[RAID_UUID], md_dev)
        else:
            debug('Raid Creation failed, Skip!')
            continue
        # FIXME: error handling.
        if len(the_dev3) > 0:
            second_level_raid_list.append({"devname": md_dev, 'children': [the_dev1, the_dev2, the_dev3]})
        elif len(the_dev2) > 0:
            second_level_raid_list.append({"devname": md_dev, 'children': [the_dev1, the_dev2]})
        else:
            second_level_raid_list.append({"devname": md_dev, 'children': [the_dev1]})

    parted_second_level_devices(setup_info)
    return next_md_idx


#
# device_list: list of (dev_pathname, size), sorted by size
# md_idx: md index to start
# Note: for virtual memory pool, only one md on second level raid
#
def create_second_level_raid_v2(setup_info, next_md_idx):
    configure = setup_info['configure']
    if configure['raidtype'] == 'RAID_0':
        raid_type = POOL_TYPE_RAID0
    elif configure['raidtype'] == 'RAID_5':
        raid_type = POOL_TYPE_RAID5
    else:
        return -1
    chunk_size = setup_info['chunk_size']
    fastsync = setup_info['fastsync']
    first_level_raid_list = setup_info['first_level_raid_list']
    setup_info['second_level_raid_list'] = []
    second_level_raid_list = setup_info['second_level_raid_list']
    dev_list = []

    for the_raid1_group in first_level_raid_list:
        for the_dev in the_raid1_group:
            dev_list.append(the_dev)

    num_raid_devices = len(dev_list)
    raid_devices_str = ""
    for dev in dev_list:
        raid_devices_str = raid_devices_str + " " + dev['devname'] + 'p1'

    ss_list = []
    if setup_info.has_key('shared_dev_list'):
        ss_list = setup_info['shared_dev_list']
    num_ss_devices = len(ss_list)
    ss_devices_str = ""
    for the_ss in ss_list:
        ss_devices_str = ss_devices_str + " " + the_ss['devname'] + '1'

    shared_storage_flag = STOR_SS_NONE
    if setup_info.has_key('shared_storage_flag'):
        shared_storage_flag = setup_info['shared_storage_flag']

    curr_md_idx = md_next_available_idx(next_md_idx)
    next_md_idx = curr_md_idx + 1
    md_dev = "/dev/md" + str(curr_md_idx)

    if shared_storage_flag == STOR_SS_ONLY:
        # if only with shared storage, always use linear
        cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                  " --level=linear" + " --chunk=" + str(chunk_size) + " --raid-devices=" + str(num_ss_devices) + " " + \
                  ss_devices_str
    elif raid_type == POOL_TYPE_RAID5:
        if fastsync == True:
            md_bitmap_str = MD_DEFAULT_BITMAP_STR
        else:
            md_bitmap_str = ""
        cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                  " --level=5" + " --chunk=" + str(chunk_size) + " --raid-devices=" + str(
            num_raid_devices + num_ss_devices) + " " + \
                  md_bitmap_str + raid_devices_str + ss_devices_str
    # " --level=5 --bitmap=internal --raid-devices=3 " + \
    elif raid_type == POOL_TYPE_RAID0:
        cmd_str = CMD_MDCREATE + ' ' + md_dev + \
                  " --level=stripe" + " --chunk=" + str(chunk_size) + " --raid-devices=" + str(
            num_raid_devices + num_ss_devices) + " " + \
                  raid_devices_str + ss_devices_str

    rc = do_system_timeout(cmd_str, 10)
    if rc == 0:
        property_dict = {}
        raid_detail(md_dev, property_dict)
        save_dev_uuid(property_dict[RAID_UUID], md_dev)
    else:
        debug('Raid Creation failed for VP!')
    # FIXME: Error handling.

    rc = create_ads_link(md_dev, configure['adsname'], configure['vgname'])

    second_level_raid_list.append({"devname": md_dev, 'children': dev_list})
    # parted_second_level_devices(setup_info)
    return next_md_idx


# generate infrastructure for regular pool or ADS
def generate_rp_infrastructure(setup_info):
    debug("Enter generate_rp_infrastructure ...")

    configure = setup_info['configure']
    if configure['raidtype'] == 'RAID_0':
        raid_type = 'stripe'
    elif configure['raidtype'] == 'RAID_5':
        raid_type = '5'
    else:
        return -1

    setup_info['infrastructure'][configure['pool_infrastructure_type']] = []
    infrastructure_list = setup_info['infrastructure'][configure['pool_infrastructure_type']]
    first_level_raid_list = setup_info['first_level_raid_list']
    second_level_raid_list = setup_info['second_level_raid_list']
    for the_raid in second_level_raid_list:  # raid0|raid5
        the_devname = the_raid['devname']
        property_dict = {}
        raid_detail(the_devname, property_dict)
        the_infrastructure = {'uuid': property_dict[RAID_UUID]}
        the_infrastructure['children'] = []
        for the_child in the_raid['children']:  # the_child is raid1
            property_dict = {}
            raid_detail(the_child['devname'], property_dict)
            the_raid1_child = {}
            the_raid1_child['uuid'] = property_dict[RAID_UUID]
            the_raid1_child['children'] = []
            for the_ibd in the_child['children']:
                if the_ibd['devname'] != 'missing':
                    the_raid1_child['children'].append(
                        {'uuid': the_ibd['import']['uuid'], 'ip': the_ibd['import']['ip']})
            the_infrastructure['children'].append(the_raid1_child)
        infrastructure_list.append(the_infrastructure)

    sub_infrastructure = setup_info['infrastructure'][configure['pool_infrastructure_type']]
    if setup_info.has_key('shared_dev_list'):
        shared_dev_list = setup_info['shared_dev_list']
        for the_ss in shared_dev_list:
            sub_infrastructure[0]['children'].append({'uuid': the_ss['uuid'], 'devname': the_ss['devname']})


# generate infrastructure for virtual pool
def vp_generate_infrastructure(setup_info):
    debug("Enter vp_generate_infrastructure ...")

    setup_info['gap_ratio'] = 0
    setup_info['chunk_size'] = 512
    setup_info['fastsync'] = True

    setup_info['infrastructure'] = {}
    infrastructure = setup_info['infrastructure']
    configure = setup_info['configure']
    for the_role in configure['roles']:
        if the_role == 'CAPACITY_POOL':
            infrastructure[DISK_INFRASTRUCTURE] = []
            infrastructure_list = infrastructure[DISK_INFRASTRUCTURE]
        elif the_role == 'MEMORY_POOL':
            infrastructure[MEMORY_INFRASTRUCTURE] = []
            infrastructure_list = infrastructure[MEMORY_INFRASTRUCTURE]
        else:
            # TODO: shared storage pool
            debug("No supported pool type %s." % the_role)
            return 1

    high_gap_group_list = setup_info['high_gap_group_list']

    print "high_gap_group_list"
    print high_gap_group_list

    for the_group in high_gap_group_list:
        for the_sub_group in the_group:
            the_pair_group = []
            num_mirror = len(the_sub_group) / 2
            for i in range(num_mirror):
                the_ibd1 = the_sub_group[2 * i]
                the_ibd2 = the_sub_group[2 * i + 1]
                pair_0 = {"ip": the_ibd1['import']['ip'], "devname": the_ibd1['import']['devname'],
                          "uuid": the_ibd1['import']['uuid'], "size": the_ibd1['size'], "free": the_ibd1['size'],
                          "adslist": []}

                if the_ibd2.has_key('import'):
                    pair_1 = {"ip": the_ibd2['import']['ip'], "devname": the_ibd2['import']['devname'],
                              "uuid": the_ibd2['import']['uuid'], "size": the_ibd2['size'], "free": the_ibd2['size'],
                              "adslist": []}
                else:
                    pair_1 = {"ip": "NULL", "devname": 'missing', "uuid": "NULL", "size": 0, "free": 0}
                the_pair_group.append([pair_0, pair_1])
            infrastructure_list.append(the_pair_group)

    debug('vp_generate_infrastructure: ', infrastructure_list)


# generate infrastructure for shared storage
def ss_generate_infrastructure(setup_info):
    debug("Enter ss_generate_infrastructure ...")

    configure = setup_info['configure']
    vgname_str = configure['vgname']

    setup_info['infrastructure'] = {}
    infrastructure = setup_info['infrastructure']
    infrastructure[SS_INFRASTRUCTURE] = []
    infrastructure_list = infrastructure[SS_INFRASTRUCTURE]

    # TODO: change the following to get vg size and free space
    vg_size = 0
    vg_free = 0
    cmd = 'vgs --unit b'
    # e.g., vCenter13421_Danzhou-CP67   3   1   0 wz--n- 74.99g 74.98g
    for vg_info in os.popen(cmd).readlines()[1:]:
        item_list = vg_info.split()
        if item_list[0] == vgname_str:
            vg_size = long(item_list[5][:-1])
            vg_free = long(item_list[6][:-1])

    if vg_size == 0:
        debug(vgname_str + " has vg size 0!")
        return 1

    the_vg = {"ip": "NULL", "vgName": vgname_str, "size": vg_size, "free": vg_free, "adslist": []}
    infrastructure_list.append(the_vg)

    return 0


def generate_infrastructure(setup_info, pool_type):
    if pool_type == REGULAR_POOL:
        generate_rp_infrastructure(setup_info)
    elif pool_type == VIRTUAL_POOL:
        vp_generate_infrastructure(setup_info)


def infrastructure_substitue_raid1_uuid(setup_info, old_uuid, new_uuid):
    configure = setup_info['configure']
    infrastructure_list = setup_info['infrastructure'][configure['pool_infrastructure_type']]
    for the_infrastructure in infrastructure_list:  # raid0|raid5
        for the_raid1 in the_infrastructure['children']:
            if the_raid1['uuid'] == old_uuid:
                the_raid1['uuid'] = new_uuid
                return 0
    return 1


#
# create a volume from the given device list
# dev_name_list: list of devices like ["/dev/md6", "/dev/md7"]
#
def create_volume_group(vg_disk_list, vgname_str):
    if len(vg_disk_list) == 0:
        return 1

    devices = ' '.join(vg_disk_list)
    # Force to create the PVs, erase any previous data!
    pvremove_cmd_str = CMD_PVREMOVE + ' -ff -y ' + ' ' + devices
    rc = do_system(pvremove_cmd_str)
    pvcreate_cmd_str = CMD_PVCREATE + ' -ff -y ' + ' ' + devices
    rc = do_system(pvcreate_cmd_str);
    if rc != 0:
        return rc

    vgcreate_cmd_str = CMD_VGCREATE + ' ' + vgname_str + ' ' + devices
    rc = do_system(vgcreate_cmd_str);
    if rc != 0:
        return rc

    vgactive_cmd_str = CMD_VGACTIVE + ' ' + vgname_str
    rc = do_system(vgactive_cmd_str)

    lvname = INTERNAL_LV_NAME
    lvsize = '1M'
    lvcreate_cmd_str = CMD_LVCREATE + ' -n ' + lvname + ' -L ' + lvsize + ' ' + vgname_str
    rc = do_system(lvcreate_cmd_str)
    if rc != 0:
        debug('Could not create ' + lvname + ' on ' + vgname_str + ' with size ' + lvsize)
    return rc


#
# create a volume from the given device list
# dev_name_list: list of devices like ["/dev/md6", "/dev/md7"]
#
def extend_volume_group(vg_disk_list, vgname_str):
    if len(vg_disk_list) == 0:
        return

    devices = ' '.join(vg_disk_list)
    pvcreate_cmd_str = CMD_PVCREATE + ' -ff -y ' + devices
    rc = do_system(pvcreate_cmd_str);
    vgextend_cmd_str = CMD_VGEXTEND + ' ' + vgname_str + ' ' + devices
    rc = do_system(vgextend_cmd_str);
    vgactive_cmd_str = CMD_VGACTIVE + ' ' + vgname_str
    rc = do_system(vgactive_cmd_str)
    return


def load_conf(fname, setup_info):
    debug('Enter load_conf ...')
    #
    # retrieve all configuration info from a Json file
    #
    rc = 0
    try:
        cfg_file = open(fname, 'r')
        cfg_str = cfg_file.read()
        cfg_file.close()
        setup_info['configure'] = json.loads(cfg_str)
        configure_str = json.dumps(setup_info['configure'], indent=4, separators=(',', ': '))
        cfg_file = open('/tmp/atlas.json', 'w')
        cfg_file.write(configure_str)
        cfg_file.close()
    except:
        debug("CAUTION: Cannot load configure json file:", fname)
        rc = 1
    if rc == 0:
        configure = setup_info['configure']
        setup_info['gap_ratio'] = 0
        if configure.has_key('gapratio'):
            setup_info['gap_ratio'] = configure['gapratio']
        if setup_info['gap_ratio'] == 0:
            setup_info['gap_ratio'] = GAP_RATIO_MAX

        if configure.has_key('sharedstoragefirst'):
            setup_info['sharedstoragefirst'] = configure['sharedstoragefirst']
        else:
            setup_info['sharedstoragefirst'] = True  # always put sharedstorage first by default

        if configure.has_key('chunk_size'):
            setup_info['chunk_size'] = configure['chunk_size']
        else:
            setup_info['chunk_size'] = 512  # 512K by default
        if setup_info['chunk_size'] <= 0:
            setup_info['chunk_size'] = 512

        if configure.has_key('fastsync'):
            setup_info['fastsync'] = configure['fastsync']
        else:
            setup_info['fastsync'] = True  # Enable MD bitmap by default.

        setup_info['storagetype'] = STOR_TYPE_UNKNOWN
        if configure.has_key('roles'):
            for the_role in configure['roles']:
                if the_role == 'CAPACITY_POOL':
                    setup_info['storagetype'] = STOR_TYPE_DISK
                elif the_role == 'MEMORY_POOL':
                    setup_info['storagetype'] = STOR_TYPE_MEMORY
        elif configure.has_key('storagetype'):
            if configure['storagetype'] == 'DISK':
                setup_info['storagetype'] = STOR_TYPE_DISK
            elif configure['storagetype'] == 'MEMORY':
                setup_info['storagetype'] = STOR_TYPE_MEMORY
    return rc


'''
def load_conf_from_amc_old(fname, setup_info):
    #
    # retrieve all configuration info from a Json file
    #
    try:
        cfg_file = open(fname, 'r')
        cfg_str = cfg_file.read()
        cfg_file.close()
        ilio = json.loads(cfg_str[1:len(cfg_str)-1])
        setup_info['configure'] = json.loads(ilio['ndstoiliojson'])
    except:
        debug("CAUTION: Cannot load configure json file:", fname)
    return

def load_conf_from_amc(fname, setup_info):
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
    return
'''


#
# sample of url for a pool:
#   https://10.15.107.2:8443/amc/model/ilio/pools/vg/test1_zc-1-cap-pool
# amcurl: like https://10.15.107.2:8443/amc
# vgname: like test1_zc-1-cap-pool
#
def load_pool_conf_from_amc(amcurl, vgname_str, pool_setup_info):
    #   try:
    #       protocol = amcurl.split(':')[0] # like: https
    #       if protocol == 'https':
    #           use_https = True
    #       else:
    #           use_https = False
    #       amcaddr = amcurl.split('/')[2]  # like: 10.15.107.2:8443
    #       amcfile = "/amc/model/ilio/pools/vg/" + vgname_str
    #       if use_https == True:
    #           conn = httplib.HTTPSConnection(amcaddr)
    #       else:
    #           conn = httplib.HTTPConnection(amcaddr)
    #       debug(amcfile)
    #       debug(amcaddr)
    #       conn.request("GET", amcfile)
    #       r1 = conn.getresponse()
    #       debug(r1.status, r1.reason)
    #       data1 = r1.read()
    #       tmp_fname = "/tmp/" + vgname_str + '.json'
    #       tmp_file = open(tmp_fname, 'w')
    #       tmp_file.write(data1)
    #       tmp_file.close()
    #   except:
    #       debug('Can not connect to AMC for config json.')
    #       return 1
    #   rc = load_conf(tmp_fname, pool_setup_info)
    #   return rc
    """
    load_pool_conf_from_amc using USX 2.0 REST API to support virtual pool management
    """
    try:
        protocol = amcurl.split(':')[0]  # like: https
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        amcaddr = amcurl.split('/')[2]  # like: 10.15.107.2:8443
        amcfile = "/usxmanager/usx/virtualpool/" + vgname_str + "/attributes/vpconfig"
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
        response = json.loads(data1)  # parse vpconfig from response
        data2 = response['vpconfig']
        configure_json = json.loads(data2)
        configure_str = json.dumps(configure_json['configure'])
        tmp_fname = "/tmp/" + vgname_str + '.json'
        tmp_file = open(tmp_fname, 'w')
        tmp_file.write(configure_str)
        tmp_file.close()
    except:
        debug('Can not connect to AMC for config json.')
        return 1
    rc = load_conf(tmp_fname, pool_setup_info)
    return rc


def load_ads_pool_info_from_amc(vgname_str, adsname_str, ads_pool_setup_info, action):
    debug('Enter load_ads_pool_info_from_amc ...')
    amcurl = LOCAL_AGENT  # Use localhost agent instead of AMC server
    vg_setup_info = {}
    rc = load_pool_conf_from_amc(amcurl, vgname_str, vg_setup_info)
    if rc != 0:
        debug("Can not load config json for %s." % vgname_str)
        return rc

    debug("vg_setup_info: " + json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))
    debug("ads_pool_setup_info: " + json.dumps(ads_pool_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    ads_pool_setup_info['configure'] = {}
    ads_pool_setup_info['storagetype'] = STOR_TYPE_UNKNOWN

    if vg_setup_info['configure'].has_key('chunk_size'):
        ads_pool_setup_info['chunk_size'] = vg_setup_info['configure']['chunk_size']
    else:
        ads_pool_setup_info['chunk_size'] = 512  # 512K by default
    if ads_pool_setup_info['chunk_size'] <= 0:
        ads_pool_setup_info['chunk_size'] = 512

    if vg_setup_info['configure'].has_key('fastsync'):
        ads_pool_setup_info['fastsync'] = vg_setup_info['configure']['fastsync']
    else:
        ads_pool_setup_info['fastsync'] = True  # always put sharedstorage first by default

    if vg_setup_info['configure'].has_key('gap_ratio'):
        ads_pool_setup_info['gap_ratio'] = vg_setup_info['configure']['gap_ratio']
    else:
        ads_pool_setup_info['gap_ratio'] = GAP_RATIO_MAX
    if ads_pool_setup_info['gap_ratio'] == 0:
        ads_pool_setup_info['gap_ratio'] = GAP_RATIO_MAX

    if vg_setup_info['configure'].has_key('sharedstorages'):
        ads_pool_setup_info['sharedstorages'] = vg_setup_info['configure']['sharedstorages']

    if vg_setup_info['configure'].has_key('sharedstoragefirst'):
        ads_pool_setup_info['sharedstoragefirst'] = vg_setup_info['configure']['sharedstoragefirst']

    configure = ads_pool_setup_info['configure']
    configure['adsname'] = adsname_str
    configure['vgname'] = vgname_str
    configure['raidtype'] = vg_setup_info['configure']['raidtype']
    configure['virtualpool'] = "YES"

    if vg_setup_info['configure'].has_key('storagetype') == False:
        debug("Not specify pool storagetype. ")
        return 1
    else:
        storagetype_str = vg_setup_info['configure']['storagetype']
        if storagetype_str.upper() == "DISK":
            configure['pool_infrastructure_type'] = DISK_INFRASTRUCTURE
            ads_pool_setup_info['storagetype'] = STOR_TYPE_DISK
        elif storagetype_str.upper() == "MEMORY":
            configure['pool_infrastructure_type'] = MEMORY_INFRASTRUCTURE
            ads_pool_setup_info['storagetype'] = STOR_TYPE_MEMORY
        elif storagetype_str.upper() == "FLASH":
            configure['pool_infrastructure_type'] = "unknown"
            # print json.dumps(ads_pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
            # detail should be set already
            detail_str = json.loads(vg_setup_info['configure']['detail'])
            tmp_setup_info = {}
            tmp_setup_info['infrastructure'] = detail_str
            if tmp_setup_info['infrastructure'].has_key(DISK_INFRASTRUCTURE):
                configure['pool_infrastructure_type'] = DISK_INFRASTRUCTURE
                ads_pool_setup_info['storagetype'] = STOR_TYPE_DISK
            elif tmp_setup_info['infrastructure'].has_key(MEMORY_INFRASTRUCTURE):
                configure['pool_infrastructure_type'] = MEMORY_INFRASTRUCTURE
                ads_pool_setup_info['storagetype'] = STOR_TYPE_MEMORY
            else:
                debug("Not supported pool type for FLASH.")
                return 1
                # print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
                # print json.dumps(tmp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
                # print json.dumps(ads_pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            debug("Not supported pool type %s." % storagetype_str)
            return 1

    # print json.dumps(ads_pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    if action == 0:
        vg_fetch_infrastructure(ads_pool_setup_info)
    else:
        fetch_ads_infrastructure(ads_pool_setup_info)

    if ads_pool_setup_info.has_key('infrastructure') == False:
        ads_pool_setup_info['infrastructure'] = {}

    if ads_pool_setup_info['infrastructure'].has_key(configure['pool_infrastructure_type']) == False:
        ads_pool_setup_info['infrastructure'][configure['pool_infrastructure_type']] = []

    debug("Leave load_ads_pool_info_from_amc: " + json.dumps(ads_pool_setup_info, sort_keys=True, indent=4,
                                                             separators=(',', ': ')))

    return rc


def load_ads_pool_conf_from_amc(vgname_str, adsname_str, ads_pool_setup_info):
    debug('Enter load_ads_pool_conf_from_amc ...')
    rc = load_ads_pool_info_from_amc(vgname_str, adsname_str, ads_pool_setup_info, 0)
    return rc


def load_ads_pool_infra_from_amc(vgname_str, adsname_str, ads_pool_setup_info):
    debug('Enter load_ads_pool_infra_from_amc ...')
    rc = load_ads_pool_info_from_amc(vgname_str, adsname_str, ads_pool_setup_info, 1)
    return rc


def update_ads_pool_info(vgname_str, adsname_str, pool_infrastructure_type):
    debug('Enter update_ads_pool_info ...')

    vp_setup_info = {}
    vp_setup_info['configure'] = {}
    configure = vp_setup_info['configure']
    configure['vgname'] = vgname_str

    # aquire lock
    rc = ads_vp_lock(vgname_str)
    if rc != 0:
        debug('Can not aquire lock for update_ads_pool_info.')
        return rc

    # fetch virtual pool infrastructure
    vg_fetch_infrastructure(vp_setup_info)
    # print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    if vp_setup_info['infrastructure'].has_key(pool_infrastructure_type):
        pair_list = vp_setup_info['infrastructure'][pool_infrastructure_type]

        for the_pair_group in pair_list:
            for the_pair in the_pair_group:
                if the_pair[0]["devname"] != "missing":
                    for the_ads in the_pair[0]["adslist"]:
                        if the_ads['adsname'] == adsname_str:
                            the_pair[0]['free'] = the_pair[0]['free'] + the_ads['used']
                            the_pair[0]["adslist"].remove(the_ads)

                if the_pair[1]["devname"] != "missing":
                    for the_ads in the_pair[1]["adslist"]:
                        if the_ads['adsname'] == adsname_str:
                            the_pair[1]['free'] = the_pair[1]['free'] + the_ads['used']
                            the_pair[1]["adslist"].remove(the_ads)

        # print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

        # udpate virtual pool infrastructure
        save_infrastructure(vp_setup_info)

    # release lock
    ads_vp_unlock(vgname_str)

    return 0


def load_devices(setup_info, timeout):
    debug('Enter load_devices, timeout: ', timeout)
    configure = setup_info['configure']

    #
    # start ibd agent
    #
    setup_info['ibd_dev_list'] = []
    setup_info['ibd_drop_list'] = []
    if configure.has_key('imports'):
        next_ibd_idx = 0
        next_ibd_idx = start_ibd_clients(setup_info, next_ibd_idx, timeout)

    #
    # start all shared disks
    #
    setup_info['shared_dev_list'] = []
    if configure.has_key("sharedstorage"):
        start_shared_storage(setup_info)
    # NOTE: Skip shared storage since they are only attached to one host at any time.
    # If we change current behavior and attach shared storage to many hosts, then we need to fence them too.
    rc = pick_start_arb_device(setup_info)
    return rc


def get_ibd_size(the_device):
    the_exportname = the_device["uuid"]
    the_ip = the_device["ip"]
    the_device["size"] = 0

    args_str = CMD_AGGSIZE + the_exportname
    (rc, out, err) = remote_exec(the_ip, 'python ', args_str)

    if rc != 0:
        debug('get ibd %s size from %s failed, errcode=%d, errmsg=%s' % (the_exportname, the_ip, rc, err))
        return rc
    else:
        for size_info in out.split('\n'):
            info = size_info.split()

            if len(info) == 0:
                continue

            if info[0] == "aggr_get_size":
                # size unit is MB
                the_device["size"] = long(info[2]) * (1024 * 1024)
                return 0;

    debug('get ibd %s size from %s failed, errmsg=no size information' % (the_exportname, the_ip))
    return 1


def get_devices_info(setup_info):
    debug('Enter get_devices_info ...')

    configure = setup_info['configure']
    all_ibd_imports = configure['imports']
    setup_info['ibd_dev_list'] = []
    setup_info['ibd_drop_list'] = []
    ibd_dev_list = setup_info['ibd_dev_list']
    ibd_drop_list = setup_info['ibd_drop_list']

    the_idx = 0
    for the_device in all_ibd_imports:
        the_exportname = the_device["uuid"]
        the_ip = the_device["ip"]
        rc = get_ibd_size(the_device)
        the_size = the_device["size"]
        # make a fake device name
        the_devname = "TBD" + str(the_idx)
        the_device['devname'] = the_devname
        the_ibd = {'devname': the_devname, 'idx': the_idx, 'size': the_size, 'import': the_device}

        if rc == 0:
            ibd_dev_list.append(the_ibd)
        else:
            debug("Remove failed import: %s:%s %s." % (the_ip, the_exportname, the_devname))
            ibd_drop_list.append({'ibd': the_ibd, 'reason': 'failed'})
        the_idx = the_idx + 1

    return


def init_devices(setup_info):
    configure = setup_info['configure']
    if configure.has_key('imports'):
        parted_ibd_devices(setup_info)
    if configure.has_key('sharedstorages'):
        parted_shared_devices(setup_info)
    else:
        debug('configure has no key sharedstorage')
    return


def create_md(setup_info):
    next_md_idx = 0
    next_md_idx = create_first_level_raid(setup_info, next_md_idx)
    next_md_idx = create_second_level_raid(setup_info, next_md_idx)
    return


def create_md_v2(setup_info):
    next_md_idx = 0
    next_md_idx = create_first_level_raid(setup_info, next_md_idx)
    next_md_idx = create_second_level_raid_v2(setup_info, next_md_idx)
    return


def get_all_new_vg_disk(setup_info):
    second_level_raid_list = setup_info['second_level_raid_list']
    shared_list = setup_info['shared_dev_list']
    setup_info['vg_disk_list'] = []
    vg_disk_list = setup_info['vg_disk_list']

    if setup_info['sharedstoragefirst'] == True:
        debug('sharedstoragefirst is true')
        for i in shared_list:
            the_devname = i["devname"] + '1'
            vg_disk_list.append(the_devname)
        for i in second_level_raid_list:
            the_devname = i["devname"] + 'p1'
            vg_disk_list.append(the_devname)
    else:
        debug('sharedstoragefirst is false')
        for i in second_level_raid_list:
            the_devname = i["devname"] + 'p1'
            vg_disk_list.append(the_devname)
        for i in shared_list:
            the_devname = i["devname"] + '1'
            vg_disk_list.append(the_devname)
    return


def create_vg(setup_info):
    debug("create_vg start ...");
    get_all_new_vg_disk(setup_info)
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    rc = create_volume_group(setup_info['vg_disk_list'], vgname_str)
    return rc


def extend_vg(setup_info):
    get_all_new_vg_disk(setup_info)
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    extend_volume_group(setup_info['vg_disk_list'], vgname_str)
    return


def start_ibdserver(vgname_str):
    cmd_str = CMD_ADSPOOL + ' -e ' + vgname_str
    debug(cmd_str)
    do_system(cmd_str)
    return


def save_dev_uuid(uuid, parent_devname):
    debug("save_dev_uuid " + parent_devname + " " + uuid)
    uuid_dev_file = "/tmp/dev-uuid-" + uuid
    f = open(uuid_dev_file, 'w')
    f.write(parent_devname)
    f.close()


def remove_dev_uuid(uuid):
    debug("remove_dev_uuid " + uuid)
    uuid_dev_file = "/tmp/dev-uuid-" + uuid
    if os.path.exists(uuid_dev_file):
        os.unlink(uuid_dev_file)


def load_dev_uuid(uuid):
    uuid_dev_file = "/tmp/dev-uuid-" + uuid
    saved_dev = None
    if os.path.exists(uuid_dev_file):
        try:
            f = open(uuid_dev_file, 'r')
            saved_dev = f.read()
            f.close()
        except:
            pass
    return saved_dev


def set_raid_speed_limit_min(newvalue):
    cmd = "echo" + ' ' + str(newvalue) + ' > ' + CMD_SET_RAID_SPEED_LIMIT_MIN
    debug(cmd)
    rc = do_system(cmd)
    return rc


def tune_raid5(raid_devname):
    # Speed up resync speed.
    cmd_str = 'echo 4096 > /sys/devices/virtual/block/%s/md/stripe_cache_size' % os.path.basename(raid_devname)
    rc = do_system(cmd_str)
    # Set raid_speed_limit_min to 1MB (default is 1000) ==> 1000 * 1000
    set_raid_speed_limit_min(1000000)
    return rc


#
# Only called by raid_assemble
#
def raid_assemble_core(setup_info):
    debug('Enter raid_assemble_core')
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    storagetype = setup_info['storagetype']
    chunk_size = setup_info['chunk_size']
    fastsync = setup_info['fastsync']
    c_infrastructure = setup_info['c_infrastructure']
    next_md_idx = 0
    curr_md_idx = next_md_idx
    infrastructure_modified = False

    to_assemble_ss_str = ''
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        for the_raid1 in the_raid['children']:  # raid1
            if the_raid1.has_key('children') == False:
                to_assemble_ss_str = to_assemble_ss_str + ' ' + the_raid1['devname'] + '1'

    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        #
        # First of all, assemble all ibd of this raid
        #
        new_create_raid1_list = []
        new_create_raid1_str = ''
        to_assemble_raid1_list = []
        to_assemble_raid1_str = ''
        to_create_raid1_list = []
        to_create_raid1_str = ''
        for the_raid1 in the_raid['children']:  # raid1
            parent_devname = the_raid1['devname']
            if the_raid1['state'] == DEV_STATE_DISCONNECT:
                #
                # the raid1 is not there at all, need assemble all ibd in this raid1
                #
                debug("raid_assemble_core: %s does not exist" % (parent_devname))
                next_md_idx = md_next_available_idx(next_md_idx)
                curr_md_idx = next_md_idx
                next_md_idx += 1
                parent_devname = '/dev/md' + str(curr_md_idx)
                to_assemble_ibd_str = ''
                to_assemble_ibd_list = []
                to_add_ibd_str = ''
                to_add_ibd_list = []
                to_create_ibd_str = ''
                to_create_ibd_list = []
                for the_ibd in the_raid1['children']:
                    if the_ibd['state'] != DEV_STATE_DISCONNECT:
                        rc = ibd_check_and_wait_array_partition(the_ibd)
                        part_ibd_devname = the_ibd['devname'] + 'p1'
                        if rc == 0:
                            to_assemble_ibd_str = part_ibd_devname + ' ' + to_assemble_ibd_str
                            to_assemble_ibd_list.append(the_ibd)
                        elif storagetype == STOR_TYPE_MEMORY:
                            rc = parted_device(the_ibd['devname'], START_SECTOR, -1, 'p1')
                            if rc == 0:
                                to_add_ibd_str = part_ibd_devname + ' ' + to_add_ibd_str
                                to_add_ibd_list.append(the_ibd)
                            else:
                                debug("failed to parted %s" % the_ibd['devname'])
                                part_ibd_devname = None
                        else:
                            debug("raid_assemble_core: invalid %s" % part_ibd_devname)
                            part_ibd_devname = None
                        # TODO

                        if part_ibd_devname != None:
                            to_create_ibd_str = part_ibd_devname + ' ' + to_create_ibd_str
                            to_create_ibd_list.append(the_ibd)

                cmd_str = CMD_MDASSEMBLE + ' ' + parent_devname + ' ' + to_assemble_ibd_str
                rc = do_system_timeout(cmd_str, 10)
                if rc == 0:
                    save_dev_uuid(the_raid1['uuid'], parent_devname)
                    the_raid1['state'] = DEV_STATE_ONLINE
                    the_raid1['devname'] = parent_devname

                    #
                    # some devices can not joined the array when do assemble due to lower events
                    # add them to join the array. It doesn't harm even if they are already in the array
                    #
                    for the_ibd in to_assemble_ibd_list:
                        part_ibd_devname = the_ibd['devname'] + 'p1'
                        cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + part_ibd_devname
                        do_system_timeout(cmd_str, 10)

                    for the_ibd in to_assemble_ibd_list:
                        #
                        # We assume all ibd in to_assemble_ibd_list have alrady joined the the_raid1.
                        # Wrong? Any thoughts?
                        #
                        the_ibd['state'] = DEV_STATE_ONLINE

                    for the_ibd in to_add_ibd_list:
                        part_ibd_devname = the_ibd['devname'] + 'p1'
                        cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + part_ibd_devname
                        rc = do_system_timeout(cmd_str, 10)
                        if rc == 0:
                            the_ibd['state'] = DEV_STATE_ONLINE
                        else:
                            debug("%s cannot be re-added" % part_ibd_devname)
                            # TODO
                    to_assemble_raid1_list.append(the_raid1)
                    to_assemble_raid1_str = parent_devname + 'p1' + ' ' + to_assemble_raid1_str

                    rc = raid1_check_and_wait_array_partition(the_raid1)
                    if rc != 0:
                        if storagetype == STOR_TYPE_MEMORY:
                            parted_device(the_raid1['devname'], START_SECTOR, -1, 'p1')

                elif storagetype == STOR_TYPE_MEMORY:
                    debug("assemble failed, try to re-create the raid1(%s)" % to_create_ibd_str),
                    md_stop(parent_devname)
                    next_md_idx = md_next_available_idx(next_md_idx)
                    curr_md_idx = next_md_idx
                    next_md_idx += 1
                    md_name = "atlas-md-" + str(curr_md_idx)
                    num_devices = len(to_create_ibd_list)
                    if len(to_create_ibd_list) == 1:
                        to_create_ibd_str = to_create_ibd_str + ' missing'
                        num_devices += 1
                    if fastsync == True:
                        md_bitmap_str = MD_DEFAULT_BITMAP_STR
                    else:
                        md_bitmap_str = ""

                    cmd_str = CMD_MDCREATE + \
                              " --chunk=" + str(chunk_size) + ' ' + \
                              parent_devname + " -N " + md_name + \
                              md_bitmap_str + \
                              " --level=raid1 --raid-devices=" + str(num_devices) + ' ' + to_create_ibd_str
                    for i in range(2):
                        rc = do_system_timeout(cmd_str, 10)
                        if rc != 0:
                            debug("raid1 re-create failed!, parted all ibd and re-create again")
                            for the_ibd in to_create_ibd_list:
                                parted_device(the_ibd['devname'], START_SECTOR, -1, 'p1')
                            continue

                        # re-create succeed
                        property_dict = {}
                        raid_detail(parent_devname, property_dict)
                        save_dev_uuid(property_dict[RAID_UUID], parent_devname)
                        remove_dev_uuid(the_raid1['uuid'])

                        #
                        # the_raid1['uuid'] should be replaced by it's new uuid: property_dict[RAID_UUID]
                        #
                        rc = infrastructure_substitue_raid1_uuid(setup_info, the_raid1['uuid'],
                                                                 property_dict[RAID_UUID])
                        debug("infrastructure_substitue_raid1_uuid: old:%s new:%s rc:%d" % (
                            the_raid1['uuid'], property_dict[RAID_UUID], rc))
                        infrastructure_modified = True
                        the_raid1['uuid'] = property_dict[RAID_UUID]
                        the_raid1['working'] = True

                        for the_ibd in to_create_ibd_list:
                            the_ibd['state'] = DEV_STATE_ONLINE
                        the_raid1['state'] = DEV_STATE_OFFLINE
                        the_raid1['devname'] = parent_devname
                        rc = parted_device(parent_devname, START_SECTOR, -1, 'p1')
                        if rc == 0:
                            new_create_raid1_list.append(the_raid1)
                            new_create_raid1_str = parent_devname + 'p1 ' + new_create_raid1_str
                        else:
                            debug("failed to parted %s" % parent_devname)
                        # TODO
                        break
                else:
                    debug("assemble failed, give up")
                    # TODO

            else:
                #
                # the raid1 has already been there, just need add not-online ibd
                #
                debug("raid_assemble_core: %s exists" % (parent_devname))
                if the_raid1['working'] == False:
                    continue
                if the_raid1.has_key('children') == False:
                    continue

                for the_ibd in the_raid1['children']:
                    if the_ibd['state'] != DEV_STATE_DISCONNECT:  # and the_ibd['state'] != DEV_STATE_ONLINE:
                        rc = ibd_check_and_wait_array_partition(the_ibd)
                        part_ibd_devname = the_ibd['devname'] + 'p1'
                        if rc != 0:
                            if storagetype == STOR_TYPE_MEMORY:
                                rc = md_fail(parent_devname, part_ibd_devname)
                                if rc == 0:
                                    rc = md_remove(parent_devname, part_ibd_devname)
                                elif rc == RC_DEVICVE_NOTEXIST:
                                    rc = 0

                                if rc == 0:
                                    the_ibd['state'] = DEV_STATE_OFFLINE
                                    rc = parted_device(the_ibd['devname'], START_SECTOR, -1, 'p1')
                                    if rc != 0:
                                        continue  # not ready, skip it
                                else:
                                    md_fail(the_raid['devname'], the_raid1['devname'] + 'p1')
                                    md_remove(the_raid['devname'], the_raid1['devname'] + 'p1')
                                    rc = md_stop(the_raid1['devname'])
                                    if rc == 0:
                                        remove_dev_uuid(the_raid1['uuid'])
                                        the_raid1['state'] = DEV_STATE_DISCONNECT
                                        for this_ibd in the_raid1['children']:
                                            if the_ibd['state'] != DEV_STATE_DISCONNECT:
                                                the_ibd['state'] = DEV_STATE_OFFLINE
                                        return 2

                            else:
                                # TODO: now we just skip this one
                                debug("%s does not exist" % part_ibd_devname)
                                continue

                        elif the_ibd['state'] == DEV_STATE_ONLINE:
                            # nothing to do it has valid partition and online
                            continue

                        if the_ibd['state'] != DEV_STATE_OFFLINE:
                            # remove a Faulty (any other state?) device
                            md_remove(parent_devname, part_ibd_devname)
                            the_ibd['state'] = DEV_STATE_OFFLINE

                        rc = md_re_add(parent_devname, part_ibd_devname)
                        if rc == 0:
                            the_ibd['state'] = DEV_STATE_ONLINE
                            # else:
                            # TODO: add failed, what I can do?
            to_create_raid1_list.append(the_raid1)
            to_create_raid1_str = parent_devname + 'p1 ' + to_create_raid1_str

        #
        # make sure all raid1 are ready
        #
        rc = raid1_check_and_wait_array_partition_list(to_assemble_raid1_list)
        if rc != 0:
            debug("raid_assemble_core: return 1 at step 1")
            return 1
        rc = raid1_check_and_wait_partition_list(new_create_raid1_list)
        if rc != 0:
            debug("raid_assemble_core: return 1 at step 2")
            return 1

        #
        # Now assemble all raid1 of this raid
        #
        parent_devname = the_raid['devname']
        if the_raid['state'] == DEV_STATE_DISCONNECT:
            next_md_idx = md_next_available_idx(next_md_idx)
            curr_md_idx = next_md_idx
            next_md_idx += 1
            parent_devname = '/dev/md' + str(curr_md_idx)

            cmd_str = CMD_MDASSEMBLE + ' --force ' + parent_devname + ' ' + to_assemble_ss_str + ' ' + to_assemble_raid1_str
            rc = do_system_timeout(cmd_str, 10)
            if rc == 0:
                save_dev_uuid(the_raid['uuid'], parent_devname)

                for the_raid1 in to_assemble_raid1_list:
                    part_raid1_devname = the_raid1['devname'] + 'p1'
                    cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + part_raid1_devname
                    do_system_timeout(cmd_str, 10)

                for this_raid1 in the_raid['children']:
                    if this_raid1['state'] != DEV_STATE_DISCONNECT:
                        this_raid1['state'] = DEV_STATE_ONLINE
                the_raid['state'] = DEV_STATE_ONLINE
                the_raid['devname'] = parent_devname
                for the_raid1 in new_create_raid1_list:
                    cmd_str = CMD_MDMANAGE + ' ' + parent_devname + ' --add ' + the_raid1['devname'] + 'p1'
                    rc = do_system_timeout(cmd_str, 10)
                    if rc == 0:
                        the_raid1['state'] = DEV_STATE_ONLINE

                if configure.has_key('virtualpool') == True:
                    create_ads_link(parent_devname, configure['adsname'], configure['vgname'])
            elif storagetype == STOR_TYPE_MEMORY:
                #
                # we cannot recover raid stroage, re-create it
                #
                debug("cannot recovery raid for memory pool ...");
                md_stop(parent_devname)
                break
            else:
                debug("cannot recovery raid for non memory pool, stuck here for failover or ...");
                md_stop(parent_devname)
                # time.sleep(10000000);

        else:
            for this_raid1 in the_raid['children']:
                if this_raid1['working'] == False:
                    continue
                if this_raid1['state'] != DEV_STATE_DISCONNECT and this_raid1['state'] != DEV_STATE_ONLINE:
                    child_devname = this_raid1['devname'] + 'p1'
                    if this_raid1['state'] != DEV_STATE_OFFLINE:
                        #
                        # sometime, a failed md just cannot be removed, try 3 times
                        #
                        for i in range(3):
                            rc = md_fail(parent_devname, child_devname)
                            if rc == 0:
                                rc = md_remove(parent_devname, child_devname)
                            if rc == 0:
                                this_raid1['state'] = DEV_STATE_OFFLINE
                                break
                            time.sleep(0.5)
                        if rc != 0:
                            continue
                    if this_raid1['state'] != DEV_STATE_OFFLINE:
                        continue

                    md_stop(this_raid1['devname'])
                    remove_dev_uuid(this_raid1['uuid'])
                    to_assemble_ibd_list = []
                    to_assemble_ibd_str = ''
                    for the_ibd in this_raid1['children']:
                        if the_ibd['state'] != DEV_STATE_DISCONNECT:
                            to_assemble_ibd_list.append(the_ibd)
                            to_assemble_ibd_str = the_ibd['devname'] + 'p1' + ' ' + to_assemble_ibd_str
                    cmd_str = CMD_MDASSEMBLE + ' --force ' + this_raid1['devname'] + ' ' + to_assemble_ibd_str
                    rc = do_system_timeout(cmd_str, 10)
                    if rc == 0:
                        save_dev_uuid(this_raid1['uuid'], this_raid1['devname'])
                        for the_ibd in to_assemble_ibd_list:
                            cmd_str = CMD_MDMANAGE + ' ' + this_raid1['devname'] + ' --add ' + the_ibd['devname'] + 'p1'
                            do_system_timeout(cmd_str, 10)

                        rc = md_re_add(parent_devname, child_devname)
                        if rc == 0:
                            this_raid1['state'] = DEV_STATE_ONLINE
                    elif storagetype == STOR_TYPE_MEMORY:
                        md_stop(parent_devname)
                        for the_ibd in to_assemble_ibd_list:
                            parted_device(the_ibd['devname'], START_SECTOR, -1, 'p1')
                        return 2
    if infrastructure_modified == True:
        debug("raid_assemble_core: save_ads_infrastructure ...")
        save_ads_infrastructure(setup_info)

    rc = 0
    c_infrastructure = setup_info['c_infrastructure']
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        if the_raid['state'] != DEV_STATE_ONLINE:
            rc = 1
            break
    debug('raid_assemble_core return %d' % rc)
    return rc


#
# The caller must already setup c_infrastructure
#
def raid_assemble(setup_info):
    debug("Enter raid_assemble ...")
    rc = 2
    while rc == 2:
        rc = raid_assemble_core(setup_info)
    find_vg_info(setup_info)  # extra info to debug
    debug('raid_assemble return %d' % rc)
    return rc


def stop_devices(setup_info):
    debug('Enter stop_devices')
    c_infrastructure = setup_info['c_infrastructure']
    configure = setup_info['configure']
    vgname_str = configure['vgname']

    print json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # stop md
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        the_raid_devname = the_raid['devname']
        if the_raid_devname != None:
            cmd_str = CMD_MDADM + ' --stop ' + the_raid_devname
            rc = do_system_timeout(cmd_str, 10)
            if rc != 0:
                debug('failed to run command: %s' % cmd_str)
            uuid_dev_file = "/tmp/dev-uuid-" + the_raid['uuid']
            cmd_str = 'rm -f ' + uuid_dev_file
            do_system(cmd_str)
        for the_raid1 in the_raid['children']:  # raid1
            the_raid1_devname = the_raid1['devname']
            if the_raid1_devname != None:
                cmd_str = CMD_MDADM + ' --stop ' + the_raid1_devname
                rc = do_system_timeout(cmd_str, 10)
                if rc != 0:
                    debug('failed to run command: %s' % cmd_str)
                uuid_dev_file = "/tmp/dev-uuid-" + the_raid1['uuid']
                cmd_str = 'rm -f ' + uuid_dev_file
                do_system(cmd_str)

    # stop arb
    arb_stop(vgname_str)

    # stop ibd
    section_list = []
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        for the_raid1 in the_raid['children']:  # raid1
            # To skip the shared storage
            if the_raid1.has_key('children') == False:
                continue

            for the_ibd in the_raid1['children']:
                # add to the section list for remove
                section_list.append(the_ibd['uuid'])

    # remove ibd connection from ibd agent configuration file
    remove_config_sections(IBD_AGENT_CONFIG_FILE, section_list)

    cmd_str = CMD_IBDMANAGER_A_UPDATE
    rc = do_system(cmd_str)
    if rc != 0:
        debug('failed to run command: %s' % cmd_str)

    # Cleanup the /dev/ibd* device nodes.
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        for the_raid1 in the_raid['children']:  # raid1
            # To skip the shared storage
            if the_raid1.has_key('children') == False:
                continue

            for the_ibd in the_raid1['children']:
                the_ibd_devname = the_ibd['devname']
                if the_ibd_devname != None:
                    cmd_str = 'rm ' + the_ibd_devname
                    rc = do_system(cmd_str)
                    if rc != 0:
                        debug('failed to run command: %s' % cmd_str)
    return 0


def destroy_devices(setup_info):
    debug('Enter destroy_devices')
    configure = setup_info['configure']
    c_infrastructure = setup_info['c_infrastructure']

    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        the_raid_devname = the_raid['devname']
        if the_raid_devname != None:
            cmd_str = CMD_MDADM + ' --stop ' + the_raid_devname
            rc = do_system_timeout(cmd_str, 10)
            if rc != 0:
                # TODO: stop a stopped device and not-exist device will fail
                #       we should check the error code, and ignore only these
                #       two cases. However, not sure how the error code are
                #       changed in the do_system, thus, only log the error
                #       and continue
                debug('Stop mdadm device ' + the_raid_devname + ' return code ' + str(rc))

            cmd_str = CMD_MDADM + ' --remove ' + the_raid_devname
            rc = do_system_timeout(cmd_str, 10)
            if rc != 0:
                # TODO: It looks like mdadm v3.2.5 does not need the remove step
                #       anymore. Add it here just in case. Ignore the error
                #       since a stopped device has been removed from /dev/
                debug('Remove mdadm device ' + the_raid_devname + ' return code ' + str(rc))

        for the_raid1 in the_raid['children']:  # raid1
            the_raid1_devname = the_raid1['devname']
            if the_raid1_devname != None:
                cmd_str = CMD_MDADM + ' --zero-superblock ' + the_raid1_devname
                rc = do_system_timeout(cmd_str, 10)
                if rc != 0:
                    # TODO: zero a device's superblock which has already been zeroed
                    #       will return error. Ignore the error, since this may be
                    #       called on a half destroyed device.
                    debug('zeror-superblock ' + the_raid1_devname + ' return code ' + str(rc))

                cmd_str = CMD_MDADM + ' --stop ' + the_raid1_devname
                rc = do_system_timeout(cmd_str, 10)
                if rc != 0:
                    debug('Stop mdadm device ' + the_raid1_devname + ' return code ' + str(rc))

                cmd_str = CMD_MDADM + ' --remove ' + the_raid1_devname
                rc = do_system_timeout(cmd_str, 10)
                if rc != 0:
                    debug('Remove mdadm device ' + the_raid1_devname + ' return code ' + str(rc))

            # To skip the shared storage
            if the_raid1.has_key('children') == False:
                continue

            for the_ibd in the_raid1['children']:
                the_ibd_devname = the_ibd['devname']
                if the_ibd_devname != None:
                    cmd_str = CMD_MDADM + ' --zero-superblock ' + the_ibd_devname
                    rc = do_system_timeout(cmd_str, 10)
                    if rc != 0:
                        debug('zeror-superblock ' + the_ibd_devname + ' return code ' + str(rc))

                    cmd_str = CMD_IBDMANAGER_A_STOP_ONE + ' ' + the_ibd_devname
                    do_system(cmd_str)
                    cmd_str = 'rm ' + the_ibd_devname
                    do_system(cmd_str)
    return


def destroy_remote_ibd(setup_info):
    debug('Enter destroy_remote_ibd ...')
    configure = setup_info['configure']
    c_infrastructure = setup_info['c_infrastructure']

    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        for the_raid1 in the_raid['children']:  # raid1
            # To skip the shared storage
            if the_raid1.has_key('children') == False:
                continue

            for the_ibd in the_raid1['children']:
                # destroy the remote ibd client
                # e.g., python /opt/milio/atlas/roles/aggregate/agexport.pyc -d memory export_name
                args_str = CMD_AGGDESTROY + the_ibd['uuid']
                (rc, out, err) = remote_exec(the_ibd['ip'], 'python ', args_str)
                if rc != 0:
                    debug('destroy the existing export %s from %s failed, errcode=%d, errmsg=%s' % (
                        the_ibd['uuid'], the_ibd['ip'], rc, err))
                    return rc
    return 0


def retry_memory_raid_assemble(setup_info):
    debug('Enter retry_memory_raid_assemble')
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    if configure['raidtype'] == 'RAID_0':
        raid_type = 'stripe'
    elif configure['raidtype'] == 'RAID_5':
        raid_type = '5'
    else:
        debug('retry_memory_raid_assemble: bad raidtype %s' % configure['raidtype'])
        return -1
    c_infrastructure = setup_info['c_infrastructure']

    #
    # first of all, we want to check if we have enough aggregator ready
    #
    ok_to_assemble = True
    all_ibd_connected = True
    for the_raid in c_infrastructure[configure['pool_infrastructure_type']]:  # raid0|raid5
        raid1_ok_counter = 0
        for the_raid1 in the_raid['children']:  # raid1
            ibd_ok_counter = 0
            for the_ibd in the_raid1['children']:
                if the_ibd['state'] != DEV_STATE_DISCONNECT:
                    the_ibd_devname = the_ibd['devname'] + 'p1'
                    cmd_str = CMD_MDADM + ' --examine ' + the_ibd_devname
                    rc = do_system_timeout(cmd_str, 10)
                    if rc == 0:
                        ibd_ok_counter += 1
                else:
                    all_ibd_connected = False
            if ibd_ok_counter >= 1:
                raid1_ok_counter += 1
        if raid1_ok_counter == len(the_raid) or \
                (raid1_ok_counter == len(the_raid) - 1 and raid_type == '5'):
            continue
        else:
            ok_to_assemble = False
            break

    if ok_to_assemble == True:
        # why need retry?
        debug('retry_memory_raid_assemble: calling raid_assemble')
        return raid_assemble(setup_info)

    #
    # we don't have enough "qualified" ibd to do assemble.
    # 1> there are not enough aggregators connected
    # 2> too many aggregators got destroyed, even if all aggregator connected
    # when an ibd not DEV_STATE_DISCONNECT, it could still not really connected since
    # it could be in persistent retry. Right now we don't have a way to konw if a ibd
    # is really "connected", so let's teperorily assume all DEV_STATE_DISCONNECT ibd
    # are really "connected". TODO is next step
    #

    #
    # TODO:
    # Temporary solution: re-create the whole vg
    #
    debug('retry_memory_raid_assemble: re-create storage, set timeout 30')
    stop_devices(setup_info)
    rc = create_storage(setup_info, VIRTUAL_POOL, 30)
    return rc


#
# The caller must already call raid_assemble and failed
#
def retry_raid_assemble(setup_info):
    debug('Enter retry_raid_assemble')
    storagetype = setup_info['storagetype']
    rc = 1

    if storagetype == STOR_TYPE_MEMORY:
        rc = retry_memory_raid_assemble(setup_info)
    return rc


def cp_up(setup_info):
    debug('Enter cp_up')
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    load_devices(setup_info, 30)
    find_vg_info(setup_info)
    rc = raid_assemble(setup_info)
    if rc != 0:
        rc = retry_raid_assemble(setup_info)
    if rc != 0:
        return rc
    start_vg(vgname_str)
    start_ibdserver(vgname_str)
    find_vg_info(setup_info)  # extra info to debug
    return rc


def logging_ibd_drop_list(setup_info):
    debug("Aggregator drop_list:")
    ibd_drop_list = setup_info['ibd_drop_list']
    for the_drop in ibd_drop_list:
        the_ibd = the_drop['ibd']
        log_str = '      ' + the_ibd['import']['uuid'] + ':' + the_ibd['import']['ip'] + ':' + the_drop['reason']
        debug(log_str)
    debug('\n')


def stop_ibd_drop_list(setup_info):
    ibd_drop_list = setup_info['ibd_drop_list']
    for the_drop in ibd_drop_list:
        the_ibd = the_drop['ibd']
        cmd_str = CMD_IBDMANAGER_DROP + ' ' + the_ibd['devname']
        do_system(cmd_str)
    return


def update_infrastructure_by_extend(pool_info, add_pool_info):
    debug("update_infrastructure_by_extend start...")
    infrastructure = pool_info['infrastructure']
    second_level_raid_list = add_pool_info['second_level_raid_list']
    for the_raid in second_level_raid_list:  # raid0|raid5
        the_devname = the_raid['devname']
        property_dict = {}
        raid_detail(the_devname, property_dict)
        the_new_raid = {}
        the_new_raid['uuid'] = property_dict[RAID_UUID]
        the_new_raid['children'] = []
        for the_raid1 in the_raid['children']:
            the_devname = the_raid1['devname']
            property_dict = {}
            raid_detail(the_devname, property_dict)
            the_new_raid1 = {}
            the_new_raid1['uuid'] = property_dict[RAID_UUID]
            the_new_raid1['children'] = []
            for the_ibd in the_raid1['children']:
                if the_ibd['devname'] == 'missing':
                    continue
                the_new_ibd = {}
                the_new_ibd['uuid'] = the_ibd['import']['uuid']
                the_new_ibd['ip'] = the_ibd['import']['ip']
                the_new_raid1['children'].append(the_new_ibd)
            the_new_raid['children'].append(the_new_raid1)
        infrastructure.append(the_new_raid)


#
# ibd is from ibd_missing_list
#
def update_infrastructure_by_replace_missing(pool_info, ibd, new_ibd):
    debug("Enter update_infrastructure_by_replace_missing ...")
    infrastructure = pool_info['infrastructure']
    for the_raid in infrastructure:
        for the_raid1 in the_raid['children']:
            if the_raid1['uuid'] == ibd['md']['uuid']:
                the_raid1['children'].append({'uuid': new_ibd['import']['uuid'], 'ip': new_ibd['import']['ip']})


def replace_missing(pool_info, add_setup_info):
    debug("enter replace_missing ...\n")
    gap_ratio = add_setup_info['gap_ratio']
    ibd_dev_list = add_setup_info['ibd_dev_list']
    set_ibd_list_size(ibd_dev_list)
    ibd_dev_list.sort(key=operator.itemgetter("size"), reverse=True)
    has_missing_list = pool_info['has_missing_list']

    #
    # Now we know both ibd_dev_list and has_missing_list are ordered by size decreasingly
    #
    the_ibd = None
    the_missing = None
    ibd_left = len(ibd_dev_list)
    for i in range(len(has_missing_list)):
        the_missing = has_missing_list.pop(0)
        the_size = the_missing['size']

        while the_missing != None and (ibd_left > 0 or the_ibd != None):
            if the_ibd == None:
                the_ibd = ibd_dev_list[0]
                ibd_left = ibd_left - 1
            if len(ibd_dev_list) > 0:
                ibd_dev_list.pop(0)  # delete this ibd from the ibd list

            diff = the_ibd['size'] - the_size
            if diff >= 0 and diff * 100 / the_size <= gap_ratio:
                cmd_str = 'mdadm --manage ' + the_missing['md']['devname'] + ' --add ' + the_ibd['devname'] + 'p1'
                do_system_timeout(cmd_str, 10)
                update_infrastructure_by_replace_missing(pool_info, the_missing, the_ibd)
                the_missing = None  # this missing is replaced
            else:
                ibd_dev_list.append(the_ibd)  # this ibd is useless
            the_ibd = None  # this ibd is consumed or useless
            if diff < 0:
                break

        if the_missing != None:
            has_missing_list.append(the_missing)  # no way to fit this missing
            the_missing = None

    if the_ibd != None:
        ibd_dev_list.append(the_ibd)

    #
    # any ibd left in ibd_dev_list should be added to the drop list
    #
    # ibd_drop_list = add_setup_info['ibd_drop_list']
    # for the_ibd in ibd_dev_list:
    #   ibd_drop_list.append({'ibd':the_ibd, 'reason':'matching'})
    return


#
# amcurl_str: like 'https://10.15.108.91:8443/amc'
#
def inform_amc_drop_list(vgname_str, amcurl_str, ibd_drop_list, use_stdout):
    debug('Enter inform_amc_drop_list ...')
    drop_str = ''
    for the_drop in ibd_drop_list:
        the_ibd = the_drop['ibd']
        drop_str += the_ibd['import']['uuid'] + ':'

    if use_stdout == True:
        print 'drop_list_return=' + drop_str + 'drop_list_return_done'
    elif drop_str != '':
        protocol = amcurl_str.split(':')[0]  # https
        amcaddr = amcurl_str.split('/')[2]  # 10.15.108.91:8443
        amcpost_str = '/usxmanager/usx/inventory/servicevm/exports?'
        amcpost_str += 'vguuid=' + vgname_str + '&'
        amcpost_str += 'exportuuids=' + drop_str + '&'
        amcpost_str += 'mode=2'
        debug(protocol)
        debug(amcaddr)
        debug(amcpost_str)
        if protocol == 'https':
            conn = httplib.HTTPSConnection(amcaddr)
        else:
            conn = httplib.HTTPConnection(amcaddr)

        conn.request("POST", amcpost_str)
        r1 = conn.getresponse()
        debug(r1.status, ' ', r1.reason)
    return 0


#
# action: 0 for GET, 1 for POST
#
def vg_access_detail(setup_info, action):
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    # detail_url = "/amc/model/ilio/pools/vg/" + vgname_str + "/detail2" # USX 1.5 REST API
    detail_url = '/usxmanager/usx/virtualpool/' + vgname_str + '/detail/attribute'  # USX 2.0 REST API
    conn = urllib2.Request("http://127.0.0.1:8080" + detail_url)
    conn.add_header('Content-type', 'application/json')
    if action == 0:
        res = urllib2.urlopen(conn)
        res_data = json.load(res)
        data = res_data['detail']
        debug('vp detail response: ', json.dumps(json.loads(data), sort_keys=True, indent=4, separators=(',', ': ')))

        if len(data) > 0:
            detail_str = json.loads(data)
            setup_info['infrastructure'] = detail_str
        else:
            setup_info['infrastructure'] = {}
        res.close()
    else:
        infrastructure = setup_info['infrastructure']
        detail_str = json.dumps(infrastructure)
        debug('VG detail: ', detail_str)
        data = {
            'detail': detail_str
        }
        res = urllib2.urlopen(conn, json.dumps(data))
        debug('POST returned response code: ' + str(res.code))
        res.close()
    return


def save_infrastructure(setup_info):
    vg_access_detail(setup_info, 1)

    #
    # also save at local for debugging
    #
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    infrastructure_list = setup_info['infrastructure']
    infrastructure_str = json.dumps(infrastructure_list, indent=4, separators=(',', ': '))
    fname = '/etc/ilio/pool_infrastructure_' + vgname_str + '.json'
    cfg_file = open(fname, 'w')
    cfg_file.write(infrastructure_str)
    cfg_file.close()


def save_c_infrastructure(setup_info):
    try:
        configure = setup_info['configure']
        vgname_str = configure['vgname']
        rt = configure['raidtype']

        c_infrastructure_list = setup_info['c_infrastructure']

        if c_infrastructure_list.has_key(DISK_INFRASTRUCTURE) and len(c_infrastructure_list[DISK_INFRASTRUCTURE]) > 0:
            for raid in c_infrastructure_list[DISK_INFRASTRUCTURE]:
                raid['raidtype'] = rt

        if c_infrastructure_list.has_key(MEMORY_INFRASTRUCTURE) and len(
                c_infrastructure_list[MEMORY_INFRASTRUCTURE]) > 0:
            for raid in c_infrastructure_list[MEMORY_INFRASTRUCTURE]:
                raid['raidtype'] = rt

        c_infrastructure_str = json.dumps(c_infrastructure_list, indent=4, separators=(',', ': '))
    except:
        debug("save_c_infrastructure cannot get data!")
        return

    fname = '/etc/ilio/c_pool_infrastructure_' + vgname_str + '.json'
    cfg_file = open(fname, 'w')
    cfg_file.write(c_infrastructure_str)
    cfg_file.close()


def vg_fetch_infrastructure(setup_info):
    vg_access_detail(setup_info, 0)

    '''
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    fname = '/etc/ilio/pool_infrastructure_' + vgname_str + '.json'
    cfg_file = open(fname, 'r')
    infrastructure_str = cfg_file.read()
    cfg_file.close()
    infrastructure = json.loads(infrastructure_str)
    setup_info['infrastructure'] =  infrastructure
    '''


#
# action: 0 for GET, 1 for POST
#
def ads_vp_access_detail(setup_info, action):
    configure = setup_info['configure']
    adsname_str = configure['adsname']
    # GET: curl -k -X GET http://10.21.87.187:8080/amc/model/ilio/ads/vCenter13421_Danzhou-ADS-AF/detail
    # POST: curl -k -X POST http://10.21.87.187:8080/amc/model/ilio/ads/vCenter13421_Danzhou-ADS-AF/detail?detail=test_detail_888
    # detail_url = "/amc/model/ilio/ads/" + adsname_str + "/detail" # USX 1.5 REST API
    detail_url = "/usxmanager/usx/inventory/volume/resources/" + adsname_str + "/detail"  # USX 2.0 REST API
    conn = urllib2.Request("http://127.0.0.1:8080" + detail_url)
    conn.add_header('Content-type', 'application/json')
    if action == 0:
        res = urllib2.urlopen(conn)
        data = json.load(res)
        data = data['detail']
        debug('detail response: ', data)
        if len(data) > 0:
            detail_str = json.loads(data)
            setup_info['infrastructure'] = detail_str
        else:
            setup_info['infrastructure'] = {}
        res.close()
    else:
        infrastructure = setup_info['infrastructure']
        detail_str = json.dumps(infrastructure)
        debug('ADS VP detail: ', detail_str)
        data = {
            'detail': detail_str
        }
        res = urllib2.urlopen(conn, json.dumps(data))
        debug('POST returned response code: ' + str(res.code))
        res.close()
    return


def save_ads_infrastructure(setup_info):
    # fetch the current ads infrastructure
    curr_setup_info = {}
    curr_setup_info['configure'] = {}
    curr_setup_info['configure']['adsname'] = setup_info['configure']['adsname']
    ads_vp_access_detail(curr_setup_info, 0)

    if setup_info['configure']['pool_infrastructure_type'] == MEMORY_INFRASTRUCTURE:
        if curr_setup_info['infrastructure'].has_key(DISK_INFRASTRUCTURE):
            setup_info['infrastructure'][DISK_INFRASTRUCTURE] = curr_setup_info['infrastructure'][DISK_INFRASTRUCTURE]
    elif setup_info['configure']['pool_infrastructure_type'] == DISK_INFRASTRUCTURE:
        if curr_setup_info['infrastructure'].has_key(MEMORY_INFRASTRUCTURE):
            setup_info['infrastructure'][MEMORY_INFRASTRUCTURE] = curr_setup_info['infrastructure'][
                MEMORY_INFRASTRUCTURE]
    else:
        debug('No exisiting ADS infrastructure')

    ads_vp_access_detail(setup_info, 1)

    configure = setup_info['configure']
    adsname_str = configure['adsname']

    #
    # save at local for debugging
    #
    infrastructure_list = setup_info['infrastructure']
    infrastructure_str = json.dumps(infrastructure_list, indent=4, separators=(',', ': '))
    fname = '/etc/ilio/pool_infrastructure_' + adsname_str + '.json'
    cfg_file = open(fname, 'w')
    cfg_file.write(infrastructure_str)
    cfg_file.close()

    # GET for debugging
    ads_vp_access_detail(setup_info, 0)


def fetch_ads_infrastructure(setup_info):
    ads_vp_access_detail(setup_info, 0)


# fetch ads infrastructure from local file
# configure = setup_info['configure']
# adsname_str = configure['adsname']
# fname = '/etc/ilio/pool_infrastructure_' + adsname_str + '.json'
# cfg_file = open(fname, 'r')
# infrastructure_str = cfg_file.read()
# cfg_file.close()
# infrastructure = json.loads(infrastructure_str)
# setup_info['infrastructure'] =  infrastructure


#
# create storage for a vg
#
def create_storage(vg_setup_info, pool_type, timeout):
    load_devices(vg_setup_info, timeout)
    grouping_ibd_list(vg_setup_info)
    logging_ibd_drop_list(vg_setup_info)
    stop_ibd_drop_list(vg_setup_info)
    if (pool_type == REGULAR_POOL):
        init_devices(vg_setup_info)
        create_md(vg_setup_info)
        rc = create_vg(vg_setup_info)
        if rc != 0:
            return rc
    time.sleep(0.5)

    #
    # generate infrastructure
    #
    generate_infrastructure(vg_setup_info, pool_type)
    save_infrastructure(vg_setup_info)

    if (pool_type == REGULAR_POOL):
        debug("create_storage: vg_setup_current_infrastructure")
        vg_setup_current_infrastructure(vg_setup_info)

    # fetch the infrastructure json
    vg_fetch_infrastructure(vg_setup_info)
    return 0


#
# allocate shared storage for a given volumeresourceuuid
#
def allocate_shared_storage(sharedstorages, volumeresourceuuid):
    # generate the input json for ADS shared storage
    input_json = {}
    input_json["volumeresourceuuid"] = volumeresourceuuid
    input_json["sharedstorages"] = sharedstorages
    debug("allocate_shared_storage: " + json.dumps(input_json, sort_keys=True, indent=4, separators=(',', ': ')))

    # add shared storage via USX 2.0 REST API
    # only on AMC: ss_url = '/amc/ilio/deploy/virtualvolume/sharedstorages'
    ss_url = '/usxmanager/usxds/volume/sharedstorages'
    conn = urllib2.Request("http://127.0.0.1:8080" + ss_url)
    debug("allocate_shared_storage: " + "http://127.0.0.1:8080" + ss_url)
    conn.add_header('Content-type', 'application/json')
    res = urllib2.urlopen(conn, json.dumps(input_json))
    debug('POST returned response code: ' + str(res.code))
    res.close()

    return 0


#
# access the resource detail for a given virtualvolumeresourceuuid
#
def resource_access_detail(virtualvolumeresourceuuid):
    res_url = '/usxmanager/usx/inventory/volume/resources/' + virtualvolumeresourceuuid
    conn = urllib2.Request("http://127.0.0.1:8080" + res_url)
    conn.add_header('Content-type', 'application/json')
    res = urllib2.urlopen(conn)
    data = json.load(res)
    res.close()

    debug("After resource_access_detail: " + json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))
    return data


#
# create storage for ADS with shared storage
#
def create_storage_ss(setup_info, ss_requried_size, raid_type, raid5_size):
    debug("Enter create_storage_ss ...")

    sharedstorages = setup_info['sharedstorages']
    if len(sharedstorages) == 0:
        debug("WARNING: Empty sharedstorages")
        return 0

    ss_requried_size_GB = int(ss_requried_size / GB_SIZE)
    allocated_size = 0
    ss_to_allocate = []

    if raid_type == 'RAID_0' or raid5_size == 0:
        for the_ss in sharedstorages:
            the_size = the_ss['requestedsize']
            if (the_size + allocated_size) < ss_requried_size_GB:
                ss_to_allocate.append({'size': the_size, 'datastorename': the_ss['datastorename']})
                allocated_size += the_size
            else:
                ss_to_allocate.append(
                    {'size': (ss_requried_size_GB - allocated_size), 'datastorename': the_ss['datastorename']})
                allocated_size = ss_requried_size_GB
                break

        if allocated_size < ss_requried_size_GB:
            debug("ERROR: failed to allocate the enough shared storage (available %d vs. requested %d) " % (
                allocated_size, ss_requried_size_GB))
            return 1
    else:
        total_ss = int(ss_requried_size / raid5_size)
        num_ss = 0
        raid5_size_GB = int(raid5_size / GB_SIZE)
        for the_ss in sharedstorages:
            the_size = the_ss['requestedsize']
            if the_size >= raid5_size_GB:
                ss_to_allocate.append({'size': raid5_size_GB, 'datastorename': the_ss['datastorename']})
                num_ss += 1

        if num_ss < total_ss:
            debug(
                "ERROR: failed to allocate the enough number of shared storage disks (available %d vs. requested %d) for RAID_5" % (
                    num_ss, total_ss))
            return 1

    # add shared storage via USX 2.0 REST API
    debug('ss_to_allocate %s ' % str(ss_to_allocate))
    allocate_shared_storage(ss_to_allocate, setup_info['configure']['adsname'])

    # retrieve the updated sharedstorages via USX 2.0 REST API
    data = resource_access_detail(setup_info['configure']['adsname'])
    setup_info["sharedstorages"] = data["data"]["sharedstorages"]
    debug("After resource_access_detail: " + json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    # obtain and partition ss_dev_list
    start_shared_storage(setup_info)
    shared_dev_list = setup_info['shared_dev_list']
    debug('shared_dev_list is %s ' % str(shared_dev_list))
    debug('sharedstorages is %s ' % str(setup_info["sharedstorages"]))

    allocated_len = 0
    for the_ss in setup_info["sharedstorages"]:
        if the_ss.has_key('scsibus'):
            allocated_len += 1

    if len(shared_dev_list) != allocated_len:
        debug(json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': ')))
        debug("ERROR: failed to allocate the desired number of shared storages (desired %d vs allocatd %d)" % (
            allocated_len, len(shared_dev_list)))
        return 1

    rc = parted_shared_devices(setup_info)
    if rc != 0:
        debug(json.dumps(setup_info, sort_keys=True, indent=4, separators=(',', ': ')))
        debug('ERROR: Failed to partition the shared storages ')
        return rc

    return 0


def vp_create_storage_core(vp_setup_info):
    # generate high_gap_group_list
    ibd_list = vp_setup_info['ibd_dev_list']
    vp_setup_info['high_gap_group_list'] = []
    high_gap_group_list = vp_setup_info['high_gap_group_list']
    the_sub_group = []
    the_group = []
    for the_ibd in ibd_list:
        the_sub_group.append(the_ibd)

    if len(the_sub_group) > 0:
        the_group.append(the_sub_group)
        high_gap_group_list.append(the_group)

    logging_ibd_drop_list(vp_setup_info)
    stop_ibd_drop_list(vp_setup_info)
    init_devices(vp_setup_info)
    create_md_v2(vp_setup_info)
    time.sleep(0.5)

    # generate ads pool infrastructure
    generate_infrastructure(vp_setup_info, REGULAR_POOL)
    save_ads_infrastructure(vp_setup_info)

    # Generate the c_infrastructure
    find_ibd_online_info(vp_setup_info)
    vg_setup_current_infrastructure_core(vp_setup_info)
    # print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
    save_c_infrastructure(vp_setup_info)


# print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

#
# create storage for a virtual pool
#
def vp_create_storage(vp_setup_info, timeout, poolsize):
    vp_setup_info['ibd_dev_list'] = []
    vp_setup_info['ibd_drop_list'] = []
    ibd_dev_list = vp_setup_info['ibd_dev_list']
    ibd_drop_list = vp_setup_info['ibd_drop_list']
    config = vp_setup_info['configure']
    adsname_str = config['adsname']
    config['imports'] = []
    all_ibd_imports = config['imports']
    # ibd agent configuration
    ini_config = ConfigParser.ConfigParser()
    try:
        ini_config.read(IBD_AGENT_CONFIG_FILE)
    except:
        debug('Cannot read %s.' % IBD_AGENT_CONFIG_FILE)
        return 1
    debug('ibd agent sections before vp_create_storage update: %s' % str(ini_config.sections()))

    try:
        ini_config.add_section(IBD_AGENT_SEC_GLOBAL)
    except:
        pass

    if config['raidtype'] == 'RAID_5':
        raid_type = POOL_TYPE_RAID5
    else:
        raid_type = POOL_TYPE_RAID0

    # print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    if vp_setup_info['infrastructure'].has_key(config['pool_infrastructure_type']) == False:
        debug("do not have the desired %s " % (config['pool_infrastructure_type']))
        return 1

    # infrastructure should be set already
    pair_list = vp_setup_info['infrastructure'][config['pool_infrastructure_type']]
    new_pair_group = []

    next_idx = 0
    cur_size = 0
    reserved_size = 0
    raid5_size = 0
    raid0_total_size = 0
    finish_flag = False
    size_list = []

    for the_pair_group in pair_list:
        for the_pair in the_pair_group:
            if (max(the_pair[0]['free'], the_pair[1]['free']) == 0) or \
                    ((the_pair[0]['free'] == 0) and (the_pair[0]['devname'] != 'missing')) or \
                    ((the_pair[1]['free'] == 0) and (the_pair[1]['devname'] != 'missing')):
                continue

            # derive the pair size for raid_1
            if min(the_pair[0]['free'], the_pair[1]['free']) == 0:
                pair_size = max(the_pair[0]['free'], the_pair[1]['free'])
            else:
                pair_size = min(the_pair[0]['free'], the_pair[1]['free'])

            if raid_type == POOL_TYPE_RAID5:
                # to check whether have enought space for RAID5
                size_list.append(pair_size)
                pair_num = len(size_list)
                if pair_num >= 3:
                    curr_total_size = min(size_list) * (pair_num - 1)
                    if curr_total_size >= poolsize:
                        raid5_size = int(math.ceil(float(poolsize) / (pair_num - 1)))
                        finish_flag = True
                        break
            else:
                # to check whether have enought space for RAID0
                raid0_total_size = raid0_total_size + pair_size
                if raid0_total_size >= poolsize:
                    finish_flag = True
                    break
        if finish_flag == True:
            break

    if raid_type == POOL_TYPE_RAID5 and raid5_size == 0:
        debug("cannot allocate ADS the required storage size %d with RAID 5" % (poolsize))
        return 1

    if raid_type == POOL_TYPE_RAID0 and raid0_total_size < poolsize:
        debug("cannot allocate ADS the required storage size (pool_size %d vs required_size %d) for RAID 0" % (
            raid0_total_size, poolsize))
        return 1

    finish_flag = False
    for the_pair_group in pair_list:
        for the_pair in the_pair_group:
            if (max(the_pair[0]['free'], the_pair[1]['free']) == 0) or \
                    ((the_pair[0]['free'] == 0) and (the_pair[0]['devname'] != 'missing')) or \
                    ((the_pair[1]['free'] == 0) and (the_pair[1]['devname'] != 'missing')):
                continue

            # derive the pair size for raid_1
            if min(the_pair[0]['free'], the_pair[1]['free']) == 0:
                pair_size = max(the_pair[0]['free'], the_pair[1]['free'])
            else:
                pair_size = min(the_pair[0]['free'], the_pair[1]['free'])

            if raid_type == POOL_TYPE_RAID5:
                reserved_size = raid5_size
                if reserved_size > pair_size:
                    continue
            else:
                if (poolsize - cur_size) >= pair_size:
                    reserved_size = pair_size
                else:
                    reserved_size = poolsize - cur_size

            # check whether ibd dev pair are online or not.
            # If any one fails, skip both of them
            rc = check_ibd_list(vp_setup_info, ini_config, the_pair, next_idx, timeout, reserved_size)

            if rc == 0:
                the_ads = {"adsname": adsname_str, "used": reserved_size}

                # update imports and free space
                if the_pair[0]['free'] > 0:
                    all_ibd_imports.append(the_pair[0])
                    the_pair[0]['free'] = the_pair[0]['free'] - reserved_size
                    the_pair[0]['adslist'].append(the_ads)
                if the_pair[1]['free'] > 0:
                    all_ibd_imports.append(the_pair[1])
                    the_pair[1]['free'] = the_pair[1]['free'] - reserved_size
                    the_pair[1]['adslist'].append(the_ads)

                new_pair_group.append(the_pair)
                cur_size = cur_size + reserved_size
                if raid_type == POOL_TYPE_RAID5:
                    if cur_size >= poolsize + raid5_size:
                        finish_flag = True
                        break;
                else:
                    if cur_size >= poolsize:
                        finish_flag = True
                        break;

        if finish_flag == True:
            break;

    if raid_type == POOL_TYPE_RAID0:
        if cur_size < poolsize:
            debug("Failed to allocate ADS the required storage size (pool_size %d vs required_size %d) for RAID 0" % (
                cur_size, poolsize))
            return 1
    else:
        if cur_size < poolsize + raid5_size:
            debug("Failed to allocate ADS the required storage size (pool_size %d vs required_size %d) for RAID 5" % (
                cur_size, poolsize))
            return 1

    debug("Allocated ADS the required storage size (allocated_size %d vs required_size %d)" % (cur_size, poolsize))

    # start ibd connnection
    cfgfile = open(IBD_AGENT_CONFIG_FILE, 'w')
    ini_config.write(cfgfile)
    debug('ibd agent sections after vp_create_storage update: %s' % str(ini_config.sections()))
    cfgfile.close()
    rc = ibd_agent_alive()
    if rc == False:
        cmd_str = CMD_IBDAGENT
    else:
        cmd_str = CMD_IBDMANAGER_A_UPDATE
    rc = do_system(cmd_str)

    cmd_str = CMD_IBDMANAGER_STAT_WU
    device_len = 0
    wait_nr = 0
    debug("device_len:" + str(device_len))
    connecting_ibds = list(ibd_dev_list)
    for the_ibd in connecting_ibds:
        if the_ibd["devname"] == "missing":
            connecting_ibds.remove(the_ibd)
    debug(connecting_ibds)

    while (len(connecting_ibds) != 0 and wait_nr < 30):
        if wait_nr != 0:
            time.sleep(1)
        wait_nr += 1
        out = ['']
        rc = do_system(cmd_str, out)
        for the_device in connecting_ibds:
            the_exportname = the_device["import"]["uuid"]
            if re.search(r'\b' + re.escape(the_exportname) + r'\b', out[0]):
                debug("Found " + the_exportname)
                connecting_ibds.remove(the_device)

    if len(connecting_ibds) != 0:
        debug("Failed to start all devices: %s " % str(connecting_ibds))
        return 1

    debug(ibd_dev_list)
    debug(ibd_drop_list)
    debug(all_ibd_imports)
    tune_all_ibd(ibd_dev_list)

    # udpate virtual pool infrastructure
    # TODO:  need to add a lock for virtual pool update
    save_infrastructure(vp_setup_info)

    # generate latest infrastructure
    del vp_setup_info['infrastructure'][config['pool_infrastructure_type']][:]
    vp_setup_info['infrastructure'][config['pool_infrastructure_type']].append(new_pair_group)

    # print json.dumps(vp_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
    vp_create_storage_core(vp_setup_info)
    return 0


def vp_create_infrastructure(vg_setup_info):
    get_devices_info(vg_setup_info)
    grouping_ibd_list(vg_setup_info)
    logging_ibd_drop_list(vg_setup_info)
    generate_infrastructure(vg_setup_info, VIRTUAL_POOL)
    save_infrastructure(vg_setup_info)
    vg_fetch_infrastructure(vg_setup_info)
    return 0


"""
 Virtual volume related methods
"""


def virtvol_load_pool_conf_from_amc(amcurl, vgname_str, pool_setup_info):
    try:
        protocol = amcurl.split(':')[0]  # like: https
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        amcaddr = amcurl.split('/')[2]  # like: 10.15.107.2:8443
        amcfile = "/usxmanager/usx/virtualpool/" + vgname_str + "/attributes/vpconfig"
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
        response = json.loads(data1)  # parse vpconfig from response
        data2 = response['vpconfig']
        configure_json = json.loads(data2)
        configure_str = json.dumps(configure_json['configure'])
        tmp_fname = "/tmp/" + vgname_str + '.json'
        tmp_file = open(tmp_fname, 'w')
        tmp_file.write(configure_str)
        tmp_file.close()
    except:
        debug('Can not connect to AMC for config json.')
        return 1
    rc = load_conf(tmp_fname, pool_setup_info)
    return 0


def virtvol_vp_save_config(setup_info):
    """
    POST Virtual Pool configuration to AMC (tag)
    """
    debug("--Save virtual pool config")
    configure = setup_info['configure']
    vgname_str = configure['vgname']
    attribute_url = "/usxmanager/usx/virtualpool/" + vgname_str + "/attributes"
    conn = urllib2.Request("http://127.0.0.1:8080" + attribute_url)
    conn.add_header('Content-type', 'application/json')
    vpconfig_str = json.dumps(setup_info)
    debug('vpconfig: ', vpconfig_str)
    data = {
        'attributes': {'vpconfig': vpconfig_str}
    }
    res = urllib2.urlopen(conn, json.dumps(data))
    debug('POST returned response code: ' + str(res.code))
    res.close()
    return


def virtvol_vp_setup(argv):
    """
    Configure Virtual Pool parameters and setup VP infrastructure
    """
    debug("Enter virtvol_vp_setup...")
    try:
        setup_info = json.loads(argv[2])

        # configure_str = json.dumps(setup_info, indent=4, separators=(',', ': '))
        # debug("------------------------ %s" % configure_str)

        configure = setup_info['configure']
        setup_info['gap_ratio'] = 0
        if configure.has_key('gapratio'):
            setup_info['gap_ratio'] = configure['gapratio']
        if setup_info['gap_ratio'] == 0:
            setup_info['gap_ratio'] = GAP_RATIO_MAX

        if not setup_info.has_key('sharedstoragefirst'):
            setup_info['sharedstoragefirst'] = True  # always put sharedstorage first by default

        if configure.has_key('chunk_size'):
            setup_info['chunk_size'] = configure['chunk_size']
        else:
            setup_info['chunk_size'] = 512  # 512K by default
        if setup_info['chunk_size'] <= 0:
            setup_info['chunk_size'] = 512

        if configure.has_key('fastsync'):
            setup_info['fastsync'] = configure['fastsync']
        else:
            setup_info['fastsync'] = True  # Enable MD bitmap by default.

        setup_info['storagetype'] = STOR_TYPE_UNKNOWN
        if configure.has_key('roles'):
            for the_role in configure['roles']:
                if the_role == 'CAPACITY_POOL':
                    setup_info['storagetype'] = STOR_TYPE_DISK
                elif the_role == 'MEMORY_POOL':
                    setup_info['storagetype'] = STOR_TYPE_MEMORY
        elif configure.has_key('storagetype'):
            if configure['storagetype'] == 'DISK':
                setup_info['storagetype'] = STOR_TYPE_DISK
            elif configure['storagetype'] == 'MEMORY':
                setup_info['storagetype'] = STOR_TYPE_MEMORY
    except ValueError, e:
        debug('JSON parse exception : ' + str(e))
        return 1

    # configure_str = json.dumps(setup_info, indent=4, separators=(',', ': '))
    #   debug("BEFORE call create infrastructure++++++++++++++++++++++++ %s" % configure_str)

    # Save the pool config info to tag (vg infor for 1.5)
    #  should save after infrastructure or before?
    #  also need to get vp size and update metrics

    virtvol_vp_save_config(setup_info)

    #   #pool_setup_info = {}
    #   #load_pool_conf_from_amc("http://127.0.0.1:8080", configure['vgname'], pool_setup_info)
    #   #configure_str = json.dumps(pool_setup_info, indent=4, separators=(',', ': '))
    #   #debug("Saved vp config: ++++++++++++++++++++++++ %s" % configure_str)

    if configure.has_key('extendvp'):
        if configure['extendvp'] == True:
            debug("--Extend the existing Virtual Pool")
            # Prepare data for cp_extend invocation
            ibdlist = {}
            for item in configure['imports']:
                ibdlist[item['uuid']] = item['ip']
            setup_info['ilioManagementid'] = configure['uuid']
            setup_info['vguuid'] = configure['vgname']
            setup_info['ibdlist'] = ibdlist
            setup_info['scsibuslist'] = 'missing'
            setup_info['gapratio'] = configure['gapratio']
            #           test = json.dumps(data, indent=4, separators=(',', ': '))
            #           debug("DATA for add agg to vp: ---------------------%s" % test)

            add_aggregates_virtualpool(setup_info)
            return 0

    debug("--Create Virtual Pool infrastructure")

    if setup_info.has_key('infrastructure') == False:
        setup_info['infrastructure'] = {}

    if configure.has_key('imports'):
        get_devices_info(setup_info)
        grouping_ibd_list(setup_info)
        logging_ibd_drop_list(setup_info)
        generate_infrastructure(setup_info, VIRTUAL_POOL)

    save_infrastructure(setup_info)
    vg_fetch_infrastructure(setup_info)

    configure_str = json.dumps(setup_info, indent=4, separators=(',', ': '))
    debug("After call create infrastructure++++++++++++++++++++++++ %s: " % configure_str)

    return 0


"""
End Virtual Volume
"""


#
# init function can only be called from bootstrap
# pool_type: REGULAR_POOL or VIRTUAL_POOL
#
def cp_init(arg_list):
    debug("Enter cp_init ...")
    pool_setup_info = {}
    rc = load_conf(CP_CFG, pool_setup_info)
    if rc != 0:
        debug("cannot load configure file: ", CP_CFG, '  exiting ...')
        return rc
    configure = pool_setup_info['configure']

    sharedstorage_flag = False
    # do nothing if it's ha node
    for the_role in configure['roles']:
        if the_role == 'HA_POOL':
            debug("Pool init on HA node do nothing.")
            return 0
        elif the_role == 'MEMORY_POOL':
            pool_type = VIRTUAL_POOL
        elif the_role == 'CAPACITY_POOL':
            pool_type = VIRTUAL_POOL
            sharedstorage_flag = True
        else:
            debug("not supported pool type: " + the_role)
            return 1

    #
    # this is per vg instead of pool.
    # But right now one pool can only contains one vg, pool_setup_info
    # is just like vg_setup_info
    #
    vgname_str = configure['vgname']
    vg_setup_info = pool_setup_info
    ss_setup_info = copy.deepcopy(vg_setup_info)

    #
    # create storage for this vg
    # pool_type: VIRTUAL_POOL
    #
    if pool_type == VIRTUAL_POOL:
        vp_create_infrastructure(vg_setup_info)

    # to handle shared storage pool
    if sharedstorage_flag == True:
        # print json.dumps(ss_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
        if ss_setup_info['configure'].has_key('imports'):
            del ss_setup_info['configure']['imports']

        # print json.dumps(ss_setup_info, sort_keys=True, indent=4, separators=(',', ': '))
        if ss_setup_info['configure'].has_key('sharedstorage'):
            disk_list = ss_setup_info['configure']['sharedstorage']
            if len(disk_list) > 0:
                create_storage_ss(ss_setup_info, 30)

    # print json.dumps(ss_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    #
    # now we have created the raid1/raid5|raid0/vg ready to use.
    # if this is a ha node, stop it now, let ha start it
    #
    if configure['ha'] == True and pool_type == REGULAR_POOL:
        my_arg_list = [" ", " ", vgname_str]
        rc = cp_vg_stop(my_arg_list)

    #
    # inform amc the drop list
    #
    ibd_drop_list = vg_setup_info['ibd_drop_list']
    amcurl_str = LOCAL_AGENT
    inform_amc_drop_list(vgname_str, amcurl_str, ibd_drop_list, False)

    print "cp_init  to return ..."
    return 0


def cp_add(arg_list):
    debug("Enter cp_add ...")
    vgname_str = arg_list[2]
    gapratio_str = arg_list[3]

    #
    # generate a temp_conf file
    #
    tmp_fname = "/tmp/" + vgname_str + '.json'
    tmp_file = open(tmp_fname, 'w')
    tmp_file.write('{')
    tmp_file.write('\"vgname\"' + ':' + '\"' + vgname_str + '\"')
    tmp_file.write(',' + '\"gapratio\"' + ':' + gapratio_str)
    tmp_file.write(',' + '\"imports\"' + ':' + '[')
    got_ip = False
    first = True
    for i in range(4, len(arg_list)):
        print arg_list[i]
        if got_ip == False:
            ip = arg_list[i]
            got_ip = True
        else:
            uuid = arg_list[i]
            if first == False:
                tmp_file.write(',{')
            else:
                tmp_file.write('{')
                first = False
            tmp_file.write('\"ip\":' + '\"' + ip + '\"')
            tmp_file.write(',\"uuid\":' + '\"' + uuid + '\"' + '}')
            got_ip = False
    tmp_file.write(']')
    tmp_file.write('}')
    tmp_file.close()

    add_setup_info = {}
    rc = load_conf(tmp_fname, add_setup_info)
    if rc != 0:
        debug('Cannot load temporary json file.')
        return rc
    load_devices(add_setup_info, 30)
    debug('drop_list after load_devices:')
    logging_ibd_drop_list(add_setup_info)
    stop_ibd_drop_list(add_setup_info)
    init_devices(add_setup_info)
    configure = add_setup_info['configure']
    vgname_str = configure['vgname']
    pool_info = {}
    pool_info['configure'] = {}
    pool_configure = pool_info['configure']
    pool_configure['vgname'] = vgname_str

    tmp_setup_info = {}
    rc = load_conf(CP_CFG, tmp_setup_info)
    raid_type = tmp_setup_info['configure']['raidtype']
    pool_configure['raidtype'] = raid_type

    find_vg_info(pool_info)
    vg_fetch_infrastructure(pool_info)
    replace_missing(pool_info, add_setup_info)

    if len(add_setup_info['ibd_dev_list']) > 0:
        grouping_ibd_list(add_setup_info)
        logging_ibd_drop_list(add_setup_info)
        stop_ibd_drop_list(add_setup_info)
        init_devices(add_setup_info)
        create_md(add_setup_info)
        extend_vg(add_setup_info)
        update_infrastructure_by_extend(pool_info, add_setup_info)

    debug('drop_list after replace_missing:')
    logging_ibd_drop_list(add_setup_info)

    #
    # inform amc the drop list. Get amcurl from local jason
    #
    # local_setup_info = {}
    # rc = load_conf(CP_CFG, local_setup_info)
    # configure = local_setup_info['configure']
    # amcurl_str = configure['amcurl']
    ibd_drop_list = add_setup_info['ibd_drop_list']
    inform_amc_drop_list(vgname_str, '', ibd_drop_list, True)

    #
    # update infrastructure
    #
    save_infrastructure(pool_info)
    vg_setup_current_infrastructure(pool_info)

    return 0


def cp_readd(arg_list):
    debug("Enter cp_readd ...")
    vgname_str = arg_list[2]
    exportname = arg_list[3]
    adsname_str = ""
    if len(arg_list) >= 5:
        adsname_str = arg_list[4]
    vg_setup_info = {}

    # decide whether this is virtual pool or not
    if len(adsname_str) > 0:
        # for virtual pool
        debug("This ads node %s has virtual pool." % adsname_str)
        rc = load_ads_pool_infra_from_amc(vgname_str, adsname_str, vg_setup_info)
        if rc != 0:
            debug('Can not load config json for re_add.')
            return rc

        configure = vg_setup_info['configure']
        configure['imports'] = []
        all_ibd_imports = configure['imports']

        infrastructure = vg_setup_info['infrastructure']
        for the_raid in infrastructure[configure['pool_infrastructure_type']]:
            for the_raid1 in the_raid['children']:
                if the_raid1.has_key('children') == False:
                    continue
                for the_raid1_child in the_raid1['children']:
                    all_ibd_imports.append(the_raid1_child)
    else:
        # for regular pool
        amcurl = LOCAL_AGENT  # Use localhost agent instead of AMC server
        rc = load_pool_conf_from_amc(amcurl, vgname_str, vg_setup_info)
        if rc != 0:
            debug('Can not load config json for re_add.')
            return rc

    storagetype = vg_setup_info['storagetype']
    find_vg_info(vg_setup_info)
    return raid_assemble(vg_setup_info)


def cp_vg_stop(arg_list):
    debug("Enter cp_vg_stop...")
    vgname_str = arg_list[2]
    debug("cp_vg_stop trying to stop vg %s..." % vgname_str)

    vg_setup_info = {}
    vg_setup_info['configure'] = {}
    configure = vg_setup_info['configure']
    configure['vgname'] = vgname_str
    vg_setup_current_infrastructure(vg_setup_info)
    cmd_str = CMD_VGDEACTIVE + ' ' + vgname_str
    do_system(cmd_str)
    stop_devices(vg_setup_info)

    return 0


def cp_vg_destroy(arg_list):
    debug("Enter cp_vg_destroy...")
    vgname_str = arg_list[2]
    debug("cp_vg_destroy trying to destroy vg %s..." % vgname_str)

    vg_setup_info = {}
    vg_setup_info['configure'] = {}
    configure = vg_setup_info['configure']
    configure['vgname'] = vgname_str

    (ret, vgused) = is_vg_used("127.0.0.1", vgname_str)
    if ret != 0:
        return ret

    vg_setup_current_infrastructure(vg_setup_info)

    cmd_str = CMD_LVDEACTIVE + ' /dev/' + vgname_str + '/' + INTERNAL_LV_NAME
    rc = do_system(cmd_str)
    # TODO: need to check the error code to bypass the internal lv has been removed case

    cmd_str = CMD_LVREMOVE + ' /dev/' + vgname_str + '/' + INTERNAL_LV_NAME
    rc = do_system(cmd_str)
    # TODO: need to check the error code to bypass the internal lv has been removed case
    # if ret != 0:
    # return ret

    cmd_str = CMD_VGDEACTIVE + ' ' + vgname_str
    do_system(cmd_str)
    cmd_str = CMD_VGREMOVE + ' ' + vgname_str
    do_system_timeout(cmd_str, 10)
    stop_devices(vg_setup_info)

    return 0


#
# Remove one ibd from the raid1 it belongs to.
#
def cp_remove(arg_list):
    debug("Enter cp_remove ...")
    vgname_str = arg_list[2]
    ibd_to_remove = arg_list[3]
    pool_info = {}
    pool_info['configure'] = {}
    configure = pool_info['configure']
    configure['vgname'] = vgname_str
    find_vg_info(pool_info)
    first_level_raid_list = pool_info['first_level_raid_list']
    for the_ibd in pool_info['ibd_dev_list']:
        the_export = the_ibd['export']
        if the_export['exportname'] != ibd_to_remove:
            continue
        the_raid = the_ibd['md']
        if (len(the_raid['ibd_list'])) <= 1:
            debug("cannot remove %s", ibd_to_remove)
            break
        cmd_str = CMD_MDADM + ' --manage ' + the_raid['devname'] + ' --fail ' + the_ibd['devname']
        do_system_timeout(cmd_str, 10)
        cmd_str = CMD_MDADM + ' --manage ' + the_raid['devname'] + ' --remove ' + the_ibd['devname']
        do_system_timeout(cmd_str, 10)
        break
    return


#
# Call AMC/VCenter to detach & attach the shared disk to HA node.
# Sample:
# curl -X POST -H "Content-Type: application/json" -d "{\"poolvgname\":\"test1_su-cap-pool-67-38\",\"haprivateip\":\"10.15.109.69\"}" http://10.15.109.50:8080/amc/iliods/movedisk/vmware
def attach_shared_storage(ha_setup_info, pool_setup_info):
    haconfigure = ha_setup_info['configure']
    vgconfigure = pool_setup_info['configure']

    vgname_str = vgconfigure['vgname']
    if haconfigure.has_key('haconfig'):
        try:
            # We need the HA node's private ip.
            # Only look at ring[0] now.
            ring = haconfigure['haconfig']['ring'][0]
            haprivateip = ring['privateip']
            debug("haprivateip: ", haprivateip)
            iliouuid = haconfigure['uuid']
        except:
            return 1
    else:
        return 0

    # amcurl = haconfigure["amcurl"]    # http://10.15.107.3:8080/amc
    # Use localhost agent instead of AMC server.
    amcurl = 'http://127.0.0.1:8080/amc'
    amcurl = amcurl + "/iliods/movedisk/vmware"
    amcdata = r' -d "{\"poolvgname\":\"%s\", \"hailiouuid\":\"%s\" }" ' % (vgname_str, iliouuid)
    cmd_str = r'curl -k -X POST -H "Content-Type: application/json" ' + amcdata + amcurl
    rc = do_system(cmd_str)
    # TODO: Error handling
    return rc


#
# Select all ibd devices as the arbitrator device for this pool resource
# and register it to the arbitrator.
# FIXME: Should use a private partition for arbitrator device.
#
def pick_start_arb_device(setup_info):
    configure = setup_info['configure']
    vgname = setup_info['configure']['vgname']
    ibd_dev_list = setup_info['ibd_dev_list']

    local_setup_info = {}
    rc = load_conf(CP_CFG, local_setup_info)
    if rc != 0:
        debug('Can not load local config json for HA check.')
        return rc
    local_configure = local_setup_info['configure']

    if local_configure['usx']['ha'] != True:
        debug('None HA mode, skip arb setup.')
        return 0
    if len(ibd_dev_list) == 0:
        debug('No active IBD imports, currently pure shared storage pool does not need arbitrator, skip.')
        return 0

    # ibd_list = copy.deepcopy(ibd_dev_list)
    # print "pre sort:", ibd_list
    # ibd_list.sort(key=operator.itemgetter("uuid"))
    # print "after sort:", ibd_list
    arb_dev_list = []
    for ibd_dev in ibd_dev_list:
        dev_name = ibd_dev['devname']
        if not dev_name.startswith('/dev/ibd'):
            # Any compond devices should do fencing inside it.
            # Only use the lowest level ibd device:
            debug('Skip non-ibd device %s for arb device.' % dev_name)
        arb_dev_list.append(dev_name)
    debug('arb_dev_list: ', arb_dev_list)
    rc = arb_start(vgname, arb_dev_list)
    if rc != True:
        debug('Pool Fencing failed!')
        return 1
    return 0


'''
def _send_alert_cp_load(vgname):
    cmd = 'date +%s'
    (ret, epoch_time) = runcmd(cmd, print_ret=True)
    checkid = ""
    ilio_type = "POOL"
    ilio_uuid = socket.gethostname()
    target = ""
    status = "OK"
    service = "HA"
    value = "HA_failover_start_time"
    cfgfile = open("/etc/ilio/atlas.json", 'r')
    s = cfgfile.read()
    cfgfile.close()
    node_dict = json.loads(s)
    usx = node_dict.get('usx')
    usx_displayname = usx.get('displayname')

    when = '"when":' + epoch_time.rstrip('\n')
    what = '"what":"' + vgname + ' ' + value + '"'
    tags = '"tags":"'
    tags += 'CHECKID:' + checkid
    tags += ',TARGET:' + target
    tags += ',ILIO_TYPE:' + ilio_type
    tags += ',ILIO_UUID:' + ilio_uuid
    tags += ',displayname:' + usx_displayname
    tags += ',STATUS:' + status
    tags += ',SERVICE:' + service
    tags += ',VALUE:' + value
    tags += '"'
    data = '"data":"' + vgname + '"'
    s = when + ',' + what + ',' + tags + ',' + data
    cmd = 'curl -X POST -H "Content-type:application/json" ' + LOCAL_AGENT + '/alerts/' + ' -d \'{' + s + '}\''
    (ret, out) = runcmd(cmd, print_ret=True)
'''


#
# only called from ha
#
def cp_ha_start(arg_list):
    debug("Enter cp_ha_start ...")
    jobid_file_exists = does_jobid_file_exist()
    does_jobid_file_need_deletion = not jobid_file_exists
    send_status("HA", 1, 0, "Pool HA", "Starting Pool HA takeover", False)

    # TODO: a workaround for TISILIO-3933: a timing issue for rw upgrade failure.
    time.sleep(20)

    local_setup_info = {}
    rc = load_conf(CP_CFG, local_setup_info)
    if rc != 0:
        debug('Can not load config json for HA.')
        return rc
    send_status("HA", 20, 0, "Pool HA", "Got HA status, getting aggregator info", False)

    #
    # retrieve all aggregator info from local agent, a Json file
    #
    vgname_str = arg_list[2]
    amcurl = LOCAL_AGENT  # Use localhost agent instead of AMC server
    vg_setup_info = {}
    send_status("HA", 50, 0, "Pool HA", "Loading pool config...", False)
    rc = load_pool_conf_from_amc(amcurl, vgname_str, vg_setup_info)
    if rc != 0:
        debug('Can not load config json for HA start from AMC.')
        return rc

    vgconfigure = vg_setup_info["configure"]
    send_alert_cp_load(vgconfigure['vgname'])
    if vgconfigure.has_key("sharedstorage"):
        send_status("HA", 75, 0, "Pool HA", "Attaching shared storage...", False)
        attach_shared_storage(local_setup_info, vg_setup_info)

    send_status("HA", 85, 0, "Pool HA", "Loading devices...", False)
    send_status("HA", 95, 0, "Pool HA", "Bringing up pool...", False)
    cp_up(vg_setup_info)
    send_status("HA", 100, 0, "Pool HA", "Finished Pool HA takeover", does_jobid_file_need_deletion)
    return 0


#
# only called from bootstrap.
#
def cp_start(arg_list):
    debug("Enter cp_start ...")
    local_setup_info = {}
    rc = load_conf(CP_CFG, local_setup_info)
    if rc != 0:
        debug('Can not load config json for start from local.')
        return rc
    configure = local_setup_info['configure']

    # remvoe ibd agent configuration file
    try:
        os.remove(IBD_AGENT_CONFIG_FILE)
    except OSError:
        pass

    #
    # if i'm a ha node, do nothing here, let ha start it
    #
    if configure['ha'] == True:
        debug("cp_start: this a ha node, let ha start the pool")
        return 0

    for the_role in configure['roles']:
        if the_role == 'MEMORY_POOL':
            debug("cp_start: this is a memory pool, do nothing.")
            return 0

    return 0


#
# ADS calls this to get the desired storage size and create raid0 from virtual pool
#
def ads_vp_init(arg_list):
    debug("Enter ads_vp_init ...")

    vgname_str = arg_list[2]
    adsname_str = arg_list[3]
    lvname_str = arg_list[4]
    # the given required size unit is GB
    requiredsize_GB = int(arg_list[5])
    requiredsize = requiredsize_GB * GB_SIZE
    pool_setup_info = {}

    # aquire lock
    rc = ads_vp_lock(vgname_str)
    if rc != 0:
        debug('Can not aquire lock for ads_vp_init.')
        return rc

    # load pool conf and infrastructure
    rc = load_ads_pool_conf_from_amc(vgname_str, adsname_str, pool_setup_info)
    if rc != 0:
        debug('Can not load config json for ads_vp_init.')
        return rc
    debug('pool_setup_info: ' + json.dumps(pool_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    # derive the storage size
    total_size = 0
    total_free = 0
    ibd_size = 0
    ibd_free = 0
    ss_size = 0
    ss_free = 0
    raid5_size = 0
    raid_type = pool_setup_info['configure']['raidtype']
    infrastructure_type = pool_setup_info['configure']['pool_infrastructure_type']
    sub_infrastructure = pool_setup_info['infrastructure'][infrastructure_type]

    shared_storage_list = []
    shared_storage_list_GB = []
    if pool_setup_info.has_key('sharedstorages'):
        sharedstorages = pool_setup_info['sharedstorages']
        for the_ss in sharedstorages:
            if the_ss['requestedsize'] > 0:
                shared_storage_list.append(the_ss['requestedsize'] * GB_SIZE)

    debug("shared_storage_list: %s" % str(shared_storage_list))
    (total_size, total_free, ibd_size, ibd_free, ss_size, ss_free, raid5_size) = derive_storage_size(sub_infrastructure,
                                                                                                     shared_storage_list,
                                                                                                     raid_type,
                                                                                                     requiredsize)

    debug("total_size = %d, total_free = %d, ibd_size = %d, ibd_free = %d, ss_size = %d, ss_free = %d, raid5_size = %d"
          % (total_size, total_free, ibd_size, ibd_free, ss_size, ss_free, raid5_size))

    if requiredsize <= 0:
        debug("Failed to allocate ADS the required storage size (invalide required_size %d) for %s " % (
            requiredsize, raid_type))
        return 1

    if total_free < requiredsize:
        debug("Failed to allocate ADS the required storage size (available_size %d vs required_size %d) for %s " % (
            total_free, requiredsize, raid_type))
        return 1

    # allocate the storage among IBD and shared storage
    pool_setup_info['shared_storage_flag'] = STOR_SS_NONE
    ss_requiredsize = 0
    ibd_requiredsize = 0
    remainingsize = 0

    if pool_setup_info.has_key('sharedstoragefirst') == False:
        pool_setup_info['sharedstoragefirst'] = False

    if pool_setup_info['sharedstoragefirst'] == True:
        if ss_free >= requiredsize:
            pool_setup_info['shared_storage_flag'] = STOR_SS_ONLY
            create_storage_ss(pool_setup_info, requiredsize, 'RAID_0', 0)
            remainingsize = 0
        else:
            if ss_size > 0:
                pool_setup_info['shared_storage_flag'] = STOR_SS_MIX

                if raid_type == 'RAID_0':
                    create_storage_ss(pool_setup_info, ss_size, 'RAID_0', 0)
                    remainingsize = requiredsize - ss_size
                else:
                    num_ss = 0
                    for the_size in shared_storage_list:
                        if the_size > raid5_size:
                            num_ss += 1
                    create_storage_ss(pool_setup_info, num_ss * raid5_size, 'RAID_5', raid5_size)
                    remainingsize = requiredsize - (num_ss - 1) * raid5_size
            else:
                pool_setup_info['shared_storage_flag'] = STOR_SS_NONE
                remainingsize = requiredsize
    else:
        if ibd_free >= requiredsize:
            pool_setup_info['shared_storage_flag'] = STOR_SS_NONE
            remainingsize = requiredsize
        else:
            if ibd_free > 0:
                pool_setup_info['shared_storage_flag'] = STOR_SS_MIX

                if raid_type == 'RAID_0':
                    create_storage_ss(pool_setup_info, requiredsize - ibd_free, 'RAID_0', 0)
                    remainingsize = ibd_free
                else:
                    num_ss = 0
                    for the_size in shared_storage_list:
                        if the_size > raid5_size:
                            num_ss += 1
                            if (num_ss - 1) * raid5_size >= (requiredsize - ibd_free):
                                break

                    create_storage_ss(pool_setup_info, num_ss * raid5_size, 'RAID_5', raid5_size)
                    remainingsize = requiredsize - (num_ss - 1) * raid5_size
            else:
                pool_setup_info['shared_storage_flag'] = STOR_SS_ONLY
                create_storage_ss(pool_setup_info, requiredsize, 'RAID_0', 0)
                remainingsize = 0

    if remainingsize > 0:
        vp_create_storage(pool_setup_info, 30, remainingsize)

    # release lock
    ads_vp_unlock(vgname_str)

    if pool_setup_info['shared_storage_flag'] == STOR_SS_ONLY:
        pool_setup_info['configure']['raidtype'] = 'RAID_0'
        pool_setup_info['first_level_raid_list'] = []
        pool_setup_info['second_level_raid_list'] = []
        create_second_level_raid_v2(pool_setup_info, 0)
        # generate ads pool infrastructure
        generate_infrastructure(pool_setup_info, REGULAR_POOL)
        save_ads_infrastructure(pool_setup_info)
        return 0

    return 0


#
# ADS calls this to start the virtual pool
#
def ads_vp_start(arg_list):
    debug("Enter ads_vp_start ...")

    vgname_str = arg_list[2]
    adsname_str = arg_list[3]

    # Right now one ADS contains only one virtual pool
    vg_setup_info = {}
    rc = load_ads_pool_infra_from_amc(vgname_str, adsname_str, vg_setup_info)
    if rc != 0:
        debug('Can not load config json for ads_vp_start.')
        return rc

    configure = vg_setup_info['configure']
    configure['imports'] = []
    all_ibd_imports = configure['imports']

    infrastructure = vg_setup_info['infrastructure']
    for the_raid in infrastructure[configure['pool_infrastructure_type']]:
        for the_raid1 in the_raid['children']:
            if the_raid1.has_key('children') == False:
                continue
            for the_ibd in the_raid1['children']:
                all_ibd_imports.append(the_ibd)

    # cp_up(vg_setup_info)
    load_devices(vg_setup_info, 30)
    find_vg_info(vg_setup_info)
    rc = raid_assemble(vg_setup_info)
    if rc != 0:
        stop_devices(vg_setup_info)
        load_devices(vg_setup_info, 100000000)  # guarantee all aggregators connected

        #
        # Try to keep orignal pairing and insert "missing" in the ibd_dev_list if necessary.
        #
        ibd_dev_list = vg_setup_info['ibd_dev_list']
        infrastructure = vg_setup_info['infrastructure']
        new_ibd_dev_list = []
        for the_raid in infrastructure[configure['pool_infrastructure_type']]:
            for the_raid1 in the_raid['children']:
                inserted = 0;
                for ibd_uuid in the_raid1['children']:
                    for the_ibd in ibd_dev_list:
                        if the_ibd['import']['uuid'] == ibd_uuid['uuid']:
                            debug('Adding %s' % str(the_ibd))
                            new_ibd_dev_list.append(the_ibd)
                            ibd_dev_list.remove(the_ibd)
                            inserted += 1
                if inserted == 1:
                    if len(the_raid1['children']) == 1:
                        debug('Insert missing dev.')
                    else:
                        debug('Replace disconnnected ibd with missing.')
                    new_ibd_dev_list.append({'devname': 'missing', 'idx': '-1', 'size': 0})
                    inserted += 1
                elif inserted == 0:
                    debug('No active ibds for raid1: %s. Has to drop it.' % str(the_raid1))
        vg_setup_info['ibd_dev_list'] = new_ibd_dev_list

        #
        # create storage
        #
        vp_create_storage_core(vg_setup_info)
    return 0


#
# ADS calls this to stop the virtual pool
#
def ads_vp_stop(arg_list):
    debug("Enter ads_vp_stop ...")

    vgname_str = arg_list[2]
    adsname_str = arg_list[3]

    # Right now one ads contains only one virtual pool
    vg_setup_info = {}
    rc = load_ads_pool_infra_from_amc(vgname_str, adsname_str, vg_setup_info)
    if rc != 0:
        debug('Can not load config json for ads_vp_stop.')
        return rc
    # print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    ads_vp_setup_current_infrastructure(vg_setup_info)
    # print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    stop_devices(vg_setup_info)

    return 0


#
# ADS calls this to free the virtual pool resources used by itself
#
def ads_vp_destroy(arg_list):
    debug("Enter ads_vp_destroy ...")

    vgname_str = arg_list[2]
    adsname_str = arg_list[3]

    # Right now one ads contains only one virtual pool
    vg_setup_info = {}
    rc = load_ads_pool_infra_from_amc(vgname_str, adsname_str, vg_setup_info)
    if rc != 0:
        debug('Can not load config json for ads_vp_destroy.')
        return rc
    # print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    ads_vp_setup_current_infrastructure(vg_setup_info)
    print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    destroy_devices(vg_setup_info)
    destroy_remote_ibd(vg_setup_info)
    update_ads_pool_info(vgname_str, adsname_str, vg_setup_info['configure']['pool_infrastructure_type'])

    return 0


#
# virtual pool garbage collection for a given ads
#
def ads_vp_clean(arg_list):
    debug("Enter ads_vp_clean ...")

    vgname_str = arg_list[2]
    adsname_str = arg_list[3]

    yes = set(['YES', 'yes'])
    no = set(['NO', 'no', 'n', 'N'])
    sys.stdout.write('Do you really want to do virtual pool garbage collection for  %s [YES/NO]:\n' % adsname_str)
    choice = raw_input().lower()
    if choice in yes:
        sys.stdout.write('Go ahead to do virtual pool garbage collection for  %s\n' % adsname_str)
    elif choice in no:
        return 1
    else:
        sys.stdout.write("Please answer 'YES' or 'NO'")
        return 1

    # Right now one ads contains only one virtual pool
    vg_setup_info = {}
    rc = load_ads_pool_conf_from_amc(vgname_str, adsname_str, vg_setup_info)
    if rc != 0:
        debug('Can not load config json for ads_vp_clean.')
        return rc
    # print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    pool_infrastructure_type = vg_setup_info['configure']['pool_infrastructure_type']
    pair_list = vg_setup_info['infrastructure'][pool_infrastructure_type]
    for the_pair_group in pair_list:
        for the_pair in the_pair_group:
            if the_pair[0]["devname"] != "missing":
                # destroy the remote ibd client
                # e.g., python /opt/milio/atlas/roles/aggregate/agexport.pyc -d export_name
                the_exportname = the_pair[0]["uuid"] + "_" + adsname_str
                args_str = CMD_AGGDESTROY + the_exportname
                (rc, out, err) = remote_exec(the_pair[0]['ip'], 'python ', args_str)
                if rc != 0:
                    debug('destroy the potential export %s from %s failed, errcode=%d, errmsg=%s' % (
                        the_exportname, the_pair[0]['ip'], rc, err))

            if the_pair[1]["devname"] != "missing":
                the_exportname = the_pair[1]["uuid"] + "_" + adsname_str
                args_str = CMD_AGGDESTROY + the_exportname
                (rc, out, err) = remote_exec(the_pair[1]['ip'], 'python ', args_str)
                if rc != 0:
                    debug('destroy the potential export %s from %s failed, errcode=%d, errmsg=%s' % (
                        the_exportname, the_pair[1]['ip'], rc, err))

    # upate the virtual pool metadata
    update_ads_pool_info(vgname_str, adsname_str, vg_setup_info['configure']['pool_infrastructure_type'])
    return 0


#
# ADS calls this to show the metadata of ads and its related virtual pool
#
def ads_vp_show(arg_list):
    debug("Enter ads_vp_show ...")

    vgname_str = arg_list[2]
    adsname_str = arg_list[3]

    amcurl = LOCAL_AGENT  # Use localhost agent instead of AMC server

    # dump virtual pool configuration
    vg_setup_info = {}
    rc = load_pool_conf_from_amc(amcurl, vgname_str, vg_setup_info)
    if rc != 0:
        debug("Can not load config json for %s." % vgname_str)
        return rc
    print vgname_str + " configuration:"
    print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # dump virtual pool detail
    vg_setup_info = {}
    vg_setup_info['configure'] = {}
    configure = vg_setup_info['configure']
    configure['vgname'] = vgname_str
    vg_fetch_infrastructure(vg_setup_info)
    print vgname_str + " detail:"
    print json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # dump ads detail
    ads_setup_info = {}
    ads_setup_info['configure'] = {}
    configure = ads_setup_info['configure']
    configure['adsname'] = adsname_str
    fetch_ads_infrastructure(ads_setup_info)
    print adsname_str + " detail:"
    print json.dumps(ads_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    return 0


#
# Deletion calls this to remove a given aggr node from vp infrastructure
#
def remove_aggr_from_vp(arg_list):
    debug("Enter remove_aggr_from_vp ...")

    vgname_str = arg_list[2]
    aggr_uuid = arg_list[3]

    vg_setup_info = {}
    vg_setup_info['configure'] = {}
    configure = vg_setup_info['configure']
    configure['vgname'] = vgname_str
    vg_fetch_infrastructure(vg_setup_info)
    debug(vgname_str + json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))

    infra_type = ""
    if vg_setup_info['infrastructure'].has_key(DISK_INFRASTRUCTURE):
        infra_type = DISK_INFRASTRUCTURE
    elif vg_setup_info['infrastructure'].has_key(MEMORY_INFRASTRUCTURE):
        infra_type = MEMORY_INFRASTRUCTURE
    else:
        debug("ERROR: not supported vp infrastructure type...")
        return 1

    pair_list = vg_setup_info['infrastructure'][infra_type]

    # remove the given aggr node from vp infrastructure
    found_flag = False
    for the_pair_group in pair_list:
        for the_pair in the_pair_group:
            if the_pair[0]['uuid'] == aggr_uuid:
                the_pair[0] = {"ip": "NULL", "devname": 'missing', "uuid": "NULL", "size": 0, "free": 0}
                found_flag = True

            if the_pair[1]['uuid'] == aggr_uuid:
                the_pair[1] = {"ip": "NULL", "devname": 'missing', "uuid": "NULL", "size": 0, "free": 0}
                found_flag = True

            if found_flag == True and the_pair[0]['uuid'] == "NULL" and the_pair[1]['uuid'] == "NULL":
                del the_pair

    if found_flag == True:
        debug("removed %s from vp %s" % (aggr_uuid, vgname_str))
        debug(vgname_str + json.dumps(vg_setup_info, sort_keys=True, indent=4, separators=(',', ': ')))
        save_infrastructure(vg_setup_info)

    return 0


def vp_get_size(argv):
    debug("Enter vp_get_size ...")
    pool_setup_info = {}

    pool_cfg = '/tmp/' + argv[2] + '.json'
    rc = load_conf(pool_cfg, pool_setup_info)
    # rc = load_conf(CP_CFG, pool_setup_info)
    if rc != 0:
        debug("cannot load configure file: ", CP_CFG, '  exiting ...')
        return rc
    configure = pool_setup_info['configure']

    infrastructure_type = ""
    # check pool type
    for the_role in configure['roles']:
        if the_role == 'CAPACITY_POOL':
            infrastructure_type = DISK_INFRASTRUCTURE
        elif the_role == 'MEMORY_POOL':
            infrastructure_type = MEMORY_INFRASTRUCTURE
        else:
            debug('not memory pool or capacity pool, exiting ...')
            return 1
            # TODO: shared storage pool

    vgname_str = configure['vgname']

    if configure['raidtype'] == 'RAID_5':
        raid_type = POOL_TYPE_RAID5
    elif configure['raidtype'] == 'RAID_0':
        raid_type = POOL_TYPE_RAID0
    else:
        debug("invalid raidtype %s " % (configure['raidtype']))
        return 1

    # print json.dumps(pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # fetch virtual pool infrastructure
    vg_fetch_infrastructure(pool_setup_info)

    total_size = 0
    total_free_size = 0

    if pool_setup_info['infrastructure'].has_key(infrastructure_type) == False:
        debug("do not have the desired infrastructure type: %s " % (infrastructure_type))
        return 1

    pair_list = pool_setup_info['infrastructure'][infrastructure_type]
    if raid_type == POOL_TYPE_RAID0:
        # calculate total_size and total_free_size for RAID_0
        for the_pair_group in pair_list:
            for the_pair in the_pair_group:
                if min(the_pair[0]['size'], the_pair[1]['size']) == 0:
                    total_size = total_size + max(the_pair[0]['size'], the_pair[1]['size'])
                    total_free_size = total_free_size + max(the_pair[0]['free'], the_pair[1]['free'])
                else:
                    total_size = total_size + min(the_pair[0]['size'], the_pair[1]['size'])
                    total_free_size = total_free_size + min(the_pair[0]['free'], the_pair[1]['free'])
    else:
        # calculate total_size and total_free_size for RAID_5
        size_list = []
        max_list = []
        for the_pair_group in pair_list:
            for the_pair in the_pair_group:
                if min(the_pair[0]['size'], the_pair[1]['size']) == 0:
                    total_size = total_size + max(the_pair[0]['size'], the_pair[1]['size'])
                    pair_size = max(the_pair[0]['free'], the_pair[1]['free'])
                else:
                    total_size = total_size + min(the_pair[0]['size'], the_pair[1]['size'])
                    pair_size = min(the_pair[0]['free'], the_pair[1]['free'])

                if pair_size > 0:
                    size_list.append(pair_size)
                    # print size_list

        pair_num = len(size_list)
        if pair_num <= 2:
            total_free_size = 0
        else:
            size_list.sort(reverse=True)
            # print size_list
            for i in range(len(size_list) - 2):
                max_raid5 = size_list[i + 2] * (i + 2)
                max_list.append(max_raid5)
            # print max_list

            if len(max_list) == 0:
                total_free_size = 0
            else:
                total_free_size = max(max_list)

    ss_size = 0
    ss_free = 0
    if pool_setup_info['infrastructure'].has_key(SS_INFRASTRUCTURE):
        for the_shared_storage in pool_setup_info['infrastructure'][SS_INFRASTRUCTURE]:
            ss_size = ss_size + the_shared_storage["size"]
            ss_free = ss_free + the_shared_storage["free"]

    if pool_setup_info['infrastructure'].has_key(SS_INFRASTRUCTURE):
        print "ibd_size:" + " " + str(total_size) + " " + str(total_free_size)
        print "ss_size: " + " " + str(ss_size) + " " + str(ss_free)

    print "vp_get_size" + " " + str(total_size + ss_size) + " " + str(total_free_size + ss_free)

    return 0


def derive_storage_size(infrastructure, shared_storage_list, raid_type, requiredsize):
    total_size = 0
    total_free = 0
    ibd_size = 0
    ibd_free = 0
    ss_size = 0
    ss_free = 0
    raid5_size = 0

    pair_list = infrastructure
    if raid_type == POOL_TYPE_RAID0:
        # calculate ibd_size and ibd_free for RAID_0
        for the_pair_group in pair_list:
            for the_pair in the_pair_group:
                if min(the_pair[0]['size'], the_pair[1]['size']) == 0:
                    ibd_size = ibd_size + max(the_pair[0]['size'], the_pair[1]['size'])
                    ibd_free = ibd_free + max(the_pair[0]['free'], the_pair[1]['free'])
                else:
                    ibd_size = ibd_size + min(the_pair[0]['size'], the_pair[1]['size'])
                    ibd_free = ibd_free + min(the_pair[0]['free'], the_pair[1]['free'])
    else:
        # calculate ibd_size and ibd_free for RAID_5
        size_list = []
        max_list = []
        for the_pair_group in pair_list:
            for the_pair in the_pair_group:
                if min(the_pair[0]['size'], the_pair[1]['size']) == 0:
                    ibd_size = ibd_size + max(the_pair[0]['size'], the_pair[1]['size'])
                    pair_size = max(the_pair[0]['free'], the_pair[1]['free'])
                else:
                    ibd_size = ibd_size + min(the_pair[0]['size'], the_pair[1]['size'])
                    pair_size = min(the_pair[0]['free'], the_pair[1]['free'])

                if pair_size > 0:
                    size_list.append(pair_size)
                    # print size_list

        pair_num = len(size_list)
        if pair_num <= 2:
            ibd_free = 0
        else:
            size_list.sort(reverse=True)
            # print size_list
            for i in range(len(size_list) - 2):
                max_raid5 = size_list[i + 2] * (i + 2)
                max_list.append(max_raid5)
            # print max_list

            if len(max_list) == 0:
                ibd_free = 0
            else:
                ibd_free = max(max_list)

    # calculate ss_free and ss_size for shared storage
    ss_free = 0
    ss_size = 0
    if len(shared_storage_list) > 0:
        ss_free = sum(shared_storage_list)
        ss_size = ss_free

    # calculate total_size and total_free
    total_size = ibd_size + ss_size
    if raid_type == POOL_TYPE_RAID0:
        # calculate total_free for RAID_0
        total_free = ibd_free + ss_free
    else:
        # calculate total_free for RAID_5
        if len(shared_storage_list) == 0:
            total_free = ibd_free
        else:
            size_list += shared_storage_list
            pair_num = len(size_list)
            if pair_num <= 2:
                total_free = 0
            else:
                size_list.sort(reverse=True)
                # print size_list
                for i in range(len(size_list) - 2):
                    max_raid5 = size_list[i + 2] * (i + 2)
                    max_list.append(max_raid5)
                # print max_list

                if len(max_list) == 0:
                    total_free = 0
                    raid5_size = 0
                else:
                    total_free = max(max_list)
                    for i in range(len(max_list)):
                        if max_list[i] >= requiredsize:
                            raid5_size = int(math.ceil(float(requiredsize) / (i + 1)))
                            if raid5_size * (i + 1) > max_list[i]:
                                raid5_size = int(math.floor(float(requiredsize) / (i + 1)))
                            break

    if len(shared_storage_list) > 0:
        total_free = max(ss_free, total_free)

    return (total_size, total_free, ibd_size, ibd_free, ss_size, ss_free, raid5_size)


def add_aggregates_sharedstorage(data):
    management_id = data['ilioManagementid']
    vgname = data['vguuid']
    ibdlist = data['ibdlist']
    scsibuslist = data['scsibuslist']
    gapratio = str(data['gapratio'])
    ibds = [k for k, v in ibdlist.items()]
    ipaddrs = [v for k, v in ibdlist.items()]
    scsi_hotscan()
    cmd = 'lsscsi'
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret != 0:
        debug('Could not find SCSI disks')
        return ret
    vg_disk_list = []
    for line in msg:
        tmp = line.split()
        host_channel_target_lun = tmp[0].replace('[', ' ').replace(']', ' ').split()[0].split(':')
        host_target = host_channel_target_lun[0] + ':' + host_channel_target_lun[2]
        if host_target in scsibuslist and tmp[-1].startswith("/dev/sd"):
            vg_disk_list.append(tmp[-1])

    extend_volume_group(vg_disk_list, vgname)

    arg_list = []
    arg_list.append("")
    arg_list.append("")
    arg_list.append(vgname)
    arg_list.append(gapratio)
    for i in range(len(ibds)):
        arg_list.append(ipaddrs[i])
        arg_list.append(ibds[i])
    return cp_add(arg_list)


def add_aggregates_virtualpool(data):
    debug("Enter add_aggregates_virtualpool...")
    management_id = data['ilioManagementid']
    vgname = data['vguuid']
    ibdlist = data['ibdlist']
    scsibuslist = data['scsibuslist']
    gapratio = str(data['gapratio'])
    ibds = [k for k, v in ibdlist.items()]
    ipaddrs = [v for k, v in ibdlist.items()]

    # print json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))

    pool_setup_info = {}

    # aquire lock
    rc = ads_vp_lock(vgname)
    if rc != 0:
        debug('Can not aquire lock for add_aggregates_virtualpool.')
        return rc

    # load pool conf and infrastructure
    rc = load_ads_pool_conf_from_amc(vgname, "missing", pool_setup_info)
    if rc != 0:
        debug('Can not load config json for add_aggregates_virtualpool.')
        return rc
    # print json.dumps(pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    pool_setup_info['ibd_dev_list'] = []
    pool_setup_info['ibd_drop_list'] = []
    config = pool_setup_info['configure']
    config['imports'] = []
    all_ibd_imports = config['imports']
    # infrastructure should be set already
    pair_list = pool_setup_info['infrastructure'][pool_setup_info['storagetype']]
    new_pair_group = []

    for the_pair_group in pair_list:
        for the_pair in the_pair_group:
            # skip already used pair
            if (the_pair[0]['free'] < the_pair[0]['size']) or (the_pair[1]['free'] < the_pair[1]['size']):
                new_pair_group.append(the_pair)
                continue

            if (the_pair[0]['devname'] != "missing") and (the_pair[0]['free'] == the_pair[0]['size']) and (
                        the_pair[0]['free'] > 0):
                all_ibd_imports.append(the_pair[0])

            if (the_pair[1]['devname'] != "missing") and (the_pair[1]['free'] == the_pair[1]['size']) and (
                        the_pair[1]['free'] > 0):
                all_ibd_imports.append(the_pair[1])

    for i in range(len(ibds)):
        ibd_dev = {"ip": ipaddrs[i], "devname": 'unknown', "uuid": ibds[i], "size": 0, "free": 0}
        all_ibd_imports.append(ibd_dev)
    # add pool roles for generate infrastructure
    pool_setup_info['configure']['roles'] = data['configure']['roles']

    get_devices_info(pool_setup_info)
    grouping_ibd_list(pool_setup_info)
    logging_ibd_drop_list(pool_setup_info)
    generate_infrastructure(pool_setup_info, VIRTUAL_POOL)
    # print json.dumps(pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # generate latest infrastructure
    pool_setup_info['infrastructure'][pool_setup_info['storagetype']].append(new_pair_group)

    # print json.dumps(pool_setup_info, sort_keys=True, indent=4, separators=(',', ': '))

    # udpate virtual pool infrastructure
    save_infrastructure(pool_setup_info)
    # release the lock
    ads_vp_unlock(vgname)

    return 0


def cp_extend(argv):
    debug("Enter cp_extend...")
    try:
        data = json.loads(argv[2])
        if data['pooltype'] == "MEMORY_POOL" or data['pooltype'] == "CAPACITY_POOL":
            return (add_aggregates_virtualpool(data))
        else:
            # TODO: shared storage pool
            return (add_aggregates_sharedstorage(data))
    except ValueError, e:
        debug('JSON parse exception : ' + str(e))
        return 1


def get_infrastructure_from_vgname(vgname):
    fname = '/etc/ilio/c_pool_infrastructure_' + vgname + '.json'
    try:
        file = open(fname, 'r')
        c_infrastructure_str = file.read()
        c_infrastructure = json.loads(c_infrastructure_str)
        file.close()
    except IOError as e:
        debug("CAUTION: Cannot load configure file, %s: [%d]%s" % (fname, e.errno, os.strerror(e.errno)))
        return None
    except:
        debug("CAUTION: Cannot load configure file, %s" % (fname))
        return None

    return c_infrastructure


def raid_device_io_error_lock(parent_devname, my_uuid):
    debug("raid_device_io_error_lock: %s, %s" % (parent_devname, my_uuid))

    lockfile = RAID_IO_ERROR_LOCK + '-' + parent_devname.split('/dev/')[1]
    if os.path.exists(lockfile):
        f = open(lockfile, 'r')
        # fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        saved_uuid = f.read()
        # fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()
        if saved_uuid != my_uuid:  # it is not my lockfile
            debug("%s can not set_io_error, locked for: %s!" % (my_uuid, saved_uuid))
            return 1
    else:
        f = open(lockfile, 'w')
        # fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(my_uuid)
        # fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()
    return 0


def raid_device_io_error_unlock(parent_devname, my_uuid):
    debug("raid_device_io_error_unlock: %s, %s" % (parent_devname, my_uuid))

    lockfile = RAID_IO_ERROR_LOCK + '-' + parent_devname.split('/dev/')[1]
    if os.path.exists(lockfile):
        f = open(lockfile, 'r')
        # fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        saved_uuid = f.read()
        # fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()
        if saved_uuid == my_uuid:  # it is my lockfile
            os.unlink(lockfile)
            return 0
    debug("raid_device_io_error_unlock: NO lockfile: %s" % lockfile)
    return 1


def raid1_device_get_parent(c_infrastructure, my_uuid):
    parent_devname = None
    got_it = False
    for infra_type in [DISK_INFRASTRUCTURE, MEMORY_INFRASTRUCTURE]:
        if (not c_infrastructure.has_key(infra_type)) or len(c_infrastructure[infra_type]) == 0:
            continue
        for the_raid in c_infrastructure[infra_type]:
            for the_raid1 in the_raid['children']:
                if the_raid1.has_key('children') == False:
                    continue
                for the_ibd in the_raid1['children']:
                    if the_ibd['uuid'] == my_uuid:
                        parent_devname = the_raid1['devname']
                        got_it = True
                        break
                if got_it == True:
                    break
            if got_it == True:
                break
        if got_it == True:
            break

    return parent_devname


def raid1_device_set_io_error(argv):
    vgname = argv[2]
    my_uuid = argv[3]
    my_dev = argv[4]
    debug("Enter raid1_device_set_io_error(%s) ..." % my_uuid)

    c_infrastructure = get_infrastructure_from_vgname(vgname)
    if c_infrastructure == None:
        return 1

    parent_devname = raid1_device_get_parent(c_infrastructure, my_uuid)
    if parent_devname == None:
        return 1

    rc = raid_device_io_error_lock(parent_devname, my_uuid)
    if rc != 0:
        return 1

    detail = {}
    raid_detail_nohung(parent_devname, detail)

    active = 0
    if detail.has_key(ACTIVE_DRIVES) == True:
        active = detail[ACTIVE_DRIVES]
    elif detail.has_key(ACTIVE_DEVICES) == True:
        active = detail[ACTIVE_DEVICES]
    debug("Active drv : %d" % active)
    if active < 1:
        raid_device_io_error_unlock(parent_devname, my_uuid)
        return 1
    if active == 1:
        if detail.has_key(my_dev):
            debug("%s : %s" % (my_dev, detail[my_dev]))
            if detail[my_dev] == "active":
                raid_device_io_error_unlock(parent_devname, my_uuid)
                return 1

    cmd = CMD_IBDMANAGER_IOERROR + ' ' + my_dev
    rc = do_system(cmd)
    return rc


def raid5_device_get_parent(c_infrastructure, my_uuid):
    got_it = False
    l2_parent_devname = None
    l1_parent_devname = None
    for infra_type in [DISK_INFRASTRUCTURE, MEMORY_INFRASTRUCTURE]:
        if (not c_infrastructure.has_key(infra_type)) or len(c_infrastructure[infra_type]) == 0:
            continue
        for l2_raid in c_infrastructure[infra_type]:
            if l2_raid[DEV_raidtype] != RAID_raid5:
                continue
            for l1_raid in l2_raid['children']:
                if l1_raid.has_key('children') == False:
                    continue
                for the_ibd in l1_raid['children']:
                    if the_ibd['uuid'] == my_uuid:
                        got_it = True
                        l2_parent_devname = l2_raid['devname']
                        l1_parent_devname = l1_raid['devname']
                        break
                if got_it == True:
                    break
            if got_it == True:
                l2_dev_num = len(l2_raid['children'])
                break
        if got_it == True:
            break
    return [l2_parent_devname, l2_dev_num, l1_parent_devname]


def raid5_device_set_io_error(argv):
    debug("Enter raid5_device_set_io_error(%s) ..." % argv[3])
    my_uuid = argv[3]

    c_infrastructure = get_infrastructure_from_vgname(argv[2])
    if c_infrastructure == None:
        return 1

    [l2_parent_devname, l2_dev_num, l1_parent_devname] = raid5_device_get_parent(c_infrastructure, my_uuid)
    if l2_parent_devname == None:
        return 1

    rc = raid_device_io_error_lock(l2_parent_devname, my_uuid)
    if rc != 0:
        return 1

    detail = {}
    raid_detail_nohung(l2_parent_devname, detail)
    active = 0
    if detail.has_key(ACTIVE_DRIVES) == True:
        active = detail[ACTIVE_DRIVES]
    elif detail.has_key(ACTIVE_DEVICES) == True:
        active = detail[ACTIVE_DEVICES]

    debug("L2: %s, L1: %s" % (l2_parent_devname, l1_parent_devname))
    debug("l2_dev_num: %d, active: %d" % (l2_dev_num, active))
    if l2_dev_num < 3:
        raid_device_io_error_unlock(l2_parent_devname, my_uuid)
        return 1
    if active < l2_dev_num - 1:
        raid_device_io_error_unlock(l2_parent_devname, my_uuid)
        return 1
    if active == l2_dev_num - 1:
        if detail.has_key(l1_parent_devname):
            debug("%s : %s" % (l1_parent_devname, detail[l1_parent_devname]))
            if detail[l1_parent_devname] == "active":
                raid_device_io_error_unlock(l2_parent_devname, my_uuid)
                return 1

    cmd = CMD_IBDMANAGER_IOERROR + ' ' + argv[4]
    rc = do_system(cmd)
    return rc


def raid_device_set_io_error(argv):
    debug("Enter raid_device_set_io_error(%s) ..." % argv[3])

    fd = node_trylock(RAID_IO_ERROR_LOCK)
    if fd is None:
        return 11
    fd.write("set " + argv[3])

    volume_uuid = argv[2]
    my_uuid = argv[3]
    my_dev = argv[4]
    reset_flag = 0
    set_io_error_output = '========set io error=========\n' \
                          '===device name {name_dev}====\n' \
                          '==time: {start_time}==\n'.format(
        name_dev=my_dev, start_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    with open(IBD_IO_STATUS, "a+") as set_io:
        set_io.write(set_io_error_output)
    if not os.path.exists("/tmp/doing_teardown") and is_stretchcluster_or_robo() and ha_has_quorum() == False:
        (stretchcluster_flag, availability_flag, tiebreakerip) = ha_stretchcluster_config()
        tiebreakerip = ha_get_tiebreakerip()
        scl_timeout = ha_get_scl_timeout()
        nodename = ha_get_local_node_name()
        result = ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)
        if len(tiebreakerip) == 2:
            if not is_vmmanager_reachable():
                if result[0] == 255 or result[1] == 255:
                    reset_flag = 1
            else:
                # If Volume could not acquire lock on all live node
                # Result should be [1,1] [1,255] [255,1]
                if (result[0] == 1 and result[1] == 1) or (result[0] + result[1]) == 256:
                    reset_flag = 1
                # If Volume acquire lock timeout on both tiebreaker
                # Result should be [255,255]
                # We need to check the power status
                # If not both are power off, reset the Volume
                elif result[0] == 255 and result[1] == 255:
                    (local_svm_power_status, remote_svm_power_status) = ha_get_local_remote_Service_VM_power_status()
                    if local_svm_power_status + remote_svm_power_status != 0:
                        reset_flag = 1
        else:
            if result[0] != 0:
                reset_flag = 1
        if reset_flag:
            debug('ERROR: Can not get the tie breaker lock, must reset.')
            time.sleep(2)
            ha_stop_volume(volume_uuid)
    rc = 0
    rc = MdStatMgr.set_io_error(my_uuid, argv[4])
    if rc is None:
        return 1
    if rc == 0:
        send_volume_storage_status('WARN')
    else:
        ha_reset_node_fake('multiple_lost_in_Raid{}'.format(my_dev.replace('/', '_')))
        send_volume_storage_status('FATAL')
    node_unlock(fd)
    # Return success to ibdagent, We don't need retry since we have got the
    # node lock
    set_io_error_successfully = '=====set io successfully=====\n'
    with open(IBD_IO_STATUS, "a+") as set_io:
        set_io.write(set_io_error_successfully)
    return 0


def rw_hook_action(argv):
    return raid_device_unset_io_error(argv)


def raid1_device_unset_io_error(argv):
    debug("Enter raid1_device_unset_io_error(%s) ..." % argv[3])
    my_uuid = argv[3]

    c_infrastructure = get_infrastructure_from_vgname(argv[2])
    if c_infrastructure == None:
        return 1

    parent_devname = raid1_device_get_parent(c_infrastructure, my_uuid)
    if parent_devname == None:
        return 1

    rc = raid_device_io_error_unlock(parent_devname, my_uuid)
    return rc


def raid5_device_unset_io_error(argv):
    debug("Enter raid5_device_unset_io_error(%s) ..." % argv[3])
    my_uuid = argv[3]

    c_infrastructure = get_infrastructure_from_vgname(argv[2])
    if c_infrastructure == None:
        return 1

    [l2_parent_devname, l2_dev_num, l1_parent_devname] = raid5_device_get_parent(c_infrastructure, my_uuid)
    if l2_parent_devname == None:
        return 1

    rc = raid_device_io_error_unlock(l2_parent_devname, my_uuid)
    return rc


def raid_device_unset_io_error(argv):
    debug("Enter raid_device_unset_io_error(%s) ..." % argv[3])
    fd = node_lock(RAID_IO_ERROR_LOCK)
    if fd is None:
        return 1
    unset_io_error_output = '********unset io error********\n' \
                            '***device uuid {name_dev}***\n' \
                            '**time: {start_time}**\n'.format(
        name_dev=argv[3], start_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    with open(IBD_IO_STATUS, "a+")as unset:
        unset.write(unset_io_error_output)
    fd.write("unset " + argv[3])
    # USX-76730 Need to make sure ibd device is readable before unset io error
    dev_name = argv[4]
    cmd_str = 'dd if={} of=/tmp/useless bs=4k count=5 oflag=direct iflag=direct'.format(dev_name)
    for _ in range(10):
        try:
            if do_system_timeout(cmd_str, 3) == 0:
                break
        except timeout_error as e:
            time.sleep(2)
    else:
        node_unlock(fd)
        return 1

    # rc = raid1_device_unset_io_error(argv)
    # c_infrastructure = get_infrastructure_from_vgname(argv[2])
    # is_find_dev = False
    # for type_raid in c_infrastructure:
    #     for type_list in c_infrastructure[type_raid]:
    #         raid_type = type_list[DEV_raidtype]
    #         for children in type_list['children']:
    #            vv for sub_dev in children['children']:
    #                 if sub_dev['uuid'] == argv[3]:
    #                     # always find this dev.
    #                     is_find_dev = True
    #                     break
    #         if is_find_dev:
    #             break
    #     if is_find_dev:
    #         break
    # if raid_type != RAID_raid1 and rc != 0:
    #     rc = raid5_device_unset_io_error(argv)  # try unlock raid5 lockfile


    rc = vv_readd(argv)
    node_unlock(fd)
    unset_io_error_successfully = '*****unset io error successfully*****\n'
    with open(IBD_IO_STATUS, "a+") as unset:
        unset.write(unset_io_error_successfully)
    return rc


def vv_pre_start_checking(argv):
    vgname = argv[2]
    my_uuid = argv[3]
    my_dev = argv[4]
    svm_ip = argv[5]

    debug("Enter vv_pre_start_checking(%s) ..." % my_uuid)
    rc = device_check_partition_oob(my_uuid, svm_ip)
    return rc


def prechecking_ioerror(argv):
    return 0  # no lock


def prechecking_readd(argv):
    return 2  # need try-lock


def prechecking_init(argv):
    return 1


def prechecking_start(argv):
    return 1


def prechecking_stop(argv):
    return 0  # no lock


def prechecking_pre_start_checking(argv):
    return 0  # no lock


def send_volume_storage_status(input_status):
    """
    USX 2.1 Compound status, collecting raid 5 IBD summary status
    """
    vv_setup_info = {}
    rc = vv_load_conf(CP_CFG, vv_setup_info)
    if rc != 0:
        return rc

    try:
        configure = vv_setup_info['configure']
        volcontainer = configure['usx']
        volresources = configure[AMC_volresources][0]
        container_uuid = volcontainer['uuid']

        data = {}
        if volresources:  # if no resource, volume is HA standby node; no need to check storage status
            resource_uuid = volresources['uuid']

            # Set storage status update JSON
            data['usxuuid'] = resource_uuid
            data['usxcontaineruuid'] = container_uuid
            data['usxtype'] = VOL_RESOURCE_TYPE
            data['usxstatuslist'] = []

            volume_storage_status = {}
            volume_storage_status['name'] = VOLUME_STORAGE_STATUS
            if input_status.upper() == 'WARN':
                volume_storage_status['value'] = input_status.upper()
            elif input_status.upper() == 'FATAL':
                volume_storage_status['value'] = input_status.upper()
            data['usxstatuslist'].append(volume_storage_status)
            debug("VOLUME_STORAGE_STATUS: " + json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))

            # Update volume storage status via local agent REST API
            usxmanager_url = LOCAL_AGENT
            api_str = '/usx/status/update'
            conn = urllib2.Request(usxmanager_url + api_str)
            conn.add_header('Content-type', 'application/json')
            try:
                res = urllib2.urlopen(conn, json.dumps(data))
            except:
                debug(traceback.format_exc())
                return 1
            debug('VOLUME STORAGE STATUS POST returned response code: ' + str(res.code))
            return 0
    except:
        return 1


def usage():
    print "Usage:" + sys.argv[
        0] + " init|vp_init|start|vp_get_size|remove_aggr_from_vp|ads_vp_init|ads_vp_start|ads_vp_stop|add|remove|ha [vgname]"


cmd_prechecking = {
    "vv_init": prechecking_init,
    "vv_start": prechecking_start,
    "readd": prechecking_readd,
    "vv_readd": prechecking_readd,
    "raid_device_set_io_error": prechecking_ioerror,
    "raid_device_unset_io_error": prechecking_ioerror,
    "vv_stop": prechecking_stop,
    "vv_pre_start_checking": prechecking_pre_start_checking,
    "rw_hook_action": prechecking_ioerror
}

cmd_options = {
    "vv_init": vv_init,
    "vv_start": vv_start,
    "vv_pre_start_checking": vv_pre_start_checking,
    "vv_readd": vv_readd,
    "vv_stop": vv_stop,
    "vdi_init": vdi_init,  # for USX 2.0 VDI volumes initial configuration
    "vdi_start": vdi_start,  # for USX 2.0 VDI volumes subsequent reboot
    "init": cp_init,  # IBD-DONE
    "start": cp_start,  # IBD-DONE
    "ads_vp_init": ads_vp_init,  # IBD-DONE, set up virtual pool during ads init
    "ads_vp_start": ads_vp_start,  # IBD-DONE start virtual pool during ads start
    "ads_vp_show": ads_vp_show,  # IBD-DONE show metadata of ads and its related virtual pool
    "add": cp_add,  # IBD-DONE
    "readd": cp_readd,  # IBD-DONE
    "remove": cp_remove,  # IBD-DONE
    "vg_stop": cp_vg_stop,  # IBD-DONE stop a vg
    "ads_vp_stop": ads_vp_stop,  # IBD-DONE stop a virtual pool
    "ads_vp_destroy": ads_vp_destroy,  # IBD-DONE free the virtual pool resources used by ads
    "ads_vp_clean": ads_vp_clean,  # IBD-DONE virtual pool garbage collection for a given ads
    "vg_destroy": cp_vg_destroy,  # IBD-DONE destroy a vg
    "ha": cp_ha_start,  # IBD-DONE
    "extend": cp_extend,  # IBD-DONE
    "vp_get_size": vp_get_size,  # IBD-DONE
    "remove_aggr_from_vp": remove_aggr_from_vp,  # IBD-DONE
    "raid_device_set_io_error": raid_device_set_io_error,  # IBD-DONE
    "raid_device_unset_io_error": raid_device_unset_io_error,  # IBD-DONE
    "rw_hook_action": rw_hook_action,
    "virtvol_vp_setup": virtvol_vp_setup,
}

debug("Entering cp-load:", sys.argv)
if len(sys.argv) < 2:
    usage()
    exit(1)

cmd_type = sys.argv[1]

if os.path.exists(IBD_AGENT_STOP_FILE):
    if cmd_type == "vv_start" or cmd_type == "vv_init":
        os.unlink(IBD_AGENT_STOP_FILE)
    else:
        debug("Doing vv_stop!")
        exit(1)
else:
    if cmd_type == "vv_stop":
        f = open(IBD_AGENT_STOP_FILE, 'w')
        f.write("Stop at %s" % datetime.datetime.now())
        f.close()

precheck_rc = 1  # need lock by default
if cmd_prechecking.has_key(cmd_type):
    precheck_rc = cmd_prechecking[cmd_type](sys.argv)
    if precheck_rc > 2:
        exit(precheck_rc)

if cmd_type in cmd_options:
    node_lock_fd = None
    if precheck_rc == 1:  # need lock
        print(precheck_rc)
        node_lock_fd = node_lock(POOL_LOCKFILE)
    elif precheck_rc == 2:  # need try-lock
        node_lock_fd = node_trylock(POOL_LOCKFILE)
        if node_lock_fd == None:  # failed to try-lock
            exit(2)

    # start retry 3 times when it running commands failed with time out
    if cmd_type == "vv_start":
        is_need_reset = True
        for i in range(3):
            try:
                rc = cmd_options[cmd_type](sys.argv)
                is_need_reset = False
                break
            except timeout_error as e:
                debug('timeout_error: %s' % e.value)
            except:
                debug(traceback.format_exc())
                debug('Exception caught on cp-load...')
                is_need_reset = False
                rc = 1
                break
        if is_need_reset == True:
            reset_vm('retry_start_vm_3')
    else:
        try:
            rc = cmd_options[cmd_type](sys.argv)
        except timeout_error as e:
            debug('timeout_error: %s' % e.value)
            rc = 1
        except:
            debug(traceback.format_exc())
            debug('Exception caught on cp-load...')
            rc = 1

    if node_lock_fd != None:
        node_unlock(node_lock_fd)

    if rc != 0:
        debug("%s Failed with %s" % (sys.argv, rc))
        exit(1)
    else:
        debug("%s Succeed with %s" % (sys.argv, rc))
        exit(0)
else:
    usage()
    exit(1)

exit(0)
