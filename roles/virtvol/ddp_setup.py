#!/usr/bin/python

import httplib
import ConfigParser
import json
import operator
import os
import sys
import logging
import re
import subprocess as sub
import fcntl
import time

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *

#VV_CFG = "/etc/ilio/atlas.json"

DEFAULT_LUN_NAME = "datastorename"

MKE2FS_CMD = "/opt/milio/bin/mke2fs -N 100000 -b 4096 -d -j -J "
#ATL_CFG = open(VV_CFG, 'r')
#ATL_CFG_STR = ATL_CFG.read()
#ATL_CFG.close()
#ATL_CFG_DICT = json.loads(ATL_CFG_STR)
MNT_POINT = "/exports/ILIO_VirtualDesktops" # Deprecated
# try:
#     if ATL_CFG_DICT['virtualvolumeresources']:
#         if ATL_CFG_DICT['virtualvolumeresources'][0].has_key('dedupfsmountpoint'):
#             MNT_POINT = ATL_CFG_DICT['virtualvolumeresources'][0]['dedupfsmountpoint']
#         else:
#             MNT_POINT = "/exports/" + ATL_CFG_DICT["virtualvolumeresources"][0]['uuid'].split('_')[-1]
#
# except:
#     debug("ERROR : %: Set MNT_POINT failed" % __file__)
#     sys.exit(1)

VSCALER_LOAD_CMD = "/opt/milio/scripts/vscaler_load "
VSCALER_NAME = "vmdata_cache"
CACHEDEV = "/dev/mapper/vmdata_cache"
NFS_START_CMD = "service nfs-kernel-server start"
DDP_MOUNT_CMD = ("mount -t dedup -o ")
VSCALER_WB_CREATE_CMD = ("/opt/milio/scripts/vscaler_create -p back vmdata_cache ")
VSCALER_WT_CREATE_CMD = ("/opt/milio/scripts/vscaler_create -p thru vmdata_cache ")
VSCALER_CREATE_CMD = ("/opt/milio/scripts/vscaler_create -p ")
CMD_BINARIES = ["/opt/milio/bin/mke2fs", "/opt/milio/scripts/vscaler_create",
               "service", "mount", "crm"]
NFS_EXPORTS_OPT = ("*(rw,no_root_squash,no_subtree_check,insecure,nohide,"
               "fsid=1,")
NFS_SYNC = "sync"
NFS_ASYNC = "async"
DEFAULT_MOUNT_DEVICE_PATH = "/dev/mapper/"
DEFAULT_MOUNT_OPTIONS = ("rw,noblocktable,noatime,nodiratime,timeout=180000,"
                         "dedupzeros,commit=30,thin_reconstruct,"
                         "data=ordered,errors=remount-ro")
DELIMITER = "}}##0##{{"

ENABLE_DEBUG = 1

INVALID_FILE = 1
INVALID_DEVICE = 2
INVALID_ARGUMENT = 3

old_mnttab = "/etc/ilio/mnttab.old"
mnttab = "/etc/ilio/mnttab"
current_index = 0
mnttab_content = []

VSCALER_RESOURCE_NAME = "atl_vscaler"
DEDUP_RESOURCE_NAME = "atl_dedup"
IP_RESOURCE_NAME = "atl_shared_ip"
NFS_RESOURCE_NAME = "atl_nfs"
ISCSI_TARGET_RESOURCE_NAME = "atl_iscsi_target"
ISCSI_LUN_RESOURCE_NAME = "atl_iscsi_lun"
LUN_FILENAME="LUN1"
HA_GROUP_NAME = "atl_ha_group"
NFS_SHARED_INFODIR = "/var/atl_ha/exports"
DEFAULT_IQN = "iqn.com.atlantiscomputing.usx"

LOG_FILENAME = '/var/log/usx-ads-pool.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(message)s')

logfd = logging.getLogger().handlers[0].stream.fileno()
flags = fcntl.fcntl(logfd, fcntl.F_GETFD)
flags |= fcntl.FD_CLOEXEC
fcntl.fcntl(logfd, fcntl.F_SETFD, flags)



# Global variables
adsname = None

class LogFile(object):
    """File-like object to log text using the `logging` module."""

    def __init__(self, name=None):
        self.logger = logging.getLogger(name)

    def write(self, msg, level=logging.INFO):
        self.logger.log(level, msg)

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()

# # Redirect stdout and stderr
# sys.stdout = LogFile('stdout')
# sys.stderr = LogFile('stderr')


def debug_msg(*args):
    """ Prints messages when ENABLE_DEBUG is set to non zero value
        Args:
            msg: string
        Returns:
            Nothing
    """
    if ENABLE_DEBUG:
        logging.debug("".join([str(x) for x in args]))
    #    print("".join([str(x) for x in args]))

def error_msg(*args):
    """ Prints messages to stderr
        Args:
            msg: string
        Returns:
            Nothing
    """
    msg = " ".join([str(x) for x in args])
    logging.debug(msg)
    print >> sys.stderr, msg
'''
def _load_conf(filename):
    cfg_file = open(filename, 'r')
    cfg_str = cfg_file.read()
    cfg_file.close()
    cfg_dict = json.loads(cfg_str)
    return cfg_dict
'''
def is_mntopts_valid(mnt_opts):
    if mnt_opts and len(mnt_opts) and mnt_opts.strip():
        return True
    else:
        return False

def get_mntopts_from_resource(vvr_dict):
    mnt_opts = vvr_dict.get("volumemountoption")
    if not is_mntopts_valid(mnt_opts):
        mnt_opts = DEFAULT_MOUNT_OPTIONS

    is_journaled = vvr_dict.get("directio") # USX 2.0

    type_str = vvr_dict["volumetype"]
    if type_str.upper() in ["SIMPLE_MEMORY"]:
        is_inmem = True
    else:
        is_inmem = False

    if is_journaled:
        mnt_opts = mnt_opts + ",journaled"
    if is_inmem:
        mnt_opts = mnt_opts + ",inmem"
    return mnt_opts

"""
    mnttab file format
    dedup_dev}}##0##{{cache_dev}}##0##{{cache_name}}##0##{{mount_point}}##0##{{mount_options
    Ex:
    /dev/sdb1}}##0##{{/dev/ssd1}}##0##{{vmdata_cache}}##0##{{/exports/<JSON ADSNAME>/}}##0##{{rw,noblocktable,thin_reconstruct,data=ordered,errors=remount-ro}}##0##{{journal_devnum

    One entry per line. No double quotes
"""
def ddp_prepare_update():
    global mnttab_content
    global current_index
    current_index = 0
    if not os.path.isfile(mnttab):
        return 0

    built_cmd = "cp " + mnttab + " " + old_mnttab
    debug_msg("ddp_prepare_update: running " + built_cmd + "\n")
    os.system(built_cmd + ' >> %s 2>&1' % LOG_FILENAME)
    return 0

def readmnttab(mnttab):
    ddp_dev = None
    cache_dev = None
    cache_name = None
    mnt_point = None
    mnt_opts = None
    jdev = None

    if os.path.isfile(mnttab):
        with open(mnttab) as f:
            mnttab_content = f.readlines()
        if len(mnttab_content) > current_index:
            read_list = mnttab_content[current_index].split(DELIMITER)
            if len(read_list) == 5:
                ddp_dev = read_list[0]
                cache_dev = read_list[1]
                cache_name = read_list[2]
                mnt_point = read_list[3]
                mnt_opts = read_list[4]
            elif len(read_list) == 6:
                ddp_dev = read_list[0]
                cache_dev = read_list[1]
                cache_name = read_list[2]
                mnt_point = read_list[3]
                mnt_opts = read_list[4]
                jdev = read_list[5]
    return [ddp_dev, cache_dev, cache_name, mnt_point, mnt_opts, jdev]


def ddp_update_device_list(config, mount_point, ddp, vs, jd):

    global mnttab_content
    global current_index

    ddp_prepare_update()
    # TODO :: Support for multiple resources in Virtual Volume container?
    vvr_dict = config['volumeresources'][0]

    [ddp_dev, cache_dev, cache_name, mnt_point, mnt_opts, jdev] = readmnttab(mnttab)

    my_list = [ddp_dev, cache_dev, cache_name, mnt_point, mnt_opts, jdev]
    debug(my_list)
    debug("----ddp_update_device_list: mount_point: %s| ddp: %s | vs:%s | jd: %s" % (mount_point, ddp, vs, jd))

    if (ddp and len(ddp) and ddp.strip()):
        ddp_dev = ddp

    if (vs and len(vs) and vs.strip()):
        cache_dev = vs
        if not (cache_name and len(cache_name) and cache_name.strip()):
            cache_name = VSCALER_NAME
    else:
        cache_dev = " "
        cache_name = " "

    if (jd and len(jd) and jd.strip()):
        out = ['']
        rc = do_system('/usr/bin/stat --printf="%02t %02T" ' + jd, out)
        output = out[0]
        #print output

        #p = sub.Popen("/usr/bin/stat --printf=%02t%02T " + jd,stdout=sub.PIPE,stderr=sub.PIPE)
        #output, errors = p.communicate()
        if (output and len(output) and output.strip()):
            major = int(output.split(' ')[0],16)
            minor = int(output.split(' ')[1],16)
            jdev = os.makedev(major, minor)
    else:
        jdev=""

    if (mount_point and len(mount_point) and mount_point.strip()):
        mnt_point = mount_point

    if not is_mntopts_valid(mnt_opts):
        mnt_opts = get_mntopts_from_resource(vvr_dict)

    is_sync = False
    is_journaled = vvr_dict.get("directio") # USX 2.0
    is_infra = is_infra_volume(config)
    is_fs_sync = vvr_dict.get('fs_sync', False)
    if is_journaled or is_infra or is_fs_sync:
        is_sync = True

    # config_exports(mnt_point, is_sync)
    if len(mnttab_content) == current_index:
        updt_str = (str(ddp_dev) + DELIMITER + str(cache_dev) + DELIMITER +
                    str(cache_name) + DELIMITER + mnt_point + DELIMITER +
                    mnt_opts + DELIMITER + str(jdev) + "\n")
        debug_msg("ddp_update_device_list: Inserting into mnttab\n" + updt_str)
        mnttab_content.insert(current_index, updt_str)
    else:
        updt_str = (str(ddp_dev) + DELIMITER + str(cache_dev) + DELIMITER +
                    str(cache_name) + DELIMITER + mnt_point + DELIMITER +
                    mnt_opts + DELIMITER + str(jdev))
        debug_msg("ddp_update_device_list: Updating mnttab with\n" + updt_str)
        mnttab_content[current_index] = updt_str

    current_index += 1

    with open(mnttab, 'w+') as f:
        for s in mnttab_content:
            debug_msg("ddp_update_device_list: Writing to file\n" + s)
            f.write(s)
    f.close()
    return 0

def which(program):
    """ Checks if a file exists and is executable
        Args:
            program: Path of a file
        Returns:
            Full path of the executable file if found
            Else returns None
    """
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

def check_binaries(binaries):
    for binfile in binaries:
        ret = which(binfile)
        if ret == None:
            error_msg(binfile + "is not a binary")
            return 1
    return 0

def config_exports(exp_dir, is_sync = False):
    """ Add a line to /etc/exports if it already doesn't exists
        Args:
            exp_dir: Mount point
        Returns:
            Returns nothing
    """
    ret = os.system("grep \"" + exp_dir + "\" /etc/exports" +
                    ' >> %s 2>&1' % LOG_FILENAME)
    if ret == 0:
        return ret

    if is_sync == True:
        sync_option = NFS_SYNC
    else:
        sync_option = NFS_ASYNC
    f = open("/etc/exports", "a+")
    f.write("\"" + exp_dir + "\" " + NFS_EXPORTS_OPT + sync_option + ")" + "\n")
    f.close()
    os.system("exportfs -r")

def simple_vscaler(vscaler_name, wmode, cache_dev, disk_dev):
    # TODO: WB mode should first try load to reuse old cache data.
    built_cmd = VSCALER_CREATE_CMD + wmode + " " + vscaler_name + " " + cache_dev + " " + disk_dev
    debug_msg("config_or_init_vscaler: Creating vscaler with\n" +
              built_cmd + "\n")
    return os.system(built_cmd + ' >> %s 2>&1' % LOG_FILENAME)

def config_or_init_vscaler(ddp_dev, vscaler_dev, enable_ha, fastsync):
    """ Load or create a vscaler cache
        Args:
            ddp_dev: dedup filesystem device
            vscaler_dev: caching device like ssd or zram
            enable_ha: Is HA enabled
	    fastsync: vscaler mode back|thru, True: back, False: thru
        Returns:
            Returns 0 on success or INVALID_ARGUMENT, INVALID_DEVICE
    """
    if not vscaler_dev:
        error_msg("config_vscaler: No vscaler device specified\n")
        return INVALID_ARGUMENT
    elif not os.path.exists(ddp_dev):
        error_msg("config_ddp: " + vscaler_dev + " device does not exists\n")
        return INVALID_DEVICE
    load_vscaler_cmd = VSCALER_LOAD_CMD + vscaler_dev + " " + VSCALER_NAME
    debug_msg("config_or_init_vscaler: Loading vscaler with\n" +
              load_vscaler_cmd + "\n")
    ret = os.system(load_vscaler_cmd + ' >> %s 2>&1' % LOG_FILENAME)
    if ret == 0:
        return ret
    if fastsync == False:
        built_cmd = VSCALER_WT_CREATE_CMD + vscaler_dev + " " + ddp_dev
    else:
        built_cmd = VSCALER_WB_CREATE_CMD + vscaler_dev + " " + ddp_dev
    debug_msg("config_or_init_vscaler: Creating vscaler with\n" +
              built_cmd + "\n")
    return os.system(built_cmd + ' >> %s 2>&1' % LOG_FILENAME)

def ha_vscaler_configured():
    cmd = "crm configure show | grep vscaler"
    if not os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME):
        return 1

def config_ha_vscaler(cache_dev, mode):
    global adsname
    cmd = ("crm configure primitive " + adsname + VSCALER_RESOURCE_NAME + ' ' +
           "ocf:heartbeat:atl-vscaler params cache_dev=" + cache_dev +
	   " mode=" + mode +
	   " op monitor interval=\"20s\" timeout=\"60s\" op start timeout=" +
           "\"60s\" op stop timeout=\"60s\" meta target-role=stopped")
    debug_msg("config_ha_vscaler: Configuring vscaler resource with:\n" + cmd)
    return os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)

def config_ha_ddp(device):
    global adsname
    cmd = ("crm configure primitive " + adsname + DEDUP_RESOURCE_NAME + ' ' +
          " ocf:heartbeat:dedup-filesystem params device=\"" + device + "\" " +
          "directory=\"" + MNT_POINT + "\" fstype=\"dedup\" run_fsck=\"no\" " +
          "options=\"rw,noblocktable,noatime,nodiratime,timeout=180000," +
          "dedupzeros,thin_reconstruct,data=ordered,commit=30,errors="
          "remount-ro\" op monitor interval=\"20s\" timeout=\"60s\" op start " +
          "timeout=\"300s\" op stop timeout=\"400s\" meta target-role=stopped")
    debug_msg("config_ha_ddp: Configuring dedup with:\n" + cmd)
    return os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)


def config_ha_ip(sharedip, netmask):
    global adsname
    cmd = ("crm configure primitive " + adsname + IP_RESOURCE_NAME + ' ' +
          " ocf:heartbeat:IPaddr params ip=" + sharedip + " cidr_netmask=" +
          netmask + " op monitor interval=\"20s\" timeout=\"60s\" op start " +
          "timeout=\"60s\" op stop timeout=\"60s\"")
    debug_msg("config_ha_ip: Configuring shared ip with:\n" + cmd)
    return os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)


def config_ha_nfs(ddp_dev, sharedip):
    global adsname
    cmd = ("crm configure primitive " + adsname + NFS_RESOURCE_NAME + ' ' +
          " ocf:heartbeat:nfsserver params nfs_init_script=\"" +
          "/etc/init.d/nfs-kernel-server\" nfs_ip=\"" + sharedip + "\" " +
          "nfs_shared_infodir=\"" + NFS_SHARED_INFODIR + "\" op monitor " +
          "interval=\"20s\" timeout=\"20s\" op start timeout=\"20s\" op stop " +
          "timeout=\"20s\" meta target-role=stopped")
    debug_msg("config_ha_nfs: Configuring nfs with:\n" + cmd)
    return os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)

def config_ha_iscsi(iqn, mntpnt):
    global adsname
    cmd = ("crm configure primitive " + str(adsname) +
           str(ISCSI_TARGET_RESOURCE_NAME) + " ocf:heartbeat:atl-SCSTTarget " +
           "params iqn=\"" + str(iqn) + "\" meta target-role=stopped")
    debug_msg("config_ha_iscsi: Configuring iscsi target with:\n" + cmd)
    ret = os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)
    cmd = ("crm configure primitive " + str(adsname) + ISCSI_LUN_RESOURCE_NAME +
           " ocf:heartbeat:atl-SCSTLun params device_name=\"LUN1\" " +
           "target_iqn=\"" + iqn + "\" path=\"" + str(mntpnt) + "/LUN1\" " +
           "handler=\"vdisk_fileio\" lun=\"0\" additional_parameters=\"" +
           "nv_cache=1\" meta target-role=stopped")
    debug_msg("config_ha_iscsi: Configuring iscsi lun with:\n" + cmd)
    ret2 = os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)
    return ret or ret2

def config_ha_group(vscaler_enabled):
    if vscaler_enabled:
        cmd = ("crm configure group " + HA_GROUP_NAME + " " +
               VSCALER_RESOURCE_NAME + " ")
    else:
        cmd = "crm configure group " + HA_GROUP_NAME + " "

    cmd = (cmd + DEDUP_RESOURCE_NAME + " " + IP_RESOURCE_NAME + " " +
          NFS_RESOURCE_NAME)
    debug_msg("config_ha_group: Configuring group with:\n" + cmd)
    return os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)

def config_ha_worker(ddp_dev, vscaler_dev):
    global adsname

    # TODO: we need to read the json from grid. Otherwise, if there is any change
    # from UI, we cannot change our configure.
    #cfg_file = open(VV_CFG, 'r')
    #cfg_str = cfg_file.read()
    #cfg_file.close()
    #cfg_dict = json.loads(cfg_str)

    cfg_dict = load_atlas_conf()
    if not cfg_dict:
        return 100

    # TODO :: support multiple resources in one Virtual Volume container?
    for resource in cfg_dict['volumeresources']:
        vvr_dict = resource

    type_str = vvr_dict["volumetype"]
    if type_str.lower() == "hybrid":
        fastsync = vvr_dict["fastsync"]
    exporttype = vvr_dict["exporttype"]
    datastorename = vvr_dict["uuid"]
    if not (datastorename and len(datastorename) and datastorename.strip()):
        datastorename = DEFAULT_LUN_NAME
    haconfig_dict = cfg_dict["haconfig"]
    sharedip = haconfig_dict["sharedip"]
    ring_dict = haconfig_dict["ring"]
    for ring in ring_dict:
        netmask = ring["privatenetmask"]

    vvruuid = vvr_dict['uuid']
    if vvruuid is None:
        debug_msg('Error getting Virtual Volume resource UUID. HA will NOT be enabled for this node.')
        return 3

    debug_msg("init_ddp: The node is of type " + type_str + "\n")
    debug_msg("init_ddp: The node exports data as " + str(exporttype) + "\n")

    os.system("mkdir -p " + NFS_SHARED_INFODIR + ' >> %s 2>&1' % LOG_FILENAME)

    if type_str.lower() == "hybrid":
        if fastsync == False:
	    mode="thru"
	else:
	    mode="back"
        ret = config_ha_vscaler(VSCALER_NAME, mode)
        if ret:
            return ret
        ret = config_ha_ddp(CACHEDEV)
    else:
        ret = config_ha_ddp(ddp_dev)
    if ret:
        return ret

    if "iscsi" in exporttype.lower():
        mod_datastorename = re.sub('[^A-Za-z0-9-]', '-', datastorename)
        ret = config_ha_iscsi(DEFAULT_IQN+":"+str(mod_datastorename), MNT_POINT)
    else:
        ret = config_ha_nfs(ddp_dev, sharedip)
    if ret:
        return ret

    """
    ret = config_ha_ip(sharedip, netmask)
    if ret:
        return ret
    We need not build a group here as resources like ip and nbd should also be
    a part in the failover group
    if type_str.lower() == "hybrid":
        ret = config_ha_group(1)
    else:
        ret = config_ha_group(0)
    if ret:
        return ret
    """

    return 0

def config_ha():
    ddp_dev = None
    cache_dev = None
    cache_name = None
    mnt_point = None
    mnt_opts = None

    if os.path.isfile(mnttab):
        with open(mnttab) as f:
            mnttab_content = f.readlines()
        if len(mnttab_content) > current_index:
            read_list = mnttab_content[current_index].split(DELIMITER)
            if len(read_list) == 5:
                ddp_dev = read_list[0]
                cache_dev = read_list[1]
                cache_name = read_list[2]
                mnt_point = read_list[3]
                mnt_opts = read_list[4]
    if not (ddp_dev or cache_dev):
        debug_msg("config_ha: device not found\n")
        return 1
    return config_ha_worker(ddp_dev, cache_dev)

def reset_ddp(config, ddp_dev=None, vdev=None, jdev=None, jsize=400, hybrid=False, conf=False):
    ret = 0
    if jdev and os.path.exists(jdev):
        debug_msg("reset_ddp: Creating journal device with : yes | mke2fs -O journal_dev " + jdev + " -b 4096")
        ret = os.system("yes | mke2fs -O journal_dev " + jdev + " -b 4096" + ' >> %s 2>&1' % LOG_FILENAME)
        if ret:
            error_msg("reset_ddp: mkjournal failed with " + str(ret) + "\n")
            return ret
        mke2fs_cmd = MKE2FS_CMD + " device=" + jdev + " " + str(ddp_dev)
    else:
        debug_msg("reset_ddp: Creating filesystem with internal journal\n")
        mke2fs_cmd = MKE2FS_CMD + " size=" + str(jsize) + " " + str(ddp_dev)

    if ddp_dev and os.path.exists(ddp_dev):
        if hybrid and not conf:
            ret = os.system("/sbin/tune2fs  -f -O ^has_journal " + ddp_dev)
            if ret:
                debug_msg("reset_ddp: failed to remove journal. Auto recovery not possible")
                return ret
            if jdev and os.path.exists(jdev):
                ret = os.system("/sbin/tune2fs  -J device=" + jdev + " " + ddp_dev)
            else:
                ret = os.system("/sbin/tune2fs  -J size=" + str(jsize) + " " + ddp_dev)
            if ret:
                debug_msg("reset_ddp: failed to create journal. Auto recovery not possible")
            return ret
        debug_msg("reset_ddp: Creating filesystem with " + mke2fs_cmd + "\n")
        ret = os.system("yes | " + mke2fs_cmd + ' >> %s 2>&1' % LOG_FILENAME)
    return ret

def config_ddp(config, mount_point, ddp_dev, vscaler_dev=None, jdev=None, jsize=400, enable_ha=0, is_small_mode=False):
    """ mkfs a new dedup file system
        Args:
            ddp_dev: dedup filesystem device
            vscaler_dev: caching device like ssd or zram
            enable_ha: Is HA enabled
        Returns:
            Returns 0 on success or INVALID_ARGUMENT, INVALID_DEVICE
            INVALID_FILE
    """
    debug("---config_ddp: mntpt: %s | ddp_dev: %s | vscaler:%s | jdev: %s" % (mount_point, ddp_dev, vscaler_dev, jdev))
    if not ddp_dev:
        error_msg("config_ddp: No device specified\n")
        if not enable_ha:
            return INVALID_ARGUMENT
    elif not os.path.exists(ddp_dev):
        error_msg("config_ddp: " + ddp_dev + " device does not exists\n")
        if not enable_ha:
            return INVALID_DEVICE
    if check_binaries(CMD_BINARIES):
        return INVALID_FILE

    ret = reset_ddp(config, ddp_dev, vscaler_dev, jdev, jsize, False, True)
    if ret and not enable_ha:
        error_msg("config_ddp: mkfs failed with " + str(ret) + "\n")
        return ret
    if is_small_mode:
        debug('DEBUG: config small dedupfs, skip exporting operation')
        return 0
    # ddp_prepare_update()

    # ddp_update_device_list(config, mount_point, ddp_dev, vscaler_dev, jdev)
    """
    if enable_ha:
        return config_ha(ddp_dev, vscaler_dev)
    """
    return 0

def init_ha():
    """
    Init of service will be done by upper lower layer. Just return 0

    cmd = "service corosync start"
    ret = os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)
    if ret:
        return ret
    cmd = "service pacemaker start"
    ret = os.system(cmd + ' >> %s 2>&1' % LOG_FILENAME)
    if ret:
        return ret
    """
    return 0

def simple_mount(mount_point, ddp_dev):
    global DDP_MOUNT_CMD
    mnt_opts = DEFAULT_MOUNT_OPTIONS
    mnt_cmd = DDP_MOUNT_CMD + mnt_opts
    mnt_cmd = mnt_cmd + " " + ddp_dev + " " + mount_point

    if not os.path.exists(mount_point):
        out = ['']
        rc = do_system('mkdir -p ' + mount_point,out)
        if rc != 0:
            error_msg("init_ddp: Error creating DEDUP mount point")
            return rc

    debug_msg("simple_mount: Mounting dedup with\n" + mnt_cmd + "\n")
    ret = os.system(mnt_cmd + ' >> %s 2>&1' % LOG_FILENAME)
    return ret

def init_ddp(config, mount_point, ddp_dev, vscaler_dev=None, jdev=None, enable_ha=0):
    """ Mount dedup filesystem
        Args:
            ddp_dev: dedup filesystem device
            vscaler_dev: caching device like ssd or zram
            enable_ha: Is HA enabled
        Returns:
            Returns 0 on success or INVALID_ARGUMENT, INVALID_DEVICE
            INVALID_FILE
    """
    global DDP_MOUNT_CMD
    if check_binaries(CMD_BINARIES):
        return INVALID_FILE

    # TODO :: Support for multiple resources in Virtual Volume container?
    vvr_dict = config['volumeresources'][0]

    debug("---config_ddp: mntpt: %s | ddp_dev: %s | vscaler:%s | jdev: %s" % (mount_point, ddp_dev, vscaler_dev, jdev))

    type_str = vvr_dict["volumetype"]

    exporttype = vvr_dict["exporttype"]
    if type_str.lower() == "hybrid_deprecate":
        fastsync = vvr_dict["fastsync"]
    debug_msg("init_ddp: The node is of type " + type_str + "\n")

    if enable_ha:
        return init_ha()

    mnt_opts = get_mntopts_from_resource(vvr_dict)

    DDP_MOUNT_CMD = DDP_MOUNT_CMD + mnt_opts

    if jdev and os.path.exists(jdev):
        out = ['']
        rc = do_system('/usr/bin/stat --printf="%02t %02T" ' + jdev, out)
        output = out[0]
        #print output
        #p = sub.Popen("stat --printf=%02t%02T " + jdev,stdout=sub.PIPE,stderr=sub.PIPE)
        #output, errors = p.communicate()
        if (output and len(output) and output.strip()):
            major = int(output.split(' ')[0],16)
            minor = int(output.split(' ')[1],16)
            jnum = os.makedev(major, minor)
        DDP_MOUNT_CMD = DDP_MOUNT_CMD + ",journal_dev=" + str(jnum)

    if type_str.lower() == "hybrid_deprecate":
        ret = config_or_init_vscaler(ddp_dev, vscaler_dev, enable_ha, fastsync)
        if ret != 0:
            error_msg("init_ddp: Error initializing vscaler device\n")
            return ret
        mnt_cmd = DDP_MOUNT_CMD + " " + CACHEDEV + " " + mount_point
    else:
        mnt_cmd = DDP_MOUNT_CMD + " " + ddp_dev + " " + mount_point

    debug_msg("init_ddp: Mounting dedup with\n" + mnt_cmd + "\n")
#Check if mount_point is present, or else create it
    if not os.path.exists(mount_point):
        out = ['']
        rc = do_system('mkdir -p ' + mount_point,out)
        if rc != 0:
            error_msg("init_ddp: Error creating DEDUP mount point")
            return rc
    # try to mount succssfully.
    for i in range(10):
        ret = os.system(mnt_cmd + ' >> %s 2>&1' % LOG_FILENAME)
        if ret == 0:
            break
        debug_msg("WARNING: DEBUG messages:")
        os.system("ls -l %s;vgs;lvs >> %s 2>&1" % (ddp_dev, LOG_FILENAME))
        time.sleep(5)
    if ret != 0:
        return ret
    return 0
    # debug_msg("Exporttype is: " + exporttype.lower())
    # if "iscsi" in exporttype.lower():
    #     datastorename = vvr_dict["uuid"]
    #     if not (datastorename and len(datastorename) and datastorename.strip()):
    #         datastorename = DEFAULT_LUN_NAME
    #     mod_datastorename = re.sub('[^A-Za-z0-9-]', '-', datastorename)
    #     lun_file_path = None
    #     try:
    #         lun_file_path = os.path.join(mount_point, LUN_FILENAME)
    #     except:
    #         lun_file_path = None
    #     if mount_point.strip().endswith('/'):
    #         mnt_pnt = mount_point.strip()[:-1]
    #     else:
    #         mnt_pnt = mount_point.strip()
    #     cmd_str = '/usr/bin/python /opt/milio/atlas/scripts/scsi-export-ads.pyc ' \
    #         + mnt_pnt + ' ' + DEFAULT_IQN + ':' + str(mod_datastorename)
    #     if (not lun_file_path) or (not os.path.exists(lun_file_path)):
    #         debug_msg("init_ddp: Configuring iscsi with\n" + cmd_str + " True\n")
    #         ret = os.system(cmd_str + " True" +  ' >> %s 2>&1' % LOG_FILENAME)
	   #  if ret:
    #             debug_msg("init_ddp: iscsi config failed\n")
    #             return ret
    #     debug_msg("init_ddp: Starting iscsi with\n" + cmd_str + " False\n")
    #     return  os.system(cmd_str + " False" +  ' >> %s 2>&1' % LOG_FILENAME)
    # else:
    #     debug_msg("init_ddp: Starting nfs server with\n" + NFS_START_CMD + "\n")
    #     return os.system(NFS_START_CMD + ' >> %s 2>&1' % LOG_FILENAME)


def test_init_ddp(ddp_dev, vscaler_dev=None, enable_ha=0):
    ENABLE_DEBUG = 1
    ret = init_ddp(ddp_dev, vscaler_dev, enable_ha)
    if ret:
        error_msg("test_init_ddp: init_ddp failed with return value " +
            str(ret) + "\n")
    return ret

def test_config_ddp(ddp_dev, vscaler_dev=None, enable_ha=0):
    ENABLE_DEBUG = 1
    ret = config_ddp(ddp_dev, vscaler_dev, enable_ha)
    if ret:
        error_msg("test_config_ddp: config_ddp failed with return value " +
                  str(ret) + "\n")
    return ret


# test_config_ddp("/dev/sdb", "/dev/sdc")
# test_init_ddp("/dev/sdb", "/dev/sdc")
