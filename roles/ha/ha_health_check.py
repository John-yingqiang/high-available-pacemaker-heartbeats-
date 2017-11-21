#!/usr/bin/python

import os, sys
import logging
import tempfile
from subprocess import *
import httplib
import json
import socket
import signal
import fcntl
import mmap
import ctypes
import ctypes.util
from time import sleep
from ha_util import *

sys.path.insert(0, "/opt/milio/libs/atlas")
from cmd import runcmd
from atl_arbitrator import *


libc = ctypes.CDLL(ctypes.util.find_library('c'))

ATLAS_CONF = '/etc/ilio/atlas.json'
ATL_ARBITRATOR_PID_FILE = "/var/run/atl_arbitrator.pid"
ATL_ARBITRATOR_DEV_DIR = "/var/run/atl_arbitrator/"


# Must be 512 bytes aligned for Direct IO
# This offset is trying to land in pool nbd private region at
# first 1MB free space, skip the maximum 17k(34 sectors) GPT.
# FIXME: We should use a separate partition as the nbd private region on Pool node.
POISON_OFFSET = 1024 * 30
# Must be 512 bytes aligned for Direct IO
POISON_LENGTH = 512

ARB_ACK_CHECK_INTERVAL = 1 
POISON_PILL = 'letmedie'
POISON_ACK = 'iwilldie'
HEALTH_PILL = 'keepwork'
SHUTDOWN_PILL = 'shutdown'
TAKEOVER_PILL = 'takeover'
PILL_LEN = 8

CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_STAT = CMD_IBDMANAGER + " -r a -s get"
CMD_IBDMANAGER_STAT_WD = CMD_IBDMANAGER + " -r a -s get_wd"
CMD_IBDMANAGER_STAT_WU = CMD_IBDMANAGER + " -r a -s get_wu"
CMD_IBDMANAGER_STAT_WUD = CMD_IBDMANAGER + " -r a -s get_wud"

ATLAS_CONF = '/etc/ilio/atlas.json'
SHUTDOWEN_FILE = '/tmp/ha_shutdown'
HC_LOG_FILENAME = '/var/log/usx-atlas-health-check.log'
set_log_file(HC_LOG_FILENAME)


def ctypes_alloc_aligned(size, alignment):
    buf_size = size + (alignment - 1)

    raw_memory = bytearray(buf_size)

    ctypes_raw_type = (ctypes.c_char * buf_size)
    ctypes_raw_memory = ctypes_raw_type.from_buffer(raw_memory)
    raw_address = ctypes.addressof(ctypes_raw_memory)
    offset = raw_address % alignment

    offset_to_aligned = (alignment - offset) % alignment
    ctypes_aligned_type = (ctypes.c_char * (buf_size - offset_to_aligned))

    ctypes_aligned_memory = ctypes_aligned_type.from_buffer(raw_memory, offset_to_aligned)
    return ctypes_aligned_memory


def read_pill_one(arb_dev, buf):
    debug('Enter read_pill_one of arb device %s.' % arb_dev)
    
    rc = 0
    #We are using direct IO for poison file because a remote node may change it's content.
    fd = os.open(arb_dev, os.O_RDWR|os.O_DIRECT)
    # Offset 1025 will not work, notice the alignment restriction.
    os.lseek(fd, POISON_OFFSET, os.SEEK_SET)
    err_code = libc.read(ctypes.c_int(fd), buf, ctypes.c_int(POISON_LENGTH))

    if err_code == -1:
        debug('Can not read poison file, error :%d. Skip feed watchdog.' % os.errno)
        # TODO: Need flush log
        return 1

    #data = buf.raw[0:err_code]
    poison = buf.raw[0:len(POISON_PILL)]
    debug("read pill: %s" % poison)
    if poison == POISON_PILL or poison == TAKEOVER_PILL:
        debug('Got a secret pill: %s ' % poison)
    else:
        rc = 1

    #We are fine.
    os.close(fd)

    return rc


def ha_check_arb_listener():
    if os.path.exists(ATL_ARBITRATOR_PID_FILE) == True:
        try:
            f = open(ATL_ARBITRATOR_PID_FILE, 'r')
            pid = int(f.read())
            f.close()

            # Sending signal 0 to a pid will raise an OSError exception if the pid is not running,
            # and do nothing otherwise.
            os.kill(pid, 0)
        except:
            debug('Could not find existing HA arbitrator process!')
            return False
        debug('Found existing HA arbitrator process %d!' % pid)
        return  True
    debug('Could not access HA arbitrator pid file!')
    return False


def ha_check_arb(arb_dev_list):
    debug('Enter ha_check_arb: %s' % str(arb_dev_list))

    # check whether arbitrator process is up or not
    ret = ha_check_arb_listener()
    if ret == False:
        debug('HA arbitrator is not running!')

    arb_read_pill(ibd_dev_list, HEALTH_PILL)


def get_working_ibds():
    debug('Enter get_working_ibds')
    cmd = CMD_IBDMANAGER_STAT_WD
    (ret, msg) = runcmd(cmd, print_ret=True)
    ibd_dev_list = []
    if ret == 0:
        ibd_dev_list = msg.split()    
    return ibd_dev_list    


def ha_check_system():
    debug('Enter  ha_check_system')

    cmd = 'df -h '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'cat /proc/meminfo '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'vmstat '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'iostat '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'ifconfig '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_shared_storage():
    debug('Enter ha_check_sharedstorage')

    cmd = 'lsscsi '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_ibd():
    debug('Enter ha_check_ibd')
    cmd = 'ps -ef|grep ibd ' 
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = CMD_IBDMANAGER_STAT
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_md():
    debug('Enter ha_check_md')
    cmd = 'cat /proc/mdstat ' 
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_vscaler():
    debug('Enter ha_check_vscaler')
    cmd = 'dmsetup table ' 
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_dedupFS():
    debug('Enter ha_check_dedupFS')

    cmd = 'mount | grep dedup | grep -v grep '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_nfs():
    debug('Enter ha_check_nfs')

    cmd = 'service nfs-kernel-server status '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_service_ip():
    debug('Enter ha_check_service_ip')

    cmd = 'ip addr show '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_pacemaker():
    debug('Enter ha_check_pacemaker')

    cmd = 'service pacemaker status '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_check_corosync():
    debug('Enter ha_check_corosync')

    cmd = 'service corosync status '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret

def ha_check_cluster():
    debug('Enter ha_check_cluster')

    cmd = 'crm_mon -1 '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'crm node list '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret

def ha_check_quorum():
    debug('Enter ha_check_quorum')    
    cmd = 'crm_node -q '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


def ha_cleanup_resource(ads_name):
    debug('Enter ha_cleanup_resource')

    if ads_name == None or len(ads_name) == 0:
        debug('ERROR: ads_name is empty')
        return 1
    
    # crm resource cleanup vCenter1201_tis29-50b-hybrid_group
    cmd = 'crm resource cleanup ' + ads_name + '_group '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return ret


# to check the health for the first HA enabling
def ha_check_health():
    debug('Enter ha_check_health')

    service_health = True
    dedup_health = True
    ip_health = True
    
    ret = ha_check_enabled()
    if ret == True:
        debug("WARN: HA has been enabled")
        return 0

    ret = ha_check_maintenance_mode()
    if ret == True:
        debug("WARN: this node is in maintenance mode")
        return 0

    # json file format:
    #"volumeresources": [
    #    {
    #        "containeruuid": "vc13417_AAA-111-53B-tis18-Hybrid-111",
    #        "dedupfsmountpoint": "/exports/AAA-111-53B-tis18-Hybrid-111",
    #        "exporttype": "NFS",
    #        "serviceip": "10.121.148.51"
    #        "volumetype": "HYBRID"
    #    }
    #]
    (containeruuid, dedupfsmountpoint, exporttype, service_ip, volumetype) = ha_retrieve_config()
    debug('%s %s %s %s %s' %(containeruuid, dedupfsmountpoint, exporttype, service_ip, volumetype))
    if exporttype == 'NFS':
        cmd = 'ps aux | grep nfsd | grep -v grep '
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        if ret != 0:
            debug("WARN: NFS is not running")
            service_health = False

    dedupfs_found = False
    if dedupfsmountpoint != '':
        cmd = 'mount | egrep "zfs|dedup|btrfs|ext4" | grep -v grep '
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        if ret != 0:
            debug("WARN: no dedupFS")
            dedup_health = False
        else:
            for the_line in msg:
                the_list = the_line.split(' ')
                the_dev = the_list[0]
                the_mntpoint = the_list[2]
                if the_mntpoint == dedupfsmountpoint:
                    dedupfs_found = True

                #TODO: test to read and write the dedupFS
                #dd if=/dev/md3  of=/dev/null  bs=512 count=1 iflag=direct,nonblock
                #sub_cmd = 'dd if=' + the_dev + ' of=/dev/null  bs=512 count=1 iflag=direct,nonblock' 
                #(sub_ret, sub_msg) = runcmd(sub_cmd, print_ret=True, lines=True)
                #if sub_ret != 0:
                #    debug("WARN: read error from dedupFS")
                #    dedup_health = False
    if dedupfsmountpoint != '' and dedupfs_found == False:
        debug('WARN: could not find dedupFS %s' % dedupfsmountpoint)
        dedup_health = False

    service_ip_found = False
    if service_ip != '':
        cmd = 'ip addr show | grep "scope global secondary" | grep -v grep '
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        if ret != 0:
            debug('WARN: no service_ip')
            ip_health = False
        else:
            for the_line in msg:
                if the_line.find(' ' + service_ip + '/') >= 0:
                    service_ip_found = True
    if service_ip != '' and service_ip_found == False:
        debug('WARN: could not find service_ip %s' % service_ip)
        ip_health = False

    if service_health == True and dedup_health == True and ip_health == True:
        return 0
    else:
        return 1


def usage():
    print "Usage:" + sys.argv[0] + " enable_start|enable_end|disable_start|disable_end|ha_start_begin|remove_orphan|cleanup_unmanaged_resource|check_consistency|force_failover|reboot|handle_enable_ha_failure|cleanup_vol_ha_status|show_servicename|disable_ha_flag|check_ha_flag|upgrade_begin|upgrade_end "


#########################################################
#                    START HERE                         #
#########################################################
num_input = 2

if len(sys.argv) < num_input:
    usage()
    sys.exit(1)

if sys.argv[1] == 'ha_start_begin':
    num_input = 3

if sys.argv[1] == 'remove_orphan':
    ha_remove_orphan(True)
    sys.exit(0)

if sys.argv[1] == 'ha_set_start_up_preparation':
    ha_set_start_up_preparation()
    sys.exit(0)

if sys.argv[1] == 'check_ha_flag':
    if len(sys.argv) < 3:
        debug('ERROR: missing volume uuid')
        sys.exit(2)
    ret = check_vol_ha_flag(sys.argv[2])
    sys.exit(ret)

if sys.argv[1] == 'cleanup_unmanaged_resource':
    ha_cleanup_unmanaged_resource()
    sys.exit(0)

if sys.argv[1] == 'show_servicename':
    ha_add_servicename_crm()
    sys.exit(0)

if sys.argv[1] == 'disable_ha_flag':
    ha_disable_ha_flag()
    sys.exit(0)

if sys.argv[1] == 'check_consistency':
    ha_check_config_consistency()
    sys.exit(0)

if sys.argv[1] == 'upgrade_begin':
    ret = ha_upgrade_begin()
    sys.exit(ret)

if sys.argv[1] == 'upgrade_end':
    ret = ha_upgrade_end()
    sys.exit(ret)

if sys.argv[1] == 'reboot':
    if os.path.exists("/tmp/doing_teardown") == False:
        ha_reset_node('IBD_UPGRADE_FAILURE')
    else:
        debug('doing teardown, abort ha_reset_node for IBD_UPGRADE_FAILURE')
    sys.exit(0)

if sys.argv[1] == 'handle_enable_ha_failure':
    if len(sys.argv) < 3:
        debug('ERROR: missing volume uuid')
        sys.exit(1)
    ha_handle_enable_ha_failure(sys.argv[2])
    sys.exit(0)

if sys.argv[1] == 'cleanup_vol_ha_status':
    cleanup_vol_ha_status()
    sys.exit(0)

if sys.argv[1] == 'test':
    tiebreakerip = ha_get_tiebreakerip()
    print tiebreakerip
    sys.exit(0)

if sys.argv[1] == 'test1':
    (stretchcluster_flag, availability_flag, tiebreakerip) = ha_stretchcluster_config()
    print stretchcluster_flag
    print availability_flag
    print str(tiebreakerip)
    sys.exit(0)

if sys.argv[1] == 'test2':
    (volume_type, volume_uuid, ilio_uuid, display_name) = get_volume_info()
    tiebreakerip = ha_get_tiebreakerip()
    scl_timeout = ha_get_scl_timeout()
    nodename = ha_get_local_node_name()
    ret = ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)
    print ret
    sys.exit(0)

if sys.argv[1] == 'test3':
    (volume_type, volume_uuid, ilio_uuid, display_name) = get_volume_info()
    tiebreakerip = ha_get_tiebreakerip()
    scl_timeout = ha_get_scl_timeout()
    nodename = ha_get_local_node_name()
    ret = ha_check_stretchcluster_lock(tiebreakerip, volume_uuid, nodename)
    print ret
    sys.exit(0)

if sys.argv[1] == 'test4':
    (volume_type, volume_uuid, ilio_uuid, display_name) = get_volume_info()
    tiebreakerip = ha_get_tiebreakerip()
    scl_timeout = ha_get_scl_timeout()
    nodename = ha_get_local_node_name()
    ret = ha_release_stretchcluster_lock(tiebreakerip, volume_uuid)
    print ret
    sys.exit(0)

if sys.argv[1] == 'force_failover':
    if len(sys.argv) < 4:
        sys.exit(1)
    ret = ha_force_failover(sys.argv[2], sys.argv[3])
    sys.exit(ret)

debug('INFO: start HA health check: %s' %sys.argv[1])
cfgfile = open(ATLAS_CONF, 'r')
s = cfgfile.read()
cfgfile.close()
try:
    node_dict = json.loads(s)
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        ilio_dict = node_dict
    ha = ilio_dict.get('ha')
    node_name = ilio_dict.get('uuid')
    debug('node_name is %s' %node_name)
except ValueError as err:
    pass

ret = ha_check_system()
ret = ha_check_enabled()
ret = ha_check_shared_storage()
ret = ha_check_ibd()
ret = ha_check_md()
ret = ha_check_vscaler()
ret = ha_check_dedupFS()
ret = ha_check_nfs()
ret = ha_check_service_ip()
ret = ha_check_pacemaker()
ret = ha_check_corosync()
ret = ha_check_cluster()
ret = ha_check_quorum()


# read the pill
retry = 0
max_num_retry = 1
found_flag = False
ibd_dev_list = get_working_ibds()
ha_check_arb(ibd_dev_list)

rc = 0
if sys.argv[1] == 'ha_start_begin':
    ads_name = sys.argv[2]
    # force to make the resource status consistent
    # ha_cleanup_resource(ads_name)
elif sys.argv[1] == 'enable_start':
    rc = ha_check_health()

debug('INFO: end HA health check')
sys.exit(rc)
