#-------------------------------------------------------------------------------
# Name:        vv-extend
# Purpose:      handle volume extend request from users
#
# Author:      huant
#
# Created:     06/02/2015
# Copyright:   (c) huant 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os, sys
import subprocess
import json
import logging
import httplib
import base64
import urllib

sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
from ha_util import *
from time import sleep
import traceback
from atl_util import * 

CMD_MDADM = "/sbin/mdadm"
CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_S_STOP_ONE = CMD_IBDMANAGER + " -r a -d"
CMD_IBDMANAGER_S_START = CMD_IBDMANAGER + " -r a -u"
CMD_IBDMANAGER_S_GET = CMD_IBDMANAGER + " -r a -s get"
CMD_IBDMANAGER_S_GET_WD   = CMD_IBDMANAGER + " -r a -s get_wd"
CMD_IBDMANAGER_S_GET_WUD = CMD_IBDMANAGER + " -r a -s get_wud"
CMD_IBDMANAGER_EN_HOOK = CMD_IBDMANAGER + " -r a -h enable"
CMD_IBDMANAGER_DIS_HOOK = CMD_IBDMANAGER + " -r a -h disable"

VV_EXTEND     = "python /opt/milio/atlas/roles/virtvol/vv-extend.pyc"

DDP_MOUNT_CMD = ("mount -t dedup -o rw,noblocktable,noatime,nodiratime,"
                 "timeout=180000,dedupzeros,thin_reconstruct,data=ordered,"
                 "commit=180,errors=remount-ro")

EXTEND_VOL_MEMORY = "for mem in `/bin/grep offline /sys/devices/system/memory/*/state | /usr/bin/cut -f 1 -d:`; do echo $mem; echo online > $mem; done"

STOP_MD_MONITOR = "ps aux|grep md_monitor|grep -v grep | awk '{print $2}' | xargs kill"
START_MD_MONITOR = CMD_MDADM + " --monitor -d 5 -yfsp /opt/milio/atlas/scripts/md_monitor.sh"

IBD_AGENT_CONF = "/etc/ilio/ibdagent.conf"
POOL_LOCKFILE = "/etc/ilio/pool_lockfile"
EXTEND_STATUS_FILE = "/tmp/extend_status_update"
DEDUP_VOLUME_EXTEND_LOCKFILE = "/etc/ilio/dedup_volume_extend_lockfile"

USX_CONF = '/etc/ilio/atlas.json'

INVALID_INPUT_ERR = 1
INSUFFICIENT_SPACE_ERR = 2
RESIZE_RAID1_ERR = 3
RESIZE_RAID5_ERR = 4
RESIZE_FS_ERR = 5
IBD_NOT_WORKING = 6
VOL_IS_BUSY = 7

LOG_FILENAME = '/var/log/usx-vv-extend.log'
set_log_file(LOG_FILENAME)

def usage():
    debug('Wrong command: %s' % str(sys.argv))
    print('Usage: python /opt/milio/atlas/roles/virtvol/vv-extend.pyc vol_res_uuid')

def run_mdadm(cmd, print_ret=False, lines=False, input_string=None, block=True, timeout=None):
    (ret, msg) = runcmd(cmd, print_ret, lines, input_string, block, timeout)
    if 0 != ret:
        sleep(1)
        (ret, msg) = runcmd(cmd, print_ret, lines, input_string, block, timeout)
    return (ret, msg)
    
def wait_for_raid_resynced():
    cnt = 600
    while cnt > 0:
        (ret, msg) = runcmd('cat /proc/mdstat', print_ret=True)
        if 0 == ret and ("recovery = " in msg or "resync = " in msg or "resync=DELAYED" in msg):
            debug('Waiting for resync to finish')
            sleep(6)
            cnt = cnt - 1
        else:
            break

def has_partition_number(dev, cnt, save_partition_file=None):
    while cnt > 0:
        (ret, msg) = runcmd("/sbin/parted -s -- " + dev + " print", print_ret=True, lines=True)
        if 0 == ret:
            for line in msg:
                if 'raid' in line and line.strip().split()[0] == '1':
                    if save_partition_file:
                        save_partition_file.write(line + '\n')
                    return True
        sleep(2)
        cnt = cnt - 1
    return False

def restore_partition_table(fname, dev):
    lines = None
    with open(fname, 'r') as fd:
        lines = fd.readlines()
    if not lines:
        return
    for line in lines:
        tmp = line.split()
        start = tmp[1]
        end = tmp[2]
        (ret, msg) = runcmd("/sbin/parted -s -- " + dev + " mklabel gpt mkpart primary " + start + " " + end + " set 1 raid on", print_ret=True)
        break

def readd_ibd_dev(raid5_md_dev, raid1_md_dev, ibd_dev):
    cnt = 12
    while cnt > 0:
        if os.path.exists(ibd_dev + "p1"):
            break
        debug('Readd_raid1: waiting for ' + ibd_dev + "p1")
        sleep(5)
        cnt = cnt - 1
    run_mdadm(CMD_MDADM + " --manage " + raid1_md_dev + " --re-add " + ibd_dev + "p1", print_ret=True)
    wait_for_raid_resynced()

    (ret, msg) = run_mdadm(CMD_MDADM + " --assemble " + raid1_md_dev + " --run " + ibd_dev + "p1", print_ret=True)
    (ret, msg) = run_mdadm(CMD_MDADM + " --grow --size max " + raid1_md_dev, print_ret=True)
    wait_for_raid_resynced()

    have_partition = has_partition_number(raid1_md_dev, 5)
    if not have_partition:
        (ret, msg) = runcmd("/sbin/parted -s -- " + raid1_md_dev + " mklabel gpt mkpart primary 2048s 100% set 1 raid on", print_ret=True)

    cnt = 12
    while cnt > 0:
        if os.path.exists(raid1_md_dev + "p1"):
            break
        debug('readd_ibd_dev: waiting for ' + raid1_md_dev + "p1")
        sleep(5)
        cnt = cnt - 1
    run_mdadm(CMD_MDADM + " --manage " + raid5_md_dev + " --re-add " + raid1_md_dev + "p1", print_ret=True)
    wait_for_raid_resynced()

def wait_for_ibd_conneciton(ibd_dev):
    cnt = 12
    while cnt > 0:
        (ret, msg) = runcmd(CMD_IBDMANAGER_S_GET_WD, print_ret=True, lines=True)
        for line in msg:
            if line == ibd_dev:
                return 0
        sleep(5)
        cnt -= 1
    return 1

def extend_ibd_dev(ibd_dev, ipaddr, encoded_raidbrick, raid1_md_dev, raid5_md_dev):
    # resize IBD device
    (ret, msg) = runcmd(CMD_IBDMANAGER_S_STOP_ONE + " " + ibd_dev, print_ret=True)

    cmd = '/usxmanager/commands?command=python%20%2Fopt%2Fmilio%2Fatlas%2Froles%2Faggregate%2Fagexport.pyc&arguments=' + '%20' + '-E' + '%20' + encoded_raidbrick + '&type=os'
    conn = httplib.HTTPConnection(ipaddr + ":8080")
    conn.request("POST", cmd)
    res = conn.getresponse()
    if int(res.status) != 200:
        debug('ERROR: result status = %s' % res.status)
        (ret, msg) = runcmd(CMD_IBDMANAGER_S_START, print_ret=True)
        readd_ibd_dev(raid5_md_dev, raid1_md_dev, ibd_dev)
        return 1

    (ret, msg) = runcmd(CMD_IBDMANAGER_S_START, print_ret=True)

    ret = json.loads(res.read())["retCode"]
    if ret != 0:
        debug('ERROR: retCode = %s' % str(ret))
        readd_ibd_dev(raid5_md_dev, raid1_md_dev, ibd_dev)
        return 1

    ret = wait_for_ibd_conneciton(ibd_dev)
    if ret != 0:
        debug('%s could not be built up!' % ibd_dev)
        return 1
    
    wait_for_raid_resynced()

    # We expect to see only 1 partition in a partition table. Before we remove it, we save it so we can restore the table if we fail to create a new table.
    tmp_fname = '/tmp/save_gpt'
    have_partition = False
    with open(tmp_fname, "w") as gpt_file:
        have_partition = has_partition_number(ibd_dev, 5, gpt_file)

    (ret, msg) = runcmd("cat /proc/mdstat", print_ret=True)

    # remove old partition table and create a new one
    if have_partition:
        (ret, msg) = runcmd("/sbin/parted -s -- " + ibd_dev + " rm 1", print_ret=True)
        (ret, msg) = runcmd("/sbin/parted -s -- " + ibd_dev + " check 1", print_ret=True)

    (ret, msg) = runcmd("/sbin/parted -s -- " + ibd_dev + " mklabel gpt mkpart primary 2048s 100% set 1 raid on", print_ret=True)
    if 0 != ret:
        restore_partition_table(tmp_fname, ibd_dev)

    # wait for first IBD partition to appear
    cnt = 12
    ready = 0
    while cnt > 0:
        sleep(2)
        (ret, msg) = runcmd("/sbin/parted -s -- " + ibd_dev + " print", print_ret=True, lines=True)
        if 0 == ret:
            for line in msg:
                if 'raid' in line and line.strip().split()[0] == '1':
                    debug('%s partition is ready' % ibd_dev)
                    ready = 1
                    break
            if ready == 1:
                break
        cnt -= 1

    (ret, msg) = run_mdadm(CMD_MDADM + " --examine " + ibd_dev + "p1", print_ret=True)
    return ret

def extend_RAID1_dev(raid1_dev, ibd_data_lst, raid5_dev):
    # detach RAID1 from RAID5
    (ret, msg) = run_mdadm(CMD_MDADM + " --manage " + raid5_dev + " --fail " + raid1_dev + "p1", print_ret=True)
    (ret, msg) = run_mdadm(CMD_MDADM + " --manage " + raid5_dev + " --remove " + raid1_dev + "p1", print_ret=True)
    (ret, msg) = run_mdadm(CMD_MDADM + " --stop " + raid1_dev, print_ret=True)

    ibds_p1 = ""
    for (ibd_dev, ipaddr, encoded_raidbrick) in ibd_data_lst:
        ret = extend_ibd_dev(ibd_dev, ipaddr, encoded_raidbrick, raid1_dev, raid5_dev)
        if 0 != ret:
            debug('Error: cannot extend %s ' % ibd_dev)
            return ret
        ibds_p1 = ibds_p1 + ' ' + ibd_dev + 'p1'

    (ret, msg) = run_mdadm(CMD_MDADM + " --assemble " + raid1_dev + " --run --force " + ibds_p1, print_ret=True)
    (ret, msg) = run_mdadm(CMD_MDADM + " --grow --assume-clean --size max " + raid1_dev, print_ret=True)
    wait_for_raid_resynced()

    (ret, msg) = runcmd("/sbin/parted -s -- " + raid1_dev + " print", print_ret=True)

    tmp_fname = '/tmp/save_gpt'
    have_partition = False
    with open(tmp_fname, "w") as gpt_file:
        have_partition = has_partition_number(raid1_dev, 5, gpt_file)
    if have_partition:
        (ret, msg) = runcmd("/sbin/parted -s -- " + raid1_dev + " rm 1", print_ret=True)
        (ret, msg) = runcmd("/sbin/parted -s -- " + raid1_dev + " check 1", print_ret=True)
    (ret, msg) = runcmd("/sbin/parted -s -- " + raid1_dev + " mklabel gpt mkpart primary 2048s 100% set 1 raid on", print_ret=True)
    if 0 != ret:
        restore_partition_table(tmp_fname, raid1_dev)

    cnt = 12
    ready = 0
    while cnt > 0:
        sleep(2)
        (ret, msg) = runcmd("/sbin/parted -s -- " + raid1_dev + " print", print_ret=True, lines=True)
        if 0 == ret:
            for line in msg:
                if 'raid' in line and line.strip().split()[0] == '1':
                    debug('%s partition is ready' % raid1_dev)
                    ready = 1
                    break
            if ready == 1:
                break
        cnt -= 1

    (ret, msg) = run_mdadm(CMD_MDADM + " --examine " + raid1_dev + "p1", print_ret=True)

    # add raid1 dev back to raid5 dev
    (ret, msg) = run_mdadm(CMD_MDADM + " --manage " + raid5_dev + " --re-add " + raid1_dev + "p1", print_ret=True)
    if 0 != ret:
        (ret, msg) = run_mdadm(CMD_MDADM + " --manage " + raid5_dev + " --add " + raid1_dev + "p1", print_ret=True)
    if 0 != ret:
        debug('Error: add back failed')
        return ret
    
    wait_for_raid_resynced()

    return 0

def resize_raid5_device(raid5_dev):
    (ret, msg) = run_mdadm(CMD_MDADM + " --grow --assume-clean --size max " + raid5_dev, print_ret=True)
    wait_for_raid_resynced()
    (ret, msg) = runcmd("/sbin/pvresize " + raid5_dev, print_ret=True)
    return ret

def get_RAID5_dev():
    (ret, msg) = runcmd('cat /proc/mdstat', print_ret=True, lines=True)
    for line in msg:
        if 'active raid5' in line:
            return "/dev/" + line.split()[0]
    return None

def check_raid_dev_state(raid_dev):
    (ret, msg) = run_mdadm(CMD_MDADM + " -D " + raid_dev, print_ret=True, lines=True)
    if ret != 0:
        debug('Error reading md device %s' % raid_dev)
        return ret
    for line in msg:
        if line.strip().startswith("State :"):
            if 'FAILED' in line or 'degraded' in line:
                return 1
    return 0

'''
def _load_conf():
    try:
        cfg_file = open(USX_CONF, 'r')
        configure = json.load(cfg_file)
        cfg_file.close()
    except:
        debug("CAUTION: Cannot load the configure json file")
        return None
    return configure
'''

def write_conf(configure):
    configure = json.dumps(configure, sort_keys=True, indent=4, separators=(',', ': '))
    try:
        with open(USX_CONF, 'w') as fd:
            fd.write(configure)
    except:
        debug('CAUTION: Cannot write the configure json file')

def is_snapshot_enabled():
    config = load_atlas_conf()
    if config == None:
    	return False

    snapshot_enabled = config['volumeresources'][0].get('snapshotenabled')

    is_snapshot_supported = False
    out = ['']
    do_system('modprobe dm-thin-pool')
    rc = do_system('dmsetup targets | grep thin', out)
    if rc == 0:
        rc = do_system('lvm version|grep LVM', out)
        if rc == 0 and out[0].split()[2] >= '2.02.98':
            is_snapshot_supported = True

    if snapshot_enabled and is_snapshot_supported:
        debug('Thin supported!')
        return True
    else:
        debug('Thin not supported!')
        return False

def restore_service(nfs_service, scst_service):
    if nfs_service:
        cmd = 'service nfs-kernel-server start'
        (ret, msg) = runcmd(cmd, print_ret=True)

    if scst_service:
        cmd = '/etc/init.d/scst start'
        (ret, msg) = runcmd(cmd, print_ret=True)

def extend_volume():
    uuid_2_ibd_dev = {}
    (ret, msg) = runcmd(CMD_IBDMANAGER_S_GET_WUD, print_ret=True, lines=True)
    for line in msg:
        tmp = line.split()
        uuid_2_ibd_dev[tmp[0]] = tmp[1][5:]

    uuid_2_ibd_ipaddr = {}
    (ret, msg) = runcmd(CMD_IBDMANAGER_S_GET, print_ret=True, lines=True)
    uuid = None
    for line in msg:
        line = line.strip()
        if line.startswith("uuid:") and not uuid:
            uuid = line.split(':')[1]
            if not uuid_2_ibd_dev.has_key(uuid):
                debug('Error: %s not working' % uuid)
                return IBD_NOT_WORKING
        elif line.startswith("ip:") and uuid:
            uuid_2_ibd_ipaddr[uuid] = line.split(':')[1]
            uuid = None

    try:
        # Unmount Dedup FS before resizing it
        (ret, msg) = runcmd('mount | grep "type dedup" | grep -v grep', print_ret=True, lines=True)
        if 0 != ret:
            return RESIZE_FS_ERR
        mount_dedup = msg[0].strip().split()
        if len(mount_dedup) >= 5 and mount_dedup[4] == "dedup":
            dedupfs_dev = mount_dedup[0]
            # stop nfs service
            cmd = 'service nfs-kernel-server status'
            (ret, msg) = runcmd(cmd, print_ret=True)
            nfs_service = False
            if 0 == ret:
                nfs_service = True
                cmd = 'service nfs-kernel-server stop'
                (ret, msg) = runcmd(cmd, print_ret=True)

            # stop scst service
            scst_service = False
            cmd = '/etc/init.d/scst status'
            (ret, msg) = runcmd(cmd, print_ret=True)
            if 0 == ret:
                scst_service = True
                cmd = '/etc/init.d/scst stop'
                (ret, msg) = runcmd(cmd, print_ret=True)

            cmd = 'umount ' + mount_dedup[2]
            (ret, msg) = runcmd(cmd, print_ret=True)
            if ret != 0:
                sleep(2)
                cmd = 'lsof ' + mount_dedup[2]
                (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                if ret == 0:
                    cmd = 'kill -9 `lsof -t ' + mount_dedup[2] + '`'
                    runcmd(cmd, print_ret=True)

                cnt = 16
                while cnt > 0:
                    cmd = 'umount ' + mount_dedup[2]
                    (ret, msg) = runcmd(cmd, print_ret=True)
                    if ret == 0:
                        break
                    sleep(2)
                    cnt -= 1

                if cnt == 0:
                    restore_service(nfs_service, scst_service)
                    return RESIZE_FS_ERR

        # Get raidplan
        cmd = "/usxmanager/usx/inventory/volume/resources/" + sys.argv[1] + "/extend/newplan"
        conn = httplib.HTTPConnection("127.0.0.1:8080")
        conn.request("GET", cmd)
        res = conn.getresponse()
        data = json.loads(res.read())
        debug('Received from AMC: ', json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))
        if 'plandetail' not in data:
            debug('No plan detail in RAID plans')
            restore_service(nfs_service, scst_service)
            return INVALID_INPUT_ERR
        plandetail = json.loads(data['plandetail'])
        if 'subplans' not in plandetail:
            debug('No subplans in plan detail')
            restore_service(nfs_service, scst_service)
            return INVALID_INPUT_ERR
            
        subplans = plandetail['subplans']
        raidbricks = subplans[0]['raidbricks']

        ibd_devs = {}
        for raidbrick in raidbricks:
            uuid = raidbrick['euuid']
            ipaddr = uuid_2_ibd_ipaddr[uuid]
            ibd_dev = uuid_2_ibd_dev[uuid]
            encoded_raidbrick = base64.urlsafe_b64encode(json.dumps(raidbrick))
            ibd_devs[ibd_dev] = ('/dev/'+ibd_dev, ipaddr, encoded_raidbrick)

        raid5_md_dev = get_RAID5_dev()

        raid1_md_devs = []
        (ret, msg) = runcmd('cat /proc/mdstat', print_ret=True, lines=True)
        for line in msg:
            if 'active raid1' in line:
                tmp = line.split()
                ibd_data_lst = []
                if len(tmp) > 4:
                    ibd_dev = tmp[4][:-5]
                    ibd_data = ibd_devs[ibd_dev]
                    ibd_data_lst.append(ibd_data)
                    if len(tmp) > 5:
                        ibd_dev = tmp[5][:-5]
                        ibd_data = ibd_devs[ibd_dev]
                        ibd_data_lst.append(ibd_data)
                ret = extend_RAID1_dev('/dev/'+tmp[0], ibd_data_lst, raid5_md_dev)
                if ret != 0:
                    debug('Error: cannot resize raid1 device %s' % raid1_md_devs)
                    return RESIZE_RAID1_ERR
                raid1_md_devs.append(tmp[0])

        ret = resize_raid5_device(raid5_md_dev)
        if ret != 0:
            debug('Error: cannot resize raid5 device %s' % raid5_md_dev)
            return RESIZE_RAID5_ERR

        if check_raid_dev_state(raid5_md_dev) != 0:
            s = ""
            if len(raid1_md_devs) > 0:
                elements = ['{0}p1'.format(element) for element in raid1_md_devs]
                s = ' '.join(elements)
                (ret, msg) = run_mdadm(CMD_MDADM + " --assemble --run --force " + raid5_md_dev + " " + s, print_ret=True)

        (ret, msg) = runcmd('cat /proc/mdstat', print_ret=True)

        rc = 0
        cmd = 'pvs --aligned --noheadings --nosuffix --units=m' # MiB = 1024*1024
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        free_size = None
        for line in msg:
            if 'dedupvg' in line:
                free_size = line.strip().split()[5]
                break
        if free_size:
            free_size = float(free_size)

            if is_snapshot_enabled():
                vgpool = '/dev/mapper/dedupvg-dedupvgpool'
                cmd = 'lvextend -L +%sm %s' % (free_size, vgpool)
                (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                if 5 == ret: #Not enough space
                    rc = ret

            cmd = 'lvextend -L +%sm %s' % (free_size, mount_dedup[0])
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
            if 5 == ret:
                rc = ret

            (ret, msg) = runcmd(EXTEND_VOL_MEMORY, print_ret=True)

        cmd = '/opt/milio/bin/e2fsck -f -n -Z 0 %s' % dedupfs_dev
        (ret, msg) = runcmd(cmd, print_ret=True)

        cmd = '/opt/milio/bin/resize2fs -d 3 -z %s' % dedupfs_dev
        (ret, msg) = runcmd(cmd, print_ret=True)
        if 0 != ret:
            rc = ret

        cmd = '/opt/milio/bin/e2fsck -f -n -Z 0 %s' % dedupfs_dev
        (ret, msg) = runcmd(cmd, print_ret=True)

        cmd = DDP_MOUNT_CMD + " " + mount_dedup[0] + " " + mount_dedup[2]
        (ret, msg) = runcmd(cmd, print_ret=True)

        restore_service(nfs_service, scst_service)

        if rc != 0:
            return RESIZE_FS_ERR
        return rc
    except ValueError as e:
        debug('%s' % e)
        return INVALID_INPUT_ERR

def update_status_file(status):
    fd = None

    try:
        fd = open(EXTEND_STATUS_FILE, "w")
        fd.write(status)
    except:
        debug("update_status_file: cannot update status file")

    if fd != None:
        fd.close()

def update_config_file(vol_id):
    configure = load_atlas_conf()

    uuid = configure['volumeresources'][0]['uuid']
    if uuid != vol_id:
        return False

    cmd = 'pvs --aligned --noheadings --nosuffix --units=g' # GiB
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    total_size = None
    for line in msg:
        if 'dedupvg' in line:
            total_size = line.strip().split()[4]
    if not total_size:
        return False
    total_size = float(total_size)
    total_size = int(total_size + 0.5)

    configure['volumeresources'][0]['volumesize'] = total_size
    write_conf(configure)

    return True

def main():
    debug("%s" % sys.argv)

    if len(sys.argv) < 2:
        usage()
        return INVALID_INPUT_ERR
    elif len(sys.argv) == 2:
        cmd_str = VV_EXTEND + " " + sys.argv[1] + " async"
        subprocess.Popen(cmd_str.split(),
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         close_fds=True)
        return 0
    elif len(sys.argv) != 3:
        return INVALID_INPUT_ERR

    update_volume_status(sys.argv[1], VOL_STATUS_WARN)
    update_status_file("WARN")
    
    node_lock_fd = None
    cnt = 30
    while cnt > 0:
        node_lock_fd = node_trylock(POOL_LOCKFILE)
        if node_lock_fd != None:	# failed to try-lock
            break;
        sleep(1)
        cnt -= 1
    if cnt <= 0:
        debug("node_trylock: cannot hold lock")
        rc = VOL_IS_BUSY
    else:
        (ret, msg) = runcmd(STOP_MD_MONITOR, print_ret=True)
        (ret, msg) = runcmd(CMD_IBDMANAGER_DIS_HOOK, print_ret=True)

        # Fix for 24163. Add another lock for dedupfs umount/mount.
        cnt = 3
        while cnt > 0:
            debug('Start lock for dedupfs status change in extend_volume.')
            node_lock_fd_1 = node_trylock(DEDUP_VOLUME_EXTEND_LOCKFILE)
            if node_lock_fd_1 != None:
                break;
            sleep(1)
            cnt -= 1
        if cnt <= 0:
            debug("WARNING: node_trylock: cannot hold lock for dedupfs status change in extend_volume.")

        rc = extend_volume()

        # Release lock for dedupfs status change in extend_volume.
        if node_lock_fd_1 != None:
            node_unlock(node_lock_fd_1)

        (ret, msg) = runcmd(CMD_IBDMANAGER_EN_HOOK, print_ret=True)
        (ret, msg) = runcmd(START_MD_MONITOR, print_ret=True)

        node_unlock(node_lock_fd)

	if rc == 0:
            update_config_file(sys.argv[1])

    cmd = "curl -k -X PUT http://127.0.0.1:8080/usxmanager/workflows/volume/" + sys.argv[1] + "/extend/status?success="

    if rc == 0:
        cmd += "true"
    elif rc == 1:
        message=urllib.quote_plus("Got wrong parameters!")
        cmd += "false&message="+message
    elif rc == 2:
        message=urllib.quote_plus("Insufficient space!")
        cmd += "false&message="+message
    elif rc == 3:
        message=urllib.quote_plus("Failed to resize Raid1!")
        cmd += "false&message="+message
    elif rc == 4:
        message=urllib.quote_plus("Failed to resize Raid5!")
        cmd += "false&message="+message
    elif rc == 5:
        message=urllib.quote_plus("Failed to resize file system!")
        cmd += "false&message="+message
    elif rc == 6:
        message=urllib.quote_plus("IBD connections down!")
        cmd += "false&message="+message
    elif rc == 7:
        message=urllib.quote_plus("Volume is busy!")
        cmd += "false&message="+message
        
    (ret, msg) = runcmd(cmd, print_ret=True)

    if rc == 0:
        update_volume_status(sys.argv[1], VOL_STATUS_OK)
        update_status_file("OK")
    else:
        update_volume_status(sys.argv[1], VOL_STATUS_FATAL)
        update_status_file("FAIL")

    return 0

if __name__ == '__main__':
    try:
        rc = main()
    except:
        debug(traceback.format_exc())
        sys.exit(1)
    sys.exit(rc)

