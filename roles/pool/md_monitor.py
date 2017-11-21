#!/usr/bin/python
import json
import sys
import re
import copy
import datetime
import urllib2

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_md import *
from cmd import *
from atl_alerts import *

sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *

AMC_volresources = "volumeresources"
CMD_ATLASROLE_DIR = "/opt/milio/atlas/roles"
CMD_VIRTUALPOOL = CMD_ATLASROLE_DIR + "/pool/cp-load.pyc"
CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_STAT_RWWUD = CMD_IBDMANAGER + " -r a -s get_rwwud"
CMD_IBDMANAGER_STAT_UD = CMD_IBDMANAGER + " -r a -s get_ud"
ATLAS_CONF = '/etc/ilio/atlas.json'
LOCAL_AGENT = 'http://127.0.0.1:8080'
VOLUME_STATUS_UPDATE_API = '/usxmanager/usx/status/update'
ALERT_API = '/usxmanager/alerts'
SYNC_FLAG_FILE = "/tmp/sync_flag_file-"


def try_readd_ibd(vv_uuid, ibd_uuid):
    cmd_str = 'python ' + CMD_VIRTUALPOOL + ' vv_readd ' + vv_uuid + ' ' + ibd_uuid
    readd = do_system(cmd_str)
    debug("try_readd_ibd(): vv_readd return: %d" % readd)
    if readd == 2:  # could not hold the lock, try again.
        time.sleep(3)
        readd = do_system(cmd_str)


def check_raid5_rebuild_status(vv_uuid, detail, rwwud_mapping):
    readd = False
    for property in detail:
        if '/dev/md' in property and detail[property] == 'active':
            raid1_detail = {}
            raid_detail_nohung(property, raid1_detail)
            for ibd in rwwud_mapping:
                if raid1_detail.has_key(ibd) and raid1_detail[ibd] == 'active':
                    rwwud_mapping[ibd] = 'active'
    for ibd in rwwud_mapping:
        if rwwud_mapping[ibd] != 'active':
            debug('%s is not fully active, %s(%s) is working, try to readd it.' % (md_dev, ibd, rwwud_mapping[ibd]))
            try_readd_ibd(vv_uuid, rwwud_mapping[ibd])
            readd = True
    return readd


def check_raid1_rebuild_status(vv_uuid, detail, rwwud_mapping):
    readd = False

    debug("Total-Devices: %s" % detail['Total-Devices'])
    for ibd in rwwud_mapping:
        if (not detail.has_key(ibd) or (detail.has_key(ibd) and detail[ibd] != 'active')):
            debug('%s is not active, %s(%s) is working, try to readd it.' % (md_dev, ibd, rwwud_mapping[ibd]))
            try_readd_ibd(vv_uuid, rwwud_mapping[ibd])
            readd = True
    return readd


'''
def _send_alert_raid_sync(ilio_id, name, status, old_status, description):
    debug('START: Send alert')

    cmd = 'date +%s'
    (ret, epoch_time) = runcmd(cmd)
    epoch_time = epoch_time.rstrip('\n')

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
        "description"		:"",
        "service"		:"MONITORING",
        "alertTimestamp"	:"",
        "usxtype"		:"VOLUME"
    }

    ad["uuid"] = ilio_id + '-raid-sync-alert-' + str(epoch_time)
    ad["checkId"] = 'RAIDSYNC'
    ad["usxuuid"] = ilio_id
    ad["displayname"] = name
    ad["target"] = "servers." + ilio_id + ".raidsync"
    ad["alertTimestamp"] = epoch_time
    ad["usxtype"] = 'VOLUME'
    ad['status'] = status
    ad['oldStatus'] = old_status
    ad['description'] = description

    code, retval =call_rest_api(LOCAL_AGENT + ALERT_API, 'POST', json.dumps(ad))
    if code != '200':
        debug("ERROR : Failed to send alert.")
        ret = False
    else:
        ret = True

    debug('END: Send alert')
    return ret
'''


def get_uuid():
    ret = {}
    try:
        fp = open(ATLAS_CONF)
        jsondata = json.load(fp)
        fp.close()
        if jsondata.has_key('usx'):  # this is a volume
            ret['container'] = jsondata['usx']['uuid']
            ret['displayname'] = jsondata['usx']['displayname']
            if len(jsondata['volumeresources']) > 0:
                ret['resource'] = jsondata['volumeresources'][0]['uuid']
    except err:
        pass

    return ret


def update_raid5_status(uuid, containeruuid, status, details=''):
    retVal = True
    data = {}
    data['usxstatuslist'] = []
    data['usxuuid'] = uuid
    data['usxcontaineruuid'] = containeruuid
    data['usxtype'] = 'VOLUME_RESOURCE'

    raid_sync_status = {}
    raid_sync_status['name'] = 'RAID_SYNC_STATUS'
    raid_sync_status['value'] = status
    raid_sync_status['details'] = details
    data['usxstatuslist'].append(raid_sync_status)

    code, ret = call_rest_api(LOCAL_AGENT + VOLUME_STATUS_UPDATE_API, 'POST', json.dumps(data))
    if code != '200':
        debug("ERROR : REST API call failed ")
        retVal = False

    debug('END: Call rest API')
    return retVal


def get_config_from_jsonfile(cfg_file):
    cfg_file = open(cfg_file, 'r')
    cfg_str = cfg_file.read()
    cfg_file.close()
    cfg = json.loads(cfg_str)
    return cfg


def volume_is_ready():
    cmd_str = CMD_IBDMANAGER_STAT_UD
    out = ['']
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    ibd_list = {}
    for ln in lines:
        uuid_ibd = ln.split()
        if len(uuid_ibd) < 2:
            continue
        exportname = uuid_ibd[0]
        dev_name = uuid_ibd[1]
        ibd_list[dev_name] = exportname
    fastfailover_list = copy.deepcopy(ibd_list)
    md_list = []
    cmd_str = 'cat /proc/mdstat'
    do_system(cmd_str, out)
    lines = out[0].split('\n')
    for ln in lines:
        if "raid" in ln and "md" in ln:
            mddev = ln.split(" ")[0]
            md_list.append(mddev)

    for md in md_list:
        detail = {}
        md = "/dev/" + md
        raid_detail_nohung(md, detail)
        for dev in detail:
            if ("/dev/ibd" in dev or "/dev/md" in dev) and (detail[dev] != "active"):
                debug("%s:%s is not active" % (dev, detail[dev]))
                return False
            if ("/dev/ibd" in dev) and ibd_list.has_key(dev):
                ibd_list[dev] = "active"

    for ibd in ibd_list:
        if ibd_list[ibd] != "active":
            ibd_server_uuid = None
            if milio_config.is_fastfailover:
                lines_ibdserver = IBDManager.get_ibdserver_status()
                if lines_ibdserver is not None:
                    for line_detail in lines_ibdserver:
                        m = re.search('uuid:(\S+)', line_detail)
                        if m is not None:
                            ibd_server_uuid = m.group(1)
                            break
                    for dev_name, uuid in fastfailover_list.items():
                        if dev_name == ibd and uuid == ibd_server_uuid and IBDManager.is_ibd_working(uuid):
                            break
                    else:
                        debug("%s is not active" % ibd)
                        return False
                    continue

            debug("%s is not active" % ibd)
            return False

    return True


def rebuild_is_start(md_dev):
    dev = md_dev.split("/")[2]
    flag_file = "/tmp/rebuild_flag_file-" + dev
    if os.path.isfile(flag_file):
        return True
    else:
        return False


def set_rebuild_flag(md_dev):
    dev = md_dev.split("/")[2]
    flag_file = "/tmp/rebuild_flag_file-" + dev
    with open(flag_file, 'w') as f:
        f.write(dev)


def clean_rebuild_flag(md_dev):
    dev = md_dev.split("/")[2]
    flag_file = "/tmp/rebuild_flag_file-" + dev
    if os.path.isfile(flag_file):
        os.remove(flag_file)


def send_rebuild_start_to_amc(md_dev):
    ret = get_uuid()
    if ret.has_key('resource') and ret.has_key('container') and ret.has_key('displayname'):
        # send alert
        send_alert_raid_sync(ret['container'], ret['displayname'], 'WARN', 'OK',
                             'Starting sync for volume %s...' % md_dev)
        # update status
        for i in range(3):
            update_raid5_status(ret['resource'], ret['container'], 'WARN', '%s, raid %s sync start...' % ('0%', md_dev))


def send_rebuild_finish_to_amc(md_dev, alert):
    ret = get_uuid()
    if ret.has_key('resource') and ret.has_key('container') and ret.has_key('displayname'):
        # send alert
        if alert == True:
            send_alert_raid_sync(ret['container'], ret['displayname'], 'OK', 'WARN', 'Volume raid sync is successful')
        # update status
        for i in range(3):
            update_raid5_status(ret['resource'], ret['container'], 'OK', 'Volume raid sync is complete')


if __name__ == "__main__":
    if sys.argv[1] == 'RebuildCheck' and len(sys.argv) > 3:
        set_log_file(sys.argv[3])

    debug("%d: Enter %s" % (len(sys.argv), sys.argv))

    if len(sys.argv) < 2:
        exit(1)

    if sys.argv[1] == 'RebuildStarted':
        md_dev = sys.argv[2]
        debug("RebuildStarted, %s recovery start." % md_dev)
        set_rebuild_flag(md_dev)
        send_rebuild_start_to_amc(md_dev)

    elif sys.argv[1] == 'RebuildFinished':
        md_dev = sys.argv[2]

        vv_configure = get_config_from_jsonfile(ATLAS_CONF)
        the_volresource = vv_configure[AMC_volresources][0]
        vv_uuid = the_volresource["uuid"]
        debug("volume: %s, dev: %s" % (vv_uuid, md_dev))

        cpool_file = '/etc/ilio/c_pool_infrastructure_' + vv_uuid + '.json'
        c_infrastructure = get_config_from_jsonfile(cpool_file)
        cfg_ibd_list = []
        for storage_type in c_infrastructure:  # disk, memory
            for level_1 in c_infrastructure[storage_type]:  # raid5 level
                dev = level_1["devname"]
                if dev == md_dev:
                    found = True
                else:
                    found = False
                for level_2 in level_1["children"]:  # raid1 level
                    dev = level_2["devname"]
                    if found == True or dev == md_dev:
                        for level_3 in level_2["children"]:  # ibd level
                            dev = level_3["devname"]
                            cfg_ibd_list.append(dev)

        cmd_str = CMD_IBDMANAGER_STAT_RWWUD
        out = ['']
        do_system(cmd_str, out)
        lines = out[0].split('\n')
        rwwud_mapping = {}
        for the_line in lines:
            line_parts = the_line.split(' ')
            if len(line_parts) < 2:
                continue
            exportname = line_parts[0]
            devname = line_parts[1]
            if devname in cfg_ibd_list:
                rwwud_mapping[devname] = exportname

        detail = {}
        raid_detail_nohung(md_dev, detail)

        readd = False
        if detail['Raid-Level'] == 'raid5':
            readd = check_raid5_rebuild_status(vv_uuid, detail, rwwud_mapping)
        elif detail['Raid-Level'] == 'raid1':
            readd = check_raid1_rebuild_status(vv_uuid, detail, rwwud_mapping)

        if readd == False:
            debug("RebuildFinished, %s recovery done." % md_dev)
            if rebuild_is_start(md_dev) == False:
                send_rebuild_start_to_amc(md_dev)
                time.sleep(1)
            else:
                clean_rebuild_flag(md_dev)

            if volume_is_ready() == True:
                debug("Volume %s is ready!!!" % vv_uuid)
                send_rebuild_finish_to_amc(md_dev, True)

    elif sys.argv[1] == 'RebuildCheck':
        if volume_is_ready() == True:
            debug("Volume is ready!!!")
            md_dev = sys.argv[2]
            send_rebuild_finish_to_amc(md_dev, False)

    elif "Rebuild" in sys.argv[1]:
        percent = sys.argv[1].split("Rebuild")
        if percent[1].isdigit():
            md_dev = sys.argv[2]
            if rebuild_is_start(md_dev) == False:
                set_rebuild_flag(md_dev)
                send_rebuild_start_to_amc(md_dev)
    elif sys.argv[1] == "Fail":
        debug('deal Fail event.')
        if len(sys.argv) < 4:
            debug('ERROR: not enough parameters for Fail event.')
            exit(1)
        raid_dev = sys.argv[2]
        ibd_dev = sys.argv[3]
        # ret = MdStatMgr.unset_primary(raid_dev, ibd_dev)
        # if ret != 0:
        #     debug('ERROR: set fail device to secondary of raid1 faild.')
        #     exit(1)
    elif sys.argv[1] == "SpareActive":
        debug('deal SpareActive event.')
        if len(sys.argv) < 4:
            debug('ERROR: not enough parameters for SpareActive event.')
            exit(1)
        raid_dev = sys.argv[2]
        ibd_dev = sys.argv[3]
        ret = MdStatMgr.set_primary(raid_dev, ibd_dev)
        if ret != 0:
            debug('ERROR: set active device to primary of raid1 faild.')
            exit(1)
    elif sys.argv[1] == "NewArray":
        debug('deal NewArray event.')
        if len(sys.argv) < 3:
            debug('ERROR: not enough parameters for NewArray event.')
            exit(1)
        raid_dev = sys.argv[2]
        ret = MdStatMgr.new_primary(raid_dev)
        if ret != 0:
            debug('ERROR: set new device to primary of raid1 faild.')
            exit(1)
    exit(0)
