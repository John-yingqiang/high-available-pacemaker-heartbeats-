import json
import sys
import urllib2
import fcntl
import struct
import time
import re

sys.path.insert(0, "/opt/milio/")
from libs.atlas.atl_vvol_util import *

def get_volume_resrouce_info(USX_MGR_URL, uuid):
    url = "%s%s/%s?api_key=%s" % (USX_MGR_URL, VOLUME_INVENTORY_API, uuid, uuid)
    ret = False
    retval = {}

    code, ret = call_rest_api(url,'GET')
    if ret:
        ret = json.loads(ret)
        retval = ret

    return retval

def get_volume_container_info(USX_MGR_URL,uuid):
    url = "%s%s/%s?api_key=%s" % (USX_MGR_URL, VOLUME_CONTAINER_API, uuid, uuid)
    ret = False
    retval = {}

    code, ret = call_rest_api(url,'GET')
    if ret:
        ret = json.loads(ret)
        retval = ret

    return retval

#backup vm infomation
def backup_vm_info(snapshot_uuid):
    cfg = {}
    out = {}

    cfg = vvol_load_cfg()
    volume_uuid = cfg['volumeresources']

    #Create backup folder
    folder = '%s/%s/%s' % (cfg['dedupfsmountpoint'], BACKUP_PATH,snapshot_uuid)
    runcmd('mkdir -p %s' % folder)

    if is_enable_vvol(cfg['usxmanagerurl'],volume_uuid):
        out = get_vvol_container_info(cfg['usxmanagerurl'],volume_uuid)
        if out:
            if out.has_key('items'):
                if out['items']:
                    value = out['items'][0]
                    if value.has_key('uuid'):
                        jsonToFile('%s/%s' % (folder, BACKUP_VVOL_CONTAINER_FILE), value)
                        out = get_vvol_volume_info_by_containerid(cfg['usxmanagerurl'], value['uuid'],volume_uuid)
                        if out:
                            jsonToFile('%s/%s' % (folder, BACKUP_VVOL_VM_FILE), out)

#rollback vm infomation
def rollback_vm_info(snapshot_uuid):
    cfg = {}
    out = {}

    cfg = vvol_load_cfg()
    volume_uuid = cfg['volumeresources']
    folder = '%s/%s/%s' % (cfg['dedupfsmountpoint'], BACKUP_PATH,snapshot_uuid)

    if path.exists('%s/%s' % (folder,BACKUP_VVOL_CONTAINER_FILE)):
        container_info = parseJson('%s/%s' % (folder,BACKUP_VVOL_CONTAINER_FILE))

        #delete vvol container
        delete_vvol_container(cfg['usxmanagerurl'],container_info['uuid'],volume_uuid)
        #Create vvol container
        create_vvol_container(cfg['usxmanagerurl'], json.dumps(container_info),volume_uuid)
        vm_info = parseJson('%s/%s' % (folder,BACKUP_VVOL_VM_FILE))

        for item in vm_info['items']:
            #delete vm
            delete_vvol_volume(cfg['usxmanagerurl'],item['uuid'],volume_uuid)
            #Create vm infomation
            create_vvol_volume(cfg['usxmanagerurl'], json.dumps(item),volume_uuid)

        #Delete tmp file
        runcmd('rm -rf %s/%s' % (cfg['dedupfsmountpoint'], BACKUP_PATH))