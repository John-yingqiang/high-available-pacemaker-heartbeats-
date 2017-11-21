import sys
import os
sys.path.append('/opt/milio/atlas/system')
#sys.path.append('/opt/milio/atlas/scripts')
sys.path.append('/opt/milio/libs/atlas')
#sys.path.append('/opt/milio/scripts')
sys.path.append('/opt/milio/atlas/vmgroup')
import json
from sshw import run_cmd, call_rest_api
from log import *
from vmg_discover import get_volume_info

LOG_FILENAME = "/var/log/usx-atp-move.log"
ATLAS_CONF = '/etc/ilio/atlas.json'
API_URL = "https://%s:8443/usxmanager/vmm/vms/%s?api_key=%s"
VMS_INFO_JSON = '/opt/milio/atlas/vmgroup/vmsinfo.json'

def check_input_json(input_json):
    if not (input_json.has_key('vmfiles') and input_json['vmfiles']):
        raise Exception('Missing key "vmfiles" or value of "vmfiles" is empty')
    if not (input_json.has_key('targetfolder') and input_json['targetfolder']):
        raise Exception('Missing key "targetfolder" or value of "targetfolder" is empty')
    return True

def main(input_json):
    err = ''
    ret_val = 0
    volume_info = get_volume_info()
    try:
        check_input_json(input_json)
        return_json = {}
        return_json['responses'] = []
        return_list =[]
        return_item_json = {}
        return_item_json['vmname'] = ''
        return_item_json['vmuuid'] = ''
        return_item_json['success'] = True
        return_item_json['message'] = 'Success'
        #Create target floder
        target_floder = '%s/%s' % (volume_info['dedupfsmountpoint'], input_json['targetfolder'])
        #if os.path.exists(target_floder):
        #    run_cmd('rm -rf %s/*' % target_floder)
        # os.mkdir(target_floder)
        for vm_item in input_json['vmfiles']:
            if not (vm_item.has_key('vmname') and vm_item['vmname']):
                debug('Missing key "vmname" or value of "vmname" is empty')
                return_item_json['success'] = False
                return_item_json['message'] = 'Missing key "vmname" or value of "vmname" is empty'
                return_list.append(return_item_json)
                ret_val = 1
                continue
            return_item_json['vmname'] = vm_item['vmname']
            if not (vm_item.has_key('vmuuid') and vm_item['vmuuid']):
                debug('Missing key "vmuuid" or value of "vmuuid" is empty')
                return_item_json['success'] = False
                return_item_json['message'] = 'Missing key "vmuuid" or value of "vmuuid" is empty'
                return_list.append(return_item_json)
                ret_val = 1
                continue
            return_item_json['vmuuid'] = vm_item['vmuuid']
            if not (vm_item.has_key('files') and vm_item['files']):
                debug('Missing key "files" or value of "files" is empty')
                return_item_json['success'] = False
                return_item_json['message'] = 'Missing key "files" or value of "files" is empty'
                return_list.append(return_item_json)
                ret_val = 1
                continue
#            if not (vm_item.has_key('targetfolder') and vm_item['targetfolder']):
#                debug('Missing key "targetfolder" or value of "targetfolder" is empty')
#                return_item_json['success'] = False
#                return_item_json['message'] = 'Missing key "targetfolder" or value of "targetfolder" is empty'
#                return_list.append(return_item_json)
#                ret_val = 1
#                continue
            for item in vm_item['files']:
                src_file_path = '%s/%s' % (volume_info['dedupfsmountpoint'], item['vmfile'])
                dst_file_path = '%s/%s' % (target_floder, item['targetfilename'])
                if not os.path.exists(src_file_path):
                    debug('File of %s was not found' % src_file_path)
                    return_item_json['success'] = False
                    return_item_json['message'] = 'File of %s was not found' % src_file_path
                    return_list.append(return_item_json)
                    ret_val = 1
                    continue
                run_cmd('mv %s %s' % (src_file_path, dst_file_path))
            
            for item in vm_item['files']:
                if not item['parent']:
                    continue
                file_path = '%s/%s' % (target_floder, item['targetfilename'])
                parent_path = '%s/%s' % (target_floder, item['parent']) 
                if not os.path.exists(file_path):
                    debug('Target File of %s was not found' % file_path)
                    return_item_json['success'] = False
                    return_item_json['message'] = 'Target File of %s was not found' % file_path 
                    return_list.append(return_item_json)
                    ret_val = 1
                    continue
                if not os.path.exists(parent_path):
                    debug('Parent File of %s was not found' % parent_path)
                    return_item_json['success'] = False
                    return_item_json['message'] = 'Parent File of %s was not found' % parent_path
                    return_list.append(return_item_json)
                    ret_val = 1
                    continue
                r = run_cmd('/opt/milio/atlas/vmgroup/vmg_vhd_util modify -n %s -p %s' % (file_path, parent_path))
                debug(r) 
            return_list.append(return_item_json)
    except Exception as err:
        debug(err)
        ret_val = 1

    return_json['responses'] = return_list
    debug(json.dumps(return_json))
    print(json.dumps(return_json))
    debug('=====End move======')
    sys.exit(ret_val)

if __name__ == '__main__':
    set_log_file(LOG_FILENAME)
    input_json = sys.stdin.read()
    debug('=====Start move======')
    debug('input: %s' % input_json)
    main(json.loads(input_json))
