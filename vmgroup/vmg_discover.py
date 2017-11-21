import sys
import os
import subprocess
sys.path.append('/opt/milio/atlas/system')
sys.path.append('/opt/milio/atlas/scripts')
sys.path.append('/opt/milio/libs/atlas')
sys.path.append('/opt/milio/scripts')
import json
from sshw import run_cmd
from log import *
from atl_util import get_master_amc_ip
from changeips_utils import check_hypervisor_type

LOG_FILENAME = "/var/log/usx-vmg-discover.log"
ATLAS_CONF = '/etc/ilio/atlas.json'
API_URL = "https://%s:8443/usxmanager/vmm/vms/%s?api_key=%s"
VMS_INFO_JSON = '/opt/milio/atlas/vmgroup/vmsinfo.json'

def get_volume_info():
    """
    Get volumes volume uuid and resource uuid
    Returns:
        a dict object with key uuid and resources
    """
    ret = {}
    try:
        with open(ATLAS_CONF) as fp:
            atlas_json = json.load(fp)
            if atlas_json.has_key('usx'):
                if len(atlas_json['volumeresources']) > 0:
                    ret['resources'] = atlas_json['volumeresources'][0]['uuid']
                else:
                    ret['resources'] = atlas_json["usx"]['volumeresourceuuids'][0]
                ret["uuid"] = atlas_json["usx"]["uuid"]
                ret['dedupfsmountpoint'] = atlas_json["volumeresources"][0]["dedupfsmountpoint"]
    except Exception as err:
        raise Exception("Failed to volume resource and container uuid")
    debug('get_volume_info: %s' % ret)
    return ret

def get_vms_info(amc_ip, resource_uuid, container_uuid):
    """
    Get vms info
    Returns:
        a dict object
    """
    json_ret = {}
    cmd = "curl -s -k -X GET http://127.0.0.1:8080/usxmanager/vmm/%s" % resource_uuid
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("x", x)

    if not x:
        debug("Remote call rest api failed")
        raise Exception('Failed to get VMs infomation from USX Manager')

    try:
        json_ret = json.loads(x)
        debug('get_vms_info: %s' % json_ret)
    except Exception as e:
        debug(e)
        raise Exception('Failed to get VMs infomation from USX Manager')
    debug('get_vms_info: %s' % json_ret)
    return json_ret

def get_local_vm(h_type, path):
    """
    Get local vms
    Returns:
        a list object
    """
    vm_list = []
    if h_type == 'VMware':
        file_name = '*.vmx'
    else:
        file_name = '*.vhd'
    ret = run_cmd('find %s -name %s' % (path, file_name))
    if ret['stderr'] == '':
        vm_list = ret['stdout'][:-1].split('\n')
    else:
        raise Exception('Failed to get local vms, detail as %s' % ret['stderr'])
    debug('get_local_vm: %s' % vm_list)
    return vm_list

def get_unregister_vms_vmware(vms_local, vms_info):
    """
    Get unregister vms names
    Returns:
        a list object
    """
    unregistered_vms = []
    for vm in vms_local:
        vm_f_path,vm_f_name=os.path.split(vm)
        vm_name = vm_f_name.split('.')[0]
        vm_export_path,vm_path=os.path.split(vm_f_path)
        vm_path = vm_path.split('.')[0]
        debug('path %s' % vm_path)
        for item in vms_info:
            if item['vmname'] == vm_name:
                break
        else:
            tmp_dict = {}
            tmp_dict['vmname'] = vm_name
            tmp_dict['path'] = "%s/%s" % (vm_path, vm_f_name)
            unregistered_vms.append(tmp_dict)
    return unregistered_vms

def load_json_file(file_name):
    """
    Parameters: <file_name>
    Returns: <boolean>
    Description: load json file
    """
    jsondata = ''
    try:
        with open(file_name) as fp:
            jsondata = json.load(fp)
    except Exception as err:
        methodName = load_json_file.__name__
        raise Exception('[%s] Failed to load json file %s: %s' % (methodName, file_name, err))
    fp.close()
    return jsondata

def get_metadata(path):
    """
    Parameters: metaata full path
    Returns: a dict
    Description: get metadata info from <uuid>.metadata
    """
    ret = {}
    try:
        metadata_info = load_json_file(path)
        ret = metadata_info['metadata']
    except Exception as err:
        methodName = load_json_file.__name__
        raise Exception('[%s] Failed to get metadata from %s: %s' % (methodName, path, err))
    return ret

def get_unregister_vms_xen(vms_local, vms_info, volume_info):
    """
    Get unregister vms names
    Returns:
        a list object
    """
    unregistered_vms = []

    for vm in vms_local:
        vm_path,vm_f_name=os.path.split(vm)
        vm_name = vm_f_name.split('.')[0]
        if not os.path.exists('%s/%s.metadata' % (vm_path, vm_name)):
            debug('Skipping, Not find %s.metadata file' % vm_name)
            continue
        vms_local_names_dict = load_json_file('%s/%s.metadata' % (vm_path, vm_name))
        if not vms_local_names_dict.has_key('vmname'):
            raise Exception('Missing key "vmname" in %s/%s.metadata file' % (vm_path, vm_name))
        for item in vms_info:
            if item['vmname'] == vms_local_names_dict['vmname']:
                break
        else:
            tmp_dict = {}
            tmp_dict['vmname'] = vms_local_names_dict['vmname']
            tmp_dict['path'] = vm.replace('%s/' % volume_info['dedupfsmountpoint'], '')
            tmp_dict['metadata'] = vms_local_names_dict['metadata']
            unregistered_vms.append(tmp_dict)

    return unregistered_vms

def generate_return_json(h_type, err, vms_list, input_json, volume_info):
    ret_dict = {}
    ret_dict['result'] = {}
    ret_dict['result'] ['unregisteredvms'] = []
    if err:
        ret_dict['status'] = 1
        ret_dict['error'] = err
        ret_dict['message'] = 'Failed to discover VMs.'
    else:
        ret_dict['status'] = 0
        ret_dict['error'] = ''
        ret_dict['message'] = 'Successfully discovered VMs.'
        tmp_list = []
        for item in vms_list:
            tmp_dict = {}
            tmp_dict['vmmanagername'] = input_json['vmmanagername']
            tmp_dict['vmname'] = item['vmname']
            tmp_dict['vmfolderpath'] = item['path']
            tmp_dict['volumeresourceuuid'] = volume_info['resources']
            if h_type == 'Xen':
                tmp_dict['metadata'] = item['metadata']
            tmp_list.append(tmp_dict)
        ret_dict['result'] ['unregisteredvms'] = tmp_list
    return ret_dict

def main(input_json):
    err = ''
    ret_val = 0
    unregist_vms_list = []
    volume_info = get_volume_info()
    try:
        if input_json.has_key('op') and input_json['op'] == 'discover':
            hyper_type = check_hypervisor_type()
            if hyper_type != 'VMware' and hyper_type != 'Xen':
                raise Exception('Only support hypervioer type is VMware or Xen')

            amc_ip = get_master_amc_ip()
            vms_local = get_local_vm(hyper_type, volume_info['dedupfsmountpoint'])
            vms_info = get_vms_info(amc_ip, volume_info['resources'], volume_info['uuid'])

            if hyper_type == 'VMware':
                unregist_vms_list = get_unregister_vms_vmware(vms_local, vms_info)
            elif hyper_type == 'Xen':
                unregist_vms_list = get_unregister_vms_xen(vms_local, vms_info, volume_info)
        else:
            raise Exception('Missing key "op" or value of "op" is not "discover"')
    except Exception as err:
        debug(err)
        ret_val = 1

    debug('unregister_vms: %s' % unregist_vms_list)
    ret = generate_return_json(hyper_type, str(err), unregist_vms_list, input_json, volume_info)
    info(json.dumps(ret))
    debug(json.dumps(ret))
    debug('=====End discover======')
    sys.exit(ret_val)

if __name__ == '__main__':
    set_log_file(LOG_FILENAME)
    input_json = sys.stdin.read()
    debug('=====Start discover======')
    debug('input: %s' % input_json)
    main(json.loads(input_json))
