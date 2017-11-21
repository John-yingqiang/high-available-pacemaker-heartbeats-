#!/usr/bin/python
#coding: utf-8

import time
import os
import re
import sys
import traceback
import json
import subprocess
import re

sys.path.append('/opt/milio/atlas/system')
sys.path.append('/opt/milio/atlas/scripts')
sys.path.append('/opt/milio/libs/atlas')
sys.path.append('/opt/milio/scripts')
from sshw import run_cmd, ssh_cmd
from log import *
from changeips_utils import get_local_ip, is_reachable
from atl_util import get_master_amc_ip
from atl_vvol_util import vvol_load_cfg

LOG_FILENAME = "/var/log/usx-vmg-verify.log"
set_log_file(LOG_FILENAME)


def get_volume_mount_status(tar_ip, containers, resources):
    '''
    Parameters: tar_ip: <string>
                containers: <dict>
                resources: <dict>
    Returns: <boolean>
    Description: get target volume mount status
    '''
    ret_value = None
    container_uuid = None

    # -- volume is Hybrid, All flash -- use export IP
    for item in resources["items"]:
        if item.has_key('serviceip') and item['serviceip'] == tar_ip:
            mount_list = item['export']['hypervisornames']
            if len(mount_list) > 0:
                ret_value = True
            else:
                ret_value = False
            return ret_value

    # -- other volume --
    # get container uuid
    for item in containers["items"]:
        for i in item["nics"]:
            if i.has_key('ipaddress') and i['ipaddress'] == tar_ip:
                container_uuid = item['uuid']

    if not container_uuid:
        debug("Not found container_uuid about %s" % tar_ip)
        raise Exception("Check volume mount status failed")
        return False

    mount_list = []

    # get mount hypervisor list
    for item in resources["items"]:
        if item['containeruuid'] == container_uuid:
            mount_list = item['export']['hypervisornames']

    if len(mount_list) > 0:
        ret_value = True
    else:
        ret_value = False

    return ret_value


def get_volume_status(tar_ip, containers, resources):
    '''
    Parameters: tar_ip: <string>
                containers: <dict>
    Returns: <boolean>
    Description: get target volume valid status through REST API
    '''
    ret_value = None

    target_uuid = get_target_uuid(tar_ip, containers, resources)
    cmd = "curl -s -k -X GET http://127.0.0.1:8080/usxmanager/usx/status/VOLUME_RESOURCE/%s" % target_uuid
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()

    if not x:
        debug("Remote call rest api failed")
        raise Exception("Get volume status failed")

    try:
        json_ret = json.loads(x)
    except Exception as e:
        debug(e)
        raise Exception("Get volume status failed")

    VOLUME_EXPORT_AVAILABILITY = ''
    VOLUME_STORAGE_STATUS = ''
    DEDUP_FILESYSTEM_STATUS = ''
    CONTAINER_STATUS = ''
    VOLUME_SERVICE_STATUS = ''

    info = {}

    for item in json_ret['usxstatuslist']:
        if item['name'] == 'VOLUME_EXPORT_AVAILABILITY':
            info['VOLUME_EXPORT_AVAILABILITY'] = item['value']

        if item['name'] == 'VOLUME_STORAGE_STATUS':
            info['VOLUME_STORAGE_STATUS'] = item['value']

        if item['name'] == 'DEDUP_FILESYSTEM_STATUS':
            info['DEDUP_FILESYSTEM_STATUS'] = item['value']

        if item['name'] == 'CONTAINER_STATUS':
            info['CONTAINER_STATUS'] = item['value']

        if item['name'] == 'VOLUME_SERVICE_STATUS':
            info['VOLUME_SERVICE_STATUS'] = item['value']

    flag = 0
    for key in info:
        if info[key] == 'OK':
            flag = flag + 1

    if flag != 5:
        ret_value = False
    else:
        ret_value = True

    return ret_value


def get_target_uuid(tar_ip, containers, resources):
    '''
    Parameters: tar_ip: <string>
                containers: <dict>
    Returns: <None> or <uuid>
    Description: get target volume uuid
    '''
    ret_value = None

    # -- volume is Hybrid, All flash -- use export IP
    for item in resources["items"]:
        if item.has_key('serviceip') and item['serviceip'] == tar_ip:
            ret_value =  item["uuid"]
            return ret_value

    # -- other volume --
    for item in containers["items"]:
        for i in item["nics"]:
            if i.has_key('ipaddress') and i['ipaddress'] == tar_ip:
                ret_value = item['volumeresourceuuids'][0]

    if not ret_value:
        debug("Not found volume resource uuids about %s" % tar_ip)
        raise Exception("Get volume status failed")
        return False

    return ret_value


def check_name_ip_match(tar_ip, containers, resources, service_name):
    '''
    Parameters: tar_ip: <string>
                containers: <dict>
                resources: <dict>
                service_name: <string>
    Returns: <boolean>
    Description: check target volume name and volume service name whether match or not
    '''
    ret_value = None
    container_uuid = None
    name = None

    # -- volume is Hybrid, All flash -- use export IP
    for item in resources["items"]:
        if item.has_key('serviceip') and item['serviceip'] == tar_ip and service_name == item['volumeservicename']:
            ret_value = True
            return ret_value

    # -- other volume --
    # get container uuid
    for item in containers["items"]:
        for i in item["nics"]:
            if i.has_key('ipaddress') and i['ipaddress'] == tar_ip:
                container_uuid = item['uuid']

    if not container_uuid:
        debug("Not found container_uuid about %s" % tar_ip)
        raise Exception("Get volume info failed")
        return False

    # get volume service name
    for item in resources["items"]:
        if item.has_key('containeruuid') and item['containeruuid'] == container_uuid:
            name = item['volumeservicename']

    if name == service_name:
        ret_value = True
    else:
        ret_value = False

    return ret_value


def rest_api_get_containers(tar_ip):
    '''
    Parameters: tar_ip: <string>
    Returns: <None> or <dict>
    Description: get containers infomation through REST API
    '''
    ret_value = None
    amc_ip = get_master_amc_ip()
    try:
        volume_uuid = vvol_load_cfg()['volumeresources']
    except Exception as e:
        raise Exception("%s is not a volume" % tar_ip)

    cmd = "curl -s -k -X GET http://127.0.0.1:8080/usxmanager/usx/inventory/volume/containers"
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()

    if not x:
        debug("Remote call rest api failed")
        raise Exception("Get volume containers failed")

    try:
        json_ret = json.loads(x)
    except Exception as e:
        debug(e)
        raise Exception("Get volume containers failed")

    return json_ret


def rest_api_get_resources(tar_ip):
    '''
    Parameters: tar_ip: <string>
    Returns: <None> or <dict>
    Description: get resources infomation through REST API
    '''
    ret_value = None
    amc_ip = get_master_amc_ip()
    volume_uuid = vvol_load_cfg()['volumeresources']

    cmd = "curl -s -k -X GET http://127.0.0.1:8080/usxmanager/usx/inventory/volume/resources"
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()

    if not x:
        debug("Remote call rest api failed")
        raise Exception("Get volume resources failed")

    try:
        json_ret = json.loads(x)
    except Exception as e:
        debug(e)
        raise Exception("Get volume resources failed")

    return json_ret


def get_io_stat():
    ret = None

    # get the dedup device name
    cmd = "/bin/ls -l `mount | grep dedup | awk '{print $1}'` | awk -F '/' '{print $NF}'"
    debug("CMD:", cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("dedup device name:\n %s" % x)
    if not x:
        debug("Failed to get dedup device name")
        return ret

    cmd = "iostat -xdmc 1 6 %s" % x
    debug("CMD:", cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("get_io_stat return:\n %s" % x)

    dev_dict = {}
    if x:
        io_list = re.split(r'Device.*', x)
        for i in range(2,len(io_list)):
            for j in re.split(r'\n*', io_list[i]):
                m = re.match(r'^([\S]+).*?(\d+.\d+)$', j)
                if m:
                    if not dev_dict.has_key(m.group(1)):
                        dev_dict[m.group(1)] = 0
                    dev_dict[m.group(1)] = float(dev_dict[m.group(1)]) + float(m.group(2))

                    if i == len(io_list)-1:
                        dev_dict[m.group(1)] = float("%.2f" % float(dev_dict[m.group(1)] / (len(io_list)-2)))

        debug("Average value of io utilization in 5 times:\n%s" % json.dumps(dev_dict , sort_keys=True, indent=4, separators=(',', ': ')))
        # return the max of io utilization
        ret = max(dev_dict.values())
    else:
        debug("Failed get io stat")

    return ret


def main(ip, service_name):

    ret = {}
    ret['error'] = ''

    try:
        containers = rest_api_get_containers(ip)
        resources = rest_api_get_resources(ip)
        ret['mount'] = get_volume_mount_status(ip, containers, resources)
        ret['valid'] = get_volume_status(ip, containers, resources)
        ret['match'] = check_name_ip_match(ip, containers, resources, service_name)
        ret['io'] = get_io_stat()
    except Exception as e:
        debug(traceback.format_exc())
        ret['error'] = e.message

    ret = json.dumps(ret)

    return ret


if __name__ == "__main__":

    tar_ip = sys.argv[1]
    service_name = sys.argv[2]

    debug("tar_ip: %s, service_name: %s" % (tar_ip, service_name))

    ret = main(tar_ip, service_name)
    warn(ret)
