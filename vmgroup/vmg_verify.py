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

SSHW = "/opt/milio/atlas/system/sshw.pyc"
REMOTE_VERIFY = "/opt/milio/atlas/vmgroup/vmg_verify_remote.pyc"
LOG_FILENAME = "/var/log/usx-vmg-verify.log"
set_log_file(LOG_FILENAME)


def get_target_host_name(tar_ip):
    '''
    Parameters: <tar_ip>
    Returns: <None> or <hostname>
    Description: get target host name
    '''
    ret_value = None
    cmd = "/bin/hostname"
    ret = ssh_cmd(tar_ip, cmd)
    debug(ret)

    if ret['stdout'] != '':
        ret_value = ret['stdout'].rstrip()

    return ret_value


def get_volume_ip():
    '''
    Parameters: <None>
    Returns: <None> or <ip>
    Description: get local host ip
    '''
    ret = None
    ret = get_local_ip()

    return ret


def get_volume_avail_size(tar_ip):
    '''
    Parameters: <tar_ip>
    Returns: <None> or <size>
    Description: get target volume avail size
    '''
    ret = None
    cmd = "df | grep export"
    ret = ssh_cmd(tar_ip, cmd)
    debug(ret)

    g = re.split(r'\s+', ret['stdout'])
    if g:
        ret = g[3]
        debug("local mount size is %s" % ret)

    return ret


def build_trust(tar_ip):
    '''
    Parameters: <tar_ip>
    Returns: 1
    Description: build trust to target volume
    '''
    cmd = "/usr/bin/python %s -i %s" % (SSHW, tar_ip)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    if not re.search(r'successful', x):
        raise Exception("Failed to build trust to %s" % tar_ip)
    return 1

def run_get_volume_mount_status_remote(tar_ip, service_name):
    ret_value = None
    cmd = "/usr/bin/python %s %s %s" % (REMOTE_VERIFY, tar_ip, service_name)
    ret = ssh_cmd(tar_ip, cmd, TIMEOUT=120)
    debug("remote return:")
    debug(ret)

    return ret


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


def main(input_json):

    ret = {}
    ret['error'] = ''

    #verify is the ip address reachable
    if is_reachable(input_json["targetvolumeip"]):
        build_trust(input_json["targetvolumeip"])
        try:
            ret_value = run_get_volume_mount_status_remote(input_json["targetvolumeip"], input_json["targetvolname"])
            if ret_value['stderr']:
                debug("Remote std error: %s" % ret_value['stderr'])
            if ret_value['error']:
                debug("Remote error: %s" % ret_value['error'])

            remote_json = json.loads(ret_value['stdout'])

            if remote_json['error']:
                ret['error'] = remote_json['error']
            else:
                ret['mount'] = remote_json['mount']
                ret['valid'] = remote_json['valid']
                ret['match'] = remote_json['match']
                ret['tar_io'] = remote_json['io']
                ret['src_io'] = get_io_stat()

        except Exception as e:
            ret['error'] = e.message
    else:
        ret['error'] = '%s unreachable' % input_json["targetvolumeip"]

    ret = json.dumps(ret)

    return ret


if __name__ == "__main__":

    input_data = sys.stdin.read()

    debug("input data:")
    debug(input_data)

    try:
        input_json = json.loads(input_data)
        debug('input JSON:')
        debug(json.dumps(input_json , sort_keys=True, indent=4, separators=(',', ': ')))
    except:
        debug(traceback.format_exc())
        errormsg('Exception exit...')
        sys.exit(1)

    if input_json["op"] != "verify":
        errormsg('Wrong opertion type.')
        sys.exit(1)

    ret = main(input_json)
    warn(ret)
