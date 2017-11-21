#!/usr/bin/python
#coding: utf-8

import time
import uuid
import os
import re
import sys
import traceback
import json
import subprocess
import re
import daemon

sys.path.append('/opt/milio/libs/atlas')
sys.path.append('/opt/milio/atlas/scripts')

LOG_FILENAME = "/var/log/usx-vmg-clearup.log"

def get_mount_path_local():

    ret = None
    cmd = "df | grep export"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    g = re.search(r'(/exports/.*)', x)
    if g:
        ret = g.group(1)
        debug("local mount path is %s" % ret)
    else:
        raise Exception("Failed to get local mount path")

    return ret


def get_mount_path_local_xen(path):
    ret = None
    cmd = "df | grep export"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(cmd)
    debug("cmd return:", x)

    g = re.search(r'(/exports/.*)', x)
    if g:
        exports_path = g.group(1)
        debug("exports_path: ", exports_path)
    else:
        raise Exception("Failed to get exports path")

    # search the uuid from path
    g = re.search(r'(.+)/.+', path)
    if g:
        uuid = g.group(1)
        ret = exports_path + os.sep + uuid
        debug("mount folder path: ", ret)
    else:
        raise Exception("Failed to get mount folder path")

    return ret


def clearup(item):

    clean_list = []

    if hyper_type == 'VMware':
        old_file = get_mount_path_local() + os.sep + item['tpath']
        debug("clearup path:", old_file)
        clean_list.append(old_file)
    elif hyper_type == 'Xen':
        mount_path = get_mount_path_local_xen(item['path'])
        old_file = mount_path + os.sep + item['tpath']
        debug("clearup path:", old_file)
        clean_list.append(old_file)

        # remove otherfilesmap file
        if item.has_key('otherfilesmap') and item['otherfilesmap']:
            for k,v in item['otherfilesmap'].items():
                if k != v:
                    tpath = mount_path + os.sep + v
                    clean_list.append(tpath)

        # remove metadata in XEN
        g = re.search(r'(.*)\.vhd', old_file)
        metadata_file = g.group(1) + ".metadata"
        clean_list.append(metadata_file)

    debug("clean_list: ", json.dumps(clean_list , sort_keys=True, indent=4, separators=(',', ': ')))
    for f in clean_list:
        cmd = "/bin/rm -rf %s" % f
        debug("CMD: ", cmd)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x, y = p.communicate()
        debug(x)

        if p.returncode != 0:
            raise Exception("Failed to clearup %s" % f)
        else:
            ret = True

    # remove USX_<uuid>.tgz file under /var/log
    cmd = "find /var/log/ -name 'USX_*.tgz' -exec /bin/rm -rf {} \;"
    debug("CMD: ", cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    if p.returncode != 0:
        raise Exception("Failed to clearup USX_<uuid>.tgz file")
    else:
        ret = True

    return ret


def return_json_result(input_json, ret_json):
    '''
    Parameters: <input_json>, <ret_json>
    Returns: <Boolean>
    Description: return json result to USX server through REST API
    '''
    request_id = input_json['requuid']

    url = "http://127.0.0.1:8080/usxmanager/usx/vmgroup/replication/workflow/cleanup/status/%s" % request_id
    cmd = "curl -s -k -X POST -H 'Content-type: application/json' -d '%s' %s" % (json.dumps(ret_json), url)
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("cmd return: %s" % x)

    try:
        if x:
            json_data = json.loads(x)
        else:
            raise Exception("send the cleanup status to USX server was failed")

        if json_data['msg']:
            raise Exception(json_data['msg'])

    except Exception as e:
        debug(e)
        raise Exception("send the cleanup status to USX server was failed")

    return True


def main(input_json):

    debug("**clearup start**")
    ret = {}
    ret["vmgroupuuid"] = input_json["vmgroupuuid"]
    ret["op"] = input_json["op"]
    ret["sourceinfo"] = []

    for item in input_json['sourceinfo']:
        s_time = None
        e_time = None
        try:
            s_time = time.time()
            clearup_ret = clearup(item)
            msg = ''
            if clearup_ret == True:
                msg = "Succssfully clearup VMs"
            e_time = time.time()
            ret["sourceinfo"].append({
                'name':item['name'],
                'uuid':item['uuid'],
                'tpath':item['tpath'],
                'status':'true',
                'start_time':s_time,
                'end_time':e_time,
                'message':msg
            })
        except Exception as e:
            debug("Exception:", e)
            e_time = time.time()
            ret["sourceinfo"].append({
                'name':item['name'],
                'uuid':item['uuid'],
                'tpath':item['tpath'],
                'status':'false',
                'start_time':s_time,
                'end_time':e_time,
                'message':str(e)
            })

    debug(json.dumps(ret , sort_keys=True, indent=4, separators=(',', ': ')))

    try:
        #returns a json result
        ret = return_json_result(input_json, ret)
    except Exception as e:
        errormsg("Exception:", e)
        ret = False

    debug("return json result status: %s" % ret)
    debug("**clearup end**")

    return ret


if __name__ == "__main__":

    input_data = sys.stdin.read()

    with daemon.DaemonContext():
        from log import set_log_file, debug, info, warn, errormsg
        from changeips_utils import check_hypervisor_type

        set_log_file(LOG_FILENAME)

        hyper_type = check_hypervisor_type()
        debug("hypertype: %s" % hyper_type)

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

        if input_json["op"] != "cleanup":
            errormsg('Wrong opertion type.')
            sys.exit(1)

        try:
            ret = main(input_json)
            warn(json.dumps(ret))
        except Exception, ex:
            traceback.print_exc(file = open(LOG_FILENAME,"a"))

    sys.exit(0)
