#!/usr/bin/python
#coding: utf-8

import sys, os
import datetime, time
import traceback
import json
import subprocess
import re
import daemon

sys.path.append('/opt/milio/atlas/system')
sys.path.append('/opt/milio/atlas/scripts')
sys.path.append('/opt/milio/libs/atlas')
sys.path.append('/opt/milio/scripts')

LOG_FILENAME = "/var/log/usx-vmg-fastclone.log"
FASTCLONE_PYC = "/opt/milio/scripts/ilio.clone.pyc"

def fastclone(item):
    mount_path = get_mount_path_local()
    path = mount_path + os.sep + item['path']
    tpath = mount_path + os.sep + item['tpath']

    cmd = "/usr/bin/python %s -s %s -d %s" % (FASTCLONE_PYC, path, tpath)
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    if y != None:
        debug("Error:", y)

    retval = p.wait()

    if p.returncode != 0:
        raise Exception("Clone %s to %s failed" % (path, tpath))

    return 1


def fastclone_xen(item):
    mount_path = get_mount_path_local_xen(item['path'])
    path = mount_path + os.path.basename(item['path'])
    tpath = mount_path + item['tpath']

    cmd = "/sbin/file.ilio.clone -F -s %s -d %s" % (path, tpath)
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("cmd return:", x)

    if y != None:
        debug("Error:", y)

    retval = p.wait()

    if p.returncode != 0:
        raise Exception("Clone %s to %s failed" % (path, tpath))
    else:
        debug("Fastclone done, %s to %s" % (path, tpath))

    # check otherfilesmap
    if not item.has_key('otherfilesmap') or not item['otherfilesmap']:
        debug("otherfilesmap was None")
        raise Exception("Incomplete parameters")

    for k,v in item['otherfilesmap'].items():
        if k != v:
            path = mount_path + k
            tpath = mount_path + v

            cmd = "/sbin/file.ilio.clone -F -s %s -d %s" % (path, tpath)
            debug(cmd)
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            x, y = p.communicate()
            debug("cmd return:", x)

            if y != None:
                debug("Error:", y)

            retval = p.wait()

            if p.returncode != 0:
                raise Exception("Clone %s to %s failed" % (path, tpath))
            else:
                debug("Fastclone done, %s to %s" % (path, tpath))

    return 1


def check_des_folder(item):
    mount_path = get_mount_path_local()
    tpath = mount_path + os.sep + item['tpath']
    ret = os.path.exists(tpath)
    if ret == True:
        raise Exception("Clone with directory %s already exists" % tpath)

    return 1


def get_mount_path_local():
    ret = None
    cmd = "df | grep export"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(cmd)
    debug("cmd return:", x)

    g = re.search(r'(/exports/.*)', x)
    if g:
        ret = g.group(1)
        debug("mount path is %s" % ret)
    else:
        raise Exception("Get mount path failed")

    return ret


def get_mount_path_local_xen(uuid):
    ret = None
    cmd = "df | grep export"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(cmd)
    debug("cmd return:", x)

    g = re.search(r'(/exports/.*)', x)
    if g:
        ret = g.group(1)
    else:
        raise Exception("Get exports path failed")

    cmd = "/usr/bin/find %s -type f | grep '%s*' -" % (ret, uuid)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(cmd)
    debug("cmd return:", x)

    g = re.search(r'(/exports/.*/).*?', x)
    if g:
        ret = g.group(1)
        debug("mount path is %s" % ret)
    else:
        raise Exception("Get mount path failed")

    return ret


def save_metadata_xen(item):
    g = re.search(r'(.*)\.vhd', item["tpath"])
    file_name = g.group(1) + ".metadata"
    mount_path = get_mount_path_local_xen(item['path'])

    ret = {}
    ret['metadata'] = item['metadata']
    ret['vmname'] = item['name']

    debug("save metadata to: %s" % mount_path + file_name)
    f = open(mount_path + file_name, 'w')

    f.write(json.dumps(ret))
    f.close()


def update_fastclone_workflow(rep_json):
    """
    Parameters:
        amc_ip: AMC server ip address.
        volume_uuid:    Uuid of the volume.
        request_id:  A string of request id of vm fastclone.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update volume fastclone request id.
    """
    request_id = rep_json['requuid']

    url = "http://127.0.0.1:8080/usxmanager/usx/vmgroup/replication/workflow/fastclone/status/%s" % request_id
    cmd = "curl -s -k -X POST -H 'Content-type: application/json' -d '%s' %s" % (json.dumps(rep_json), url)
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("cmd return: %s" % x)

    try:
        if x:
            json_data = json.loads(x)
        else:
            raise Exception("send the fastclone status to USX server was failed")

        if json_data['msg']:
            raise Exception(json_data['msg'])

    except Exception as e:
        debug(e)
        raise Exception("send the fastclone status to USX server was failed")

    return True


def remove_old_fastclone_file(item):
    """
    Parameters:
        item: {dict}
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        Before creating a new Fastclone, remove old Fastclone of this vm.
    """
    ret = False

    if hyper_type == 'VMware':
        old_file = get_mount_path_local() + os.sep + item['tpath']
        debug("old fastclone path:", old_file)
    elif hyper_type == 'Xen':
        mount_path = get_mount_path_local_xen(item['path'])
        old_file = mount_path + item['tpath']
        debug("old fastclone path:", old_file)

        # remove metadata in XEN
        g = re.search(r'(.*)\.vhd', old_file)
        metadata_file = g.group(1) + ".metadata"
        try:
            if os.path.exists(metadata_file):
                os.remove(metadata_file)
        except:
            debug("Failed to remove metadata file %s" % metadata_file)
            raise Exception("Remove old fastclone file failed")
            return ret

    cmd = "/bin/rm -rf %s" % old_file
    debug("CMD: ", cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    if p.returncode != 0:
        debug("Failed to remove %s" % old_file)
        raise Exception("Remove old fastclone file failed")
    else:
        ret = True

    return ret


def main(input_json):

    debug("**fastclone start**")
    ret = {}
    ret["vmgroupuuid"] = input_json["vmgroupuuid"]
    ret["op"] = input_json["op"]
    ret["sourceinfo"] = []
    ret["requuid"] = input_json["requuid"]

    os.system("sync")

    for item in input_json['sourceinfo']:
        s_time = None
        e_time = None
        try:
            s_time = str(time.time())

            remove_old_fastclone_file(item)

            if hyper_type == 'VMware':
                check_des_folder(item)
                fastclone(item)
            elif hyper_type == 'Xen':
                fastclone_xen(item)

            e_time = str(time.time())
            ret["sourceinfo"].append({
                'name':item['name'],
                'uuid':item['uuid'],
                'tpath':item['tpath'],
                'status':'true',
                'start_time':s_time,
                'end_time':e_time,
                'message':''
            })

            # record metadata
            if hyper_type == 'Xen':
                save_metadata_xen(item)

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

    debug(json.dumps(ret))

    res = True
    if ret['sourceinfo'] == []:
        res = False
    for src_info in ret['sourceinfo']:
        if src_info["status"] == "false":
            res = False
    if res:
        debug('Successful to do fastclone.')
    else:
        errormsg('Failed to do fastclone, please refer to %s for details.' % (LOG_FILENAME))

    try:
        #returns a json result
        ret = update_fastclone_workflow(ret)
    except Exception as e:
        errormsg("Exception:", e)
        ret = False

    debug("return json result status: %s" % ret)
    debug("**fastclone end**")

    return res

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

        if input_json["op"] != "fastclone":
            errormsg('Wrong opertion type.')
            sys.exit(1)

        try:
            ret = main(input_json)
            warn(ret)
        except Exception, ex:
            traceback.print_exc(file = open(LOG_FILENAME,"a"))

    sys.exit(0)