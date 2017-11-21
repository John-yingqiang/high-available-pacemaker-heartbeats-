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
import types
import argparse

sys.path.append('/opt/milio/atlas/system')
sys.path.append('/opt/milio/atlas/scripts')
sys.path.append('/opt/milio/libs/atlas')
sys.path.append('/opt/milio/scripts')

LOG_FILENAME = "/var/log/usx-vmg-teleport.log"

LOCK_FILE = "/run/lock/teleport.lock"
SSHW = "/opt/milio/atlas/system/sshw.pyc"

def argument_parser():
    parser = argparse.ArgumentParser(description='Atlantis USX replication teleport')
    parser.add_argument('-b', '--debug', nargs='?', help='Enable debug mode for the script.', type=bool, default=False, const=True)
    return parser.parse_args()

def create_uuid():
    '''
    Parameters: <None>
    Returns: <uuid>
    Description: create a uuid with head "USX_"
    '''
    vol_uuid = uuid.uuid4()
    return  "USX_" + str(vol_uuid)

def check_teleport_status(target_ip, VM_file):
    '''
    Parameters: target_ip
    Returns: string
    Description: Check teleport how to fail
    '''
    err = ''
    #check disk usage
    cmd = " df | grep exports | awk '{print $4}'"
    ret = ssh_cmd(target_ip, cmd)

    if ret['stderr']:
        debug("ret:", ret)
        raise Exception("Failed to ssh <%s>, error message as:%s" % (target_ip, ret['stderr']))
    target_size = ret['stdout'].replace('\n', '')
    cmd = "du %s | awk '{print $1}'" % VM_file
    ret = run_cmd(cmd, 10)
    if ret['stderr']:
        debug("ret:", ret)
        raise Exception("Failed to get VM size")
    VM_size = ret['stdout'].replace('\n', '')

    if int(VM_size) > int(target_size):
        err = "Teleport failed due to no enough disk space on target volume."
    return err

def get_mount_path_local():
    '''
    Parameters: <None>
    Returns: <path>
    Description: get local mount path
    '''
    ret = None
    cmd = "df | grep export"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(cmd)
    debug("cmd return:", x)

    g = re.search(r'(/exports/.*)', x)
    if g:
        ret = g.group(1)
        debug("local mount path is %s" % ret)
    else:
        raise Exception("Failed to get local mount path")
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
        raise Exception("Failed to get local exports path")

    cmd = "/usr/bin/find %s -type f | grep '%s*' -" % (ret, uuid)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(cmd)
    debug("cmd return:", x)
    if not x:
        raise Exception("Not found vhd file with uuid %s" % uuid)

    g = re.search(r'(/exports/.*/).*?', x)
    if g:
        ret = g.group(1)
        debug("mount path is %s" % ret)
    else:
        raise Exception("Failed to get local mount path")

    return ret


def get_mount_path_remote(ip):
    '''
    Parameters: <None>
    Returns: <path>
    Description: get remote mount path
    '''
    cmd = "df | grep export"
    ret = ssh_cmd(ip, cmd)
    debug("get_mount_path_remote:", ret)

    if ret['stderr']:
        debug("Run cmd failed to %s" % ip)
        raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret['stderr']))

    g = re.search(r'(/exports/.*)', ret['stdout'])
    if g:
        ret = g.group(1)
        debug("remote mount path is %s" % ret)
    else:
        raise Exception("Failed to get remote mount path")
    return ret


def build_trust(ip):
    '''
    Parameters: <ip>
    Returns: <Boolean>
    Description: build trust to target ip
    '''
    cmd = "/usr/bin/python %s -i %s" % (SSHW, ip)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    if not re.search(r'successful', x):
        raise Exception("Failed to build trust to %s" % ip)
    return True


def keep_one_teleport(ip, r_path):
    '''
    Parameters: <ip> and <path>
    Returns: <Boolean>
    Description: check the target volume whether run teleport or not.
    '''
    temp_size = 0
    size_flag = 0
    uuid_flag = 0
    not_flag = 0

    while 1:
        # check lock file
        cmd = "/bin/ls %s" % LOCK_FILE
        debug("cmd: %s" % cmd)
        ret = ssh_cmd(ip, cmd)
        if re.search(r'No such', str(ret['stderr'])):
            cmd2 = "/usr/bin/touch %s" % LOCK_FILE
            debug("cmd2: %s" % cmd2)
            ret2 = ssh_cmd(ip, cmd2)
            if ret2['stderr']:
                debug("ret2:", ret2)
                raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret2['stderr']))
            debug("Not found %s, start new teleport and create new %s" % (LOCK_FILE, LOCK_FILE))
            break
        elif ret['stderr']:
            debug("ret:", ret)
            raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret['stderr']))
        else:
            time.sleep(10)

            #read LOCK_FILE
            cmd3 = "/bin/cat %s" % LOCK_FILE
            debug("cmd3: %s" % cmd3)
            ret3 = ssh_cmd(ip, cmd3)
            if re.search(r'No such', str(ret3['stderr'])):
                debug("ret3:", ret3)
            elif ret3['stderr']:
                debug("ret3:", ret3)
                raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret3['stderr']))

            uuid = ret3['stdout']

            if not uuid:
                uuid_flag = uuid_flag + 1
                debug("uuid_flag - %s" % str(uuid_flag))
                if uuid_flag > 6:
                    debug("Wait 1 minute, no teleport write uuid to %s, start new teleport" % LOCK_FILE)
                    break
                continue

            check_path = r_path + os.sep + ".dedup_private/teleport/" + uuid.rstrip()

            #check temp folder
            cmd4 = "/bin/ls %s" % check_path
            debug("cmd4: %s" % cmd4)
            ret4 = ssh_cmd(ip, cmd4)
            if re.search(r'No such', str(ret4['stderr'])):
                not_flag = not_flag + 1
                if not_flag > 6:
                    debug("Check 6 times, not found %s temp folder, start new teleport." % check_path)
                    break
                continue
            elif ret4['stderr']:
                debug("ret4:", ret4)
                raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret4['stderr']))

            #get temp folder size
            cmd5 = "du %s | egrep %s$" % (check_path, uuid.rstrip())
            debug("cmd5: %s" % cmd5)
            ret5 = ssh_cmd(ip, cmd5)
            if ret5['stderr']:
                debug("ret5:", ret5)
                raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret5['stderr']))
            g = re.search(r'^(\d+)\s+', ret5['stdout'])
            new_temp_size = 0
            if g:
                new_temp_size = g.group(1)
                debug("new_temp_size - %s" % new_temp_size)

            if int(new_temp_size) > temp_size:
                size_flag = 0
                temp_size = new_temp_size
            else:
                size_flag = size_flag + 1

            if size_flag > 60:
                debug("Wait 10 mins, temp folder %s was not increase, start new teleport." % check_path)
                break
    return True

def write_uuid_to_lock_file(ip, uuid):
    '''
    Parameters: <ip> and <uuid>
    Returns: <Boolean>
    Description: write uuid to lock file
    '''
    cmd = "/bin/echo %s > %s" % (uuid, LOCK_FILE)
    ret = ssh_cmd(ip, cmd)
    debug("write_uuid_to_lock_file:", ret)
    if ret["stderr"]:
        debug("Failed to write uuid to lock file")
        raise Exception("Failed to ssh <%s>, error message as:%s" % (ip, ret['stderr']))
    return True


def remove_lock_file(ip):
    '''
    Parameters: <ip>
    Returns: <Boolean>
    Description: remove lock file
    '''
    cmd = "/bin/rm -rf %s" % LOCK_FILE
    debug("CMD: %s" % cmd)
    ret = ssh_cmd(ip, cmd)
    debug('remove_lock_file:', ret)
    if ret["stderr"]:
        debug("Failed to remove lock file")

    return True


def teleport(item_name, item_path, item_tpath, dst_ip, src_ip, local_mount_path, remote_mount_path, flag):
    '''
    Parameters: <item>, <dst_ip>, <src_ip>, <local_mount_path>, <remote_mount_path>
    Returns: <Boolean>
    Description: do teleport
    '''

    debug("**teleport start**")
    keep_one_teleport(dst_ip, remote_mount_path)

    if hyper_type == 'VMware':
        tpath = local_mount_path + os.sep + item_tpath
        debug("tpath:", tpath)
        debug("item_name:", item_name)
        g1 = re.search(r'(.*)/(.*)', tpath)
        src_folder = g1.group(1)
        src_target_folder = g1.group(2)

        spath = remote_mount_path + os.sep + item_path
        debug("spath:", spath)
        g2 = re.search(r'(.*)/(.*)', spath)
        des_folder = g2.group(1)
        #des_target_folder = g2.group(2)
        des_target_folder = item_name
    elif hyper_type == 'Xen':
        mount_path = get_mount_path_local_xen(item_path)
        tpath = mount_path + item_tpath
        debug("tpath:", tpath)
        g1 = re.search(r'(.*)/(.*/.*)', tpath)
        src_folder = g1.group(1)
        src_target_folder = g1.group(2)

        spath = remote_mount_path + os.sep + item_tpath
        if globals().has_key('tmp_tpath'):
            debug('Use temp folder for Xen vm teleport.')
            spath = remote_mount_path + os.sep + tmp_tpath
        debug("spath:", spath)
        g2 = re.search(r'(.*)/(.*)', spath)
        des_folder = g2.group(1)
        des_target_folder = g2.group(2)

        # copy metadata file to target
        if flag == 1:
            spath_folder = os.path.dirname(spath)
            cmd_scp_pre = "[ ! -d %s ] && mkdir -p %s" % (spath_folder, spath_folder)
            debug("Check remote path if existed : %s" % (cmd_scp_pre))
            ret = ssh_cmd(dst_ip, cmd_scp_pre)
            if ret['stderr']:
                debug("Failed to create remote path due to %s." % (ret))
            scp_metadata(dst_ip, tpath, spath)

    uuid = create_uuid()

    # write uuid to LOCK_FILE
    write_uuid_to_lock_file(dst_ip, uuid)

    cmd = "python /opt/milio/atlas/teleport/atp.pyc start -j '%s' -f '%s' -t '%s' -s '%s' -d '%s' -m '%s' -M '%s' -c 'python /opt/milio/bin/atp_helper.pyc' -T 'TELEPORT' -i 1" \
    % (uuid, src_ip, dst_ip, src_target_folder, des_target_folder, src_folder, des_folder)
    debug("CMD: ", cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    r = False
    retcode = p.returncode
    if retcode == 0:
        debug("Strat waiting...")
        r = wait_teleport(uuid, src_folder, dst_ip)

    err = ''
    if r == True:
        ret = (True, "")
    else:
        err = check_teleport_status(dst_ip, tpath)
        debug('Check teleport status is: %s' % err)
        if not err:
            err = "Teleport terminate unexpectedly"
        ret = (False, err)

    debug("Waiting finished, teleport finished")
    return ret


def wait_teleport(uuid, src_folder, dst_ip):
    '''
    Parameters: <dst_ip>, <remote_mount_path>, <uuid>
    Returns: <Boolean>
    Description: check teleport tmp file whether there or not
    '''
    ret = 0

    process_no_found_flag = 20
    while 1:
        #read state file
        cmd1 = "/bin/cat /var/tmp/%s.stat" % uuid
        debug("CMD1: ", cmd1)
        p = subprocess.Popen(cmd1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x, y = p.communicate()
        if re.search(r'No such', x):
            debug("Teleport helper not return result, keep waiting.")
        elif re.search(r'Successfully', x):
            debug("Teleport helper return result: %s" % x)
            ret = True
            break
        elif re.search(r'Failed', x):
            debug("Teleport helper return result: %s" % x)
            ret = False
            break
        # check databroker process at source side
        cmd2 = "ps -ef --width=1000 | grep -v grep | grep 'databroker.*%s'" % src_folder
        debug("CMD2: ", cmd2)
        p = subprocess.Popen(cmd2, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x, y = p.communicate()
        debug(x)

        # check atp and rysync process with uuid at source side
        cmd3 = "ps -ef --width=1000 | grep -v grep | grep %s" % uuid
        debug("CMD3: ", cmd3)
        p = subprocess.Popen(cmd3, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x1, y1 = p.communicate()
        debug(x1)

        # check atp and rsync process with uuid at target side
        cmd4 = 'ssh %s "ps -ef --width=1000 | grep -v grep | grep %s"' % (dst_ip, uuid)
        debug("CMD4: ", cmd4)
        p = subprocess.Popen(cmd4, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x2, y2 = p.communicate()
        debug(x2)

        # check mapper process at source side
        cmd5 = "ps -ef --width=1000 | grep -v grep | grep '/opt/milio/bin/mapper'"
        debug("CMD5: ", cmd5)
        p = subprocess.Popen(cmd5, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x3, y3 = p.communicate()
        debug(x3)

        # check redirector process at target side
        cmd6 = '''ssh %s "ps -ef --width=1000 | grep -v grep | grep '/opt/milio/bin/redirector'"''' % (dst_ip)
        debug("CMD6: ", cmd6)
        p = subprocess.Popen(cmd6, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        x4, y4 = p.communicate()
        debug(x4)

        if not x and not x1 and not x2 and not x3 and not x4:
            process_no_found_flag = process_no_found_flag - 1
        else:
            process_no_found_flag = 20

        if x1 or x2:
            process_no_found_flag = 20

        if process_no_found_flag < 0:
            debug("Check 20 times, not found related process of teleport")
            ret = False
            break

        time.sleep(10)

    # delete state file
    cmd = "/bin/rm -rf /var/tmp/%s.stat" % uuid
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug(x)

    return ret


def scp_metadata(dst_ip, local_path, remote_path):

    g = re.search(r'(.*)\.vhd', local_path)
    src_path = g.group(1) + ".metadata"
    tar_path = re.sub(r'vhd', 'metadata', remote_path)

    cmd = "scp %s %s:%s" % (src_path, dst_ip, tar_path)
    debug(cmd)

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()

    if re.search(r'No such', x):
        debug("cmd return: %s" % x)
        raise Exception("Not found metadata file, skip this teleport")
    debug('scp done.')
    return True


def get_ip():
    '''
    Parameters: None
    Returns: <string>
    Description: Get storage network ip
    '''
    ip = None
    vm_uuid = _get_atlas_json_from_file()['usx']['uuid']

    cmd = "curl -s -k -X GET http://127.0.0.1:8080/usxmanager/usx/inventory/volume/containers/%s" % vm_uuid
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()

    if re.search(r'not found', x):
        debug("cmd return: %s" % x)
        debug("Volume container not found")
        raise Exception("Get storage network ip failed")
    elif re.search(r'<title>.*</title>', x):
        debug("cmd return: %s" % x)
        debug("Get volume info-containers failed")
        raise Exception("Get storage network ip failed")

    return_json = json.loads(x)

    # get storage network ip
    for net in return_json['data']['nics']:
        if len(return_json['data']['nics']) > 1 and net['storagenetwork'] == True:
            if net.has_key('ipaddress'):
                ip = net['ipaddress']
        else:
            if net.has_key('ipaddress'):
                ip = net['ipaddress']

    debug("Local ip is: %s" % ip)
    if not ip:
        raise Exception("Get storage network ip failed")

    return ip


def return_json_result(input_json, ret_json):
    '''
    Parameters: <input_json>, <ret_json>
    Returns: <Boolean>
    Description: return json result to USX server through REST API
    '''
    request_id = input_json['requuid']

    url = "http://127.0.0.1:8080/usxmanager/usx/vmgroup/replication/workflow/teleport/status/%s" % request_id
    cmd = "curl -s -k -X POST -H 'Content-type: application/json' -d '%s' %s" % (json.dumps(ret_json), url)
    debug(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    x, y = p.communicate()
    debug("cmd return: %s" % x)

    try:
        if x:
            json_data = json.loads(x)
        else:
            raise Exception("send the teleport status to USX server was failed")

        if json_data['msg']:
            raise Exception(json_data['msg'])

    except Exception as e:
        debug(e)
        raise Exception("send the teleport status to USX server was failed")

    return True

def rename_remote_temp_folder_xen(dst_ip, src_path, dst_path):
    cmd_rename_remote = '[ -d %s ] && rm -rf %s;mkdir -p %s && mv %s/* %s && rm -rf %s' % (dst_path, dst_path, dst_path, src_path, dst_path, src_path)
    debug("Rename CMD: %s" % (cmd_rename_remote))
    ret = ssh_cmd(dst_ip, cmd_rename_remote)
    if ret['stderr']:
        debug("Failed to rename temp folder ret: %s" % (ret))
        clean_remote_temp_folder_xen(dst_ip, src_path)
        raise Exception("Failed to rename temp folder to VM folder.")

def clean_remote_temp_folder_xen(dst_ip, src_path):
    if globals().has_key('vmg_tel_args') and not vmg_tel_args:
        cmd_clean_remote = 'rm -rf %s' % (src_path)
        debug("Clean CMD: %s" % (cmd_clean_remote))
        ret = ssh_cmd(dst_ip, cmd_clean_remote)
        if ret['stderr']:
            debug("Failed to clean temp folder ret: %s" % (ret))
            raise Exception("Failed to clean temp folder.")
    else:
        debug("In debug mode, remote host %s path %s will not be deleted." % (dst_ip, src_path))


def main(input_json):

    debug("**teleport main start**")
    ret = {}
    ret["requuid"] = input_json["requuid"]
    ret["vmgroupuuid"] = input_json["vmgroupuuid"]
    ret["op"] = input_json["op"]
    ret["targetvolumeip"] = input_json["targetvolumeip"]
    ret["targetvolname"] = input_json['targetvolname']
    ret["sourceinfo"] = []
    teleport_flag = 1

    try:
        build_trust(input_json["targetvolumeip"])
        remote_mount_path = get_mount_path_remote(input_json["targetvolumeip"])
        local_mount_path = get_mount_path_local()
        src_ip = get_ip()
    except Exception as e:
        teleport_flag = 0
        for item in input_json['sourceinfo']:
            ret["sourceinfo"].append({
                'name':item['name'],
                'uuid':item['uuid'],
                'tpath':item['tpath'],
                'status':'false',
                'start_time':str(0),
                'end_time':str(0),
                'message':str(e)
            })

    if teleport_flag == 1:
        for item in input_json['sourceinfo']:
            s_time = None
            e_time = None
            try:
                s_time = str(time.time())

                tel_dict = {}
                tel_dict[item['path']] = item['tpath']

                #copy otherfilesmap to target in XEN
                if hyper_type == 'Xen' and type(item['otherfilesmap']) is types.DictType:
                    for k,v in item["otherfilesmap"].items():
                        tel_dict[v] = v

                debug("tel_dict:", json.dumps(tel_dict , sort_keys=True, indent=4, separators=(',', ': ')))
                # For USX-77919, if target platform is Xen, we need to teleport the VMs
                # to a remote temp folder first, then rename the temp folder to vm folder
                # after all successful, if failed, clean the template folder.
                for path, tpath in tel_dict.items():
                    # when here match, will copy metadata to target
                    if path == item['path']:
                        flag = 1
                    else:
                        flag = 0
                    if hyper_type == 'Xen':
                        global tmp_tpath
                        tmp_tpath = 'vmg_replication/%s/%s' % (item['uuid'], tpath)
                    ret_status, msg = teleport(item['name'], path, tpath, input_json["targetvolumeip"], src_ip, local_mount_path, remote_mount_path, flag)
                    remove_lock_file(input_json["targetvolumeip"])
                    if ret_status:
                        teleport_ret = 'true'
                    else:
                        teleport_ret = 'false'
                        break
                src_path = remote_mount_path + os.sep + 'vmg_replication/%s' % (item['uuid'])
                if teleport_ret == 'true':
                    msg = "Succssfully telport VMs"
                    if hyper_type == 'Xen':
                        # rename the temp folder to vm folder
                        dst_path = remote_mount_path + os.sep + "USX_" + item['tpath']
                        dst_path = dst_path.replace(".vhd","")
                        rename_remote_temp_folder_xen(input_json["targetvolumeip"], src_path, dst_path)
                else:
                    msg = "Failed to teleport VMs"
                    if hyper_type == 'Xen':
                        # clean the temp folder
                        clean_remote_temp_folder_xen(input_json["targetvolumeip"], src_path)

                e_time = str(time.time())
                ret["sourceinfo"].append({
                    'name':item['name'],
                    'uuid':item['uuid'],
                    'tpath':item['tpath'],
                    'status':teleport_ret,
                    'start_time':s_time,
                    'end_time':e_time,
                    'message': msg
                })
            except Exception as e:
                remove_lock_file(input_json["targetvolumeip"])
                debug("Exception:", e)
                e_time = str(time.time())
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
    debug("**teleport main end**")
    return ret


if __name__ == "__main__":

    input_data = sys.stdin.read()
    global vmg_tel_args
    vmg_tel_args = argument_parser()
    with daemon.DaemonContext():
        from sshw import run_cmd, ssh_cmd
        from changeips_utils import get_local_ip, check_hypervisor_type, get_sys_net_ifaces, _get_atlas_json_from_file
        from atl_util import get_master_amc_ip
        from log import set_log_file, debug, info, warn, errormsg

        set_log_file(LOG_FILENAME)
        global hyper_type
        hyper_type = check_hypervisor_type()
        debug("hypertype: %s" % hyper_type)

        debug("input data:")
        debug(input_data)

        try:
            input_json = json.loads(input_data)
            debug('input JSON:')
            debug(json.dumps(input_json , sort_keys=True, indent=4, separators=(',', ': ')))
        except:
            errormsg(traceback.format_exc())
            errormsg('Exception exit...')
            sys.exit(1)

        if input_json["op"] != "teleport":
            errormsg('Wrong opertion type.')
            sys.exit(1)

        try:
            ret = main(input_json)
            warn(ret)
        except Exception, ex:
            traceback.print_exc(file = open(LOG_FILENAME,"a"))

    sys.exit(0)
