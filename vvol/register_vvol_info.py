import json
import sys
import urllib2
import fcntl
import struct
import time
import re
import subprocess
from os import path

sys.path.insert(0, "/opt/milio/")
from libs.atlas.atl_vvol_util import *

"""
Run cmd
Params:
    cmd:      command

Return:
   Success: return a dictionary with 2 key stdout and stderr
   Failure: False
"""
def run_cmd(cmd, timeout=300):
    rtn_dict = {}
    obj_rtn = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    start_time = time.time()
    run_time = 0
    while True:
        if obj_rtn.poll() != None:
            break

        end_time = time.time()

        run_time = end_time - start_time

        if run_time > timeout:
            obj_rtn.terminate()
            raise Exception('Run command timeout: %s' % cmd)

        time.sleep(0.1)

    out = obj_rtn.stdout.read()
    err = obj_rtn.stderr.read()

    rtn_dict['stdout'] = out
    rtn_dict['stderr'] = err
    rtn_dict['returncode'] = obj_rtn.returncode
    return rtn_dict

#register vvol infomation
def main():
    print('=====Strat to register vvol infomation=====')
    container_uuid = ''
    vm_list=[]

    cfg = vvol_load_cfg()
    volume_uuid = cfg['volumeresources']

    if not is_enable_vvol(cfg['usxmanagerurl'],volume_uuid):
        enable_vvol(cfg['usxmanagerurl'],volume_uuid)

    out = get_vvol_container_info(cfg['usxmanagerurl'],volume_uuid)
    if out:
        if out.has_key('items'):
            if out['items']:
                value = out['items'][0]
                if value.has_key('uuid'):
                    container_uuid = value['uuid']

    out = run_cmd('ls %s | grep -v lost+found ' % cfg['dedupfsmountpoint'])
    vm_list = out['stdout'].split('\n')
    vm_list.remove('')

    for vm_uuid in vm_list:
        out = get_vvol_volume_info_by_uuid(cfg['usxmanagerurl'], vm_uuid ,volume_uuid)

        if out:
            if out.has_key('items'):
                if out['items']:
                    value = out['items'][0]
                    if value['container'] != container_uuid:
                        value['container'] = container_uuid
                        delete_vvol_volume(cfg['usxmanagerurl'],vm_uuid,volume_uuid)
                        create_vvol_volume(cfg['usxmanagerurl'], json.dumps(value),volume_uuid)
        else:
            print('Infomation of VM %s not found from VVol data!')
    print('=====End=====')

if __name__ == "__main__":
    main()
