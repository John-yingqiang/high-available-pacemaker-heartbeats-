#!/usr/bin/python
#coding: utf-8
import os
import sys
import json
sys.path.append('/opt/milio/atlas/system')
sys.path.append('/opt/milio/atlas/vmgroup')
sys.path.append('/opt/milio/libs/atlas')
from log import *
from vmg_discover import get_volume_info
from sshw import run_cmd

def get_vhd_file(path):
    """
    Get vhd files
    Returns:
        a list object
    """
    vm_list = []
    ret = run_cmd('find %s -name "*.vhd"' % path)
    if ret['stderr'] == '':
        vm_list = ret['stdout'][:-1].split('\n')
        vm_list = [ c.split('/')[-1].replace('.vhd', '') for c in vm_list]
    else:
        raise Exception('Failed to get vhd, detail as %s' % ret['stderr'])
    debug('vmg_metadata_clearup: get_vhd_file: %s' % vm_list)
    return vm_list

def get_medadata_file(path):
    """
    Get medatata files
    Returns:
        a list object
    """
    vm_list = []
    ret = run_cmd('find %s -name "*.metadata"' % path)
    if ret['stderr'] == '':
        if ret['stdout']:
            vm_list = ret['stdout'][:-1].split('\n')
    else:
        raise Exception('Failed to get medadata file, detail as %s' % ret['stderr'])
    debug('vmg_metadata_clearup: get_medadata_file: %s' % vm_list)
    return vm_list

def main():
    flag = False
    ret = 0
    try:
        volume_info = get_volume_info()
        m_file_list = get_medadata_file(volume_info['dedupfsmountpoint'])
        if m_file_list:
            v_file_list = get_vhd_file(volume_info['dedupfsmountpoint'])
            for m_full_name in m_file_list:
                m_path, m_f_name=os.path.split(m_full_name)
                m_name = m_f_name.split('.')[0]
                if m_name not in v_file_list:
                    debug('vmg_metadata_clearup: remove invalid metadata file %s' % m_full_name)
                    os.remove(m_full_name)
                    flag = True
            if flag:
                cmd = "cd %s; find %s -type d -empty | awk -F '/' '{print $NF}' | grep USX_ | xargs rm -rf" % (volume_info['dedupfsmountpoint'], volume_info['dedupfsmountpoint'])
                run_cmd(cmd)

    except Exception as err:
        debug('vmg_metadata_clearup: %s' % err)
        ret = 1
    return ret

if __name__ == "__main__":
    LOG_FILENAME = "/var/log/usx-vmg-discover.log"
    set_log_file(LOG_FILENAME)
    debug('========Start vmg metadat clearup=========')
    ret = main()
    debug('========End vmg metadat clearup=========')
    sys.exit(ret)
