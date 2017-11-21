#!/usr/bin/python

import os, sys, stat
import re
import time
sys.path.insert(0, '/opt/milio/libs/atlas')
from log import *
from atl_util import *

sys.path.insert(0, '/opt/milio/atlas/roles')
from usx_config import UsxConfig

LOG_FILENAME = '/var/log/usx-ads-pool.log'
NFS_START_CMD = "service nfs-kernel-server start"
NFS_EXPORTS_OPT = ("*(rw,no_root_squash,no_subtree_check,insecure,nohide,"
               "fsid=1,")
NFS_SYNC = "sync"
NFS_ASYNC = "async"


class UsxServiceManager(object):

    @staticmethod
    def check_service_type():
        debug('Enter check_service_type.')
        return UsxConfig().export_type.lower()

    @staticmethod
    def start_iscsi(mount_point):
        debug('Enter start_iscsi.')
        default_lun_name = 'datastorename'
        lun_filename = 'LUN1'
        default_iqn = 'iqn.com.atlantiscomputing.usx'
        datastorename = load_usx_conf()['volumeresources'][0]["uuid"]
        if not (datastorename and len(datastorename) and datastorename.strip()):
            datastorename = default_lun_name
        mod_datastorename = re.sub('[^A-Za-z0-9-]', '-', datastorename)
        lun_file_path = None
        try:
            lun_file_path = os.path.join(mount_point, lun_filename)
        except:
            lun_file_path = None
        if mount_point.strip().endswith('/'):
            mnt_pnt = mount_point.strip()[:-1]
        else:
            mnt_pnt = mount_point.strip()
        cmd_str = '/usr/bin/python /opt/milio/atlas/scripts/scsi-export-ads.pyc ' \
            + mnt_pnt + ' ' + default_iqn + ':' + str(mod_datastorename)
        if (not lun_file_path) or (not os.path.exists(lun_file_path)):
            debug('Configuring iscsi with\n' + cmd_str + ' True\n')
            ret = os.system(cmd_str + ' True' +  ' >> %s 2>&1' % LOG_FILENAME)
            if ret:
                debug("iscsi config failed\n")
                return ret
        debug('Starting iscsi with\n' + cmd_str + ' False\n')
        return os.system(cmd_str + '  False' +  ' >> %s 2>&1' % LOG_FILENAME)

    @staticmethod
    def start_nfs(mount_point):
        debug('Enter start_nfs.')
        exp_dir = mount_point
        ret = os.system("grep \"" + exp_dir + "\" /etc/exports" +
                    ' >> %s 2>&1' % LOG_FILENAME)
        if ret != 0:
            usx_conf = load_usx_conf()
            is_infra = is_infra_volume(usx_conf)
            is_fs_sync = usx_conf['volumeresources'][0].get('fs_sync', False)

            if UsxConfig().is_journaled or is_infra or is_fs_sync:
                sync_option = NFS_SYNC
            else:
                sync_option = NFS_ASYNC
            f = open("/etc/exports", "a+")
            f.write("\"" + exp_dir + "\" " + NFS_EXPORTS_OPT + sync_option + ")" + "\n")
            f.close()
            os.system("exportfs -r")
        return os.system(NFS_START_CMD + ' >> %s 2>&1' % LOG_FILENAME)

    @staticmethod
    def start_service(mount_point):
        debug('Enter start_service.')
        if 'iscsi' in UsxServiceManager.check_service_type():
            ret = UsxServiceManager.start_iscsi(mount_point)
        else:
            ret = UsxServiceManager.start_nfs(mount_point)
        return 0

    @staticmethod
    def stop_export_service():
        export_type = UsxServiceManager.check_service_type()
        debug('INFO: Enter stop_service [{}]...'.format(export_type))
        if 'nfs' in export_type:
            exp_srv_type = 'nfs-kernel-server'
        elif 'iscsi' in export_type:
            exp_srv_type = 'scst'
        else:
            debug('ERROR: not support')
            return 1
        for cnt in range(10):
            ret = do_system('/etc/init.d/{} stop'.format(exp_srv_type))
            if ret == 0:
                deadtime = time.time() + 60
                while time.time() < deadtime:
                    if not UsxServiceManager.is_service_running(exp_srv_type):
                        return 0
                    time.sleep(1)
        debug('ERROR: cannot stop {} service'.format(exp_srv_type))
        return 1

    @staticmethod
    def get_service_status(srv_type):
        out = ['']
        do_system('/etc/init.d/{} status'.format(srv_type), out)
        return out[0].replace('\n', '')

    @staticmethod
    def is_service_running(srv_type):
        status = UsxServiceManager.get_service_status(srv_type)
        if 'not running' in status.lower():
            return False
        return True
