#!/usr/bin/python

import json
import sys
import os
import time
import datetime
import traceback
import ConfigParser
import shutil

from ibdserver_conf import config_support_vdi, reset_ibdserver_config, modify_drw_config_cache_vdi, \
    apply_new_drw_channel
from usx_config import UsxConfig
from cmd_utils import runcmd, is_new_simple_hybrid
from usx_settings import UsxSettings
from md_stat import MdStatMgr
from comm_utils import *
from usx_service import UsxServiceManager as ServiceMgr
from ibdmanager import IBDManager
from upgrade_status import UpgradeStatus
sys.path.append("/opt/milio/libs/atlas")
from log import errormsg, debug, set_log_file
from cmd import runcmd
from atl_util import load_json_conf, load_usx_conf, do_system_timeout, do_system


sys.path.append("/opt/milio/atlas/roles/ha")
from ha_util import ha_check_enabled, ha_unmange_one_resouce, ha_manage_one_resouce, ha_cleanup_failed_resource

SVM_EXPORTS = '/etc/ilio/svm_exports.json'
ATLAS_JSON = '/etc/ilio/atlas.json'

IBDSERVERCONFIGFILE_DEF = '/etc/ilio/ibdserver.conf'
IBDSERVERCONFIGFILE_TMP = '/etc/ilio/ibdserver.conf.tmp'
IBDSERVERCONFIGFILE_UP = '/etc/ilio/ibdserver.conf.upgrade'
NEW_IBDMANAGER = '/usr/local/bin/ibdmanager.new'
OLD_IBDMANAGER = '/usr/local/bin/ibdmanager.org'
IBDMANAGER = '/usr/local/bin/ibdmanager'

LOG_FILENAME = '/var/log/usx-patch-post.log'
set_log_file(LOG_FILENAME)



class UpgradeError(Exception):
    def __init__(self, desc):
        self.desc = desc

    def __str__(self):
        return repr(self.desc)

@singleton
class UpgradeIBDMgr(object):
    def __init__(self):
        self._map_ibd_version = {
            True: 'FastFailover',
            False: 'NonFastFailover'
        }

    def get_volume_mode(self, is_fastfailover):
        type_volume = None
        try:
            type_volume = UsxConfig().volume_type
        except Exception as e:
            debug('{error}'.format(error=e))
        debug('node type was {volume}'.format(volume=type_volume))
        self.is_need_flush = False
        if is_fastfailover in self._map_ibd_version:
            if is_fastfailover:
                self.is_need_flush = True
                return globals()[self._map_ibd_version[is_fastfailover]]()
            else:
                if type_volume in ['SIMPLE_HYBRID'] and os.path.isfile('/bufdevice'):
                    self.is_need_flush = True
                    return globals()['NonFastFailoverSimple']()
                elif type_volume in ['SVM']:
                    return globals()['NonFastFailoverServer']()
                else:
                    return globals()[self._map_ibd_version[is_fastfailover]]()
        else:
            UpgradeError('get volume mode failed!')


class BaseIBDUpgrade(object):
    def __init__(self):
        self.volume_type = None
        try:
            self.volume_type = UsxConfig().volume_type
        except Exception as e:
            debug('volume type was service vm {error}'.format(error=e))
        self.is_new_simple = False

    def start_export_service(self):
        rc= ServiceMgr.start_service(self._mount_point)
        if rc != 0:
            raise UpgradeError('start nfs failed.')

    def stop_export_service(self):
        ServiceMgr.stop_export_service()
        if rc != 0:
            raise UpgradeError('stop nfs failed.')

    def mount_dedupfs(self):
        rc = do_system('mount -t {} -o {} {} {}'.format(self._mount_type, self._mount_opt, self._mount_dev, self._mount_point))
        if rc != 0:
            raise UpgradeError('mount dedupfs failed.')

    def umount_dedupfs(self):
        rc = self._kill_fsuser(self._mount_point)
        if rc != 0:
            raise UpgradeError('kill fs user failed.')
        rc = do_system('umount {}'.format(self._mount_point))
        if rc != 0:
            raise UpgradeError('umount dedupfs failed.')

    def _kill_fsuser(self, export_path):
        out = ['']
        do_system('lsof -t -x +D ' + export_path, out)
        fsusers = out[0].split()
        for pid in fsusers:
            do_system('kill -9 ' + pid)
            time.sleep(2)
            if os.path.exists('/proc/' + pid):
                debug('Cannot kill ' + pid)
                return 1
            debug('Killed ' + pid)
        return 0

    def get_export_mount_info(self):
        rc, msg = runcmd('mount | grep {} | grep -v grep'.format(UsxConfig().volume_dedup_mount_point))
        if rc != 0:
            raise UpgradeError('get mount info failed.')
        info = msg.split()
        self._mount_dev = info[0]
        self._mount_point = info[2]
        self._mount_type = info[4]
        self._mount_opt = info[5][1:-1]

    def checking_ibd(self):
        self._check_agent_ready()

    def _check_agent_ready(self):
        debug("check the ibd working state")
        while not IBDManager.is_local_ibdserver_working(True):
            time.sleep(1)

    def _stop_ibdsever(self):
        debug('stop ibdserver!')
        cmd_stop = '/bin/ibdmanager -r s -S'
        runcmd(cmd_stop, print_ret=True)
        # Check the process again.
        try:
            self._waiting_for_ibdserver_stop(30)
        except Exception as e:
            cmd_run = 'ps -ef | grep ibdserver | grep -v upgrade_ibdserver | grep -v grep'
            cmd_kill = 'pkill -9 ibdserver'
            (ret, msg) = runcmd(cmd_run, print_ret=True)
            if ret == 0:
                runcmd(cmd_kill, print_ret=True)

            self._waiting_for_ibdserver_stop(60)
        else:
            debug('successfully stop ibdserver')

    def _waiting_for_ibdserver_stop(self, time_out):
        debug('wait for ibdserver normal closed.')
        deadtime = time.time() + int(time_out)
        while time.time() < deadtime:
            cmd_run = 'ps -ef | grep ibdserver | grep -v upgrade_ibdserver | grep -v grep'
            (ret, msg) = runcmd(cmd_run, print_ret=True)
            if ret != 0:
                debug('ibdserver was closed clearly')
                break
            time.sleep(0.5)
        else:
            raise UpgradeError('waiting for ibdserver closed more than 60 seconds.')

    def _load_mount_point(self):
        self.dedup_export_name = UsxConfig().volume_dedup_mount_point
        cmd_mount = 'mount|grep {device_export}|grep -v grep'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_mount, print_ret=True)
        if len(msg) <= 0:
            errormsg('can\'t get the file system mount ponit')
            raise UpgradeError('failed to load mount point!')

    def _freeze_filesystem(self):
        cmd_str = 'fsfreeze -f {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_str, print_ret=True)
        if rc != 0:
            if 'Device or resource busy' in msg:
                debug('FS already freezed')
            else:
                errormsg('ERROR: Failed to freeze {device_export}'.format(device_export=self.dedup_export_name))
                raise UpgradeError('{msg}'.format(msg=msg))
        debug('fsfreeze file system successfully!')

    def _unfreeze_filesystem(self):
        cmd_unfreeze = 'fsfreeze -u {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_unfreeze, print_ret=True)
        if rc != 0:
            if 'Invalid argument' in msg:
                debug('fs already unfreezed')
            elif 'no filename specified' in msg:
                debug('fs not mounted. Skipping...')
            else:
                raise UpgradeError('try to unfreeze failed!')
        debug('unfreeze file system successfully!')

    def _flush_cache(self):
        debug('start flush write cache data on ibdserver!')
        rc, msg = runcmd('/bin/ibdmanager -r s -b ff', print_ret=True)
        if rc != 0:
            raise UpgradeError('start flush data failed with {msg}'.format(msg=msg))

    def _finish_flush_cache(self):
        debug('check ibdserver flush data status!')
        # now proceed with flushing data
        assigned_cmd = 'ibdmanager -r s -s get | grep seq_assigned | cut -d ":" -f2'
        flushed_cmd = 'ibdmanager -r s -s get | grep seq_flushed | cut -d ":" -f2'
        (rc_assigned, msg_assigned) = runcmd(assigned_cmd, print_ret=True, lines=True)
        (rc_flushed, msg_flushed) = runcmd(flushed_cmd, print_ret=True, lines=True)
        str_assigned = msg_assigned[0]
        str_flushed = msg_flushed[0]
        flush_cmd = "ibdmanager -r s -b ff"
        debug("str_assigned = %s, str_flushed = %s" % (str_assigned, str_flushed))
        while int(str_assigned) > int(str_flushed):
            (rc_flush, msg_flush) = runcmd(flush_cmd, print_ret=True, lines=True)
            time.sleep(3)
            (rc_assigned, msg_assigned) = runcmd(assigned_cmd, print_ret=True, lines=True)
            (rc_flushed, msg_flushed) = runcmd(flushed_cmd, print_ret=True, lines=True)
            str_assigned = msg_assigned[0]
            str_flushed = msg_flushed[0]
            debug("str_assigned = %s, str_flushed = %s" % (str_assigned, str_flushed))

        debug('Flush data completed, stop ff.')
        cmd_stop_flush = 'ibdmanager -r s -b stop_ff'
        rc, msg = runcmd(cmd_stop_flush, print_ret=True)
        if rc != 0:
            errormsg('flush failed {msg}'.format(msg=msg))
            raise UpgradeError('stop flush cache data failed!')

    def flush_cache(self):
        try:
            # self._freeze_filesystem()
            self._flush_cache()
        except Exception as exp_flush:
            raise exp_flush
        finally:
            try:
                self._finish_flush_cache()
                # self._unfreeze_filesystem()
            except Exception as exp_flush:
                raise exp_flush


class NewIBDUpgrade(BaseIBDUpgrade):
    def __init__(self):
        super(NewIBDUpgrade, self).__init__()

    def _change_ibdmanager_version(self):
        if os.path.exists(NEW_IBDMANAGER):
            try:
                shutil.copyfile(IBDMANAGER, OLD_IBDMANAGER)
                debug('Save ibdmanger of old version successfully!')
                shutil.copyfile(NEW_IBDMANAGER, IBDMANAGER)
            except Exception as e:
                errormsg('rename ibdmanager failed with {error}'.format(error=e))
        debug('Finish to change ibdmanager binary file.')

    def _upgrade_server_new(self):
        self.wc_dev = None
        self.export_dev = None
        self.device_uuid = None
        self._config_parser = ConfigParser.ConfigParser()
        if os.path.exists(IBDSERVERCONFIGFILE_TMP):
            ibdconfig_file = IBDSERVERCONFIGFILE_TMP
        else:
            ibdconfig_file = IBDSERVERCONFIGFILE_DEF
        self._config_parser.read(ibdconfig_file)
        debug('Enter upgrade_server_new.')
        # Reset configuration.
        # backup ibd configuration file
        shutil.copyfile(ibdconfig_file, IBDSERVERCONFIGFILE_UP)
        config_support_vdi()
        if self.volume_type in ['SVM']:
            config = load_json_conf(SVM_EXPORTS)
            for value in config.values():
                self.device_uuid = value['euuid']
                try:
                    has_uuid = self._config_parser.has_section(self.device_uuid)
                    self.export_dev = value['exportpath']
                except Exception as check_section:
                    raise UpgradeError('{msg}'.format(msg=check_section))
                if not has_uuid:
                    raise UpgradeError('volume uuid not match! Please check it!')
                rc = apply_new_drw_channel(self.device_uuid, self.export_dev, is_need_online_commands=False)
                if rc != 0:
                    raise UpgradeError(
                        'server try to add channel failed with {server_uuid}'.format(server_uuid=self.device_uuid))
        elif self.volume_type in ['SIMPLE_HYBRID', 'HYBRID']:
            try:
                self._stop_ibdsever()
                self._change_ibdmanager_version()
                self.device_uuid = UsxConfig().ibdserver_resources_uuid
                has_uuid = self._config_parser.has_section(self.device_uuid)
                if not has_uuid:
                    raise UpgradeError('volume uuid not match! Please check it!')
                if self.volume_type in ['SIMPLE_HYBRID']:
                    self.wc_dev = '/bufdevice'
                    if self.is_new_simple:
                        self.export_dev = '/dev/dedupvg/deduplv'
                    else:
                        self.export_dev = self._config_parser.get(self.device_uuid, 'exportname')
                else:
                    self.wc_dev = '/dev/usx-default-wc'
                    self.export_dev = '/dev/usx-default-tg'
            except Exception as check_section:
                raise UpgradeError('{msg}'.format(msg=check_section))
            rc = apply_new_drw_channel(self.device_uuid, self.export_dev, self.wc_dev)
            if rc != 0:
                raise UpgradeError(
                    'volume try to add channel failed with uuid {volume_uuid}'.format(volume_uuid=self.device_uuid))


class OldIBDUpgrade(BaseIBDUpgrade):
    def __init__(self):
        super(OldIBDUpgrade, self).__init__()

    def _load_svm_config(self):
        if not os.path.isfile(SVM_EXPORTS):
            raise UpgradeError('there is no {svm_file} file in the volume '.format(svm_file=SVM_EXPORTS))
        try:
            svm_exports_file = open(SVM_EXPORTS, 'r')
            data = svm_exports_file.read()
            svm_exports_file.close()
            self.all_exports = json.loads(data)
        except:
            debug(traceback.format_exc())
            raise UpgradeError('load json file failed!')

    def _upgrade_server_new(self):
        if 'SIMPLE_HYBRID' in self.volume_type:
            self._stop_ibdsever()
            self.all_exports = {}
            self.cache_size = '4'
            self._conf_parser = ConfigParser.ConfigParser()
            self._load_svm_config()
            for deatil_uuid in self.all_exports:
                for deatil_subd in self.all_exports[deatil_uuid]:
                    if 'subdevices' in deatil_subd:
                        for device in self.all_exports[deatil_uuid][deatil_subd]:
                            if 'WHOLEDISK' in device['storagetype'].upper():
                                self.uuid = device['uuid']
                                exp_path = None
                                self._conf_parser.read(IBDSERVERCONFIGFILE_DEF)
                                if self._conf_parser.has_section(self.uuid):
                                    exp_path = self._conf_parser.get(self.uuid, 'exportname')
                                else:
                                    UpgradeError('volume uuid not match!')
                                wc_device = self._conf_parser.get('global', 'io_bufdevice')
                                cache_size = self._conf_parser.get('global', 'io_bufdevicesz')
                                reset_ibdserver_config()
                                config_support_vdi()
                                self.exp_path = device['devicepath']
                                rc = modify_drw_config_cache_vdi(self.uuid, exp_path, wc_device, cache_size)
                                if rc != 0:
                                    raise UpgradeError('change the ibdserver config failed!')
                                self._start_ibdserver()
                        break
                else:
                    raise UpgradeError('get subdevices failed!')
        # else:
        #     debug('don\'t need configure ibdserver configuration again!')
        #     self._start_ibdserver()

    def _start_ibdserver(self):
        debug('start ibdserver')
        cmd_start = '/bin/ibdserver'
        runcmd(cmd_start, print_ret=True)


class FastFailover(NewIBDUpgrade if UsxSettings().enable_new_ibdserver else OldIBDUpgrade):
    def __init__(self):
        super(FastFailover, self).__init__()
        if UsxConfig().is_contains_volume:
            self._load_mount_point()
            self._zero_wc_devcie = '/dev/ibd-wc-vg/ibd-wc-lv'

    def configuration_ibserver(self):
        self._upgrade_server_new()
        time_flag = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n'
        with open('/etc/ilio/patch_upgrading', 'w') as upgrade_w:
            upgrade_w.write('upgrade-flag\n')
            upgrade_w.write(time_flag)


    def upgrade_lvm_conf(self):
        debug('Don\'t need upgrade lvm conf')

    def _zero_wc_device(self, wc_dev):
        self._stop_ibdsever()
        debug('WARNING: start to zero the cache device header...')
        if wc_dev is None:
            debug('Cannot find write cache device.')
            return 1
        out = ['']
        cmd_str = 'blockdev --getsz {wc_device}'.format(wc_device=wc_dev)
        do_system(cmd_str, out)
        # dd zero on wc header and tail
        # header:size_in_byte/4096*32 + 10M
        # tail: last 40M.
        dd_tail_size = 40
        count_num = int(out[0].strip()) / 2 / (1024 * 1024) * 8 + 10
        cmd_str = 'dd if=/dev/zero of={wc_device} bs=1M count={count_num} oflag=sync conv=notrunc'.format(
            wc_device=wc_dev, count_num=count_num)
        try:
            # Set the timeout as 300s.
            do_system_timeout(cmd_str, 300)
            debug('Zero on cache device header finished.')
        except:
            # Ignore the timeout error.
            debug('WARNNING: cannot zero the header of %s' % wc_dev)
        seek_num = int(out[0].strip()) / 2 / 1024 - int(dd_tail_size)
        cmd_str = 'dd if=/dev/zero of={wc_device} bs=1M seek={seek_num} count={count_num} oflag=sync conv=notrunc'.format(
            wc_device=wc_dev, seek_num=seek_num, count_num=dd_tail_size)
        try:
            # Set the timeout as 300s
            do_system_timeout(cmd_str, 300)
            debug('Zero on cache device tail finished.')
        except:
            # Ignore the timeout error.
            debug('WARNNING: cannot zero the tail of {wc_device}'.format(wc_device=wc_dev))

    def zero_write_cache(self):
        self._zero_wc_device(self._zero_wc_devcie)


class NonFastFailover(NewIBDUpgrade if UsxSettings().enable_new_ibdserver else OldIBDUpgrade):
    def __init__(self):
        super(NonFastFailover, self).__init__()

    def _load_mount_point(self):
        pass

    def _freeze_filesystem(self):
        debug('fsfreeze file system successfully!')

    def _unfreeze_filesystem(self):
        debug('unfreeze file system successfully!')

    def _flush_cache(self):
        debug('start flush write cache data on ibdserver!')

    def _finish_flush_cache(self):
        debug('Flush data completed!')

    def configuration_ibserver(self):
        if UsxConfig().volume_type not in ['HAVM']:
            time_flag = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n'
            with open('/etc/ilio/patch_upgrading', 'w') as upgrade_w:
                upgrade_w.write('upgrade-flag\n')
                upgrade_w.write(time_flag)

    def upgrade_lvm_conf(self):
        debug('Don\'t need upgrade lvm conf')

    def zero_write_cache(self):
        debug('Don\'t need zero write cache')


class NonFastFailoverServer(NonFastFailover):
    def __init__(self):
        super(NonFastFailoverServer, self).__init__()

    def configuration_ibserver(self):
        self._upgrade_server_new()


class NonFastFailoverSimple(FastFailover):
    def __init__(self):
        super(NonFastFailoverSimple, self).__init__()
        self.is_new_simple = is_new_simple_hybrid()
        self._zero_wc_devcie = '/bufdevice'

    def upgrade_lvm_conf(self):
        debug('Enter upgrade_lvm_conf.')
        if not self.is_new_simple:
            lvm_conf = '/etc/lvm/lvm.conf'
            lvm_conf_bk = '/etc/lvm/lvm.conf.bk'
            rc = 0
            ret, msg = runcmd('grep -q \"/dev/sda\" %s' % lvm_conf)
            if ret == 0:
                debug('lvm_conf: Allow every block device on filter.')
                runcmd('cp %s %s' % (lvm_conf, lvm_conf_bk))
                runcmd("sed -i -e 's/filter = \[\"a|\/dev\/sda|\", .* \]/filter = \[ \"a\/\.\*\/\" \]/g' %s" % lvm_conf)
                rc = 1

            ret, msg = runcmd('grep -q \"/dev/sdb\" %s' % lvm_conf)
            if ret == 0:
                debug('lvm_conf: Comment sdb disk on filter.')
                runcmd(
                    "sed -i -e 's/filter = \[ \"r|\/dev\/sdb|\" \]/# filter = \[ \"r|\/dev\/cdrom|\" \]/g' %s" % lvm_conf)
                rc = 1

            if rc == 1:
                debug('lvm_conf: Update initramfs.')
                runcmd("update-initramfs -u -k `uname -r`")
        debug('lvm config upgrade completed!')

    def _zero_wc_device(self, wc_dev):
        self._stop_ibdsever()
        cmd_dd = 'dd if=/dev/zero of={wc_device} bs=1M count=4096 oflag=sync conv=notrunc'.format(wc_device=wc_dev)
        try:
            ret, msg = runcmd(cmd_dd, print_ret=True)
        except Exception as e:
            raise UpgradeError(e)
        if ret != 0:
            errormsg('zero write device of simple Hybrid failed with {msg}'.format(msg=msg))
            raise UpgradeError('zero write device of simple Hybrid failed with {msg}'.format(msg=msg))


def upgrade_md_stat():
    debug('Entering upgrade_md_stat.')
    if UsxConfig().is_contains_volume and not MdStatMgr.is_stat_created():
        # volume node will create md stat record.
        ret = MdStatMgr.create_stat()
        if ret != 0:
            raise Exception('ERROR: cannot create md stat record.')


if __name__ == "__main__":
    debug('Enter %s' % sys.argv[0])
    rc = 0
    try:
        upgrade_md_stat()
        if UsxConfig().volume_type not in ['SIMPLE_HYBRID', 'HYBRID']:
            upgrade_ibd = UpgradeIBDMgr().get_volume_mode(UsxConfig().is_fastfailover)
            if UpgradeIBDMgr().is_need_flush:
                ha_enabled = ha_check_enabled()
                if ha_enabled:
                    vv_uuid = UsxConfig().volume_uuid
                    flag_file = '/tmp/HASM_MOVEDISK_{}'.format(vv_uuid)
                    do_system('touch {}'.format(flag_file))
                    ha_unmange_one_resouce(vv_uuid)
                upgrade_ibd.get_export_mount_info()
                upgrade_ibd.stop_export_service()
                upgrade_ibd.umount_dedupfs()
            upgrade_ibd.flush_cache()
            upgrade_ibd.upgrade_lvm_conf()
            upgrade_ibd.zero_write_cache()
            upgrade_ibd.configuration_ibserver()
            if UpgradeIBDMgr().is_need_flush:
                upgrade_ibd.checking_ibd()
                upgrade_ibd.mount_dedupfs()
                upgrade_ibd.start_export_service()
                if ha_enabled:
                    ha_manage_one_resouce(vv_uuid)
                    do_system('rm {}'.format(flag_file))
                    ha_cleanup_failed_resource()
        elif UsxConfig().volume_type == 'SIMPLE_HYBRID':
            UpgradeStatus().run_state('upgrading')
        elif UsxConfig().volume_type == 'HYBRID':
            # Just set patch_upgrading flag here.
            time_flag = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n'
            with open('/etc/ilio/patch_upgrading', 'w') as upgrade_w:
                upgrade_w.write('upgrade-flag\n')
                upgrade_w.write(time_flag)
    except Exception as e:
        debug('{e}'.format(e=e))
        rc = 1
    debug('upgrade ibdserver script exit with {num}'.format(num=rc))
    sys.exit(rc)
