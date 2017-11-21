import sys
import json
import shutil
import time
import re
import ConfigParser
import os
from collections import OrderedDict
from comm_utils import singleton
from ibdserver_conf import config_support_vdi, IBDServerBaseConfig, IBDServerNew
from usx_config import UsxConfig
from cmd_utils import lines, stop_ibdserver
from ibdmanager import IBDManager

sys.path.append("/opt/milio/libs/atlas")
from log import errormsg, debug
from cmd import runcmd
from atl_util import vgchange_active_sync, can_open_dev

IBDSERVERCONFIGFILE_DEF = '/etc/ilio/ibdserver.conf'
IBDSERVERCONFIGFILE_TMP = '/etc/ilio/ibdserver.conf.tmp'
IBDSERVERCONFIGFILE_UP = '/etc/ilio/ibdserver.conf.upgrade'
NEW_IBDMANAGER = '/usr/local/bin/ibdmanager.new'
OLD_IBDMANAGER = '/usr/local/bin/ibdmanager.org'
IBDMANAGER = '/usr/local/bin/ibdmanager'
NEW_IBDSERVER = '/usr/local/bin/ibdserver.new'
OLD_IBDSERVER = '/usr/local/bin/ibdserver.org'
IBDSERVER = '/usr/local/bin/ibdserver'
binary_list = [NEW_IBDMANAGER, OLD_IBDMANAGER, NEW_IBDSERVER, OLD_IBDSERVER]


@singleton
class UpgradeStatus(object):
    def __init__(self):
        self._map_dir = (
            ('upgrading', self.__upgrading),
            ('zero_wc', self.__zero_write_cache),
            ('upgrade_lvmcf', self.__upgrade_lvm_conf),
            ('config_ibd', self.__config_ibdserver_conf),
            ('done', self.__done))
        self._map = OrderedDict(self._map_dir)
        self._status = {}
        self._file_name = '/etc/ilio/upgrade_status.json'
        if not os.path.exists(self._file_name):
            self.__set_val('upgrade', None)
            self.save()

    def __save(self):
        with open(self._file_name, 'w') as write_file:
            json.dump(self._status, write_file, indent=4)
            write_file.flush()
            os.fsync(write_file.fileno())

    def __set_val(self, key, value):
        self._status[key] = value

    def _get_val(self, key):
        self.__load()
        return self._status[key]

    def __load(self):
        with open(self._file_name, 'r') as read_file:
            self._status = json.load(read_file)

    def run_state(self, running_status):
        rc = 0
        try:
            tp = self._map[running_status]
            tp()
        except Exception as e:
            debug(e)
            rc = 1
        else:
            self.set_status(running_status)
        return rc

    def upgrade_boot_up(self):
        debug('Entering upgrade boot up')
        rc = 0
        while True:
            running_status = self.get_status()
            if self._get_val('upgrade') is None:
                break
            rc = self.run_state(running_status)
            if rc != 0:
                break
            if running_status == 'done':
                break
        return rc

    def set_status(self, status):
        self.__set_val('upgrade', status)
        self.save()
        debug('set status {} successfully'.format(status))

    def __get_next_status(self):
        next_status_flag = False
        current_status = self._get_val('upgrade')
        if current_status == 'done':
            return current_status
        for status in self._map:
            if status == current_status:
                next_status_flag = True
                continue
            if next_status_flag:
                return status

    def get_status(self):
        return self.__get_next_status()

    def save(self):
        try:
            self.__save()
        except Exception as e:
            raise e

    def __zero_wc_device(self, wc_dev):
        cmd_dd = 'dd if=/dev/zero of={wc_device} bs=1M count=4096 oflag=sync conv=notrunc'.format(wc_device=wc_dev)
        try:
            ret, msg = runcmd(cmd_dd, print_ret=True)
        except Exception as e:
            raise OSError(e)
        if ret != 0:
            errormsg('zero write device of simple Hybrid failed with {msg}'.format(msg=msg))
            raise SystemError('zero write device of simple Hybrid failed with {msg}'.format(msg=msg))

    def __flush_cache(self):
        rc = IBDManager.flush_ibd_write_cache_sync()
        if rc != 0:
            raise OSError('ERROR:Flush cache failed.')

    def __zero_write_cache(self):
        try:
            self.__start_ibd_server()
            self.__flush_cache()
        except Exception as e:
            raise e
        # stop ibdserver
        stop_ibdserver()
        self.__zero_wc_device('/bufdevice')

    def __start_ibd_server(self):
        # Back up ibdserver configuration
        self._check_binary_list(binary_list)
        shutil.copyfile(IBDSERVERCONFIGFILE_DEF, IBDSERVERCONFIGFILE_UP)
        shutil.copyfile(OLD_IBDSERVER, IBDSERVER)
        shutil.copyfile(OLD_IBDMANAGER, IBDMANAGER)
        try:
            if self._is_new_simply_hybrid():
                rc = vgchange_active_sync('dedupvg')
                if rc != 0:
                    raise OSError('ERROR: cannot start lv for simple hybird')
                # check whether can open the /dev/dedupvg/deduplv
                dev = '/dev/dedupvg/deduplv'
                if not can_open_dev(dev):
                    raise OSError('cannot open device {}'.format(dev))

            ret, msg = runcmd('/bin/ibdserver', print_ret=True)
            if ret != 0:
                raise OSError('start ibdserver failed')
            ret = IBDServerBaseConfig(IBDSERVERCONFIGFILE_DEF).is_work_ibdserver()
            if ret != 0:
                raise OSError('ibdserver was not working')
            ret = IBDManager.waiting_ibdserver_bio_status_to_active(30)
            if ret != 0:
                raise OSError('ibdserver bio status not work to active')
        except Exception as e:
            raise e
        else:
            debug('successfully start ibd server')

    def _is_new_simply_hybrid(self):
        is_new = False
        if UsxConfig().volume_type in ['SIMPLE_HYBRID']:
            cmd_pvs = 'pvs --noheadings -o pv_name'
            out_put = lines(cmd_pvs, True)
            for l in out_put:
                m = re.search('sdb|xvdb', l)
                if m is not None:
                    is_new = True
                    break
        else:
            is_new = False
        return is_new

    def __upgrade_lvm_conf(self):
        debug('Enter upgrade_lvm_conf.')
        if not self._is_new_simply_hybrid():
            lvm_conf = '/etc/lvm/lvm.conf'
            lvm_conf_bk = '/etc/lvm/lvm.conf.bk'
            rc = 0
            ret, msg = runcmd('grep -q \"/dev/sda\" %s' % lvm_conf)
            if ret == 0:
                debug('lvm_conf: Allow every block device on filter')
                runcmd('cp %s %s' % (lvm_conf, lvm_conf_bk))
                runcmd("sed -i -e 's/filter = \[\"a|\/dev\/sda|\", .* \]/filter = \[ \"a\/\.\*\/\" \]/g' %s" % lvm_conf)
                rc = 1

            ret, msg = runcmd('grep -q \"/dev/sdb\" %s' % lvm_conf)
            if ret == 0:
                debug('lvm_conf: Comment sdb disk on filter')
                runcmd(
                    "sed -i -e 's/filter = \[ \"r|\/dev\/sdb|\" \]/# filter = \[ \"r|\/dev\/cdrom|\" \]/g' %s" % lvm_conf)
                rc = 1

            if rc == 1:
                debug('lvm_conf: Update initramfs.')
                runcmd("update-initramfs -u -k `uname -r`")
        debug('lvm config upgrade completed')

    def _check_binary_list(self, list_binary):
        for binary_path in list_binary:
            if not os.path.exists(binary_path):
                raise OSError('There is not exist binary of {}'.format(binary_path))

    def __config_ibdserver_conf(self):
        if not os.path.exists(IBDSERVERCONFIGFILE_UP):
            shutil.copyfile(IBDSERVERCONFIGFILE_DEF, IBDSERVERCONFIGFILE_UP)
        config_parser = ConfigParser.ConfigParser()
        config_parser.read(IBDSERVERCONFIGFILE_UP)
        resources_uuid = UsxConfig().ibdserver_resources_uuid
        if not config_parser.has_section(resources_uuid):
            raise OSError('there is not has resources uuid {} in the {}'.format(resources_uuid, IBDSERVERCONFIGFILE_UP))
        if self._is_new_simply_hybrid():
            export_dev = '/dev/dedupvg/deduplv'
        else:
            export_dev = config_parser.get(resources_uuid, 'exportname')

        wc_dev = '/bufdevice'
        try:
            config_support_vdi()
            IBDServerNew(IBDSERVERCONFIGFILE_DEF).add_channel(resources_uuid, export_dev, wc_dev,
                                                              is_need_online_commands=False)
        except Exception as e:
            shutil.copyfile(OLD_IBDMANAGER, IBDMANAGER)
            shutil.copyfile(IBDSERVERCONFIGFILE_UP, IBDSERVERCONFIGFILE_DEF)
            raise e
        shutil.copyfile(NEW_IBDSERVER, IBDSERVER)
        shutil.copyfile(NEW_IBDMANAGER, IBDMANAGER)

    def __upgrading(self):
        debug('start upgrade status')
        with open('/etc/ilio/patch_upgrading', 'w') as fb:
            fb.flush()
            os.fsync(fb.fileno())

    def __done(self):
        if os.path.exists('/etc/ilio/patch_upgrading'):
            os.rename('/etc/ilio/patch_upgrading', '/etc/ilio/patch_upgraded')
        debug('Finish to upgrade')


def usage():
    debug('python script.py set/get status_name')


if __name__ == '__main__':
    status_c = UpgradeStatus()
    cmd_options = {
        'set': status_c.set_status,
        'get': status_c.get_status
    }
    if len(sys.argv) < 2:
        usage()
        exit(1)
    cmd_type = sys.argv[1]
    if cmd_type == 'set':
        cmd_options[cmd_type](sys.argv[2])
    elif cmd_type == 'get':
        cmd_options[cmd_type]()
