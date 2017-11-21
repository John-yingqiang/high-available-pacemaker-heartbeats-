#!/usr/bin/python
import sys
import os
import time
import traceback
from comm_utils import singleton
from usx_config import UsxConfig
from usx_service import UsxServiceManager

sys.path.append('/opt/milio/atlas/roles/virtvol/')
import ddp_setup

sys.path.insert(0, '/opt/milio/libs/atlas')
from log import debug
from atl_util import do_system, udev_trigger


ZFS_POOL = 'usx-zpool'
ZFS_CMD = '/usr/local/sbin/zfs'
ZPOOL_CMD = '/usr/local/sbin/zpool'
MODPROBE_CMD = '/sbin/modprobe'
LSMOD_CMD = '/bin/lsmod'
ASHIFT = '13'
ZFS_BLOCK_DEVICE = 'usx-block-device'
ZFS_BLOCK_DEV_PATH = '/dev/zvol/' + ZFS_POOL + '/' + ZFS_BLOCK_DEVICE


class ZfsTool(object):

    @staticmethod
    def load_kernel_module(kernel_module):
        debug('Enter load_kernel_module.')
        if kernel_module is None:
            debug('Cannot find kernel module, exit.')
            return 1

        # Create /etc/zfs directory if needed.
        zfs_dir = '/etc/zfs'
        if not os.path.exists(zfs_dir):
            debug('Create %s directory.' % zfs_dir)
            os.mkdir(zfs_dir)

        # Run ldconfig first.
        cmd_str = 'ldconfig'
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Running ldconfig failed.')
            return rc

        # Check whether it is in loaded status.
        cmd_str = LSMOD_CMD + ' ' + '| grep' + ' ' + kernel_module
        rc = do_system(cmd_str)
        if rc != 0:
            # Load zfs module
            cmd_str = MODPROBE_CMD + ' ' + kernel_module
            rc = do_system(cmd_str)
            if rc != 0:
                debug('Load kernel module %s failed.' % kernel_module)
                return rc

        # Set zfs_arc_max=total memory * 20%
        out = ['']
        get_total_memory_cmd = " free -b | grep Mem | awk {'print $2'} "
        do_system(get_total_memory_cmd, out)
        total_memory = out[0].strip().split()[0]
        zfs_need_memory = int(total_memory) * 0.2

        # Set zfs module property.
        zfs_module_property = {
            '/sys/module/zfs/parameters/zfs_arc_max':                   int(zfs_need_memory),
            '/sys/module/zfs/parameters/spa_load_verify_data':          0,
            '/sys/module/zfs/parameters/spa_load_verify_metadata':      0,
            '/sys/module/zfs/parameters/zfs_txg_timeout':               1,
            '/sys/module/zfs/parameters/zil_slog_limit':                16777216}

        for key, value in zfs_module_property.items():
            rc = ZfsTool.zfs_module_set_property(key, value)
            if rc != 0:
                return rc
        return 0

    @staticmethod
    def zfs_module_set_property(key, value, pool_name=ZFS_POOL):
        debug('Enter zfs_module_set_property.')
        cmd_str = 'echo  %s  >  %s' % (value, key)
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Set zfs module property %s failed.' % key)
            return rc
        return 0

    @staticmethod
    def zfs_set_property(key, value, pool_name=ZFS_POOL):
        debug('Enter zfs_set_property.')
        cmd_str = ZFS_CMD + ' set ' + key + '=' + value + ' ' + pool_name
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Set zfs property %s failed.' % key)
            return rc

        return 0

    @staticmethod
    def zfs_block_set_property(key, value, pool_name=ZFS_POOL, block_device=ZFS_BLOCK_DEVICE):
        debug('Enter zfs_block_set_property')
        cmd_str = '%s set %s=%s %s/%s' % (ZFS_CMD, key, value, pool_name, block_device)
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Set zfs block property %s failed.' % key)
            return rc
        return 0

    @staticmethod
    def zfs_init(mount_point, ibd_device):
        debug('Enter zfs_init.')

        # Load zfs firt
        kernel_module = 'zfs'
        rc = ZfsTool.load_kernel_module(kernel_module)
        if rc != 0:
            return rc

        # Init zfs
        cmd_str = ZPOOL_CMD + ' create -f -o ashift=' + ASHIFT + ' -m ' + mount_point + ' ' + ZFS_POOL + ' ' + ibd_device
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Initialize ZFS failed.')
            return rc

        # Set zfs property
        zfs_property = {
            'dedup': 'on',
            'recordsize': '4096',
            'dedup': 'on',
            'checksum': 'off'}
        for key, value in zfs_property.items():
            rc = ZfsTool.zfs_set_property(key, value)
            if rc != 0:
                return rc
        return 0

    @staticmethod
    def zfs_import(dir_path=None):
        debug('Enter zfs_import')

        # Load zfs firt
        kernel_module = 'zfs'
        rc = ZfsTool.load_kernel_module(kernel_module)
        if rc != 0:
            return rc

        # Import zfs.
        cmd_str = ZPOOL_CMD + ' import ' + ZFS_POOL
        if dir_path:
            cmd_str = '%s import -d %s %s' % (ZPOOL_CMD, dir_path, ZFS_POOL)

        deadtime = time.time() + 60
        while True and time.time() < deadtime:
            rc = do_system(cmd_str)
            if rc != 0:
                debug('Import ZFS failed. get the status and will retry.')
                cmd_check = cmd_str.replace(ZFS_POOL, '')
                do_system(cmd_check)
                time.sleep(1.25)
            else:
                return 0
        return 1

    @staticmethod
    def zfs_export():
        debug('Enter zfs_export')

        cmd_str = ZPOOL_CMD + ' export ' + ZFS_POOL
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Export ZFS failed.')
            return rc

        return 0

    @staticmethod
    def zfs_create_pool(raid_device, log_dev=None):
        debug('Enter zfs_create_pool.')

        # Load zfs firt
        kernel_module = 'zfs'
        rc = ZfsTool.load_kernel_module(kernel_module)
        if rc != 0:
            return rc
        if log_dev:
            cmd_str = '%s create -f -m none -o ashift=%s %s %s  log %s' % (ZPOOL_CMD, ASHIFT, ZFS_POOL, raid_device, log_dev)
        else:
            cmd_str = '%s create -f -m none -o ashift=%s %s %s' % (ZPOOL_CMD, ASHIFT, ZFS_POOL, raid_device)
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Create zfs pool failed.')
            return rc
        return 0

    @staticmethod
    def zfs_create_block_device(raid_device, log_dev=None):
        debug('Enter zfs_create_block_device.')
        # create zfs pool first.
        rc = ZfsTool.zfs_create_pool(raid_device, log_dev)
        if rc != 0:
            return None
        # check zfs pool size.
        out = ['']
        get_size_str = ZFS_CMD + " list -o space -p | grep " + ZFS_POOL + " | awk {'print $2'} "
        do_system(get_size_str, out)
        # Reserve 5% space.
        zpool_free_space = int(float(out[0].strip()) * 0.95)
        # Rounded up to the nearest 128 Kbytes to ensure that the volume has an integral number of blocks regardless of blocksize.
        modulo_num = zpool_free_space % (128 * 1024)
        block_device_size = zpool_free_space - modulo_num
        if block_device_size < 1:
            debug('Failed to get block device size from zfs pool.')
            return None
        # create a zfs volume 10 times larger than it's real size .
        # temp disable it for performance test.
        #block_device_size = block_device_size * 10
        # Create block device.
        zvol_create_option = 'volblocksize=8K'
        cmd_str = '%s create -s -V %s -o %s %s/%s' % (ZFS_CMD, str(block_device_size), zvol_create_option, ZFS_POOL, ZFS_BLOCK_DEVICE)
        rc = do_system(cmd_str)
        if rc != 0:
            debug('Failed to create block device.')
            return None

        # Set zfs block device property.
        zfs_block_property = {
            'snapdev':          'visible',
            'refreservation':   'none'
        }

        for key, value in zfs_block_property.items():
            rc = ZfsTool.zfs_block_set_property(key, value)
            if rc != 0:
                return None

        # Set some zfs level perperty as well.
        zfs_level_property = {
            'dedup':            'on',
            'atime':            'off',
            'logbias':          'latency',
            'recordsize':       '8K'
        }
        for key, value in zfs_level_property.items():
            rc = ZfsTool.zfs_set_property(key, value)
            if rc != 0:
                return None

        block_device_path = ZFS_BLOCK_DEV_PATH
        return block_device_path

    def zfs_add_log_dev(self, log_dev, zfs_pool=ZFS_POOL):
        cmd_str = '%s add %s log %s' % (ZPOOL_CMD, zfs_pool, log_dev)
        ret = do_system(cmd_str)
        if ret != 0:
            debug('ERROR: add log device for zfs volume failed.')
        return ret


class BtrfsTool(object):

    @staticmethod
    def mkfs(target_dev):
        cmd_str = 'mkfs.btrfs -f %s' % target_dev
        return do_system(cmd_str)

    @staticmethod
    def mount(mount_point, target_dev, option=None):
        if option is None:
            cmd_str = 'mount -t btrfs %s %s' % (target_dev, mount_point)
        else:
            cmd_str = 'mount -t btrfs -o %s %s %s' % (option, target_dev, mount_point)
        return do_system(cmd_str)


class Ext4Tool(object):

    @staticmethod
    def mkfs(target_dev):
        lazy_itable_init_option = 'lazy_itable_init=0'
        lazy_journal_init_option = 'lazy_journal_init=0'
        cmd_str = 'mkfs.ext4 -E %s,%s -F %s' % (lazy_itable_init_option, lazy_journal_init_option, target_dev)
        return do_system(cmd_str)

    @staticmethod
    def mount(mount_point, target_dev, option=None):
        if option is None:
            cmd_str = 'mount -t ext4 %s %s' % (target_dev, mount_point)
        else:
            cmd_str = 'mount -t ext4 -o %s %s %s' % (option, target_dev, mount_point)
        return do_system(cmd_str)


@singleton
class FsManager(object):

    def __init__(self):
        self._fs_map = {
            'zfs': 'ZfsDevice',
            'btrfs': 'BtrfsDevice',
            'ext4': 'Ext4Device',
            'dedup': 'DedupfsDevice'
        }

    def get_dev(self, fs_type, dev_name, **config):
        try:
            if fs_type.lower() in self._fs_map:
                return globals()[self._fs_map[fs_type.lower()]](dev_name, **config)
            else:
                debug('ERROR: not support fs type [%s]' % fs_type)
                return None
        except Exception as e:
            debug(e)
            debug(traceback.format_exc())
            return None


class FsError(Exception):
    def __init__(self, desc):
        self.desc = desc

    def __str__(self):
        return repr(self.desc)


class FsDevice(object):
    """docstring for ZfsDevice"""

    def __init__(self, dev_name, **config):
        self._dev_name = dev_name
        self._vscaler_dev = config.get('vscaler_dev')
        self._log_dev = config.get('log_dev')
        self._log_size = config.get('log_size', 400)
        self._old_mnttab = "/etc/ilio/mnttab.old"
        self._mnttab = "/etc/ilio/mnttab"
        self._DELIMITER = "}}##0##{{"

    def init_with_mount(self, mount_point, mnt_option=None, **config):
        if mnt_option:
            self._def_mnt_option = mnt_option
        self._mount_point = mount_point
        if not os.path.exists(mount_point):
            os.mkdir(mount_point)
        try:
            self._initialize(**config)
            self._setup_mnttable()
            ret = UsxServiceManager.start_service(self._mount_point)
            return ret
        except Exception, e:
            debug('ERROR: %s' % e)
            debug(traceback.format_exc())
            return 1

    def start_with_mount(self, mount_point, mnt_option=None, **config):
        if mnt_option:
            self._def_mnt_option = mnt_option
        self._mount_point = mount_point
        if not os.path.exists(mount_point):
            os.mkdir(mount_point)
        try:
            self._start(**config)
            ret = UsxServiceManager.start_service(self._mount_point)
            return ret
        except Exception, e:
            debug('ERROR: %s' % e)
            debug(traceback.format_exc())
            return 1
        return 0

    def setup_mnttable_for_ha(self, **config):
        try:
            self._mount_point = config.get('mount_point', '')
            self._setup_mnttable(**config)
            return 0
        except Exception, e:
            debug('ERROR: %s' % e)
            debug(traceback.format_exc())
            return 1
        return 0

    def reset_fs(self, **config):
        try:
            self._reset_fs(**config)
            return 0
        except Exception, e:
            debug('ERROR: %s' % e)
            debug(traceback.format_exc())
            return 1
        return 0

    def _initialize(self, **config):
        raise FsError('NotImplement')

    def _start(self, **config):
        raise FsError('NotImplement')

    def _setup_mnttable(self, **config):
        if os.path.exists(self._mnttab):
            os.rename(self._mnttab, self._old_mnttab)
        cache_dev = ''
        cache_name = ''
        if self._vscaler_dev:
            # cache_dev = '/dev/mapper/vmdata_cache'
            cache_dev = self._vscaler_dev
            # cache_name = 'vmdata_cache'
            cache_name = 'vmdata_cache'
        jdev = ''
        if self._log_dev:
            out = ['']
            rc = do_system('/usr/bin/stat --printf="%02t %02T" ' + self._log_dev, out)
            output = out[0]
            if rc == 0 and output and output.strip():
                major = int(output.split(' ')[0], 16)
                minor = int(output.split(' ')[1], 16)
                jdev = os.makedev(major, minor)

        mnt_tab = [self._dev_name, cache_dev, cache_name, self._mount_point, self.mount_option, jdev]
        with open(self._mnttab, 'w') as fd:
            tab_str = self._DELIMITER.join(mnt_tab)
            fd.write(tab_str)
            fd.write('\n')
            fd.flush()
            os.fsync(fd.fileno())


class ZfsDevice(FsDevice):
    """docstring for ZfsDevice"""

    def init_block_dev(self, log_dev=None):
        try:
            block_dev_path = ZfsTool.zfs_create_block_device(self._dev_name, log_dev)
            if block_dev_path and self._wait_dev_ready(block_dev_path, 60):
                assert(block_dev_path == ZFS_BLOCK_DEV_PATH)
            else:
                raise FsError('zfstool error cannot got %s.' % block_dev_path)
            return 0
        except Exception, e:
            debug('ERROR: cannot make zfs block device. [%s]' % e)
            return 1

    def start_block_dev(self):
        try:
            block_dev_path = ZFS_BLOCK_DEV_PATH
            ret = ZfsTool.zfs_import(os.path.dirname(self._dev_name))
            if ret != 0:
                raise FsError('zfstool error')
            udev_trigger()
            if not self._wait_dev_ready(block_dev_path, 3600):
                raise FsError('cannot find the zfs block device: %s' % ZFS_BLOCK_DEV_PATH)
            return 0
        except Exception, e:
            debug('ERROR: cannot start zfs block device. [%s]' % e)
            return 1

    def export_block_dev(self, link_dev_path):
        try:
            self._block_dev = ZFS_BLOCK_DEV_PATH
            self._create_link(self._block_dev, link_dev_path)
            return 0
        except Exception, e:
            debug('ERROR: cannot link the device [%s] to [%s] [%s]' % (self._block_dev, link_dev_path, e))
            return 1

    def _initialize(self, **config):
        ret = ZfsTool.zfs_init(self._mount_point, self._dev_name)
        if ret != 0:
            raise FsError('cannot initialize zfs!')

    def _start(self, **config):
        ret = ZfsTool.zfs_import(os.path.dirname(self._dev_name))
        if ret != 0:
            raise FsError('cannot start zfs!')

    def _setup_mnttable(self, **config):
        pass

    def _reset_fs(self, **config):
        pass

    def _wait_dev_ready(self, dev, timeout):
        deadtime = time.time() + timeout
        while not os.path.exists(dev) and time.time() < deadtime:
            time.sleep(0.1)
        return os.path.exists(dev)

    def _create_link(self, src_dev, dst_dev):
        cmd_str = '/bin/ln -f -s %s %s' % (src_dev, dst_dev)
        ret = do_system(cmd_str)
        if ret != 0:
            raise FsError('create link [%s] failed' % cmd_str)


class BtrfsDevice(FsDevice):
    """docstring for  BtrfsDevice"""

    def _initialize(self, **config):
        ret = BtrfsTool.mkfs(self._dev_name)
        if ret != 0:
            raise FsError('make btrfs failed!')
        ret = BtrfsTool.mount(self._mount_point, self._dev_name)
        if ret != 0:
            raise FsError('mount btrfs failed')

    def _start(self, **config):
        ret = BtrfsTool.mount(self._mount_point, self._dev_name)
        if ret != 0:
            raise FsError('mount btrfs failed')

    # def _setup_mnttable(self, **config):
    #     pass

    def _reset_fs(self, **config):
        pass


class Ext4Device(FsDevice):
    """docstring for  Ext4Device"""

    mount_option = 'noatime,nodiratime'

    def _initialize(self, **config):
        ret = Ext4Tool.mkfs(self._dev_name)
        if ret != 0:
            raise FsError('make ext4 failed!')
        ret = Ext4Tool.mount(self._mount_point, self._dev_name, self.mount_option)
        if ret != 0:
            raise FsError('mount ext4 failed')

    def _start(self, **config):
        ret = Ext4Tool.mount(self._mount_point, self._dev_name, self.mount_option)
        if ret != 0:
            raise FsError('mount ext4 failed')

    # def _setup_mnttable(self, **config):
    #     pass

    def _reset_fs(self, **config):
        ret = Ext4Tool.mkfs(self._dev_name)
        if ret != 0:
            raise FsError('make ext4 failed!')


class DedupfsDevice(FsDevice):

    def _initialize(self, **config):
        ddp_dev = self._dev_name
        vscaler_dev = self._vscaler_dev
        log_dev = self._log_dev
        log_size = self._log_size
        mnt_point = UsxConfig().volume_dedup_mount_point
        ha = UsxConfig().is_ha
        conf = UsxConfig().atltis_conf
        debug("Calling ddp_setup.config_ddp with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        ret = ddp_setup.config_ddp(config, mnt_point, ddp_dev, vscaler_dev, log_dev, log_size, ha)
        if ret != 0:
            raise FsError('config dedupfs failed.')
        if ha:
            debug('HA node, skip mounting.')
            return
        debug("None HA, Start DDP by calling ddp_setup.init_ddp with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        ret = ddp_setup.init_ddp(conf, mnt_point, ddp_dev, vscaler_dev, log_dev, ha)
        if ret != 0:
            raise FsError('start dedupfs failed.')

    def _start(self, **config):
        ddp_dev = self._dev_name
        vscaler_dev = self._vscaler_dev
        log_dev = self._log_dev
        # log_size = self._log_size
        mnt_point = UsxConfig().volume_dedup_mount_point
        ha = UsxConfig().is_ha
        conf = UsxConfig().atltis_conf
        # debug("Calling ddp_setup.ddp_update_device_list with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        # ret = ddp_setup.ddp_update_device_list(config, mnt_point, ddp_dev, vscaler_dev, log_dev)
        # if ret != 0:
        #     raise FsError('update dedupfs failed.')
        # if ha:
        #     debug('HA node, skip mounting.')
        #     return
        debug("None HA, Start DDP by calling ddp_setup.init_ddp with ddp_dev:", ddp_dev, ' vscaler_dev:', vscaler_dev)
        ret = ddp_setup.init_ddp(conf, mnt_point, ddp_dev, vscaler_dev, log_dev, ha)
        if ret != 0:
            raise FsError('start dedupfs failed.')

    # def _setup_mnttable(self, **config):
    #     pass

    def _reset_fs(self, **config):
        ddp_dev = self._dev_name
        vscaler_dev = self._vscaler_dev
        log_dev = self._log_dev
        log_size = self._log_size
        # mnt_point = UsxConfig().volume_dedup_mount_point
        conf = UsxConfig().atltis_conf
        ret = ddp_setup.reset_ddp(conf, ddp_dev, vscaler_dev, log_dev, log_size)
        if ret != 0:
            raise FsError('reset dedupfs failed.')

    def _setup_mnttable(self, **config):
        ddp_dev = self._dev_name
        vscaler_dev = self._vscaler_dev
        log_dev = self._log_dev
        # log_size = self._log_size
        mnt_point = UsxConfig().volume_dedup_mount_point
        # ha = UsxConfig().is_ha
        conf = UsxConfig().atltis_conf
        debug("Calling ddp_setup.ddp_update_device_list with ddp_dev:",  ddp_dev, ' vscaler_dev:', vscaler_dev)
        ret = ddp_setup.ddp_update_device_list(conf, mnt_point, ddp_dev, vscaler_dev, log_dev)
        if ret != 0:
            raise FsError('update dedupfs failed.')
