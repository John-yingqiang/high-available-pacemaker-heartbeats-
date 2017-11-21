import sys
import os
import ConfigParser
import time

from ibdmanager import IBDManager
from usx_config import UsxConfig
from cmd_utils import runcmd
from usx_settings import UsxSettings
from ibdserver_conf import SrvConfName

sys.path.append("/opt/milio/libs/atlas")
from log import debug, errormsg

IBDSERVER_CONF = '/etc/ilio/ibdserver.conf'


class IBDFlushError(Exception):
    def __init__(self, desc):
        self.desc = desc

    def __str__(self):
        return repr(self.desc)


class LvsFlushWriteCacheManger(object):
    def __init__(self):
        try:
            self.volume_type = UsxConfig().volume_type
        except Exception as e:
            raise IBDFlushError('{error}'.format(error=e))
        self._map = {
            True: 'FastFailoverFlush',
            False: 'NonFastFailoverFlush'
        }

    def get_mode(self, is_fast):
        if is_fast in self._map:
            if self.volume_type in ['SIMPLE_HYBRID'] and os.path.exists('/bufdevice'):
                return globals()['NonFastFiloverFlushSimple']()
            else:
                return globals()[self._map[is_fast]]()
        else:
            raise IBDFlushError('get a wrong mode for Lvs flush write cache!')


class SrvSnapConfig(SrvConfName):
    def __init__(self, dev_uuid):
        super(SrvSnapConfig, self).__init__(dev_uuid)
        self._gen_config()

    def _gen_config(self):
        self.snap_config = {
            'type_c': 'bwc',
            'channel_uuid': super(SrvSnapConfig, self).wc,
            'uuid': super(SrvSnapConfig, self).uuid
        }

    @property
    def snapconf(self):
        return self.snap_config


class IBDFlushWriteCache(object):
    def __init__(self):
        self.dedup_export_name = None
        self.dedup_export_name = UsxConfig().volume_dedup_mount_point

    def _freeze(self):
        IBDFlushError('Invalid freeze function')

    def _unfreeze(self):
        IBDFlushError('Invalid unfreeze function')

    def _flush(self, device_path, wc_path):
        IBDFlushError('Invalid unfreeze function')


class IBDOldVersion(IBDFlushWriteCache):
    def __init__(self):
        super(IBDOldVersion, self).__init__()

    def _freeze(self):
        rc = IBDManager.write_freeze()
        if rc != 0:
            raise IBDFlushError('freeze ibdserver failed!')
        debug('freeze ibdserver successfully! ')

    def _unfreeze(self):
        rc = IBDManager.write_unfreeze()
        if rc != 0:
            raise IBDFlushError('unfreeze ibdserver failed!')
        debug('unfreeze ibdserver successfully! ')

    def _flush(self, device_path, wc_path):
        ibdserver_conf = ConfigParser.ConfigParser()
        ibdserver_conf.read(IBDSERVER_CONF)
        io_bufdevicesz = ibdserver_conf.get('global', 'io_bufdevicesz')
        io_poolsz = ibdserver_conf.get('global', 'io_poolsz')
        io_pagesz = ibdserver_conf.get('global', 'io_pagesz')
        cmd_flush = '/usr/local/bin/ibdcacheflush {write_cache} {bufsize} {poolsize} {pagesize} {export_device}'.format(
            write_cache=wc_path, bufsize=io_bufdevicesz, poolsize=io_poolsz, pagesize=io_pagesz,
            export_device=device_path)
        rc, msg = runcmd(cmd_flush, print_ret=True)
        if rc != 0:
            errormsg('ibdserver flush cache failed with {out_msg}'.format(out_msg=msg))
            raise IBDFlushError('flush cache failed!')
        debug('successfully ibdserver flush cache with \'wc_devcie = {wc_name} export_device = {export_name}\''.format(
            wc_name=wc_path, export_name=device_path))


class IBDNewVersion(IBDFlushWriteCache):
    def __init__(self):
        super(IBDNewVersion, self).__init__()
        self.snapshot_channel_info = SrvSnapConfig(UsxConfig().ibdserver_resources_uuid).snap_config

    def _freeze(self):
        rc = IBDManager.freeze_snapshot(self.snapshot_channel_info)
        if rc != 0:
            raise IBDFlushError('freeze ibdserver failed!')
        debug('freeze ibdserver successfully! ')

    def _unfreeze(self):
        rc = IBDManager.unfreeze_snapshot(self.snapshot_channel_info)
        if rc != 0:
            raise IBDFlushError('unfreeze ibdserver failed!')
        debug('unfreeze ibdserver successfully! ')

    def _flush(self, device_path, wc_path):
        debug('New ibdserver don\'t need flush cache')
        debug('write device name : {wc}'.format(wc=wc_path))
        debug('export device name: {exp}'.format(exp=device_path))


class IBDOldVersionSimple(IBDOldVersion):
    def __init__(self):
        super(IBDOldVersionSimple, self).__init__()
        if self.dedup_export_name is None:
            raise IBDFlushError('IBD get mount point failed from json file!')

    def _freeze(self):
        cmd_str = 'fsfreeze -f {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_str, print_ret=True)
        if rc != 0:
            if 'Device or resource busy' in msg:
                debug('FS already freezed')
            else:
                errormsg('ERROR: Failed to freeze {device_export}'.format(device_export=self.dedup_export_name))
                raise IBDFlushError('{msg}'.format(msg=msg))
        debug('fsfreeze file system successfully!')

    def _unfreeze(self):
        cmd_unfreeze = 'fsfreeze -u {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_unfreeze, print_ret=True)
        if rc != 0:
            if 'Invalid argument' in msg:
                debug('fs already unfreezed')
            elif 'no filename specified' in msg:
                debug('fs not mounted. Skipping...')
            else:
                raise IBDFlushError('try to unfreeze failed!')
        debug('unfreeze file system successfully!')

    def _flush(self, device_path, wc_path):
        self._flush_cache()
        self._finish_flush_cache()

    def _flush_cache(self):
        debug('start flush write cache data on ibdserver!')
        rc, msg = runcmd('/bin/ibdmanager -r s -b ff', print_ret=True)
        if rc != 0:
            raise IBDFlushError('start flush data failed with {msg}'.format(msg=msg))

    def _get_seq_assigend_size(self):
        assigned_cmd = '/bin/ibdmanager -r s -s get | grep seq_assigned | cut -d ":" -f2'
        rc, msg = runcmd(assigned_cmd, print_ret=True, lines=True)
        if rc != 0:
            raise IBDFlushError('get ibdserver seq_assigned size failed!')
        return msg[0]

    def _get_seq_flushed_size(self):
        flushed_cmd = 'ibdmanager -r s -s get | grep seq_flushed | cut -d ":" -f2'
        rc, msg = runcmd(flushed_cmd, print_ret=True, lines=True)
        if rc != 0:
            raise IBDFlushError('get ibdserver seq_flushed size failed!')
        return msg[0]

    def _finish_flush_cache(self):
        debug('get ibdserver status of flush cache!')
        need_flush = True
        while need_flush:
            time.sleep(2)
            str_assigned = self._get_seq_assigend_size()
            str_flushed = self._get_seq_flushed_size()
            debug(
                "str_assigned = {assigned}, str_flushed = {flushed}".format(assigned=str_assigned, flushed=str_flushed))
            if int(str_assigned) <= int(str_flushed):
                need_flush = False
        cmd_stop_flush = 'ibdmanager -r s -b stop_ff'
        rc, msg = runcmd(cmd_stop_flush, print_ret=True)
        if rc != 0:
            errormsg('flush failed {msg}'.format(msg=msg))
            raise IBDFlushError('stop flush cache data failed!')


class FlushWriteCache(object):
    def _load_mount_point(self):
        cmd_mount = 'mount|grep {device_export}|grep -v grep'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_mount, print_ret=True)
        if len(msg) <= 0:
            errormsg('can\'t get the file system mount ponit')
            raise IBDFlushError('failed to load mount point!')

    def flush_cache(self, device_path=None, wc_path=None):
        self.flush_device(device_path, wc_path)

    def freeze(self, need_freeze=True):
        if need_freeze:
            self._load_mount_point()
            self.freeze_device()
        debug('Finished freeze of flush write cache!')

    def unfreeze(self, need_unfreeze=True):
        if need_unfreeze:
            self.unfreeze_device()
        debug('Finished unfreeze of flush write cache!')

    def freeze_device(self):
        pass

    def unfreeze_device(self):
        pass

    def flush_device(self, device_path, wc_path):
        pass


class FastFailoverFlush(FlushWriteCache):
    def __new__(cls):
        if UsxSettings().enable_new_ibdserver:
            FastFailoverFlush.__bases__ += (IBDNewVersion,)
        else:
            FastFailoverFlush.__bases__ += (IBDOldVersion,)
        return super(FastFailoverFlush, cls).__new__(cls)

    def __init__(self):
        super(FastFailoverFlush, self).__init__()

    def freeze_device(self):
        self._freeze()

    def unfreeze_device(self):
        self._unfreeze()

    def flush_device(self, device_path, wc_path):
        self._flush(device_path, wc_path)


class NonFastFailoverFlush(FlushWriteCache):
    def __init__(self):
        super(NonFastFailoverFlush, self).__init__()

    def freeze_device(self):
        debug('Didn\'t need freeze device in this volume!')

    def unfreeze_device(self):
        debug('Didn\'t need unfreeze device in this volume!')

    def flush_device(self, device_path, wc_path):
        debug('Didn\'t need flush device in this volume!')


class NonFastFiloverFlushSimple(NonFastFailoverFlush):
    def __new__(cls):
        if UsxSettings().enable_new_ibdserver:
            NonFastFailoverFlush.__bases__ += (IBDNewVersion,)
        else:
            NonFastFailoverFlush.__bases__ += (IBDOldVersionSimple,)
        return super(NonFastFailoverFlush, cls).__new__(cls)

    def __init__(self):
        super(NonFastFiloverFlushSimple, self).__init__()

    def freeze_device(self):
        self._freeze()

    def unfreeze_device(self):
        self._unfreeze()

    def flush_device(self, device_path, wc_path):
        self._flush(device_path, wc_path)

