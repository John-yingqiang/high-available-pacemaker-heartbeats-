#! /usr/bin/python
import os
import json
from usx_config import UsxConfig as UC
from proclock import SimpleLock
from functools import update_wrapper
import traceback
import sys
sys.path.append("/opt/milio/libs/atlas")
from log import debug
sys.path.append("/opt/milio/atlas/roles/ha")
import ha_util


def debugmethod(f):
    def wrapper_func(self, *args, **kwargs):
        if not self.DEBUG:
            return 0
        return f(self, *args, **kwargs)
    return update_wrapper(wrapper_func, f)


class IBDSrvExportConfig(object):
    """Sample

    >>>
        from utils import *

        ibd_exp_conf = IBDSrvExportConfig()
        print 'default configuration for ibdserver: '
        ibd_exp_conf.show()
        ibd_exp_conf.high_water_mark = 80
        ibd_exp_conf.low_water_mark = 60
        ibd_exp_conf.write_delay_first_level = 40
        ibd_exp_conf.write_delay_second_level = 20
        print 'high_water_mark: ', ibd_exp_conf.high_water_mark
        print 'low_water_mark: ', ibd_exp_conf.low_water_mark
        print 'write_delay_first_level: ', ibd_exp_conf.write_delay_first_level
        print 'write_delay_second_level: ', ibd_exp_conf.write_delay_second_level
        ibd_exp_conf.set_conf(high_water_mark=120, low_water_mark=100)
        ibd_exp_conf.set_conf(high_water_mark=120, low_water_mark=100, write_delay_first_level=60,write_delay_second_level=40)

    <<<
        default configuration for ibdserver:
        INFO: load configruation.
        {'high_water_mark': 60, 'low_water_mark': 40, 'write_delay_first_level': 0, 'write_delay_second_level': 0}
        INFO: set ibdserver configuration [high_water_mark = 80]
        INFO: save configruation.
        {
            "high_water_mark":80,
            "low_water_mark":40,
            "write_delay_first_level":0,
            "write_delay_second_level":0
        }
        INFO: set ibdserver configuration [low_water_mark = 60]
        INFO: save configruation.
        {
            "high_water_mark":80,
            "low_water_mark":60,
            "write_delay_first_level":0,
            "write_delay_second_level":0
        }
        INFO: set ibdserver configuration [write_delay_first_level = 40]
        INFO: save configruation.
        {
            "high_water_mark":80,
            "low_water_mark":60,
            "write_delay_first_level":40,
            "write_delay_second_level":0
        }
        INFO: set ibdserver configuration [write_delay_second_level = 20]
        INFO: save configruation.
        {
            "high_water_mark":80,
            "low_water_mark":60,
            "write_delay_first_level":40,
            "write_delay_second_level":20
        }
        high_water_mark:  80
        low_water_mark:  60
        write_delay_first_level:  40
        write_delay_second_level:  20
        INFO: set ibdserver configuration [{'high_water_mark': 120, 'low_water_mark': 100}]
        INFO: save configruation.
        {
            "high_water_mark":120,
            "write_delay_first_level":40,
            "write_delay_second_level":20,
            "low_water_mark":100
        }
        INFO: set ibdserver configuration [{'high_water_mark': 120, 'low_water_mark': 100, 'write_delay_second_level': 40, 'write_delay_first_level': 60}]
        INFO: save configruation.
        {
            "high_water_mark":120,
            "write_delay_first_level":60,
            "write_delay_second_level":40,
            "low_water_mark":100
        }
    """

    # enable debug by default
    DEBUG = True

    # ibdserver export config module's file lock for multi-processes.
    IBDSRV_EXPORT_CONFIG_LOCK_FILE = '/tmp/ibdsrv_exp_conf.lck'

    # ibdserver export configuration file
    IBDSRV_EXPORT_CONFIG_FILE = '/etc/ilio/ibdsrv_exp_conf_{}.json'

    # container configuration.
    usx_conf = UC()

    # global file lock
    __lock = SimpleLock(IBDSRV_EXPORT_CONFIG_LOCK_FILE)

    # export configuration
    __export_conf = {}

    # default export configuration, all config must be included.
    __def_export_conf = {
        'high_water_mark': 60,
        'low_water_mark': 40,
        'write_delay_first_level': 0,
        'write_delay_second_level': 0,
        'write_delay_first_level_max_us': 200000,
        'write_delay_second_level_max_us': 500000
    }

    def __getitem__(self, key):
        if key in self.__def_export_conf:
            return self.__get_conf(key)
        else:
            raise AttributeError('not support attr [{}]'.format(key))

    def __getattr__(self, key):
        if key in self.__def_export_conf:
            return self.__get_conf(key)
        else:
            return super(IBDSrvExportConfig, self).__getattribute__(key)

    def __setitem__(self, key, value):
        if key in self.__def_export_conf:
            self.__set_conf(key)
        else:
            raise AttributeError('not support attr [{}]'.format(key))

    def __setattr__(self, key, value):
        if key in self.__def_export_conf:
            self.__set_conf(key, value)
        else:
            super(IBDSrvExportConfig, self).__setattr__(key, value)

    def set_conf(self, **config):
        with self.__lock:
            if not all(key in self.__def_export_conf.keys() for key in config.keys()):
                raise AttributeError('not suport attr [{}]'.format(config))
            try:
                self.dbg_print('INFO: set ibdserver configuration [{}]'.format(config))
                if not self.__export_conf:
                    self.__load()
                for key, value in config.items():
                    self.__export_conf[key] = value
                self.__save()
            except Exception, e:
                self.dbg_print(traceback.format_exc())
                debug('ERROR: cannot set config [{}], [{}]'.format(config, e))
                raise e

    def __get_conf(self, key):
        with self.__lock:
            try:
                if not self.__export_conf:
                    self.__load()
                return self.__export_conf[key]
            except Exception as e:
                self.dbg_print(traceback.format_exc())
                debug('ERROR: cannot get config [{}], [{}]'.format(key, e))
                raise e

    def __set_conf(self, key, value):
        with self.__lock:
            try:
                self.dbg_print('INFO: set ibdserver configuration [{} = {}]'.format(key, value))
                if not self.__export_conf:
                    self.__load()
                self.__export_conf[key] = value
                self.__save()
            except Exception, e:
                self.dbg_print(traceback.format_exc())
                debug('ERROR: cannot set config [{}], [{}]'.format(key, e))
                raise e

    @debugmethod
    def dbg_print(self, *args):
        msg = " ".join([str(x) for x in args])
        print(msg)
        debug(msg)

    @debugmethod
    def show(self):
        try:
            if not self.__export_conf:
                self.__load()
            self.dbg_print(self.__export_conf)
        except Exception, e:
            raise e

    def __init__(self, volume_uuid=None):
        if volume_uuid is None:
            self.__volume_uuid = self.usx_conf.volume_uuid
        self.__exp_conf_file = self.IBDSRV_EXPORT_CONFIG_FILE.format(self.__volume_uuid)

    def __load(self):
        self.dbg_print('INFO: load configruation.')
        self.__load_from_local()
        if ha_util.ha_check_enabled():
            try:
                self.__load_from_remote()
            except Exception, e:
                self.dbg_print('load configuration from remote failed.')
                raise e

    def __save(self):
        self.dbg_print('INFO: save configruation.')
        self.__save_to_local()
        if ha_util.ha_check_enabled():
            try:
                self.__load_from_remote()
            except Exception, e:
                self.dbg_print('save configuration to remote failed.')
                raise e

    def __load_from_local(self):
        try:
            self.__export_conf.update(self.__def_export_conf)
            if not os.path.exists(self.__exp_conf_file):
                return
            with open(self.__exp_conf_file, 'r') as fd:
                tmp_conf = json.load(fd)
            self.__export_conf.update(tmp_conf)
        except Exception as e:
            self.dbg_print(traceback.format_exc())
            raise e

    def __save_to_local(self):
        try:
            if os.path.exists(self.__exp_conf_file):
                # backup
                bak_file = '{}.bak'.format(self.__exp_conf_file)
                os.rename(self.__exp_conf_file, bak_file)
            with open(self.__exp_conf_file, 'w') as fd:
                self.dbg_print(json.dumps(self.__export_conf, indent=4, separators=(',', ':')))
                json.dump(self.__export_conf, fd, indent=4, separators=(',', ':'))
                fd.flush()
                os.fsync(fd.fileno())
        except Exception, e:
            self.dbg_print(traceback.format_exc())
            raise e

    def __load_from_remote(self):
        try:
            debug('TODO: implement __load_from_remote')
        except Exception, e:
            self.dbg_print(traceback.format_exc())
            raise e

    def __save_to_remote(self):
        try:
            debug('TODO: implement __save_to_remote')
        except Exception, e:
            self.dbg_print(traceback.format_exc())
            raise e
