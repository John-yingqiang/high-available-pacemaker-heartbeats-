import ConfigParser
import argparse
import os
import sys
import subprocess
import json

sys.path.append("/opt/milio/libs/atlas")
from log import debug, info, warn, errormsg

sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *

IBDSERVERCONFIGFILE_DEF = '/etc/ilio/ibdserver.conf'
IBDSERVERCONFIGFILE_TMP = '/etc/ilio/ibdserver.conf.tmp'


class IBDManagerChangeValueBase(object):
    def __init__(self):
        if milio_config.volume_type not in ['SIMPLE_HYBRID', 'HYBRID']:
            raise OSError('Script not support this node.')
        self._config_ibdserver = IBDSrvExportConfig()

    def change_bwc_water_mark(self, channel_uuid, high_water_mark, low_water_mark):
        if high_water_mark is None:
            high_water_mark = self._config_ibdserver.high_water_mark
        if low_water_mark is None:
            low_water_mark = self._config_ibdserver.low_water_mark
        self._config_ibdserver.set_conf(high_water_mark=high_water_mark, low_water_mark=low_water_mark)
        ibdserver_name_conf = SrvConfName(channel_uuid)
        self.change_bwc_water_mark_online(ibdserver_name_conf.wc, self._config_ibdserver.high_water_mark,
                                          self._config_ibdserver.low_water_mark)
        self._save_config(ibdserver_name_conf.wc, high_water_mark=high_water_mark, low_water_mark=low_water_mark)

    def change_bwc_flow_control(self, channel_uuid, delay_first_level, delay_second_level, delay_first_level_max,
                                delay_second_level_max):
        if delay_first_level is None:
            delay_first_level = self._config_ibdserver.write_delay_first_level
        if delay_second_level is None:
            delay_second_level = self._config_ibdserver.write_delay_second_level
        self._config_ibdserver.set_conf(write_delay_first_level=delay_first_level,
                                        write_delay_second_level=delay_second_level,
                                        write_delay_first_level_max_us=delay_first_level_max,
                                        write_delay_second_level_max_us=delay_second_level_max)
        ibdserver_name_conf = SrvConfName(channel_uuid)
        self.change_flow_control_online(ibdserver_name_conf.wc, self._config_ibdserver.write_delay_first_level,
                                        self._config_ibdserver.write_delay_second_level,
                                        delay_first_level_max,
                                        delay_second_level_max)
        self._save_config(ibdserver_name_conf.wc, write_delay_first_level=delay_first_level,
                          write_delay_second_level=delay_second_level,
                          write_delay_first_level_max_us=delay_first_level_max,
                          write_delay_second_level_max_us=delay_second_level_max)

    def _save_config(self, uuid, **conf):
        self._load_confg()
        if not self.config_parser.has_section(uuid):
            raise OSError('not has section {} in config file {}'.format(uuid, self.config_file_name))
        for key, value in conf.items():
            if not self.config_parser.has_option(uuid, key):
                raise OSError('not has option {} in config file {}'.format(uuid, self.config_file_name))
            self.config_parser.set(uuid, key, value)
        with open(self.config_file_name, 'w') as w:
            self.config_parser.write(w)

    def _load_confg(self):
        self.config_file_name = IBDSERVERCONFIGFILE_DEF
        if os.path.exists(IBDSERVERCONFIGFILE_TMP):
            self.config_file_name = IBDSERVERCONFIGFILE_TMP
        try:
            self.config_parser = ConfigParser.ConfigParser()
            self.config_parser.read(self.config_file_name)
        except Exception as e:
            raise OSError(e)


class OldIBDManagerChangeValue(IBDManagerChangeValueBase):
    def __init__(self):
        super(OldIBDManagerChangeValue, self).__init__()

    def change_bwc_water_mark_online(self, channel_uuid, high_water_mark, low_water_mark):
        pass

    def change_flow_control_online(self, channel_uuid, first_level, second_level, first_level_max, second_level_max):
        pass


class NewIBDManagerChangeValue(IBDManagerChangeValueBase):
    def __init__(self):
        super(NewIBDManagerChangeValue, self).__init__()
        self.ibdserver_name_conf = None

    def change_bwc_water_mark_online(self, channel_uuid, high_water_mark_value, low_water_mark_value):
        rc = IBDManager.bwc_water_mark_change(channel_uuid, high_water_mark_value, low_water_mark_value)
        if rc != 0:
            raise IOError('online change water mark failed!')

    def change_flow_control_online(self, channel_uuid, first_level, second_level, first_level_max, second_level_max):
        rc = IBDManager.bwc_flow_control_change(channel_uuid, first_level, second_level, first_level_max,
                                                second_level_max)
        if rc != 0:
            raise IOError('online change flow control failed!')


class IBDTuneManager(object):
    def get_version(self):
        if milio_settings.enable_new_ibdserver:
            return globals()['NewIBDManagerChangeValue']()
        else:
            return globals()['OldIBDManagerChangeValue']()


class LoadInputData(object):
    __def_flow_control = {
        'first_level_max': 200000,
        'second_level_max': 500000,
        'high_water_mark': None,
        'low_water_mark': None,
        'write_delay_first_level': None,
        'write_delay_second_level': None,
        'uuid': None
    }

    def __init__(self, data):
        try:
            self.data_info = json.loads(data)
        except Exception as e:
            raise e

    def __getitem__(self, item):

        if item in self.data_info:
            return self.data_info[item]
        elif item in self.__def_flow_control:
            return self.__def_flow_control[item]
        else:
            raise AttributeError('not support attr [{}]'.format(item))

    def __getattr__(self, item):
        if item in self.data_info:
            return self.data_info[item]
        elif item in self.__def_flow_control:
            return self.__def_flow_control[item]
        else:
            return super(LoadInputData, self).__getattribute__(item)


managet_ibd = IBDTuneManager().get_version()


def water_mark_change(args):
    managet_ibd.change_bwc_water_mark(args.channel_uuid, args.high_water, args.low_water)


def flow_control_change(args):
    managet_ibd.change_bwc_flow_control(args.channel_uuid, args.first_level, args.second_level, args.first_level_max,
                                        args.second_level_max)


def tune_threading(args):
    load_data = LoadInputData(args.json_dir)
    if load_data.uuid is None:
        load_data.uuid = milio_config.ibdserver_resources_uuid
    if load_data.high_water_mark is not None or load_data.low_water_mark is not None:
        managet_ibd.change_bwc_water_mark(load_data.uuid, load_data.high_water_mark, load_data.low_water_mark)
    if load_data.write_delay_first_level is not None or load_data.write_delay_second_level is not None:
        managet_ibd.change_bwc_flow_control(load_data.uuid, load_data.write_delay_first_level,
                                            load_data.write_delay_second_level, load_data.first_level_max,
                                            load_data.second_level_max)


def set_commands():
    """

    Usage :
    1. change water mark
    python ibd_tune.py wm [channel_uuid] -hw [high_water_mark] -lw [low_water_mark]

    2. change flow control
    python ibd_tune.py fc [channel_uuid] -fl [delay_first_level] -sl [delay_second_level] -flm [delay_first_level_max](default values is 200000, you can ignore it) -slm [delay_second_level](default values is 500000, you can ignore it))


    """
    parser = argparse.ArgumentParser(description='Change ibdserver configuration API.')
    subparsers = parser.add_subparsers()
    paser_wm = subparsers.add_parser('wm')
    paser_wm.add_argument('-u', '--channel-uuid', type=str, required=True, help='input channel uuid that you want!')
    paser_wm.add_argument('-hw', '--high-water', required=False, default=None,
                          help='input values of high water mark that you want!')
    paser_wm.add_argument('-lw', '--low-water', required=False, default=None,
                          help='input values of low water mark that you want!')
    paser_wm.set_defaults(func=water_mark_change)

    paser_fc = subparsers.add_parser('fc')
    paser_fc.add_argument('-u', '--channel-uuid', type=str, required=True, help='input channel uuid that you want!')
    paser_fc.add_argument('-fl', '--first-level', required=False, default=None,
                          help='input delay first level that you want!')
    paser_fc.add_argument('-sl', '--second-level', required=False, default=None,
                          help='input delay second level that you want!')
    paser_fc.add_argument('-flm', '--first-level-max', type=str, required=False, default='200000',
                          help='input delay first level max that you want!')
    paser_fc.add_argument('-slm', '--second-level-max', type=str, required=False, default='500000',
                          help='input delay second level max that you want!')
    paser_fc.set_defaults(func=flow_control_change)

    paser_sl = subparsers.add_parser('m')
    paser_sl.add_argument('-jd', '--json-dir', required=True)
    paser_sl.set_defaults(func=tune_threading)
    args = parser.parse_args()
    debug('({detail_info})'.format(detail_info=args))
    try:
        args.func(args)
    except Exception as e:
        errormsg('Script has something wrong with it')
        errormsg(e)
        rc = 1
    else:
        rc = 0

    info('ibd_tune scrpit exit with {num}'.format(num=rc))


if __name__ == '__main__':
    set_commands()
