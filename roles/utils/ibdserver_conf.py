import ConfigParser
import sys
import time
from comm_utils import printcall, BaseParam, BaseFoo
from proclock import synclock
from ibdmanager import IBDManager
from cmd_utils import BlockDevice
from usx_settings import UsxSettings
from usx_config import UsxConfig
from ibdserver_exp_conf import IBDSrvExportConfig

sys.path.append("/opt/milio")
from libs import bio

sys.path.append("/opt/milio/libs/atlas")
from log import debug, info, warn, errormsg
from atl_util import do_system

DEFUALT_IBD_CONF_LOCK = '/tmp/default-ibdserver-conf.lock'
IBDSERVERCONFIGFILE_DEF = '/etc/ilio/ibdserver.conf'
IBDSERVERCONFIGFILE_TMP = '/etc/ilio/ibdserver.conf.tmp'
IBDSERVERCONFIGFILE_BACK_UP = '/etc/ilio/back_up_ibdserver_'
NODE_INFO_FILE = '/etc/ilio/usx-node-info.json'
USX_RC_DEV_NAME = 'default_rc_device'
USX_RC_DEV_PATH = '/dev/usx/default_rc_device'
IBDSERVER_FLAG_FILE = '/etc/ilio/flag-ibdserver-init'
BEFORE_TIMEOUT = 360
WAIT_INTERVAL = 10
support_vdi = {
    'support_bio': 1,
    'support_bwc': 1,
    'support_bwcc': 1
}

support_volume = {
    'support_inic': 1,
    'support_bio': 1,
    'support_crc': 1,
    'support_crcc': 1,
    'support_wcc': 1,
    'support_bwc': 1,
    'support_bwcc': 1
}

support_server = {
    'support_inic': 1,
    'support_doac': 1
}


class SupportParam(BaseParam):
    def __init__(self, ibdserver_setup=None):
        if ibdserver_setup is None:
            ibdserver_setup = {}
        self.support_bio = 0
        self.support_cds = 1
        self.support_crc = 0
        self.support_crcc = 0
        # self.support_dck = 0
        self.support_drc = 0
        self.support_drw = 1
        self.support_drwc = 1
        self.support_cxa = 0
        self.support_cxac = 0
        self.support_cxp = 0
        self.support_cxpc = 0
        self.support_cxt = 0
        self.support_dxa = 0
        self.support_dxac = 0
        self.support_dxp = 0
        self.support_dxpc = 0
        self.support_dxt = 0
        self.support_doac = 0
        self.support_mrw = 0
        self.support_mrwc = 0
        self.support_bwc = 0
        self.support_bwcc = 0
        self.support_nwc0 = 0
        self.support_nwc0c = 0
        self.support_rcc = 0
        self.support_rio = 1
        self.support_sac = 1
        self.support_sacc = 1
        self.support_sdsc = 1
        self.support_ppc = 1
        self.support_sds = 1
        self.support_smc = 1
        self.support_wcc = 0
        self.support_tp = 1
        self.support_twc = 0
        self.support_twcc = 0
        self.support_shm = 0
        self.support_shmc = 0
        self.support_shr = 0
        self.support_tpc = 1
        self.support_inic = 0
        self.support_rs = 0
        self.support_rsm = 0
        BaseParam.__init__(self, ibdserver_setup)

    def parse(self, ibdserver_setup):
        for keys, val in ibdserver_setup.items():
            self.__dict__[keys] = val


class SrvConfName(object):
    # TODO: separate the configuration of SVM, HybridVM, SimpleHybridVM
    #    service VM will share the same pp
    #    sds and tp
    def __init__(self, volume_uuid):
        self.uuid_s = volume_uuid

    @property
    def sds_pp(self):
        return self.uuid_s + '-sds_pp'

    @property
    def rc_pp(self):
        return self.uuid_s + '-rc_pp'

    @property
    def wc(self):
        return self.uuid_s + '-wcache'

    @property
    def crc(self):
        return self.uuid_s + '-crc'

    @property
    def sds(self):
        return self.uuid_s + '-sds'

    @property
    def drw(self):
        return self.uuid_s + '-drw'

    @property
    def mrw(self):
        return self.uuid_s + '-mrw'

    @property
    def rc(self):
        return self.uuid_s + '-rcache'

    @property
    def uuid(self):
        return self.uuid_s

    @property
    def tp(self):
        return self.uuid_s + '-tp'


class IBDServerManger(object):
    def __init__(self, config_file=IBDSERVERCONFIGFILE_DEF):
        self._version_map = {
            True: 'IBDServerNew',
            False: 'IBDServerOld'
        }
        self.ibdserver_file = config_file

    def get_version(self, ibd_version):
        if ibd_version in self._version_map:
            return globals()[self._version_map[ibd_version]](self.ibdserver_file)


class IBDServerBaseConfig(object):
    def __init__(self, ibd_server_conf):
        self._filename = ibd_server_conf
        self._filename_tmp = self._filename + '.tmp'
        self._conf_parser = ConfigParser.ConfigParser()
        self.volume_type = None
        self.role_is_volume = UsxConfig().is_volume
        self.volume_type = UsxConfig().volume_type

    def _reset_conf(self):
        with open(self._filename, 'w'):
            debug('reset the ibdserver configuration because of startup.')
            pass

    def _set_sections(self, sections, option, value):
        try:
            self._conf_parser.set(sections, option, str(value))
        except ConfigParser.NoSectionError:
            self._conf_parser.add_section(sections)
            self._conf_parser.set(sections, option, str(value))
        except Exception as e:
            errormsg('update Sectons failed in %s' % self._filename)
            errormsg('except with %s' % e)
            raise e

    def _load(self):
        self._conf_parser.read(self._filename)

    def config_support_default_base(self):
        try:
            rc = self._conf_support_default()
        except Exception as e:
            errormsg('try to modify support of ibdserver failed %s ' % e)
            rc = 1
        return rc

    def del_channel_base(self, dev_uuid):
        debug('Entering to del channel of ibdserver by uuid %s' % dev_uuid)
        return self.del_channel(dev_uuid)

    def _save(self):
        try:
            f = open(self._filename, 'w')
            self._conf_parser.write(f)
        except Exception as e:
            errormsg("Cannot save ibd config file!")
            errormsg("except with %s" % e)
            raise e
        return 0

    def save_tmp(self):
        debug('not need to save tmp file of ibd!')

    def is_work_ibdserver(self):
        ibdserver_cmd = '/bin/ibdmanager -r s -s get'
        debug('try to check ibdserver working status!')
        deadtime = time.time() + 30
        while True:
            out = ['']
            ret = do_system(ibdserver_cmd, out)
            if ret == 0:
                return 0
            if time.time() > deadtime:
                debug('ERROR: TIMEOUT ibdserver is not working!')
                return 1
            time.sleep(0.75)


class SrvDRWConfig(SrvConfName):
    def __init__(self, dev_uuid, dev_exp_path, write_cache, read_cache):
        super(SrvDRWConfig, self).__init__(dev_uuid)
        self.dev_uuid = dev_uuid
        self.dev_exp_path = dev_exp_path
        self.write_cache = write_cache
        self.read_cache = read_cache
        self.write_cache_uuid = self.wc
        self.read_cahce_uuid = self.rc
        self.node_type = UsxConfig().volume_type
        self.tp_bio_rio = self.tp
        self.io_page_sz = 8
        if write_cache is None:
            self.write_cache_uuid = 'NONE'
            self._wc_size = 0
        else:
            self._wc_size = BlockDevice(write_cache).size_mb

        if read_cache is None:
            self.read_cahce_uuid = 'NONE'
            self._do_fp = 0
            self._alignment = 0
            self._rc_size = 0
        else:
            self._do_fp = 1
            self._alignment = 4096
            self._rc_size = BlockDevice(read_cache).size_mb
        self._gen_config_info()

    @property
    def pp_info(self):
        return self._pp_info

    @property
    def sds_info(self):
        return self._sds_info

    @property
    def drw_info(self):
        return self._drw_info

    @property
    def mwc_info(self):
        return self._mwc_info

    @property
    def channel_info(self):
        return self._channel_info

    @property
    def crc_pp_info(self):
        return self._crc_pp_info

    @property
    def crc_info(self):
        return self._crc_info

    @property
    def tp_info(self):
        return self._tp_info

    @property
    def wc_tp_info(self):
        return self._wc_tp_info

    def get_pool_size(self, sz_value):
        return sz_value / self.io_page_sz + 8

    def _gen_config_info(self):
        self._ibd_exp_conf = IBDSrvExportConfig()
        self._pp_info = {
            'pp_name': self.sds_pp,
            'owner_type': 'sds',
            'pool_size': self.get_pool_size(self._wc_size),
            'page_size': self.io_page_sz
        }

        self._crc_pp_info = {
            'pp_name': self.rc_pp,
            'owner_type': 'crc',
            'pool_size': 16,
            'page_size': 64
        }

        self._crc_info = {
            'crc_uuid': self.read_cahce_uuid,
            'crc_pp_name': self.rc_pp,
            'cache_device': self.read_cache,
            'cache_size': str(self._rc_size)
        }

        default_policy = 'bfp1'
        default_policy_dict = {}
        if default_policy == 'bfp1':
            default_policy_dict = {
                'ssd_mode': 0,
                'max_flush_size': 1,
                'write_delay_first_level': self._ibd_exp_conf.write_delay_first_level,
                'write_delay_second_level': self._ibd_exp_conf.write_delay_second_level,
                'flush_policy': 'bfp1',
                'high_water_mark': self._ibd_exp_conf.high_water_mark,
                'low_water_mark': self._ibd_exp_conf.low_water_mark,
                'write_delay_first_level_max_us': self._ibd_exp_conf.write_delay_first_level_max_us,
                'write_delay_second_level_max_us': self._ibd_exp_conf.write_delay_second_level_max_us
            }
        elif default_policy == 'bfp4':
            default_policy_dict = {
                'flush_policy': 'bfp4',
                'throttle_ratio': '0.6',
                'load_ctrl_level': 1,
                'flush_delay_ctl': 10,
                'load_ratio_min': '0.1',
                'load_ratio_max': '0.95'
            }
        elif default_policy == 'bfp5':
            default_policy_dict = {
                'ssd_mode': 0,
                'max_flush_size': 1,
                'write_delay_first_level': self._ibd_exp_conf.write_delay_first_level,
                'write_delay_second_level': self._ibd_exp_conf.write_delay_second_level,
                'flush_policy': 'bfp5',
                'load_ratio_min': '0.11',
                'load_ratio_max': '0.99',
                'load_ctrl_level': '1',
                'flush_delay_ctl': '0',
                'throttle_ratio': '0.6',
                'coalesce_ratio': '0'
            }
        default_info_dict = {
            'mwc_uuid': self.write_cache_uuid,
            'bufdev': self.write_cache,
            'bufdev_size': str(self._wc_size),
            'rw_sync': 1,
            'two_step_read': 0,
            'do_fp': self._do_fp,
            'tp_name': 'default_wc_tp'
        }
        self._mwc_info = default_policy_dict.copy()
        self._mwc_info.update(default_info_dict)
        debug(self._mwc_info)
        self._sds_info = {
            'sds_name': self.sds,
            'sds_pp_name': self._pp_info['pp_name'],
            'sds_wc_uuid': self.write_cache_uuid,
            'sds_rc_uuid': self.read_cahce_uuid
        }

        self._drw_info = {
            'drw_name': self.drw,
            'exportname': self.dev_exp_path
        }

        self._channel_info = {
            'c_uuid': self.dev_uuid,
            'sync': 1 if self.node_type in ['SVM'] else 0,
            'direct_io': 0 if self.node_type in ['SVM'] else 1,
            'alignment': self._alignment,
            'ds_name': self._sds_info['sds_name'],
            'dev_name': self._drw_info['drw_name'],
            'tp_name': self.tp_bio_rio,
            'exportname': self.dev_exp_path,
            'dev_size': None,
            'enable_kill_myself': 1 if self.node_type in ['SVM'] else 0
        }
        self._tp_info = {
            'tp_uuid': self.tp_bio_rio,
            'number_work': 8
        }

        self._wc_tp_info = {
            'tp_uuid': 'default_wc_tp',
            'number_work': 8
        }


class IBDServerNew(IBDServerBaseConfig):
    def __init__(self, conf_file):
        super(IBDServerNew, self).__init__(conf_file)
        #        debug('Entering class %s ' % self.__class__.__name__)
        self.default_tp_name = 'default_tp'
        self.default_wc_tp_name = 'default_wc_tp'
        self.default_cds_name = 'default_cds'
        self.default_scg_name = 'default_scg'
        self.default_dck_name = 'default_dck'

    def _conf_support_default(self):
        self._reset_conf()
        if self.role_is_volume:
            if 'SIMPLE_HYBRID' in self.volume_type:
                support_dir = SupportParam(support_vdi).dict_param
            else:
                support_dir = SupportParam(support_volume).dict_param
        else:
            support_dir = SupportParam(support_server).dict_param
        self._type_config('global', 'global')
        for support_key, support_val in support_dir.items():
            self._set_sections('global', support_key, support_val)
        self._config_default()
        self._save()

    def _type_config(self, sections, section_type):
        self._set_sections(sections, 'type', section_type)

    def _config_default(self):
        try:
            self._type_config(self.default_tp_name, 'tp')
            self._set_sections(self.default_tp_name, 'num_workers', 8)
            self._type_config(self.default_scg_name, 'scg')
            self._set_sections(self.default_scg_name, 'wtp_name', self.default_tp_name)
            self._type_config(self.default_cds_name, 'cds')
            self._set_sections(self.default_cds_name, 'page_size', 8)
            self._set_sections(self.default_cds_name, 'page_nr', 4)
            # self._type_config(self.default_dck_name, 'dck')
        except Exception as e:
            errormsg('%s' % e)
            return 1
        return 0

    def config_crc(self, crc_section_name, read_cache_path, read_cache_size, pp_name):
        # read_cache_size = read_cache_size * 1024
        self._type_config(crc_section_name, 'crc')
        self._set_sections(crc_section_name, 'cache_device', read_cache_path)
        self._set_sections(crc_section_name, 'cache_size', read_cache_size)
        self._set_sections(crc_section_name, 'pp_name', pp_name)

    def config_nwc_bwc(self, nwc_section_name, type_nb, write_cache_path, write_cache_size, tp_name=None, do_fp=None,
                       rw_sync=None, flush_policy=None, two_step_read=None, write_first_level=None,
                       write_second_level=None,
                       low_water_mark=None, high_water_mark=None, delay_first_max=None, delay_second_max=None):
        # write_cache_size = write_cache_size * 1024
        self._type_config(nwc_section_name, type_nb)
        self._set_sections(nwc_section_name, 'cache_device', write_cache_path)
        self._set_sections(nwc_section_name, 'cache_size', write_cache_size)
        self._set_sections(nwc_section_name, 'rw_sync', rw_sync)
        self._set_sections(nwc_section_name, 'tp_name', tp_name)
        self._set_sections(nwc_section_name, 'ssd_mode', '0')
        self._set_sections(nwc_section_name, 'max_flush_size', '1')
        self._set_sections(nwc_section_name, 'write_delay_first_level', write_first_level)
        self._set_sections(nwc_section_name, 'write_delay_second_level', write_second_level)
        if flush_policy == 'bfp1':
            self._set_sections(nwc_section_name, 'flush_policy', flush_policy)
            self._set_sections(nwc_section_name, 'low_water_mark', low_water_mark)
            self._set_sections(nwc_section_name, 'high_water_mark', high_water_mark)
            if delay_first_max is not None:
                self._set_sections(nwc_section_name, 'write_delay_first_level_max_us', delay_first_max)
            if delay_second_max is not None:
                self._set_sections(nwc_section_name, 'write_delay_second_level_max_us', delay_second_max)

        elif flush_policy == 'bfp4':
            self._set_sections(nwc_section_name, 'flush_policy', flush_policy)
            self._set_sections(nwc_section_name, 'load_ratio_min', '0.1')
            self._set_sections(nwc_section_name, 'load_ratio_max', '0.95')
            self._set_sections(nwc_section_name, 'load_ctrl_level', 1)
            self._set_sections(nwc_section_name, 'flush_delay_ct', 10)
            self._set_sections(nwc_section_name, 'throttle_ratio', '0.6')
        elif flush_policy == 'bfp5':
            self._set_sections(nwc_section_name, 'flush_policy', flush_policy)
            self._set_sections(nwc_section_name, 'load_ratio_min', '0.11')
            self._set_sections(nwc_section_name, 'load_ratio_max', '0.99')
            self._set_sections(nwc_section_name, 'load_ctrl_level', '1')
            self._set_sections(nwc_section_name, 'flush_delay_ct', '0')
            self._set_sections(nwc_section_name, 'throttle_ratio', '0.6')

        self._set_sections(nwc_section_name, 'coalesce_ratio', '0')
        self._set_sections(nwc_section_name, 'do_fp', do_fp)
        self._set_sections(nwc_section_name, 'two_step_read', two_step_read)

    def config_sds(self, sds_section_name, sds_pp_name, nwc_name=None, crc_name=None):
        self._type_config(sds_section_name, 'sds')
        self._set_sections(sds_section_name, 'pp_name', sds_pp_name)
        if nwc_name is not None:
            self._set_sections(sds_section_name, 'wc_uuid', nwc_name)
        if crc_name is not None:
            self._set_sections(sds_section_name, 'rc_uuid', crc_name)

    def config_tp(self, tp_setcion_name, number_work):
        self._type_config(tp_setcion_name, 'tp')
        self._set_sections(tp_setcion_name, 'num_workers', number_work)

    def config_drw_export(self, drw_section_name, drw_export_path):
        self._type_config(drw_section_name, 'drw')
        self._set_sections(drw_section_name, 'exportname', drw_export_path)

    def config_mrw_export(self, mrw_section_name):
        self._type_config(mrw_section_name, 'mrw')

    def config_sac_drw(self, sac_section_name, alignment, ds_name, dev_name, tp_name=None):
        self._type_config(sac_section_name, 'sac')
        self._set_sections(sac_section_name, 'sync', 1 if self.volume_type in ['SVM'] else 0)
        self._set_sections(sac_section_name, 'direct_io', 0 if self.volume_type in ['SVM'] else 1)
        self._set_sections(sac_section_name, 'alignment', alignment)
        self._set_sections(sac_section_name, 'ds_name', ds_name)
        self._set_sections(sac_section_name, 'dev_name', dev_name)
        self._set_sections(sac_section_name, 'enable_kill_myself', 1 if self.volume_type in ['SVM'] else 0)
        if tp_name:
            self._set_sections(sac_section_name, 'tp_name', tp_name)

    def config_sac_mrw(self, sac_section_name, alignment, dev_size, tp_name, ds_name, dev_name, volume_uuid):
        self._type_config(sac_section_name, 'sac')
        self._set_sections(sac_section_name, 'sync', 1)
        self._set_sections(sac_section_name, 'direct_io', 0)
        self._set_sections(sac_section_name, 'alignment', alignment)
        self._set_sections(sac_section_name, 'tp_name', tp_name)
        self._set_sections(sac_section_name, 'ds_name', ds_name)
        self._set_sections(sac_section_name, 'dev_name', dev_name)
        self._set_sections(sac_section_name, 'dev_size', dev_size)
        self._set_sections(sac_section_name, 'volume_uuid', volume_uuid)

    def default_page_pool(self, section_name, type_s, pool_size, page_size):
        self._type_config(section_name, type_s)
        self._set_sections(section_name, 'pool_size', pool_size)
        self._set_sections(section_name, 'page_size', page_size)

    def add_channel_online(self, ibdserver_conf, write_dev_path=None, read_dev_path=None):
        # Step 1. Add pp
        rc = IBDManager.pp_add(ibdserver_conf.pp_info)
        if rc != 0:
            warn('Add pp failed.Maybe it is already exist.')

        # Step 2. Add sds
        rc = IBDManager.sds_add(ibdserver_conf.sds_info)
        if rc != 0:
            warn('Add sds failed.Maybe it is already exist.')
        # Step 3. Add tp
        rc = IBDManager.tp_add(ibdserver_conf.tp_info)
        if rc != 0:
            errormsg('Add tp for %s failed.' % ibdserver_conf.dev_uuid)
            warn('Add tp failed.Maybe it is already exist.')
        # Step 3. add nwc0 and crc
        if write_dev_path is not None:
            rc = IBDManager.tp_add(ibdserver_conf.wc_tp_info)
            if rc != 0:
                warn('Add tp failed.Maybe it is already exist.')
            rc = IBDManager.mwc_add(ibdserver_conf.mwc_info)
            if rc != 0:
                errormsg('Add wc for %s failed.' % ibdserver_conf.dev_uuid)
                return 1
            debug('update bwc flow control')
            rc = IBDManager.bwc_flow_control_change(ibdserver_conf.mwc_info['mwc_uuid'], ibdserver_conf.mwc_info['write_delay_first_level'], ibdserver_conf.mwc_info['write_delay_second_level'], ibdserver_conf.mwc_info['write_delay_first_level_max_us'], ibdserver_conf.mwc_info['write_delay_second_level_max_us'])
            if rc != 0:
                errormsg('change bwc flow control failed.')
                # return 1
        if read_dev_path is not None:
            rc = IBDManager.pp_add(ibdserver_conf.crc_pp_info)
            if rc != 0:
                debug('ERROR: add pp for crc failed.')
                return 1
            rc = IBDManager.crc_add(ibdserver_conf.crc_info)
            if rc != 0:
                errormsg('Add crc for %s failed.' % ibdserver_conf.dev_uuid)
                return 1

        # Step 4. Add drw
        rc = IBDManager.drw_add(ibdserver_conf.drw_info)
        if rc != 0:
            warn('Add drw failed. Maybe it is already exist.')
        # skip add mwc, this is none bio.
        # Step 5. Add channel
        rc = IBDManager.channel_add(ibdserver_conf.channel_info)
        if rc != 0:
            errormsg('Add sac for %s failed.' % ibdserver_conf.dev_uuid)
            return 1
        # Step 6, recover mwc
        if write_dev_path is not None:
            rc = self.recover_mwc(ibdserver_conf.mwc_info)
            if rc != 0:
                return 1
        return 0

    def add_channel(self, dev_uuid, dev_exp_path, write_dev_path=None, read_dev_path=None,
                    sac_is_drw=True, dev_size=None, volume_uuid=None, type_nb="bwc", **option):
        """

        Args:
            dev_size:
            sac_is_drw:
            read_dev_path:
            write_dev_path(str):
            type_nb(str):
            dev_exp_path(str):
            dev_uuid(str):
            volume_uuid (int):

        """
        ret = 0
        is_need_online_commands = option.get('is_need_online_commands', True)
        if is_need_online_commands:
            if do_system('ibdmanager -r s -s get') != 0:
                ret = do_system('/bin/ibdserver')
            if ret == 0:
                ret = self.is_work_ibdserver()
                if ret != 0:
                    errormsg("ibdserver was not started work!")
                    return 1
            else:
                debug('ERROR: cannot start the ibdserver.')
                return 1
        self._load()
        # config all section name
        section_name = SrvDRWConfig(dev_uuid, dev_exp_path, write_dev_path, read_dev_path)
        try:
            if is_need_online_commands:
                rc = self.add_channel_online(section_name, write_dev_path, read_dev_path)
                if rc != 0:
                    raise Exception('try to add channel failed with online commands!')
            # add tp config
            self.config_tp(section_name.tp_info['tp_uuid'], section_name.tp_info['number_work'])
            # add sds_pp config
            self.default_page_pool(section_name.pp_info['pp_name'], 'pp', section_name.pp_info['pool_size'],
                                   section_name.pp_info['page_size'])

            if read_dev_path is not None:
                # add rc_pp config
                self.default_page_pool(section_name.crc_pp_info['pp_name'], 'pp', section_name.crc_pp_info['pool_size'],
                                       section_name.crc_pp_info['page_size'])
                # add crc config
                self.config_crc(section_name.crc_info['crc_uuid'], read_dev_path, section_name.crc_info['cache_size'],
                                section_name.crc_info['crc_pp_name'])

            if write_dev_path is not None:
                # add nwc0 config
                self.config_tp(section_name.wc_tp_info['tp_uuid'], section_name.wc_tp_info['number_work'])
                self.config_nwc_bwc(section_name.mwc_info['mwc_uuid'], type_nb, write_dev_path,
                                    section_name.mwc_info['bufdev_size'],
                                    section_name.mwc_info['tp_name'], section_name.mwc_info['do_fp'],
                                    section_name.mwc_info['rw_sync'], section_name.mwc_info['flush_policy'],
                                    section_name.mwc_info['two_step_read'],
                                    write_first_level=section_name.mwc_info['write_delay_first_level'],
                                    write_second_level=section_name.mwc_info['write_delay_second_level'],
                                    low_water_mark=section_name.mwc_info.get('low_water_mark', None),
                                    high_water_mark=section_name.mwc_info.get('high_water_mark', None),
                                    delay_first_max=section_name.mwc_info.get('write_delay_first_level_max_us', None),
                                    delay_second_max=section_name.mwc_info.get('write_delay_second_level_max_us', None))
            # add sds config
            self.config_sds(section_name.sds_info['sds_name'], section_name.sds_info['sds_pp_name'],
                            section_name.sds_info['sds_wc_uuid'] if write_dev_path else None,
                            section_name.sds_info['sds_rc_uuid'] if read_dev_path else None)
            if sac_is_drw:
                # add drw config
                self.config_drw_export(section_name.drw_info['drw_name'], dev_exp_path)
                self.config_sac_drw(section_name.channel_info['c_uuid'], section_name.channel_info['alignment'],
                                    section_name.channel_info['ds_name'], section_name.channel_info['dev_name'],
                                    section_name.channel_info['tp_name'])
            else:
                # add mwr config
                self.config_mrw_export(section_name.mrw)
                # add sac config for mrw
                assert isinstance(volume_uuid, str)
                self.config_sac_mrw(section_name.uuid, 4096, dev_size, self.default_tp_name, section_name.sds,
                                    section_name.mrw, volume_uuid)
            if self.volume_type in ['SIMPLE_HYBRID', 'SVM']:
                self._save()
            else:
                self.save_tmp()
        except Exception as e:
            errormsg('failed with add channel %s ' % e)
            return 1
        return 0

    def recover_mwc(self, mwc_info):
        # Step 1. Recover mwc
        rc = IBDManager.recover_start(mwc_info)
        if rc != 0:
            errormsg('Start recover on mwc failed.')
            return 1

        deadtime = time.time() + BEFORE_TIMEOUT
        # Step 1. Check mwc recover status
        debug("vVector recover is doing...")
        while True:
            # Check if the vVector recover status each 10 seconds
            (rc, status) = IBDManager.recover_status(mwc_info)
            if status == 2:
                debug("vVector recover is finished")
                return 0
            elif time.time() > deadtime:
                debug('ERROR: recover is not finished before timeout')
                return 1
            elif status == 0:
                debug("vVector recover is not started")
                time.sleep(0.1)
            elif status == 1:
                # recover's status is doing
                time.sleep(1)
        return 0

    def get_config_by_uuid(self, dev_uuid):
        self.dictionary_of_server = {}
        for uuid in self._conf_parser.sections():
            if dev_uuid in uuid:
                top1 = self.dictionary_of_server.setdefault(uuid, {})
                for key, val in self._conf_parser.items(uuid):
                    top1.setdefault(key, val)

    def remove_channel_by_devuuid(self, dev_uuid):
        self.get_config_by_uuid(dev_uuid)
        debug('get ibdserver sections')
        debug('{all}'.format(all=self._conf_parser.sections()))
        if self.dictionary_of_server:
            debug('channel dir was %s ' % self.dictionary_of_server)
            try:
                for section_k in self.dictionary_of_server:
                    for option_k in self.dictionary_of_server[section_k]:
                        self._conf_parser.remove_option(section_k, option_k)
                    self._conf_parser.remove_section(section_k)
            except Exception as e:
                errormsg('%s' % e)
                errormsg('failed to remove chanel %s' % dev_uuid)
                return 1

            return 0
        return 1

    def del_channel(self, dev_uuid):
        srv_config = SrvConfName(dev_uuid)
        del_dir = {'sac': srv_config.uuid, 'sds': srv_config.sds, 'pp': srv_config.sds_pp, 'drw': srv_config.drw,
                   'tp': srv_config.tp}

        def mycmp(x, y):
            d = {'sac': 1, 'sds': 2, 'pp': 3, 'drw': 4, 'tp': 5}

            x = d[x]
            y = d[y]
            if x > y:
                return 1
            elif x == y:
                return 0
            else:
                return -1

        after_cmp = sorted(del_dir.items(), cmp=mycmp, key=lambda x: x[0])
        try:
            for value_str in after_cmp:
                rc = IBDManager.channel_del(value_str[0], value_str[1])
                if rc != 0:
                    raise OSError('delete ibdserver channel failed , please check ibd commands.')
            else:
                debug('Finish to delete ibd channel!')
        except Exception as e:
            errormsg('{}'.format(e))
            return 1
        finally:
            self._load()
            rc = self.remove_channel_by_devuuid(dev_uuid)
            if rc != 0:
                return rc
            self._save()
        wait = True
        while wait:
            out = ['']
            wait = False
            do_system("ibdmanager -r s -s get|grep uuid:", out)
            for line in out:
                if dev_uuid in line:
                    wait = True
                    break
            if wait:
                time.sleep(2)
        return rc

    def save_tmp(self):
        try:
            with open(self._filename_tmp, 'w') as f:
                self._conf_parser.write(f)
        except Exception as e:
            errormsg("Cannot save ibd config file!")
            errormsg("except with %s" % e)
            raise e


class IBDServerOld(IBDServerBaseConfig):
    def __init__(self, conf_file):
        super(IBDServerOld, self).__init__(conf_file)
        self._ibd_exp_conf = None

    #        debug('Entering class %s ' % self.__class__.__name__)

    def _conf_support_default(self):
        if self.role_is_volume:
            self.init_on_vol()
        else:
            self.init_on_svm()

    def init_on_svm(self):
        self._reset()
        self._load()
        self._set_sections('global', 'num_workers', UsxConfig().num_workers)
        self._save()
        return 0

    def init_on_vol(self):
        self._reset()
        self._load()
        self._set_sections("global", "num_workers", 8)
        self._save()
        return 0

    def add_channel(self, ibd_uuid, exportname, wc_dev=None, wc_size_s=None, **ksw):
        self._load()
        if wc_dev:
            self._ibd_exp_conf = IBDSrvExportConfig()
            bio_ssd_mode_enable = False
            bio_ssd_mode_value = 0
            blk_dev = BlockDevice(wc_dev)
            io_pagesz = 8
            io_poolsz = blk_dev.size_gb * 1024 / io_pagesz + 8
            bfp_high_water_mark_value = self._ibd_exp_conf.high_water_mark
            bfp_low_water_mark_value = self._ibd_exp_conf.low_water_mar
            bio_write_delay_first_level_value = self._ibd_exp_conf.write_delay_first_level
            bio_write_delay_second_level_value = self._ibd_exp_conf.write_delay_second_level
            if bio_ssd_mode_enable:
                io_poolsz = 512
                bio_ssd_mode_value = 1
            # if UsxConfig().volume_type in ['SIMPLE_HYBRID' ]:
            #    io_poolsz = 512
            #    bio_ssd_mode_value = 0
            #    bfp_high_water_mark_value = 75
            #    bfp_low_water_mark_value = 63
            #    bio_write_delay_first_level_value = 0
            #    bio_write_delay_second_level_value = 0
            bio.modprobe('ibd')
            # NOTE: Bio must use split_ds
            self._set_sections("global", "ds_type", "split_ds")
            # NOTE: Bio do not use ds_pool
            self._set_sections("global", "io_type", "bio")
            self._set_sections("global", "io_pagesz", io_pagesz)
            self._set_sections("global", "io_poolsz", io_poolsz)
            self._set_sections("global", "io_bufdevice", wc_dev)
            self._set_sections("global", "io_bufdevicesz", blk_dev.size_gb)
            self._set_sections("global", "bio_ssd_mode", bio_ssd_mode_value)

            # NOTE: We want to start fast flush earlier
            self._set_sections("global", "bfp_high_water_mark", bfp_high_water_mark_value)
            self._set_sections("global", "bfp_low_water_mark", bfp_low_water_mark_value)

            # NOTE: We want to start flow control later
            self._set_sections("global", "bio_write_delay_first_level", bio_write_delay_first_level_value)
            self._set_sections("global", "bio_write_delay_second_level", bio_write_delay_second_level_value)

            self._set_sections(ibd_uuid, "exportname", exportname)
            self._set_sections(ibd_uuid, "direct_io", 1)
        else:
            self._set_sections(ibd_uuid, "exportname", exportname)
            self._set_sections(ibd_uuid, "enable_kill_myself", 1)
        self._save()
        return self._update()

    def del_channel(self, ibd_uuid):
        self._load()
        if self._conf_parser.has_section(ibd_uuid):
            exp_path = self._conf_parser.get(ibd_uuid, 'exportname')
            ret = self._conf_parser.remove_section(ibd_uuid)
            if ret is False:
                # It is OK if the section is already gone.
                debug(
                    'WARNING: Failed to remove export %s from ibdserver config. Maybe it is already deleted.' % ibd_uuid)
            self._save()
            return self._update()
        else:
            debug('WARNING: Cannot find export to delete: ' + ibd_uuid)
            return 1

    def _reset(self):
        with open(self._filename, 'w'):
            pass

    def _update(self):
        cmd_status = '/bin/ibdmanager -r s -s get'
        ret = do_system(cmd_status)
        if ret == 0:
            cmd_start = '/bin/ibdmanager -r s -u'
        else:
            cmd_start = '/bin/ibdserver'
        ret = do_system(cmd_start)
        if ret != 0:
            debug('ERROR: cannot start ibdserver')
        return ret


ibdserver_conf_module = IBDServerManger().get_version(UsxSettings().enable_new_ibdserver)


def apply_new_drw_channel(dev_uuid, dev_exp_path, write_cache=None, read_cache=None, **kw):
    if UsxSettings().enable_manual_ibdserver_config:
        debug('WARNING: use the manual configuration file of ibdserver, skip online adding!')
        if not UsxSettings().enable_new_ibdserver:
            bio.modprobe('ibd')
        ret = 0
        if do_system('ibdmanager -r s -s get') != 0:
            ret = do_system('/bin/ibdserver')
        if ret == 0:
            ret = ibdserver_conf_module.is_work_ibdserver()
        return ret
    rc = ibdserver_conf_module.add_channel(dev_uuid, dev_exp_path, write_cache, read_cache, **kw)
    if rc != 0:
        errormsg('try to add channel failed !!!!!!')
    return rc


@printcall
def config_support_server():
    rc = ibdserver_conf_module.config_support_default_base()
    return rc


@printcall
def config_support_vdi():
    rc = ibdserver_conf_module.config_support_default_base()
    return rc


@printcall
def config_support_volume():
    rc = ibdserver_conf_module.config_support_default_base()
    return rc


@printcall
def reset_ibdserver_config():
    with open(IBDSERVERCONFIGFILE_DEF, 'w') as e:
        debug('%s' % e)
        debug('reset the ibdserver configuration because of startup.')


@printcall
def modify_drw_config_dis_cache(uuid, export_path):
    return apply_new_drw_channel(uuid, export_path)


@printcall
def modify_drw_config_cache_vdi(uuid, export_path, cache_path, cache_size=None):
    return apply_new_drw_channel(uuid, export_path, cache_path)


@synclock(DEFUALT_IBD_CONF_LOCK)
@printcall
def del_sac_channel(dev_uuid):
    rc = ibdserver_conf_module.del_channel_base(dev_uuid)
    return rc
