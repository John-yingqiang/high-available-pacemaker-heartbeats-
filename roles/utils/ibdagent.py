'''
'''
import ConfigParser
import os
from comm_utils import printcall, singleton, BaseParam
from proclock import synclock
from usx_settings import UsxSettings
import sys
sys.path.append("/opt/milio/libs/atlas")
from log import debug, info, warn, errormsg

DEFUALT_IBD_CONF_LOCK = '/tmp/default-ibd-conf.lock'
CMD_STATUS_S = 'python /opt/milio/atlas/system/status_check.pyc'
CMD_LOAD_S = 'python /opt/milio/atlas/roles/pool/cp-load.pyc'
CMD_HA_S = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc'


@printcall
def load_ibd_conf():
    return IBDAgent().load_conf()


@printcall
def add_ibd_channel(ibd_setup_info, resoure_uuid = None, is_ha_flag = None):
    rc = IBDAgent().add_channel(ibd_setup_info, resoure_uuid, is_ha_flag)
    return rc


@printcall
def rm_ibd_channel(sections):
    rc = IBDAgent().rm_channel(sections)
    if rc != 0:
        errormsg('Try to remove ibd chanel of config file failed')
    return 0


@printcall
def reset_ibdagent_config():
    IBDAgent().reset_conf()


class IBDAgentManger(BaseParam):
    """docstring for IBDAentManger"""
    def __init__(self, resoure_setup_info):
        self.ibd_uuid = ''
        self.cache_ip = ''
        self.devname = ''
        self.minor_num = ''
        self.global_type = 'global'
        self.channel_type = 'asc'
        BaseParam.__init__(self, resoure_setup_info)

    def parse(self, resoure_setup_info):
        self.ibd_uuid = resoure_setup_info['devuuid']
        self.cache_ip = resoure_setup_info['cacheip']
        self.devname = resoure_setup_info['devexport']
        self.minor_num = resoure_setup_info['minornum']


@singleton
class IBDAgent(object):
    def __init__(self):
        self.IBDAENT_CONF_FILE = '/etc/ilio/ibdagent.conf'
        self.ibd_cnf = {}
        self.cf = ConfigParser.ConfigParser()

    def load_conf(self):
        try:
            self.cf.read(self.IBDAENT_CONF_FILE)
        except:
            errormsg('Read ibdagent conf file failed')
            return self.ibd_cnf
        sections_list = self.cf.sections()
        for section_key in sections_list:
            self.ibd_cnf[section_key] = {}
            for keys, val in self.cf.items(section_key):
                self.ibd_cnf[section_key][keys] = val

        return self.ibd_cnf

    def get_ibd_devname_by_uuid(self, uuid):
        try:
            return (0, self.load_conf()[uuid]['devname'])
        except:
            return (1, '')

    def reset_conf(self):
        try:
            f_ibdagent = open(self.IBDAENT_CONF_FILE, 'w')
            f_ibdagent.close()
        except Exception, e:
            errormsg(e)

    def add_channel(self, set_up_info, write_cache_uuid, is_ha_flag = None):
        try:
            self.cf.read(self.IBDAENT_CONF_FILE)
        except:
            errormsg('Read ibdagent conf file failed')
            return 1
        ibdmanger = IBDAgentManger(set_up_info)
        debug(ibdmanger)
        try:
            self.cf.add_section(ibdmanger.global_type)
        except ConfigParser.DuplicateSectionError:
            pass
        if UsxSettings().enable_new_ibdserver:
            self.update_channel(ibdmanger.global_type, 'type', ibdmanger.global_type)
            self.update_channel(ibdmanger.ibd_uuid, 'type', ibdmanger.channel_type)
        self.update_channel(ibdmanger.ibd_uuid, 'devname', ibdmanger.devname)
        self.update_channel(ibdmanger.ibd_uuid, 'minor', ibdmanger.minor_num)
        if is_ha_flag:
            self.update_channel(ibdmanger.ibd_uuid, 'access', 'r')
        rc = self.update_channel(ibdmanger.ibd_uuid, 'ip', ibdmanger.cache_ip)
        if rc != 0:
            return rc
        if write_cache_uuid is not None:
            the_iw_hook = '%s reachable %s OK; %s vv_readd %s %s' %(CMD_STATUS_S, ibdmanger.cache_ip,\
                CMD_LOAD_S, write_cache_uuid, ibdmanger.ibd_uuid)
            the_wr_hook = '%s reachable %s FATAL; %s raid_device_set_io_error %s %s %s' \
            %(CMD_STATUS_S, ibdmanger.cache_ip, CMD_LOAD_S, write_cache_uuid,ibdmanger.ibd_uuid, ibdmanger.devname)
            the_rw_hook = '%s reachable %s OK; %s rw_hook_action %s %s %s' \
            %(CMD_STATUS_S, ibdmanger.cache_ip, CMD_LOAD_S, write_cache_uuid, ibdmanger.ibd_uuid, ibdmanger.devname)
            the_up_hook = '%s reboot' %CMD_HA_S
            self.update_channel(ibdmanger.ibd_uuid, 'iw_hook', the_iw_hook)
            self.update_channel(ibdmanger.ibd_uuid, 'wr_hook', the_wr_hook)
            self.update_channel(ibdmanger.ibd_uuid, 'rw_hook', the_rw_hook)
            self.update_channel(ibdmanger.ibd_uuid, 'up_hook', the_up_hook)
        rc = self.save_conf()
        if rc != 0:
            return rc
        return 0

    def update_channel(self, sections, keys, value):
        try:
            self.cf.set(sections, keys, value)
        except ConfigParser.NoSectionError:
            self.cf.add_section(sections)
            self.cf.set(sections, keys, value)
        except:
            return 1
        return 0

    def rm_channel(self, sections):
        sections_dir = self.load_conf()
        if not sections_dir:
            return 1
        for section_key in sections_dir:
            if section_key == sections:
                for keys_ibd in sections_dir[section_key]:
                    self.cf.remove_option(section_key, keys_ibd)
                self.cf.remove_section(section_key)
                self.save_conf()
                break
        return 0

    def save_conf(self):
        try:
            f = open(self.IBDAENT_CONF_FILE, 'w')
            self.cf.write(f)
            f.flush()
            os.fsync(f.fileno())
            f.close()
        except:
            debug("Cannot save ibdagent conf file")
            return 1
        return 0
