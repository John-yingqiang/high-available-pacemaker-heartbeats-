#!/usr/bin/python
import os
import glob
import json
import httplib
import base64
import zlib
import socket
from comm_utils import singleton
from cmd_utils import runcmd
from functools import update_wrapper
import sys
sys.path.insert(0, "/opt/milio/libs/atlas")
from log import debug


def reloadproperty(f):
    def wrapper_func(self, *args, **kwargs):
        self._load_config()
        self._parse_config()
        return f(self, *args, **kwargs)
    return update_wrapper(wrapper_func, f)


@singleton
class UsxConfig(object):
    """Maybe need to lock these operation"""

    def __init__(self):
        self._load_config()
        self._parse_config()

    @property
    def volume_uuid(self):
        return self._volume_uuid

    @property
    def is_fastfailover(self):
        return self._is_fastfailover

    @property
    def rc_size(self):
        return self._rc_size

    @property
    def wc_size(self):
        return self._wc_size

    @property
    def is_contains_volume(self):
        rc = 'volumeresources' in self._atlas_conf and len(self._atlas_conf['volumeresources']) > 0
        return rc and (not self.is_ha or len(glob.glob("/tmp/*.devlist") + glob.glob("/tmp/current_volume")) > 0)

    @property
    def is_ha(self):
        return 'ha' in self._atlas_conf['usx'] and self._atlas_conf['usx']['ha']

    @property
    def is_journaled(self):
        return self._atlas_conf['volumeresources'][0]['directio']

    @property
    def export_type(self):
        return self._atlas_conf['volumeresources'][0]['exporttype']

    @property
    def volume_size(self):
        return self._atlas_conf['volumeresources'][0]['volumesize']

    @property
    def serviceip(self):
        return self._atlas_conf['volumeresources'][0]['serviceip']

    @property
    def volume_type(self):
        return self._volume_type

    @property
    def is_svm(self):
        return self.volume_type == 'SVM'

    @property
    def volume_dedup_mount_point(self):
        return self._dedup_mount_point

    @property
    def usx_url(self):
        return self._usx_url

    @property
    def volume_server_name(self):
        return self._volume_server_name

    @property
    def volume_resources(self):
        return self._resources

    @property
    def num_workers(self):
        num = self._atlas_conf.get('num_workers')
        if num is not None and num > 0:
            return num
        return 4

    @property
    def atltis_conf(self):
        return self._atlas_conf

    @property
    def is_volume(self):
        return self._is_role_volume()

    @property
    def is_mirror_volume(self):
        return self._is_mirror_volume

    @property
    def is_raid1_volume(self):
        return self._is_raid1_volume

    @property
    def is_stretchcluster_volume(self):
        return self._is_stretchcluster_volume

    @property
    def is_sharedstorage_volume(self):
        return self._is_sharedstorage_volume

    @property
    def is_snapshotenabled(self):
        return self._is_snapshotenabled

    @property
    def original_volumesize(self):
        return self._original_volumesize

    @property
    def node_name(self):
        try:
            return socket.gethostname()
        except:
            return None

    @property
    def snapshot_space_ratio(self):
        return (100 - self._snapshot_space_ratio) / 100.0

    @property
    def snapshot_space(self):
        return self._snapshot_space_ratio

    @property
    def export_fs_type(self):
        if self._atlas_conf and self._atlas_conf.get('volumeresources'):
            dct = self._atlas_conf['volumeresources'][0]
            return dct.get('export_fs_type')
        return None

    def ha_reload(self, volume_uuid):
        try:
            debug('Entering ha_reload...')
            if self._ha_reload_from_amc(volume_uuid) or self._ha_reload_from_crm(volume_uuid):
                self._parse_volume()
                return 0
            else:
                debug('ERROR: ha reload volume configuration failed')
                return 1
        except Exception, e:
            debug('ERROR: ha reload volume configuration failed [%s]' % e)
            return 1

    def _ha_reload_from_amc(self, volume_uuid):
        try:
            apiurl = 'http://127.0.0.1:8080/usxmanager/'
            apistr = '/usxmanager/usx/inventory/volume/resources/%s' % volume_uuid
            apiaddr = apiurl.split('/')[2]
            conn = httplib.HTTPConnection(apiaddr)
            conn.request("GET", apistr)
            response = conn.getresponse()
            if response.status != 200 and response.reason != 'OK':
                debug('ERROR: can not get configuration from amc')
                return False
            else:
                data = response.read()
                volume_list = []
                volume_list.append(json.loads(data)['data'])
                self._atlas_conf['volumeresources'] = volume_list
                debug('Got volume resource configuration from amc')
                return True
        except Exception, e:
            debug('ERROR: can not get configuration from amc [%s]' % e)
            return False

    def _ha_reload_from_crm(self, volume_uuid):
        cmd_str = 'crm resource param ' + volume_uuid + '_ds show ' + 'resource' + 'Json'
        (ret, msg) = runcmd(cmd_str, print_ret=False, lines=False)
        if ret == 0:
            data = zlib.decompress(base64.decodestring(msg))
            volume_list = []
            atlas_json = json.loads(data)
            volume_list.append(atlas_json['volumeresources'][0])
            self._atlas_conf['volumeresources'] = volume_list
            debug('Got volume resource configuration from crm')
            return True
        debug('ERROR: can not get the volume[%s] configuration from crm' % volume_uuid)
        return False

    def _load_config(self):
        """
        Load the configuration file from /etc/ilio/atlas.json
        """
        try:
            with open('/etc/ilio/atlas.json', 'r') as fd:
                self._atlas_conf = json.load(fd)
        except Exception, e:
            debug('ERROR: got exception when loading configuration from /etc/ilio/atlas.json [%s]' % e)

    def _parse_config(self):
        try:
            self._parse_volume()
        except Exception, e:
            debug(
                'ERROR: got exception when parsing configuration from /etc/ilio/atlas.json [%s], it may not be a volume role.' % e)

    def _is_role_volume(self):
        if 'usx' in self._atlas_conf:
            roles = self._atlas_conf["usx"]["roles"]
        else:
            roles = self._atlas_conf["roles"]
        if 'VOLUME' in roles:
            return True
        return False

    def _parse_volume(self):
        self._volume_uuid = None
        self._is_fastfailover = False
        self._rc_size = None
        self._wc_size = None
        self._is_mirror_volume = None
        self.ibdserver_resources_uuid = None
        if 'usx' in self._atlas_conf:
            self._usx_url = self._atlas_conf['usx']['usxmanagerurl']
        else:
            self._usx_url = self._atlas_conf['usxmanagerurl']
        if self.is_contains_volume:
            self._volume_uuid = self._atlas_conf['volumeresources'][0]['uuid']
            self._volume_type = self._atlas_conf['volumeresources'][0]['volumetype'].upper()
            self._volume_server_name = self._atlas_conf['volumeresources'][0]['volumeservicename']
            self._dedup_mount_point = self._atlas_conf['volumeresources'][0]['dedupfsmountpoint']
            self._resources = self._atlas_conf['volumeresources'][0]
            self._is_stretchcluster_volume = self._atlas_conf['volumeresources'][0].get('stretchcluster', True)
            self._is_raid1_volume = self._atlas_conf['volumeresources'][0]['raidplans'][0].get('raid1enabled', False)
            self._is_snapshotenabled = self._atlas_conf['volumeresources'][0].get('snapshotenabled', False)
            self._is_sharedstorage_volume = (
                len(self._atlas_conf['volumeresources'][0]['raidplans'][0].get("sharedstorages", [])) > 0)
            self._parse_fastfailover()
            if self._volume_type in ['SIMPLE_HYBRID']:
                self._parse_simplehybrid()
            else:
                self.ibdserver_resources_uuid = self._volume_uuid
        else:
            if self._is_role_volume():
                self._volume_type = 'HAVM'
            else:
                self._volume_type = 'SVM'

    def _parse_fastfailover(self):
        atlas_json = self._atlas_conf
        plan_detail = json.loads(atlas_json['volumeresources'][0]['raidplans'][0]['plandetail'])
        self._is_mirror_volume = plan_detail.get('mirrorplan', True)
        self._snapshot_space_ratio = plan_detail.get('snapshot_volumesize', 0)
        self._original_volumesize = plan_detail.get('original_volumesize', 0)
        if 'fastfailover' in plan_detail:
            self._is_fastfailover = plan_detail['fastfailover']
        if not self._is_fastfailover:
            return
        self._wc_size = plan_detail['wcsize']
        self._rc_size = plan_detail['rcsize']

    def _parse_simplehybrid(self):
        atlas_json = self._atlas_conf
        plan_detail = json.loads(atlas_json['volumeresources'][0]['raidplans'][0]['plandetail'])
        for plan_key in plan_detail['subplans']:
            if len(plan_key['raidbricks']) > 0:
                storage_type_key = 'raidbricks'
            else:
                storage_type_key = 'sharedstorages'
            for keys in plan_key[storage_type_key]:
                for subdevices in keys['subdevices']:
                    if subdevices.get('storagetype') in ['WHOLEDISK']:
                        self.ibdserver_resources_uuid = subdevices['uuid']
