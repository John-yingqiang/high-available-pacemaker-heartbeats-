#!/usr/bin/python
import json
import os
from comm_utils import singleton
from usx_config import UsxConfig
import sys
sys.path.append("/opt/milio/libs/atlas")
from log import debug

USX_SETTINGS_FILE = '/etc/ilio/usx_settings.json'
DEF_STORAGE_EXPORT_FS_MODE = 'dedup'
DEF_STORAGE_MODE = 'lvm_dedup'


@singleton
class UsxSettings(object):
    """docstring for UsxSettings"""

    default_settings = dict({
        'enable_settings':                      False,
        'enable_manual_ibdserver_config':       False,
        'export_fs_mode':                       UsxConfig().export_fs_type if UsxConfig().export_fs_type else DEF_STORAGE_EXPORT_FS_MODE,
        'enable_new_ibdserver':                 True,
        'storage_mode':                         DEF_STORAGE_MODE,
    })

    def __init__(self):
        self._load_default_settings()
        # disable configuration file mode for release version.
        self._load_file_settings()
        self._gen_exp_prop()

    def ha_reset_fs_mode(self):
        if not self.enable_settings:
            self.export_fs_mode = DEF_STORAGE_EXPORT_FS_MODE if UsxConfig().is_fastfailover else 'dedup'
            self._gen_exp_prop()
            self._save()

    @property
    def is_btrfs(self):
        return self._is_btrfs

    @property
    def is_zfs(self):
        return self._is_zfs

    @property
    def is_ext4fs(self):
        return self._is_ext4fs

    @property
    def is_need_zvol(self):
        return self.is_ext4fs or self.is_btrfs

    def _load_default_settings(self):
        for key, val in self.default_settings.items():
            self.__dict__[key] = val

    def _load_file_settings(self):
        if os.path.exists(USX_SETTINGS_FILE):
            try:
                with open(USX_SETTINGS_FILE, 'r') as fd:
                    settings = json.load(fd)
                if settings.get('enable_settings'):
                    for key, val in settings.items():
                        self.__dict__[key] = val
                    return
            except Exception, e:
                debug('ERROR: Got exception from loading %s [%s], force to reset it.' % (USX_SETTINGS_FILE, e))
        self._save()

    def _save(self):
        with open(USX_SETTINGS_FILE, 'w') as fd:
            json.dump(self.__dict__, fd, indent=4, separators=(',', ':'))

    def _gen_exp_prop(self):
        self._is_zfs = False
        self._is_btrfs = False
        self._is_ext4fs = False
        if self.export_fs_mode == 'btrfs':
            self._is_btrfs = True
        elif self.export_fs_mode == 'zfs':
            self._is_zfs = True
        elif self.export_fs_mode == 'ext4':
            self._is_ext4fs = True

    def __getitem__(self, key):
        # if key is of invalid type or value, the list values will raise the error
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def __getattr__(self, key):
        return self.__dict__[key]

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def __str__(self):
        return str(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)

    def __contains__(self, value):
        return value in self.__dict__
