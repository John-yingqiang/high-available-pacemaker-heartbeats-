import subprocess
import usx_settings
import usx_config


def get_ibd_version():
    if usx_settings.UsxSettings().enable_new_ibdserver:
        return 'V2'
    else:
        return 'V1'


class IBDServerState(object):
    """Get the states of ibdserver when the server is working.

    Usage:
        import ibdserver_state

        state = ibdserver_state.IBDServerState()
        # Magneto ibdserver's property
        print 'bfe_page_counter: ', state.channel.bfe_page_counter
        # Old ibdserver's property
        print 'rreadycounter: ', state.channel.rreadycounter
        print 'seq_assigned: ', state.wc.seq_assigned

    Attributes:
        channel_id (string): The uuid of the ibd channel.
                             if only one channel exists, It's not needed to set.
    """
    @property
    def wc(self):
        return getattr(self, '_wc', None)

    @property
    def channel(self):
        return getattr(self, '_channel', None)

    def __init__(self, channel_id=None):
        self._channel_id = channel_id
        self._version = get_ibd_version()
        self.__load_state()

    def __load_state(self):
        self.__all_state = IBDSrvAllState(self._channel_id, self._version)
        self._wc = self.__all_state.wc
        self._channel = self.__all_state.channel


class IBDSrvAllState(object):
    """docstring for ServerStat"""
    def __init__(self, channel_id, version):
        if channel_id is None:
            self._channel_id = self._get_def_channel_id()
        else:
            self._channel_id = channel_id
        self._version = version
        self.wc = self._get_wc_state()
        self.channel = self._get_channel_state()

    def _get_wc_state(self):
        class_name = 'WriteCacheIOState{version}'.format(version=self._version)
        return globals()[class_name](self._channel_id)

    def _get_channel_state(self):
        class_name = 'ChannelState{version}'.format(version=self._version)
        return globals()[class_name](self._channel_id)

    def _get_def_channel_id(self):
        channel_id = None
        try:
            # lines = get_ibdserver_state()
            # for line in lines:
            #     line = line.strip()
            #     if line.startswith('uuid:'):
            #         channel_id = line.split(':')[1]
            #         break

            channel_id = usx_config.UsxConfig().ibdserver_resources_uuid
        except Exception, e:
            print e
        finally:
            return channel_id


class ChannelState(object):
    """docstring for ChannelState"""
    __INNER_KEYS = ['_channel_id', '_channel_info']

    def __init__(self, channel_id):
        self._channel_id = channel_id
        self._channel_info = {}

    def __getitem__(self, key):
        if key in self.__INNER_KEYS:
            return self.__getattribute__(key)
        return self._get_state(key)

    def __getattr__(self, key):
        if key in self.__INNER_KEYS:
            return self.__getattribute__(key)
        return self._get_state(key)

    def _get_state(self, key):
        self._get_all_state()
        if self._channel_id and self._channel_info and len(self._channel_info) > 0:
            return self._channel_info.get(key, None)
        return None

    def _get_all_state(self):
        raise NotImplementedError('Should define the method')


class ChannelStateV1(ChannelState):
    def _get_all_state(self):
        channels = {}
        try:
            lines = get_ibdserver_state()
            if len(lines) > 0:
                channel = {}
                for line in lines:
                    info = line.strip()
                    suffix = 'Agent Channel:'
                    if info.endswith(suffix):
                        if len(channel) > 0:
                            channels[channel['uuid']] = channel
                        channel = {}
                    elif not info.startswith('ibds ') and info.find(':') > 0:
                        item = info.split(':')
                        channel[item[0].strip()] = item[1].strip()
                channels[channel['uuid']] = channel
        except Exception as e:
            print e
        if len(channels) > 0 and self._channel_id in channels:
            self._channel_info = channels[self._channel_id]


class ChannelStateV2(ChannelState):
    def _get_all_state(self):
        channel = {}
        try:
            lines = get_ibdserver_sac_state(self._channel_id)
            if len(lines) > 0:
                for line in lines:
                    line = line.strip()
                    if line.find(':') < 0 or line.endswith(':'):
                        continue
                    item = line.split(':')
                    channel[item[0].strip()] = item[1].strip()
        except Exception, e:
            print e
        if len(channel) > 0:
            self._channel_info = channel


class WriteCacheIOState(object):
    """docstring for WriteCacheIOState"""
    __INNER_KEYS = ['_wc_uuid', '_wc_info']

    def __init__(self, channel_id):
        self._wc_uuid = self._get_wc_uuid_by_channel_id(channel_id)
        self._wc_info = {}

    def __getitem__(self, key):
        if key in self.__INNER_KEYS:
            return self.__getattribute__(key)
        return self._get_state(key)

    def __getattr__(self, key):
        if key in self.__INNER_KEYS:
            return self.__getattribute__(key)
        return self._get_state(key)

    def _get_state(self, key):
        self._get_all_state()
        if self._wc_uuid and self._wc_info and len(self._wc_info) > 0:
            return self._wc_info.get(key, None)
        return None

    def _get_wc_uuid_by_channel_id(self, channel_id):
        raise NotImplementedError('Should define the method')

    def _get_all_state(self):
        raise NotImplementedError('Should define the method')


class WriteCacheIOStateV1(WriteCacheIOState):
    def _get_wc_uuid_by_channel_id(self, channel_id):
        return channel_id

    def _get_all_state(self):
        wcio_info = {}
        try:
            lines = get_ibdserver_state()
            if len(lines) > 0:
                is_io_info = False
                for line in lines:
                    line = line.strip()
                    suffix = 'IO Infomation:'
                    if line.endswith(suffix):
                        is_io_info = True
                    if not is_io_info or line.find(':') < 0 or line.endswith(':'):
                        continue
                    item = line.split(':')
                    wcio_info[item[0].strip()] = item[1].strip()
        except Exception as e:
            print e
        if len(wcio_info) > 0:
            self._wc_info = wcio_info


class WriteCacheIOStateV2(WriteCacheIOState):
    def _get_wc_uuid_by_channel_id(self, channel_id):
        return '{id}-wcache'.format(id=channel_id)

    def _get_all_state(self):
        wcio_info = {}
        try:
            lines = get_ibdserver_bwc_state(self._wc_uuid)
            if len(lines) > 0:
                for line in lines:
                    line = line.strip()
                    if line.find(':') < 0 or line.endswith(':'):
                        continue
                    item = line.split(':')
                    wcio_info[item[0].strip()] = item[1].strip()
        except Exception as e:
            print e
        if len(wcio_info) > 0:
            self._wc_info = wcio_info


def get_ibdserver_state():
    try:
        cmd_str = 'ibdmanager -r s -s get'
        return exec_cmd(cmd_str)
    except Exception, e:
        return []


def get_ibdserver_sac_state(channel_uuid):
    try:
        return get_ibdserver_mod_state('sac', channel_uuid)
    except Exception, e:
        return []


def get_ibdserver_bwc_state(wc_uuid):
    try:
        return get_ibdserver_mod_state('bwc', wc_uuid)
    except Exception, e:
        return []


def get_ibdserver_mod_state(mod, uuid):
    cmd_str = 'ibdmanager -r s -m {mod} -i {uuid} info stat'.format(mod=mod, uuid=uuid)
    return exec_cmd(cmd_str)


def exec_cmd(cmd_str):
    p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    ret = p.wait()
    if ret != 0:
        raise Exception('ERROR: Cannot get state')
    lines = p.stdout.readlines()
    return lines
