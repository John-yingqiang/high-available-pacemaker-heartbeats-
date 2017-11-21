# !/usr/bin python
from cmd_utils import runcmd, retry_cmd
from ibdserver_state import IBDServerState
import errno
import sys
import time
import re

sys.path.append("/opt/milio/libs/atlas")
from atl_util import do_system_timeout, timeout_error, do_system
from log import debug, info, warn, errormsg

RETRY_NUM = 2
TIMEOUT = 10
_CMD_LOCK = 'ibdmanager --role server --module doa'
_IBDMANAGER = '/bin/ibdmanager'
_IBDMANAGER_ORG = '/usr/local/bin/ibdmanager.org'
_IBDMANAGER_A_UPDATE = _IBDMANAGER + ' -r a -u'
_IBDMANAGER_S_UPDATE = _IBDMANAGER + ' -r s -u'
_IBDMANAGER_STATUS = _IBDMANAGER + ' -r a -s get'
_IBDMANAGER_STAT_WUD = _IBDMANAGER + ' -r a -s get_wud'
_IBDMANAGER_A_UPGRADE = _IBDMANAGER + " -r a -U"
_IBDMANAGER_STAT_RWWUD = _IBDMANAGER + " -r a -s get_rwwud"
_IBDMANAGER_STAT_UD = _IBDMANAGER + " -r a -s get_ud"
_IBDMANAGER_S_SATUS = _IBDMANAGER + ' -r s -s get'
_IBDMANAGER_SET_iO_ERROR = _IBDMANAGER + ' -r a -e %s'
# channel add/delete/switch(to new mwc)
# ibdmanager -r s -m sac -i [c_uuid] add [sync]:[direct_io]:[enable_kill_myself]:[alignment]:[tp_name]:[ds_name]:[dev_name]:[exportname]:[dev_size]
_IBDMANAGER_CHANNEL_ADD = _IBDMANAGER + " -r s -m sac -i %s add %s:%s:%s:%s:%s:%s:%s:%s:%s"

# ibdmanager -r s -m sac -i [c_uuid] del
_IBDMANAGER_CHANNEL_DEL = _IBDMANAGER + " -r s -m %s -i %s del"
# ibdmanager -r s -m sac -i [c_uuid] swm [mwc_uuid]
_IBDMANAGER_CHANNEL_SWITCH_MWC = _IBDMANAGER + " -r s -m sac -i %s swm %s"

# pp add
# ibdmanager -r s -m pp -i [pp_name] add [owner_type]:[pool_size]:[page_size]
_IBDMANAGER_PP_ADD = _IBDMANAGER + " -r s -m pp -i %s add %s:%s:%s"

# sds add
# ibdmanager -r s -m sds -i [sds_name] add [sds_pp_name]:[sds_wc_uuid]:[sds_rc_uuid]
_IBDMANAGER_SDS_ADD = _IBDMANAGER + " -r s -m sds -i %s add %s:%s:%s"

# mwc add/remove
# ibdmanager -r s -m mwc -i [mwc_uuid] add [bufdev]:[bufdev_size]:[rw_sync]:[two_step_read]:[do_fp]:[tp_name]:[flush_policy]
_IBDMANAGER_MWC_ADD = _IBDMANAGER + " -r s -m bwc -i %s add %s:%s:%s:%s:%s:%s"
# ibdmanager -r s -m mwc -i [mwc_uuid] rm
_IBDMANAGER_MWC_DEL = _IBDMANAGER + " -r s -m bwc -i %s rm"

# crc add
# ibdmanager -r s -m crc -i [crc_uuid] add [crc_pp_name]:[cache_device]:[cache_size]
_IBDMANAGER_CRC_ADD = _IBDMANAGER + " -r s -m crc -i %s add %s:%s:%s"

# tp add
# ibdmanager -r s -m tp -i [tp name] add [worker number]
_IBDMANAGER_TP_ADD = _IBDMANAGER + " -r s -m tp -i %s add %s"

# mwc recover
# ibdmanager -r s -m mwc -i [mwc_uuid] rcv start
_IBDMANAGER_MWC_RECOVER_START = _IBDMANAGER + " -r s -m bwc -i %s rcv start"
# ibdmanager -r s -m mwc -i [mwc_uuid] rcv get
_IBDMANAGER_MWC_RECOVER_STATUS = _IBDMANAGER + " -r s -m bwc -i %s rcv get"

# channel specific fast flush
# ibdmanager -r s -m mwc -i [mwc_uuid] -c [c_uuid] ff start
_IBDMANAGER_FLUSH_START = _IBDMANAGER + " -r s -m bwc -i %s -c %s ff start"
# ibdmanager -r s -m mwc -i [mwc_uuid] -c [c_uuid] ff stop
_IBDMANAGER_FLUSH_STOP = _IBDMANAGER + " -r s -m bwc -i %s -c %s ff stop"
# ibdmanager -r s -m mwc -i [mwc_uuid] -c [c_uuid] ff get
_IBDMANAGER_FLUSH_GET_STATUS = _IBDMANAGER + " -r s -m bwc -i %s -c %s ff get"

# mrw add
# ibdmanager -r s -m mrw -i [mrw_name] add
_IBDMANAGER_MRW_ADD = _IBDMANAGER + " -r s -m mrw -i %s add"

# drw add
# ibdmanager -r s -m drw -i [drw_name] add [exportname]
_IBDMANAGER_DRW_ADD = _IBDMANAGER + " -r s -m drw -i %s add %s"

# snapshot freeze
_IBDMANAGER_SNAP_FREEZE = _IBDMANAGER + " -r s -m %s -i %s -c %s freeze stage1"
# snapshot unfreeze
_IBDMANAGER_SNAP_UNFREEZE = _IBDMANAGER + " -r s -m %s -i %s -c %s unfreeze"

# snapshot flush
_IBDMANAGER_SNAP_FLUSH = _IBDMANAGER + " -r s -m %s -i %s -c %s freeze stage2"
# ibdserver write flush freeze
_IBDMANAGER_WRITE_FREEZE = _IBDMANAGER + " -r s -z freeze"
# ibdserver write flush unfreeze
_IBDMANAGER_WRITE_UNFREEZE = _IBDMANAGER + " -r s -z unfreeze"

# ibdserver config bwc high_water_mark ,low_water_mark ,
_IBDMANAGER_BWC_WATER_MARK_CHANGE = _IBDMANAGER + " -r s -m bwc -i %s uwm %s:%s"

# ibdserver config bwc flow control
_IBDMANAGER_BWC_FLOW_CONTROL = _IBDMANAGER + " -r s -m bwc -i %s ud %s:%s:%s:%s"


class IBDManager(object):
    """
    assemble the functions supported by IBD manager binary.
    """

    # def __init__(self):

    @staticmethod
    def acquire_res_lock(tiebreaker_ip, res_id, node_name, timeout):
        debug('%s acquire the resource lock from %s' % (res_id, tiebreaker_ip))
        cmd_str = "%s -r -a %s -n %s -l %s -t %s" % (_CMD_LOCK, tiebreaker_ip, node_name, res_id, timeout)
        rc = 0
        for i in range(2):
            try:
                rc = do_system_timeout(cmd_str, 2)
                if rc == 0:
                    debug('doa lock request successfully.')
                    break
            except timeout_error as e:
                warn('TIMEOUT: doa lock request: %d [%s]' % (i, e))
        return rc == 0

    @staticmethod
    def get_res_lock_status(tiebreaker_ip, res_id, node_name):
        debug('%s get the resource lock from %s' % (res_id, tiebreaker_ip))
        cmd_str = "%s -r -c %s -n %s -l %s" % (_CMD_LOCK, tiebreaker_ip, node_name, res_id)
        (ret, msg) = runcmd(cmd_str, print_ret=True, lines=True)
        if ret == 0:
            for the_line in msg:
                # owner of the lock: {char* nid}
                if the_line.find('owner of the lock:') >= 0:
                    tmp = the_line.split()
                    print tmp
                    if len(tmp) >= 5:
                        the_node = '{' + node_name + '}'
                        if tmp[4] == the_node:
                            return True
        return False

    @staticmethod
    def release_res_lock(tiebreaker_ip, res_id):
        debug('%s release the resource lock from %s' % (res_id, tiebreaker_ip))
        cmd_str = "%s -r -d %s -l %s" % (_CMD_LOCK, tiebreaker_ip, res_id)
        (ret, msg) = runcmd(cmd_str, print_ret=True)
        return ret == 0

    @staticmethod
    def is_server_working(srv_uuid):
        """
        check the server is working on ibd agent.
        """
        cmd_str = 'ibdmanager -r a -s get_wu'
        assert isinstance(srv_uuid, str)
        (ret, msg) = runcmd(cmd_str, print_ret=True, lines=True)
        if ret == 0:
            if srv_uuid in msg:
                return True
        return False

    @staticmethod
    def ibdagent_update():
        """
        update ibdagent from ibdagent.conf file
        """
        out = ['']
        rc = do_system(_IBDMANAGER_A_UPDATE, out)
        if rc != 0:
            errormsg('update ibdagent configuration failed!')
            errormsg('%s' % out[0])
            return rc
        return rc

    @staticmethod
    def ibdserver_update():
        """
        update ibdserver from ibdserver.conf file
        """
        out = ['']
        rc = do_system(_IBDMANAGER_S_UPDATE, out)
        if rc != 0:
            errormsg('update ibdserver configuration failed!')
            errormsg('%s' % out[0])
        return rc

    @staticmethod
    def find_ibd_status(level, ignore_local_flag=False):
        debug('find ibd status with %s' % level)
        cmd = _IBDMANAGER_STATUS
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        debug("check the ibd working state: " + str(msg))
        the_ibd = None
        the_uuid = None
        the_ip = None
        the_state = False
        ibd_found = False
        working_ibd_list = []
        working_uuid_list = []
        for line in msg:
            if line.find("Service Agent Channel") >= 0:
                the_ibd = None
                the_uuid = None
                the_state = False
                ibd_found = True
            elif line.find("uuid") >= 0:
                tmp = line.split(":")
                the_uuid = tmp[1]
            elif line.find("ip") >= 0:
                tmp = line.split(":")
                the_ip = tmp[1]
            elif line.find("devname") >= 0:
                tmp = line.split(":")
                the_ibd = tmp[1]
            elif line.find("state:working") >= 0:
                the_state = True
            elif level == "all" or line.find(level) >= 0:
                if the_ibd and the_uuid and (the_state or level == "all") and ibd_found \
                        and (not ignore_local_flag or (ignore_local_flag and the_ip != "127.0.0.1")):
                    working_ibd_list.append(the_ibd)
                    working_uuid_list.append(the_uuid)
                    the_ibd = None
                    ibd_found = False

        debug("working_ibd_list: " + str(working_ibd_list))
        debug("working_uuid_list: " + str(working_uuid_list))
        return (working_ibd_list, working_uuid_list)


    @staticmethod
    def is_local_ibdserver_working(old_ibdmanager_flag=False):
        cmd = _IBDMANAGER_STATUS.replace(_IBDMANAGER, _IBDMANAGER_ORG) if old_ibdmanager_flag else _IBDMANAGER_STATUS
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        debug("check the ibd working state: " + str(msg))
        the_ibd = None
        the_uuid = None
        the_ip = None
        the_state = False
        ibd_found = False
        working_ibd_list = []
        working_uuid_list = []
        level = 'all'
        for line in msg:
            if line.find("Service Agent Channel") >= 0:
                the_ibd = None
                the_uuid = None
                the_state = False
                ibd_found = True
            elif line.find("uuid") >= 0:
                tmp = line.split(":")
                the_uuid = tmp[1]
            elif line.find("ip") >= 0:
                tmp = line.split(":")
                the_ip = tmp[1]
            elif line.find("devname") >= 0:
                tmp = line.split(":")
                the_ibd = tmp[1]
            elif line.find("state:working") >= 0:
                the_state = True
            elif level == "all" or line.find(level) >= 0:
                if the_ibd and the_uuid and the_state and ibd_found \
                        and the_ip == "127.0.0.1":
                    working_uuid_list.append(the_uuid)
                    break
        if len(working_uuid_list) > 0:
            return True
        return False

    @staticmethod
    def find_working_ibd():
        out = ['']
        do_system(_IBDMANAGER_STAT_WUD, out)
        return out[0]

    @staticmethod
    def is_ibd_working(channel_uuid):
        if channel_uuid in IBDManager.find_working_ibd():
            return True
        return False

    @staticmethod
    def ibdanget_upgrade():
        (readonly_ibd_list, readonly_uuid_list) = IBDManager.find_ibd_status("alevel:readonly")
        for devname in readonly_ibd_list:
            cmd_str = '%s %s' % (_IBDMANAGER_A_UPGRADE, devname)
            rc = do_system(cmd_str)
            if rc == errno.EBUSY:
                for i in range(5):
                    time.sleep(5)
                    rc = do_system(cmd_str)
                    if rc == 0:
                        break
            if rc != 0:
                warn("WARNING: upgrade for IBD %s failed!" % devname)

    @staticmethod
    def find_write_read_working_ibd():
        out = ['']
        do_system(_IBDMANAGER_STAT_RWWUD, out)
        return out[0]

    @staticmethod
    def find_all_ibd():
        out = ['']
        do_system(_IBDMANAGER_STAT_UD, out)
        return out[0]

    @staticmethod
    def find_ibd_mapping():
        setup_info = {'wud_mapping': {}, 'rwwud_mapping': {}, 'ud_mapping': {}}
        ud_mapping = setup_info['ud_mapping']
        rwwud_mapping = setup_info['rwwud_mapping']
        wud_mapping = setup_info['wud_mapping']
        working_ibd = IBDManager.find_working_ibd()
        lines = working_ibd.split('\n')
        for the_line in lines:
            line_parts = the_line.split(' ')
            if len(line_parts) < 2:
                continue
            exportname = line_parts[0]
            devname = line_parts[1]
            wud_mapping[exportname] = devname
        IBDManager.ibdanget_upgrade()

        wr_working_ibd = IBDManager.find_write_read_working_ibd()
        lines = wr_working_ibd.split('\n')
        for the_line in lines:
            line_parts = the_line.split(' ')
            if len(line_parts) < 2:
                continue
            exportname = line_parts[0]
            devname = line_parts[1]
            rwwud_mapping[exportname] = devname
        all_ibd = IBDManager.find_all_ibd()
        lines = all_ibd.split('\n')
        for the_line in lines:
            line_parts = the_line.split(' ')
            if len(line_parts) < 2:
                continue
            exportname = line_parts[0]
            devname = line_parts[1]
            ud_mapping[exportname] = devname

        return setup_info

    @staticmethod
    def get_ibdserver_status():
        ret, msg = runcmd(_IBDMANAGER_S_SATUS, print_ret=True, lines=True)
        if ret != 0:
            return None
        return msg

    @staticmethod
    def channel_add(channel_info, ip=None):

        debug("channel add for %s with sync: %s, direct_io: %s, alignment: %s, tp_name: %s, \
                ds_name: %s, dev_name: %s, exportname: %s, dev_szie: %s, enable_kill_myself: %s" %
              (channel_info['c_uuid'], channel_info['sync'], channel_info['direct_io'],
               channel_info['alignment'], channel_info['tp_name'], channel_info['ds_name'],
               channel_info['dev_name'], channel_info['exportname'], channel_info['dev_size'], channel_info['enable_kill_myself']))
        cmd_str = _IBDMANAGER_CHANNEL_ADD % (channel_info['c_uuid'], channel_info['sync'],
                                             channel_info['direct_io'], channel_info['enable_kill_myself'],
                                             channel_info['alignment'],
                                             channel_info['tp_name'],
                                             channel_info['ds_name'], channel_info['dev_name'],
                                             channel_info['exportname'],
                                             channel_info['dev_size'])
        cmd_str = cmd_str.replace(':None', '')
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT, ip)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add channel successfully.')
            rc = 0
        else:
            debug('add channel failed.')
        return rc

    @staticmethod
    def channel_del(del_type, channel_name):
        debug("{} del for {} ".format(del_type, channel_name))
        cmd_str = _IBDMANAGER_CHANNEL_DEL % (del_type, channel_name)
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('del channel successfully.')
            rc = 0
        else:
            debug('del channel failed.')
        return rc

    @staticmethod
    def channel_switch_mwc(channel_uuid, mwc_uuid):
        debug("channel %s switch to mwc %s" % (channel_uuid, mwc_uuid))
        cmd_str = _IBDMANAGER_CHANNEL_SWITCH_MWC % (channel_uuid, mwc_uuid)
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('channel switch mwc successfully.')
            rc = 0
        else:
            debug('channel switch mwc failed.')
        return rc

    @staticmethod
    def pp_add(pp_info):
        debug("pp add for %s with owner_type: %s, pool_size: %s, page_size: %s"
              % (pp_info['pp_name'], pp_info['owner_type'], pp_info['pool_size'], pp_info['page_size']))
        cmd_str = _IBDMANAGER_PP_ADD % (pp_info['pp_name'], pp_info['owner_type'],
                                        pp_info['pool_size'], pp_info['page_size'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add pp successfully.')
            rc = 0
        else:
            debug('add pp failed.')
        return rc

    # If without wc and rc, please set them to None in parameters
    @staticmethod
    def sds_add(sds_info):
        debug("sds add for %s with sds_pp_name: %s, sds_wc_uuid: %s, sds_rc_uuid: %s"
              % (sds_info['sds_name'], sds_info['sds_pp_name'], sds_info['sds_wc_uuid'],
                 sds_info['sds_rc_uuid']))
        cmd_str = _IBDMANAGER_SDS_ADD % (sds_info['sds_name'], sds_info['sds_pp_name'],
                                         sds_info['sds_wc_uuid'], sds_info['sds_rc_uuid'])
        # To handle the situation without wc and rc
        # cmd_str = cmd_str.replace(':None:None', '')
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add sds successfully.')
            rc = 0
        else:
            debug('add sds failed.')
        return rc

    @staticmethod
    def mwc_add(mwc_info):
        for key, value in mwc_info.items():
            debug('mwc add for %s with %s.' % (key, value))

        cmd_str = _IBDMANAGER_MWC_ADD % (mwc_info['mwc_uuid'], mwc_info['bufdev'],
                                         mwc_info['bufdev_size'], mwc_info['rw_sync'], mwc_info['two_step_read'],
                                         mwc_info['do_fp'], mwc_info['tp_name'])
        if mwc_info['flush_policy'] == 'bfp4':
            cmd_str += ':%s:%s:%s:%s:%s:%s' % (
                str(mwc_info['flush_policy']), str(mwc_info['load_ratio_min']), str(mwc_info['load_ratio_max']),
                str(mwc_info['load_ctrl_level']), str(mwc_info['flush_delay_ctl']),
                str(mwc_info['throttle_ratio']))

        if mwc_info['flush_policy'] == 'bfp5':
            cmd_str += ':%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s' % (
                str(mwc_info['ssd_mode']), str(mwc_info['max_flush_size']), str(mwc_info['write_delay_first_level']),
                str(mwc_info['write_delay_second_level']), str(mwc_info['flush_policy']),
                str(mwc_info['load_ratio_min']),
                str(mwc_info['load_ratio_max']),
                str(mwc_info['load_ctrl_level']), str(mwc_info['flush_delay_ctl']),
                str(mwc_info['throttle_ratio']), str(mwc_info['coalesce_ratio']))

        if mwc_info['flush_policy'] == 'bfp1':
            cmd_str += ':%s:%s:%s:%s:%s:%s:%s' % (
                str(mwc_info['ssd_mode']), str(mwc_info['max_flush_size']), str(mwc_info['write_delay_first_level']),
                str(mwc_info['write_delay_second_level']), str(mwc_info['flush_policy']),
                str(mwc_info['high_water_mark']),
                str(mwc_info['low_water_mark']))

        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add mwc successfully.')
            rc = 0
        else:
            debug('add mwc failed.')
        return rc

    @staticmethod
    def mwc_del(mwc_info):
        debug("mwc del for %s" % mwc_info['mwc_uuid'])
        cmd_str = _IBDMANAGER_MWC_DEL % (mwc_info['mwc_uuid'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('del mwc successfully.')
            rc = 0
        else:
            debug('del mwc failed.')
        return rc

    @staticmethod
    def crc_add(crc_info):
        debug("crc add for %s with crc_pp_name: %s, cache_device: %s, cache_size: %s"
              % (crc_info['crc_uuid'], crc_info['crc_pp_name'], crc_info['cache_device'],
                 crc_info['cache_size']))
        cmd_str = _IBDMANAGER_CRC_ADD % (crc_info['crc_uuid'], crc_info['crc_pp_name'],
                                         crc_info['cache_device'], crc_info['cache_size'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add crc successfully.')
            rc = 0
        else:
            debug('add crc failed.')
        return rc

    @staticmethod
    def tp_add(tp_info):
        debug("tp add for %s with number work %s" % (tp_info['tp_uuid'], tp_info['number_work']))
        cmd_str = _IBDMANAGER_TP_ADD % (tp_info['tp_uuid'], tp_info['number_work'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add tp successfully.')
            rc = 0
        else:
            debug('add tp failed.')
        return rc

    @staticmethod
    # Should be called when all sac in the mwc are joined
    def recover_start(mwc_info):
        debug("start recover data on mwc %s" % (mwc_info['mwc_uuid']))
        cmd_str = _IBDMANAGER_MWC_RECOVER_START % (mwc_info['mwc_uuid'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'done' in msg[0]:
            debug('start recover data successfully.')
            rc = 0
        else:
            debug('start recover data failed.')
        return rc

    @staticmethod
    # Command will return "none", "doing", "done"
    # In this method, use 0,1,2 instead
    def recover_status(mwc_info):
        status_dict = {"none": 0, "doing": 1, "done": 2}
        debug("get recover data status on mwc %s" % (mwc_info['mwc_uuid']))
        rc = 1
        status = 0
        cmd_str = _IBDMANAGER_MWC_RECOVER_STATUS % (mwc_info['mwc_uuid'])
        (rc, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 1:
            for key in status_dict:
                if key in msg[1]:
                    status = status_dict[key]
                    rc = 0
        return (rc, status)

    @staticmethod
    def flush_start(channel_uuid, mwc_uuid):
        debug("start flush data from channel %s on mwc %s" % (channel_uuid, mwc_uuid))
        cmd_str = _IBDMANAGER_FLUSH_START % (mwc_uuid, channel_uuid)
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'done' in msg[0]:
            debug('start flush data successfully.')
            rc = 0
        else:
            debug('start flush data failed.')
        return rc

    @staticmethod
    def flush_stop(channel_uuid, mwc_uuid):
        debug("stop flush data from channel %s on mwc %s" % (channel_uuid, mwc_uuid))
        cmd_str = _IBDMANAGER_FLUSH_STOP % (mwc_uuid, channel_uuid)
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('stop flush data successfully.')
            rc = 0
        else:
            debug('stop flush data failed.')
        return rc

    @staticmethod
    # Command will return "none", "doing", "done"
    # In this method, use 0,1,2 instead
    def flush_status(channel_uuid, mwc_uuid):
        status_dict = {"none": 0, "doing": 1, "done": 2}
        debug("get flush data status from channel %s on mwc %s" % (channel_uuid, mwc_uuid))
        rc = 1
        status = 0
        cmd_str = _IBDMANAGER_FLUSH_GET_STATUS % (mwc_uuid, channel_uuid)
        (rc, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 1:
            for key in status_dict:
                if key in msg[1]:
                    status = status_dict[key]
                    rc = 0
        return (rc, status)

    @staticmethod
    def mrw_add(mrw_info, ip=None):
        debug("mrw add for %s" % (mrw_info['mrw_name']))
        cmd_str = _IBDMANAGER_MRW_ADD % (mrw_info['mrw_name'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT, ip)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add mrw successfully.')
            rc = 0
        else:
            debug('add mrw failed.')
        return rc

    @staticmethod
    def drw_add(drw_info, ip=None):
        debug("drw add for %s with exportname: %s" % (drw_info['drw_name'], drw_info['exportname']))
        cmd_str = _IBDMANAGER_DRW_ADD % (drw_info['drw_name'], drw_info['exportname'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT, ip)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('add drw successfully.')
            rc = 0
        else:
            debug('add drw failed.')
        return rc

    @staticmethod
    def replicate(
            vol_id,
            snap_start_id,
            snap_end_id,
            tag_vol_ip,
            tag_vol_id,
            job_id):
        """
        Ibdmanager replicate
            -u vol_id -s snap-start-id
            -e snap-end-id -I target-volume-ip
            -n target-volume-id -j job-id
        """
        cmd_str = "ibdmanager replicate -u %s -s %s -e %s -I %s -n %s -j %s" % (
            vol_id, snap_start_id, snap_end_id, tag_vol_ip, tag_vol_id, job_id)
        out = ['']
        rc = do_system(cmd_str, out)
        if rc != 0:
            errormsg('ibdmanager do replicate failed with %d [%s]' % (rc, out[0]))
        return rc

    @staticmethod
    def freeze_snapshot(channel_info):
        debug('freeze snapshot in channel %s and write cache %s' % (channel_info['uuid'], channel_info['channel_uuid']))
        cmd_str = _IBDMANAGER_SNAP_FREEZE % (channel_info['type_c'], channel_info['channel_uuid'], channel_info['uuid'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'success' in msg[0]:
            debug('snapshot freeze successfully.')
            rc = 0
        else:
            debug('snapshot freeze failed.')
        return rc

    @staticmethod
    def flush_snapshot(channel_info):
        debug('freeze snapshot in channel %s and write cache %s' % (channel_info['uuid'], channel_info['channel_uuid']))
        cmd_str = _IBDMANAGER_SNAP_FLUSH % (channel_info['type_c'], channel_info['channel_uuid'], channel_info['uuid'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'success' in msg[0]:
            debug('snapshot freeze successfully.')
            rc = 0
        else:
            debug('snapshot freeze failed.')
        return rc

    @staticmethod
    def unfreeze_snapshot(channel_info):
        debug(
            'Unfreeze snapshot in channel %s and write cache %s' % (channel_info['uuid'], channel_info['channel_uuid']))
        cmd_str = _IBDMANAGER_SNAP_UNFREEZE % (
            channel_info['type_c'], channel_info['channel_uuid'], channel_info['uuid'])
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'success' in msg[0]:
            debug('snapshot unfreeze successfully.')
            rc = 0
        else:
            debug('snapshot unfreeze failed.')
        return rc

    @staticmethod
    def write_freeze():
        debug('ibdserver write freeze')
        out = ['']
        rc = do_system(_IBDMANAGER_WRITE_FREEZE, out)
        if rc != 0:
            errormsg('%s' % out[0])
        return rc

    @staticmethod
    def write_unfreeze():
        debug('ibdserver write unfreeze')
        out = ['']
        rc = do_system(_IBDMANAGER_WRITE_UNFREEZE, out)
        if rc != 0:
            errormsg('%s' % out[0])
        return rc

    @staticmethod
    def bwc_water_mark_change(bwc_uuid, h_water_mark, l_water_mark):
        debug('Entering change bwc mark values!')
        cmd_str = _IBDMANAGER_BWC_WATER_MARK_CHANGE % (bwc_uuid, h_water_mark, l_water_mark)
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('Set water_mark successfully.')
            rc = 0
        else:
            debug('Set water_mark failed.')
        return rc

    @staticmethod
    def bwc_flow_control_change(bwc_uuid, write_delay_first_level, write_delay_second_level,
                                write_delay_first_level_max,
                                write_delay_second_level_max):
        debug('Entering change bwc mark values!')
        cmd_str = _IBDMANAGER_BWC_FLOW_CONTROL % (
            bwc_uuid, write_delay_first_level, write_delay_second_level, write_delay_first_level_max,
            write_delay_second_level_max)
        rc = 1
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if len(msg) > 0 and 'successfully' in msg[0]:
            debug('Set flow control successfully.')
            rc = 0
        else:
            debug('Set flow control failed.')
        return rc

    @staticmethod
    def set_io_error(ibd_dev):
        cmd_str = _IBDMANAGER_SET_iO_ERROR % ibd_dev
        out = ['']
        ret = do_system(cmd_str)
        if ret != 0:
            errormsg('%s' % out[0])
        return ret

    @staticmethod
    def ibd_layout_check(wc_dev, **config):
        cmd_str = '/usr/local/bin/bwc_ver_cmp {}'.format(wc_dev)
        (ret, msg) = retry_cmd(cmd_str, RETRY_NUM, TIMEOUT)
        if ret != 0:
            debug('WARN: run bwc_ver_cmp failed, skip layout checking.')
            return (False, False)
        if 'Version compatible' in msg:
            return (False, False)
        if 'Version not compatible' in msg:
            return (True, True)
        if 'Magic destroyed' in msg:
            return (False, True)
        return (False, False)

    @staticmethod
    def flush_ibd_write_cache_sync():
        ret = IBDManager.waiting_ibdserver_bio_status_to_active(30)
        if ret != 0:
            debug('ERROR: waiting ibd bio to active failed.')
            return ret
        debug('INFO: start flush write cache data on ibdserver')
        ret, msg = runcmd('/bin/ibdmanager -r s -b ff', print_ret=True)
        if ret != 0:
            debug('ERROR: flush write cache failed')
            return ret
        debug('INFO: check ibdserver flush data status')
        # now proceed with flushing data
        assigned_cmd = '/bin/ibdmanager -r s -s get | grep seq_assigned | cut -d ":" -f2'
        flushed_cmd = '/bin/ibdmanager -r s -s get | grep seq_flushed | cut -d ":" -f2'
        (rc_assigned, msg_assigned) = runcmd(assigned_cmd, print_ret=True, lines=True)
        (rc_flushed, msg_flushed) = runcmd(flushed_cmd, print_ret=True, lines=True)
        str_assigned = msg_assigned[0]
        str_flushed = msg_flushed[0]
        flush_cmd = "/bin/ibdmanager -r s -b ff"
        debug("str_assigned = %s, str_flushed = %s" % (str_assigned, str_flushed))
        while int(str_assigned) > int(str_flushed):
            (rc_flush, msg_flush) = runcmd(flush_cmd, print_ret=True, lines=True)
            time.sleep(3)
            (rc_assigned, msg_assigned) = runcmd(assigned_cmd, print_ret=True, lines=True)
            (rc_flushed, msg_flushed) = runcmd(flushed_cmd, print_ret=True, lines=True)
            str_assigned = msg_assigned[0]
            str_flushed = msg_flushed[0]
            debug("str_assigned = %s, str_flushed = %s" % (str_assigned, str_flushed))

        debug('Flush data completed, stop ff')
        cmd_stop_flush = '/bin/ibdmanager -r s -b stop_ff'
        ret, msg = runcmd(cmd_stop_flush, print_ret=True)
        if ret != 0:
            debug('ERROR: flush failed {msg}'.format(msg=msg))
        return ret

    @staticmethod
    def waiting_ibdserver_bio_status_to_active(time_out):
        debug('INFO: waiting ibd BIO status to active')
        deadtime = time.time() + int(time_out)
        while True:
            ret, msg = runcmd('/bin/ibdmanager -r s -s get', print_ret=True)
            if ret != 0:
                debug('ERROR: get ibdserver status failed.')
                return ret
            search_str = re.search('\s*BIO:\s*state:(\S+)', msg)
            if search_str is not None:
                if search_str.group(1) in ['active(1)']:
                    return 0
                elif search_str.group(1) in ['unknown(0)']:
                    try:
                        old_state = IBDServerState()
                        if old_state.wc.state in ['active(1)']:
                            return 0
                    except Exception as e:
                        errormsg('try to get ibdserver state failed {}'.format(e))
                        return 1
            if time.time() > deadtime:
                debug('ERROR: TIMEOUT waiting ibdserver status to active failed.')
                return 1
            time.sleep(0.75)