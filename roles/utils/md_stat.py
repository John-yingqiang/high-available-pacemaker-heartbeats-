#! /usr/bin/python
import os
import json
import time
import traceback
from proclock import SimpleLock
from usx_config import UsxConfig
from ibdmanager import IBDManager as IBDMgr
from functools import update_wrapper
import sys

sys.path.append("/opt/milio/libs/atlas")
from log import debug
from atl_util import *

sys.path.append("/opt/milio/atlas/roles/ha")
import ha_util


def debugmethod(f):
    def wrapper_func(cls, *args, **kwargs):
        if not cls.DEBUG:
            return 0
        return f(cls, *args, **kwargs)

    return update_wrapper(wrapper_func, f)


def validmethod(f):
    def wrapper_func(cls, *args, **kwargs):
        if 'SIMPLE' in cls.usx_conf.volume_type:
            debug('WARN: simple volume type should not use this module.')
            return 0
        cls.dbg_print('INFO: Entering {}'.format(f.__name__))
        return f(cls, *args, **kwargs)

    return update_wrapper(wrapper_func, f)


class MdStatMgr(object):
    """docstring for MdStatMgr"""
    MD_STAT_LOCK_FILE = '/tmp/md_stat.lck'
    LOCAL_MD_STAT_FILE = '/etc/ilio/md_stat_local_{volume_uuid}.json'
    SET_IO_LOCK_FILE = '/tmp/raid_io.lck'
    DEBUG = True
    usx_conf = UsxConfig()
    __md_lock = SimpleLock(MD_STAT_LOCK_FILE)
    __md_stat = None

    @classmethod
    @validmethod
    def get_stat(cls):
        return cls.__md_stat

    @classmethod
    @validmethod
    def create_stat(cls):
        with cls.__md_lock:
            try:
                raw_stat = cls.__get_raw_stat()
                md_stat = {}
                md_stat['edition'] = 0
                md_stat['data'] = {}
                md_stat['raid5_data'] = {}
                data = md_stat['data']
                raid5_data = md_stat['raid5_data']
                for key, val in raw_stat.items():
                    for stat in val:
                        data[stat['planid']] = []
                        if stat['raidtype'] == 'RAID_5':
                            raid5_data[stat['planid']] = {}

                        for child_raw in stat['children']:
                            ibd_stat = {}
                            ibd_uuid = []
                            for child in child_raw['children']:
                                ibd_uuid.append(child['uuid'])
                                ibd_stat[child['uuid']] = True
                            if stat['raidtype'] == 'RAID_5':
                                raid5_data[stat['planid']][cls.__join_ibd_str(ibd_uuid)] = True
                            data[stat['planid']].append(ibd_stat)
                cls.__md_stat = md_stat
                cls.save()
                return 0
            except Exception as e:
                debug('ERROR: cannot create md stat record file [{}]'.format(e))
                cls.dbg_print(traceback.format_exc())
                return 1

    @classmethod
    @validmethod
    def new_primary(cls, raid_dev):
        with cls.__md_lock:
            if not cls.is_stat_created():
                cls.dbg_print('skip new stat setting, wait create_stat to do this.')
                return 0
            try:
                raidplan_uuid = cls.__get_planuuid_by_name(raid_dev)
                cls.load()
                if cls.__is_raid5_stat(raid_dev):
                    for val in cls.__md_stat['raid5_data'][raidplan_uuid]:
                        ibd_list = cls.__recreate_ibd_list(val)
                        raid1_primary = False
                        for ibd_id in ibd_list:
                            for ibd_stat_dir in cls.__md_stat['data'][raidplan_uuid]:
                                if ibd_stat_dir.get(ibd_id):
                                    if ibd_stat_dir[ibd_id] and IBDMgr.is_ibd_working(ibd_id):
                                        raid1_primary = True
                                        break
                            if raid1_primary:
                                break
                        if [is_primary for is_primary in cls.__md_stat['raid5_data'][raidplan_uuid].values() if
                            not is_primary] and (not raid1_primary):
                            debug('ERROR: raid5 device just can unset one device to primary.')
                            # return 1
                        elif cls.__md_stat['raid5_data'][raidplan_uuid][val] is not raid1_primary:
                            cls.__md_stat['raid5_data'][raidplan_uuid][val] = raid1_primary
                    cls.save()
                    return 0
                if cls.__is_need_set_raid5_stat(raid_dev):
                    raid1_id = cls.__get_raid1_id_by_name(raid_dev)
                    cls.dbg_print('INFO: get raid1 info id.[{}]'.format(raid1_id))
                    new_stat = cls.__md_stat['raid5_data'][raidplan_uuid].get(raid1_id)
                    if new_stat is not None:
                        cls.__md_stat['raid5_data'][raidplan_uuid][raid1_id] = True
                        cls.save()
                        return 0
                    else:
                        cls.dbg_print('ERROR: cannot set new array for raid5.')
                        return 1
                if cls.__is_raid1_stat(raid_dev):
                    raw_stat = cls.__get_raw_stat()
                    data = {}
                    find_old_stat = False
                    for key, val in raw_stat.items():
                        for stat in val:
                            if raidplan_uuid != stat['planid']:
                                continue
                            find_old_stat = True
                            data[stat['planid']] = []
                            for child_raw in stat['children']:
                                ibd_stat = {}
                                device_cnt = len(child_raw['children'])
                                for child in child_raw['children']:
                                    # double check
                                    ibd_stat[child['uuid']] = True
                                    if not IBDMgr.is_ibd_working(child['uuid']) and device_cnt > 1:
                                        ibd_stat[child['uuid']] = False
                                data[stat['planid']].append(ibd_stat)
                    if find_old_stat:
                        cls.__md_stat['data'][raidplan_uuid] = data[raidplan_uuid]
                        cls.save()
                        return 0
                    else:
                        debug('ERROR: cannot find the new raid1 stat.')
                        return 1
                else:
                    cls.dbg_print('skip new non-raid1 stat.')
                    return 0
            except Exception as e:
                debug('ERROR: cannot new md stat record file [{}]'.format(e))
                cls.dbg_print(traceback.format_exc())
                return 1

    @classmethod
    @validmethod
    def set_io_error(cls, ibd_uuid, ibd_dev):
        try:

            md_dev = cls.__get_raid1name_by_ibduuid(ibd_uuid)
            plan_id = cls.__get_planuuid_by_name(md_dev)
            cls.load()
            need_run_raid1_commands = False
            need_run_raid5_commands = False
            for data in cls.__md_stat['data'][plan_id]:
                if ibd_uuid in data:
                    if len([is_primary for uuid, is_primary in data.items() if is_primary]) == 2:
                        need_run_raid1_commands = True
            if not need_run_raid1_commands:
                if cls.__md_stat['raid5_data'].get(plan_id):
                    if len([is_primary for is_primary in cls.__md_stat['raid5_data'][plan_id].values() if
                            not is_primary]) == 0:
                        need_run_raid5_commands = True
            if need_run_raid5_commands or need_run_raid1_commands:
                rc = IBDMgr.set_io_error(ibd_dev)
            else:
                rc = 1
            if rc == 0:
                if need_run_raid1_commands:
                    cls.__unset_primary(md_dev, ibd_dev)
                if need_run_raid5_commands:
                    # raid5_dev = cls.__get_resource_raid_name_by_ibduuid(ibd_uuid)
                    cls.__update_raid5_primary(md_dev, ibd_dev, False)
            return rc
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    @validmethod
    def can_rejoin_raid1(cls, ibd_uuid, is_need_reload=True):
        try:
            cls.dbg_print('ibd uuid %s' % ibd_uuid)
            md_dev = cls.__get_raid1name_by_ibduuid(ibd_uuid)
            plan_id = cls.__get_planuuid_by_name(md_dev)
            if is_need_reload:
                cls.load()
            raid1_rejoin = False
            for data in cls.__md_stat['data'][plan_id]:
                if ibd_uuid in data:
                    for uuid, is_primary in data.items():
                        if is_primary and IBDMgr.is_ibd_working(uuid):
                            raid1_rejoin = True
                            break
            return raid1_rejoin
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    @validmethod
    def can_rejoin_raid5(cls, ibd_uuid):
        try:
            md_dev = cls.__get_raid1name_by_ibduuid(ibd_uuid)
            resource_mddev = cls.__get_resource_raid_name_by_ibduuid(ibd_uuid)
            raid5_rejoin = True
            if cls.__is_raid5_stat(resource_mddev):
                plan_id = cls.__get_planuuid_by_name(md_dev)
                cls.load()
                if cls.__md_stat['raid5_data'].get(plan_id):
                    pass_raid1 = [cls.can_rejoin_raid1(ibd_id, False) for ibd_str, is_prmary in
                                  cls.__md_stat['raid5_data'][plan_id].items() if is_prmary for ibd_id in
                                  cls.__recreate_ibd_list(ibd_str)]
                    if pass_raid1.count(True) >= len(cls.__md_stat['raid5_data'][plan_id]) - 1:
                        raid5_rejoin = True
                    else:
                        raid5_rejoin = False

            return raid5_rejoin

        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    @validmethod
    def set_primary(cls, raid_dev, ibd_dev):
        rc = cls.__update_primary(raid_dev, ibd_dev, True)
        if rc == 0:
            return cls.__update_raid5_primary(raid_dev, ibd_dev, True)
        return rc

    @classmethod
    def __unset_primary(cls, raid_dev, ibd_dev):
        rc = cls.__update_primary(raid_dev, ibd_dev, False)
        # if rc == 0:
        #     rc = cls.__update_raid5_primary(raid_dev, ibd_dev, False)

        return rc

    @classmethod
    @validmethod
    def __update_raid5_primary(cls, raid_dev, ibd_dev, is_primary):
        with cls.__md_lock:
            try:
                raidplan_uuid = cls.__get_planuuid_by_name(raid_dev)
                device_uuid = cls.__get_ibduuid_by_name(ibd_dev)
                cls.dbg_print(raidplan_uuid, device_uuid)
                if raidplan_uuid is None:
                    raise ValueError('ERROR: invalid raid device name.')
                cls.load()
                raid5_stat = cls.__md_stat['raid5_data'].get(raidplan_uuid)
                if raid5_stat is None:
                    # raise ValueError('ERROR: raid device does not match.')
                    # print 'the device is not in raid5. skip'
                    return 0
                if device_uuid is None:
                    # maybe the device is raid1 device.try to update it.
                    raid1_id = cls.__get_raid1_id_by_name(ibd_dev)
                    if raid1_id is not None:
                        raid5_stat[raid1_id] = is_primary
                        cls.save()
                        return 0
                    else:
                        raise ValueError('ERROR: raid1 device not match.')
                for raid1_id, is_primary_r in raid5_stat.items():
                    if device_uuid in raid1_id:
                        ibd_id_list = cls.__recreate_ibd_list(raid1_id)
                        if is_primary:
                            is_work_ibd_primary = False
                            for ibd_id in ibd_id_list:
                                for ibd_dir in cls.__md_stat['data'][raidplan_uuid]:
                                    if not ibd_dir.get(ibd_id):
                                        continue
                                    elif ibd_dir[ibd_id] and IBDMgr.is_ibd_working(ibd_id):
                                        is_work_ibd_primary = True
                                        break
                                if is_work_ibd_primary:
                                    break
                            else:
                                debug('ERROR: ibd device state was error')
                                return 1
                        elif not is_primary:
                            if [is_primary_f for is_primary_f in raid5_stat.values()
                                if not is_primary_f]:
                                debug('ERROR: raid5 can not unset primary.')
                                return 0

                        raid5_stat[raid1_id] = is_primary
                        cls.save()
                        return 0
                else:
                    raise ValueError('ERROR: ibd_uuid {} not match in raid1_id'.format(device_uuid))
            except Exception as e:
                cls.dbg_print(traceback.format_exc())
                debug('ERROR: cannot update md stat [{}]'.format(e))
                return 1

    @classmethod
    def __update_primary(cls, raid_dev, ibd_dev, is_primary):
        with cls.__md_lock:
            if not cls.__is_raid1_stat(raid_dev):
                cls.dbg_print('skip update non-raid1 stat.')
                return 0
            try:
                raidplan_uuid = cls.__get_planuuid_by_name(raid_dev)
                device_uuid = cls.__get_ibduuid_by_name(ibd_dev)
                if not (raidplan_uuid and device_uuid):
                    raise ValueError('invalid raid device name or invalid ibd device name.')
                cls.load()
                cls.dbg_print(raidplan_uuid, device_uuid)
                if cls.__md_stat['data'].get(raidplan_uuid):
                    for raid1 in cls.__md_stat['data'][raidplan_uuid]:
                        cls.dbg_print(raid1)
                        if device_uuid in raid1:
                            if len(raid1) < 2:
                                return 0
                            elif not is_primary and len([elem for elem in raid1.values() if elem is True]) < 2:
                                return 0
                            raid1[device_uuid] = is_primary
                            cls.save()
                            return 0
                    else:
                        debug('ERROR: ibd device does not match')
                        return 1
                else:
                    debug('ERROR: raid device does not match.')
                    return 1
                return 0
            except Exception as e:
                cls.dbg_print(traceback.format_exc())
                debug('ERROR: cannot update md stat [{}]'.format(e))
                return 1

    @classmethod
    @validmethod
    def wait_stat_ok(cls, timeout=None, interval=1):
        if timeout:
            deadtime = time.time() + timeout
        while True:
            if cls.__is_stat_ok():
                return 0
            if timeout and time.time() > deadtime:
                debug('ERRROR: timeout occurred when waiting md stat to ok.')
                return 1
            time.sleep(interval)

    @classmethod
    @validmethod
    def push_local_to_remote(cls):
        try:
            cls.__load_from_local()
            if cls.__md_stat != cls.__get_from_pacemaker() or cls.__md_stat != cls.__get_from_usxmanager():
                cls.__save_to_remote()
            else:
                cls.dbg_print("same md stat, no need to save to remote")
            return 0
        except Exception as e:
            debug('ERROR: cannot push local md stat to remote [{}].'.format(e))
            return 1

    @classmethod
    @debugmethod
    def dbg_print(cls, *args):
        msg = " ".join([str(x) for x in args])
        print(msg)
        debug(msg)

    @classmethod
    def __is_raid1_stat(cls, raid_dev_name):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for stat in val:
                    if raid_dev_name == stat['devname']:
                        return stat['raidtype'] == 'RAID_1'
            return True
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __is_raid5_stat(cls, raid_dev_name):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for stat in val:
                    if raid_dev_name == stat['devname']:
                        return stat['raidtype'] == 'RAID_5'
            return False
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __is_need_set_raid5_stat(cls, raid_dev):
        try:
            if not cls.__is_raid1_stat(raid_dev):
                return False
            return len(list(cls.__get_all_ibduuid_by_raid1_dev(raid_dev))) < 2
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __is_cache_dev(cls, plan_id):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for stat in val:
                    if plan_id == stat['planid']:
                        if stat.get('iscache'):
                            return stat['iscache']
                        else:
                            return False
            else:
                raise ValueError('ERROR: cannot get detail infor of {}'.format(plan_id))
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_planuuid_by_name(cls, raid_dev_name):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for stat in val:
                    if raid_dev_name == stat.get('devname'):
                        return stat['planid']
                    for child_stat in stat['children']:
                        if raid_dev_name == child_stat['devname']:
                            return stat['planid']
            debug('ERROR: cannot find the plan in stat record.')
            return None
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_resource_raid_name_by_ibduuid(cls, ibd_uuid):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for child in val:
                    for child_stat in child['children']:
                        for stat in child_stat['children']:
                            if stat['uuid'] == ibd_uuid:
                                return child['devname']
            debug('ERROR: cannot find the resource name in stat record.')
            return None
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_planid_by_raid5_name(cls, raid5_devname):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for child in val:
                    if raid5_devname == child['devname']:
                        return child['planid']
            debug('ERROR: cannot find the resource name in stat record.')
            return None
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_raid1_uuid_by_raiddev(cls, raid1_dev):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for child in val:
                    for child_stat in child['children']:
                        if child_stat['devname'] in raid1_dev:
                            return child_stat['uuid']
            debug('ERROR: cannot find the raid1 uuid in stat record.')
            return None
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_ibduuid_by_name(cls, dev_name):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for child in val:
                    for child_stat in child['children']:
                        for stat in child_stat['children']:
                            if stat['devname'] in dev_name:
                                return stat['uuid']
            debug('ERROR: cannot find the ibd uuid in stat record.')
            return None
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_raid1_id_by_name(cls, raid_dev):
        try:
            new_raid_id = cls.__join_ibd_str(list(cls.__get_all_ibduuid_by_raid1_dev(raid_dev)))
            for raid5_stat in cls.__md_stat['raid5_data'].values():
                for real_id in raid5_stat.keys():
                    if new_raid_id in real_id:
                        return real_id
            return 'None'
        except:
            return' None'

    @classmethod
    def __get_all_ibduuid_by_raid1_dev(cls, raid1_dev):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for child in val:
                    for child_stat in child['children']:
                        devname = child_stat.get('devname')
                        if devname and devname in raid1_dev:
                            for stat in child_stat['children']:
                                yield stat['uuid']
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_raid1name_by_ibduuid(cls, ibd_uuid):
        try:
            raw_stat = cls.__get_raw_stat()
            for key, val in raw_stat.items():
                for child in val:
                    for child_stat in child['children']:
                        for stat in child_stat['children']:
                            if stat['uuid'] in ibd_uuid:
                                return child_stat['devname']
            debug('ERROR: cannot find the md name in stat record.')
            return None
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            return None

    @classmethod
    def __is_stat_ok(cls):
        with cls.__md_lock:
            try:
                cls.load()
                max_num_ibds = 0
                valid_active_ibd_cnt = 0
                for key, val in cls.__md_stat['data'].items():
                    for raid1 in val:
                        for ibd_uuid, is_primary in raid1.items():
                            if is_primary and IBDMgr.is_ibd_working(ibd_uuid):
                                valid_active_ibd_cnt += 1
                            max_num_ibds += 1
                if cls.usx_conf.is_mirror_volume:
                    if cls.usx_conf.is_raid1_volume:
                        max_num_disconnect = max_num_ibds / 2
                    else:
                        max_num_disconnect = max_num_ibds / 2 + 1
                else:
                    if cls.usx_conf.is_fastfailover:
                        max_num_disconnect = 2
                    else:
                        max_num_disconnect = 1
                cls.dbg_print(max_num_ibds, valid_active_ibd_cnt, max_num_disconnect)
                if valid_active_ibd_cnt < (max_num_ibds - max_num_disconnect):
                    return False
                cls.dbg_print('INFO: the raid stat is ok now.')
                return True
            except Exception as e:
                cls.dbg_print(traceback.format_exc())
                debug('ERROR: cannot check stat [{}]'.format(e))
                return False

    @classmethod
    def __compare_current_stat_with_remote(cls):
        try:
            return True
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            debug('ERROR: cannot check stat [{}]'.format(e))
            return False

    @classmethod
    @validmethod
    def is_primary_enough(cls):
        with cls.__md_lock:
            try:
                cls.load()
                failed_raid1_list = dict()
                for key, val in cls.__md_stat['data'].items():
                    failed_raid1_list[key] = []
                    num_passed_raid1 = 0
                    # Ignore non-raid1 subplan
                    if val.count({}) == len(val):
                        continue
                    for raid1 in val:
                        for ibd_uuid, is_primary in raid1.items():
                            if is_primary and IBDMgr.is_ibd_working(ibd_uuid):
                                num_passed_raid1 += 1
                                break
                        else:
                            cls.dbg_print("This subplan does not have primary node available." + str(raid1))
                            failed_raid1_list[key].append(cls.__join_ibd_str(raid1.keys()))
                    if len(failed_raid1_list[key]) > 1 or num_passed_raid1 < 1:
                        return False

                raid5_data = cls.__md_stat['raid5_data']
                for plan_id, ibd_str_list in failed_raid1_list.items():
                    if ibd_str_list:
                        for ibd_str in ibd_str_list:
                            if ibd_str in raid5_data[plan_id].keys():
                                del raid5_data[plan_id][ibd_str]
                        if len([value for value in raid5_data[plan_id].values() if value]) + 1 < len(cls.__md_stat['data'][plan_id]):
                            return False

                cls.dbg_print("Primary node is enough for this Volume.")
                return True

            except Exception as e:
                cls.dbg_print(traceback.format_exc())
                debug('ERROR: cannot check stat [{}]'.format(e))
                return False

    @classmethod
    def __get_raw_stat(cls):
        filename = '/etc/ilio/c_pool_infrastructure_{}.json'.format(cls.usx_conf.volume_uuid)
        with open(filename, 'r') as fd:
            raw_stat = json.load(fd)
        return raw_stat

    @classmethod
    def load(cls):
        try:
            # if cls.__md_stat is not None:
            #     cls.dbg_print('INFO: the stat info has been loaded. skip')
            #     return
            if ha_util.ha_check_enabled():
                cls.__load_from_remote()
            else:
                cls.__load_from_local()
        except ValueError:
            cls.__load_from_local()
            cls.dbg_print(traceback.format_exc())
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
        cls.dbg_print(json.dumps(cls.__md_stat, indent=4, separators=(',', ':')))

    @classmethod
    def save(cls):
        if not cls.__md_stat:
            raise ValueError('invalid md stat.')
        if cls.__md_stat != cls.__get_from_pacemaker() or cls.__md_stat != cls.__get_from_usxmanager():
            cls.__md_stat['edition'] += 1
            if ha_util.ha_check_enabled():
                cls.__save_to_remote()
        else:
            cls.dbg_print("same md stat, no need to save to remote")
        cls.__save_to_local()

    @classmethod
    def is_stat_created(cls):
        try:
            if not os.path.exists('/usr/share/ilio/configured'):
                return False
            with open(cls.LOCAL_MD_STAT_FILE.format(volume_uuid=cls.usx_conf.volume_uuid), 'r') as fd:
                md_stat = json.load(fd)
                if md_stat:
                    return True
            return False
        except:
            return False

    @classmethod
    def __load_from_local(cls):
        try:
            with open(cls.LOCAL_MD_STAT_FILE.format(volume_uuid=cls.usx_conf.volume_uuid), 'r') as fd:
                cls.__md_stat = json.load(fd)
                if not cls.__md_stat:
                    raise ValueError('invalid md stat.')
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __save_to_local(cls):
        try:
            with open(cls.LOCAL_MD_STAT_FILE.format(volume_uuid=cls.usx_conf.volume_uuid), 'w') as fd:
                cls.dbg_print(json.dumps(cls.__md_stat, indent=4, separators=(',', ':')))
                json.dump(cls.__md_stat, fd, indent=4, separators=(',', ':'))
                fd.flush()
                os.fsync(fd.fileno())
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __join_ibd_str(cls, ibd_list):
        return ','.join(ibd_list)

    @classmethod
    def __recreate_ibd_list(cls, ibd_str):
        return ibd_str.split(',')

    @classmethod
    def __load_from_remote(cls):
        try:
            cls.__md_stat = cls.__get_from_pacemaker()
            md_stat_from_usxm = cls.__get_from_usxmanager()
            if md_stat_from_usxm.get('edition', 0) > cls.__md_stat.get('edition', 0):
                cls.__md_stat = md_stat_from_usxm
            if not cls.__md_stat:
                raise ValueError('invalid md stat.')
            if not cls.is_stat_created():
                cls.__save_to_local()
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e

    @classmethod
    def __get_from_pacemaker(cls):
        try:
            return json.loads(ha_util.ha_get_conf_from_crm(ha_util.RAID1PRIMARY, cls.usx_conf.volume_uuid))
        except:
            return {}

    @classmethod
    def __get_from_usxmanager(cls):
        try:
            return ha_util.ha_get_conf_from_usxm(ha_util.RESOURCE, ha_util.RAID1PRIMARY, cls.usx_conf.volume_uuid)
        except:
            return {}

    @classmethod
    def __save_to_remote(cls):
        need_leave_retry_flag = True
        try:
            ret1 = ha_util.ha_update_conf_to_crm(ha_util.RAID1PRIMARY, cls.usx_conf.volume_uuid,
                                                 json.dumps(cls.__md_stat))
            ret2 = ha_util.ha_update_conf_to_usxm(ha_util.RESOURCE, ha_util.RAID1PRIMARY, cls.usx_conf.volume_uuid,
                                                  cls.__md_stat)
            if not (ret1 or ret2):
                need_leave_retry_flag = False
        except Exception as e:
            cls.dbg_print(traceback.format_exc())
            raise e
        finally:
            if need_leave_retry_flag:
                ha_util.ha_file_flag_operation(ha_util.PUSH_RAID1_PRIMARY_INFO_FLAG, "set")
