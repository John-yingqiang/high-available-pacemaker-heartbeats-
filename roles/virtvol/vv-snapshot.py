import argparse
import errno
import json
import os
import re
import subprocess
import sys
import traceback
import urllib
import copy
import time
import datetime
import ConfigParser

from backup_rollback_data import backup_vm_info, rollback_vm_info

try:
    sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
    from ha_util import ha_unmanage_resources, ha_manage_resources, send_volume_alert
except:
    errormsg('Can not import ha functions!')

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from log import *
from status_update import make_urlencoded

sys.path.insert(0, "/opt/milio/")
from libs.exporters import scst as scsi
from libs.atlas.cmd import runcmd
from libs.atlas.atl_util import lvchange_active_sync

sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *

SCST_CONF = '/etc/scst.conf'
NFS_CONF = '/etc/exports'
USX_INTERNAL_LV_LIST = ['deduplv', 'dedupvgpool', 'usx-zpool/usx-block-device', 'ibd-target-vgpool', 'ibd-target-vg',
                        'ibd-target-lv', 'ibd-wc-vgpool', 'ibd-wc-vg', 'ibd-wc-lv']
BACKUP_PATH = '/.snapshot_metadata'
VOLUME_INVENTORY_API = '/usx/inventory/volume/resources'
VVOL_CONTAINERS_API = '/usx/vvol/containers'
BACKUP_VVOL_CONTAINER_FILE = 'vvol_container.json'
VVOL_VOLUMES_API = '/usx/vvol/volumes'
SVM_EXPORTS = '/etc/ilio/svm_exports.json'
USX_LOCAL_AGENT = "http://127.0.0.1:8080/usxmanager"
ROLLBACK_LOCK_FILE = "/tmp/rollback_lock_file.lck"
VV_SNAPSHOT = 'python /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc'
NFS_EXPORTS_OPT = ("*(rw,no_root_squash,no_subtree_check,insecure,nohide,")

DEFAULT_MOUNT_OPTIONS = ("rw,noblocktable,noatime,nodiratime,timeout=180000,"
                         "dedupzeros,commit=30,thin_reconstruct,"
                         "data=ordered,errors=remount-ro")

AMC_URL_JOBSTATUS_SUFFIX = "/model/jobstatus"
CURL_PREFIX_SEND_STATUS = 'curl -f --silent -k -X POST -H "Content-Type:text/plain" '

IBDSERVER_TMP = '/etc/ilio/ibdserver.conf.tmp'
IBDSERVER_CONF = '/etc/ilio/ibdserver.conf'
RET_ASYNC = 100
USED_SPACE_THRESHOLD = 50.00
MIN_SNAPSHOT_SIZE = 0.5
MIN_SPACE_RATIO = 0.90
VOLUME_REBOOT_THRESHOLD = 70.00
ALERT_TIMESTAMP_FILE = "/tmp/alert_timestamp.txt"
USX_ZPOOL = 'usx-zpool'
ZFS_CMD = '/usr/local/sbin/zfs'
USX_BLOCK_DEVICE = 'usx-block-device'
REPLICATION_LOCK_FILE = '/tmp/replication_lock_file'
SNAPSHOT_LOCK_FILE = '/tmp/snapshot_lock_file'
SNAPSHOT_EXPORT_LOCK = '/tmp/snapshot_export_file'

# error codes
ERR_VOLUME_ID_MISMATCH = 2
ERR_VOLUME_LAYOUT_WRONG = 3
ERR_VOLUME_REPLICATION_RUNNING = 102
ERR_LVS_HANG = 4
ERR_VOLUME_NAME_MISMATCH = 5
ERR_TARGET_VOLUME_TOO_SMALL = 6
ERR_INFRA_VOLUME = 7
ERR_TARGET_VOLUME_MOUNT = 8
RC_SUCCESS = 0
ERR_UNKNOWN = 1000
UPGREP_VERSION = '/etc/ilio/snapshot-version'
SNAPSHOT_LOG = '/var/log/usx-snapshot.log'


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


class SnapshotBase(object):
    def __init__(self):
        # self.ibd_version_snapshot = IBDSnapshotVersion().get_version(UsxSettings().enable_new_ibdserver)
        self.zfs = '/usr/local/sbin/zfs'
        self.zvol_name = 'usx-zpool'
        self.snap_name = 'usx-zpool/usx-block-device@'
        self.atltis = milio_config.atltis_conf
        self.export_type = milio_config.export_type
        self.volume_type = milio_config.volume_type
        self.volume_uuid = milio_config.volume_uuid
        self.dedup_mount_point = milio_config.volume_dedup_mount_point
        self.usx_url = milio_config.usx_url
        self.vol_name = milio_config.volume_server_name
        self.resources = milio_config.volume_resources

    def verify_volume_id(self, args):
        if self.atltis.get("volumeresources"):
            for vr in self.atltis['volumeresources']:
                if "vol_id" in args:
                    if vr["uuid"] == args.vol_id:
                        return True
                elif "target_volume_name" in args:
                    if vr["volumeservicename"] == args.target_volume_name:
                        return True
            return False
        else:
            return False

    def _check_snapshot_used_space(self, snapshot_name):
        debug('Enter check_snapshot_used_space')
        voluuid = None
        try:
            node_dict = self.atltis
            if node_dict.has_key('volumeresources'):
                if len(node_dict['volumeresources']) > 0:
                    voluuid = node_dict['volumeresources'][0]['uuid']
        except ValueError as err:
            debug('Exception caught within ha_retrieve_config')
            debug('%s' % err)
        # snapshot_supported = os.path.isfile('/var/run/snapshot_supported')
        if voluuid is not None:
            # new_args = copy.deepcopy(args)
            self._freespace()

    @staticmethod
    def _is_enabled_snap_from_json():
        try:
            sources = milio_config.volume_resources
            snapshot = sources.get('snapshotenabled')
        except:
            return False
        return snapshot

    @staticmethod
    def _is_enabled_snap():
        try:
            snapshot_enabled = SnapshotBase._is_enabled_snap_from_json()
            if not snapshot_enabled:
                return False
            is_snapshot_supported = False
            out = ['']
            do_system('modprobe dm-thin-pool')
            rc = do_system('dmsetup targets | grep thin', out)
            if rc == 0:
                rc = do_system('/sbin/lvm version|grep LVM', out)
                list_lvm_ver = out[0].split('\n')
                for stri in list_lvm_ver:
                    m = re.search('LVM\s\S*\s*(\S*)\(\S*\)', stri)
                    if m is not None:
                        if rc == 0 and m.group(1) >= '2.02.98':
                            is_snapshot_supported = True
            if snapshot_enabled and is_snapshot_supported:
                debug('Thin supported!')
                return True
            else:
                debug('Thin not supported!')
                return False
        except:
            return False

    def _load_exports(self):
        if not os.path.isfile(SVM_EXPORTS):
            self.all_exports = {}
        try:
            svm_exports_file = open(SVM_EXPORTS, 'r')
            data = svm_exports_file.read()
            svm_exports_file.close()
            self.all_exports = json.loads(data)
        except:
            debug(traceback.format_exc())

    def type_check(self, args):
        debug('Enter volume %s' % args.target_volume_name)
        debug('%s' % self.__class__.__name__)
        if self.__class__.__name__ in ['SnapshotLvs']:
            return 0
        else:
            return 1

    def create_base(self, args):
        debug('Enter create_base.')
        ret = 0
        if args.wait:
            rc = self._create_snapshot(args.snap_id, args.snap_name)
            if rc == 0:
                self.amc_create_snapshot(args.vol_id, args.snap_id, args.snap_name, args.scheduled_snapshot)
                self.send_update_status("SNAPSHOT", 0, "Create snapshot", "Successfully created snapshot",
                                        args.job_id, args.vol_id)
            else:
                self.send_update_status("SNAPSHOT", 1, "Create snapshot", "Failed to create snapshot",
                                        args.job_id, args.vol_id)
                ret = 1
        else:
            if args.job_id:
                cmd_str = VV_SNAPSHOT + ' create -w -u %s -s %s -m %s -j %s' % (
                    args.vol_id, args.snap_id, args.snap_name, args.job_id)
                if args.scheduled_snapshot:
                    cmd_str = VV_SNAPSHOT + ' create -w -u %s -s %s -m %s -j %s -c' % (
                        args.vol_id, args.snap_id, args.snap_name, args.job_id)
                subprocess.Popen(cmd_str.split(),
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 close_fds=True)
                ret = 100
            else:
                errormsg('need the job id argument for async')
                return 1

        return ret

    def _create_snapshot_flush(self, snapshot_id):
        debug('Enter _create_snapshot_flush')
        do_system('sync')
        backup_vm_info(snapshot_id)
        try:
            self._create(snapshot_id)
        except Exception as e:
            errormsg('Create snapshot error:')
            errormsg(traceback.format_exc())
            errormsg('%s' % e)
            raise e

    def _create_snapshot(self, snapshot_id, snapshot_name):
        debug('Enter _create_snapshot')
        rc = 0
        # Add lock for snapshot here.
        snap_lock_fd = None
        snap_lock_fd = node_trylock(SNAPSHOT_LOCK_FILE)
        if snap_lock_fd == None:
            errormsg("A snapshot creation is already in progress. Only one creation per volume is allowed at a time")
            return 102

        self._check_snapshot_used_space(snapshot_name)
        try:
            self._create_snapshot_flush(snapshot_id)
        except Exception as e:
            rc = 1

        # Rlease lock.
        node_unlock(snap_lock_fd)

        return rc

    def delete_base(self, args):
        return self._delete_snapshot(args.snap_id, args.snap_name)

    def _delete_snapshot(self, snapshot_id, snapshot_name):
        debug('Enter delete snapshot')
        self._check_snapshot_used_space(snapshot_name)
        rc = self._delete(snapshot_id)
        return rc

    def delete_all_base(self, args):
        return self._delete_all(args.snap_name)

    def _delete_all(self, snapshot_name):
        debug('Enter delete all snapshot')
        self._check_snapshot_used_space(snapshot_name)
        ret = 0
        snapshot_list = self.list_snapshot_internal()
        if snapshot_list:
            for snap_id in snapshot_list:
                if snap_id.startswith('WARNING') or snap_id in USX_INTERNAL_LV_LIST:
                    continue
                rc = self._delete(snap_id)
                if rc != 0:
                    errormsg('delete snapshot failed with %s' % snap_id)
                    ret = 1
        return ret

    def list_snap_base(self, args):
        debug('Enter show snapshot')
        rc = self._list_snapshot()
        return rc

    def unmount_base(self, args):
        return self._unmount_snapshot(args.snap_id)

    def _unmount_snapshot(self, snapshot_id):
        debug('Enter umount  snapshot')
        mnt_dir = self.snapshot2mntdir(snapshot_id)
        if mnt_dir is None:
            errormsg('can not find mount point for %s!' % snapshot_id)
            return 1
        rc = self.del_export(mnt_dir)
        if rc != 0:
            errormsg('nfs unexport %s error!' % mnt_dir)
            return rc
        self.kill_fsuser(mnt_dir)
        rc = self.do_umount(mnt_dir, snapshot_id)
        if rc != 0:
            return rc
        os.rmdir(mnt_dir)
        return 0

    def unmount_all_base(self, args):
        return self._unmount_all()

    def _unmount_all(self):
        debug('Enter unmountall_snapshots')
        rc = 0
        snapshot_list = self.list_snapshot_internal()
        for snap_id in snapshot_list:
            if snap_id in USX_INTERNAL_LV_LIST:
                continue
            debug('unmount snapshot: %s' % snap_id)
            rc = self._unmount_snapshot(snap_id)
            if rc != 0:
                break
        return rc

    def mount_snapshot_base(self, args):
        debug('Enter mount snapshot')
        ret = 0
        if args.wait:
            # Add lock for snapshot here.
            try:
                snap_lock_fd = None
                snap_lock_fd = node_trylock(SNAPSHOT_EXPORT_LOCK)
                if snap_lock_fd is None:
                    raise SnapError('Only one export per volume is allowed at a time')
                if args.snap_id not in self.list_snapshot_internal():
                    raise SnapError('snapshot %s does not exist, can not mount!' % args.snap_id)
                mnt_dir = self.snapshot2mntdir(args.snap_id)
                if mnt_dir is not None:
                    raise SnapError('snapshot %s already mounted to %s!' % (args.snap_id, mnt_dir))
                rc = self.mount_snapshot_readonly(args.snap_id, args.mount_dir)
                if rc != 0:
                    raise SnapError('export snapshot failed')
                rc = self.add_export(args.snap_name, args.snap_id, args.mount_dir, args.export_id)
                if rc != 0:
                    raise SnapError('add export failed')
            except Exception as e:
                errormsg('snapshot API error: %s' % e)
                errormsg(traceback.format_exc())
                if 'Only one export per volume is allowed at a time' in str(e):
                    ret = 102
                else:
                    ret = 1
            finally:
                # Rlease lock.
                node_unlock(snap_lock_fd)
            if ret == 0:
                self.update_export_url(args.vol_id, args.export_id, args.snap_id, args.mount_dir)
                self.send_update_status("SNAPSHOT", 0, "Export Snapshot", "Successfully exported snapshot",
                                        args.job_id, args.vol_id)
            elif ret == 1:
                self.send_update_status("SNAPSHOT", 1, "Export Snapshot", "Failed to export snapshot",
                                        args.job_id, args.vol_id)
        else:
            if args.job_id:
                cmd_str = '{script_name} export -w -u {volume_id} -s {snapshot_id} -i {export_id} -m {snapshot_name} -j {job_id} -p {mount_point}'.format(
                    script_name=VV_SNAPSHOT, volume_id=args.vol_id, snapshot_id=args.snap_id, export_id=args.export_id,
                    snapshot_name=args.snap_name, job_id=args.job_id, mount_point=args.mount_dir)
                subprocess.Popen(cmd_str.split(),
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 close_fds=True)
                ret = 100
            else:
                errormsg('need the job id argument for async')
                ret = 1
        return ret

    def update_export_url(self, vol_id, export_id, snapshot_id, export_path):
        amcpost_str = '/usx/dataservice/volumeextensions/snapshot?api_key=%s' % vol_id
        apiurl_str = USX_LOCAL_AGENT + amcpost_str  # actual API url

        timestamp = int(time.time() * 1000)
        json_str = '{\\"uuid\\":\\"%s\\",\\"volumeresourceuuid\\":\\"%s\\",\\"ctime\\":%d,\\"mountedpoint\\":\\"%s\\",\\"number\\":%s}' % (
            snapshot_id, vol_id, timestamp, export_path, export_id)
        # actual curl command to send a JSON formatted body
        cmd = r'curl -s -k -X PUT -H "Content-Type:application/json" -d "%s" %s' % (json_str, apiurl_str)
        rc = do_system(cmd, log=True)
        if rc != 0:  # curl system call failed, return error
            return False
        return True

    def get_mount_list(self, vol_id):
        debug("Entering add_all_exports()")
        amcpost_str = '/usx/dataservice/snapshots?query=.[volumeresourceuuid=\'%s\']&sortuuid&order=ascend&page=0&pagesize=100&composite=false' % vol_id
        apiurl_str = USX_LOCAL_AGENT + amcpost_str
        out = ['', '']
        cmd = r'curl -k --globoff "%s"' % apiurl_str
        rc = do_system(cmd, out, log=True)

        # now to parse the output
        debug("output:" + str(out))
        mount_info_list = []
        output = out[0]
        index = output.find("{")
        out2 = output[index:]

        config_json = json.loads(out2)

        count = config_json['count']
        if count <= 0:
            # No snapshots to export.  Return w/o error
            debug("No snapshots to mount.")
            return mount_info_list

        debug("add_exports = count = %d" % (int(count)))
        i = 0
        mount_info_list = []
        while i < count:
            next_ss = config_json["items"][i]

            if "mountedpoint" not in next_ss:
                i += 1
                continue
            snapshot_id = next_ss["uuid"]
            mounted_point = next_ss["mountedpoint"]
            snapshot_name = next_ss["snapshotname"]
            volume_number = next_ss["number"]
            vol_resource_id = next_ss["volumeresourceuuid"]
            debug("snapshot_id = %s, mounted_point = %s, snapshot_name = %s\n" % (
                snapshot_id, mounted_point, snapshot_name))
            if (vol_resource_id == vol_id) and (mounted_point != ""):
                mount_info = MountInfo()
                mount_info.snapshot_name = snapshot_name
                mount_info.snapshot_id = snapshot_id
                mount_info.mount_point = mounted_point
                mount_info.volume_number = str(volume_number)
                mount_info_list.append(mount_info)
            debug(mount_info_list)
            i += 1
        return mount_info_list

    def mount_all_snapshot_base(self, args):
        ret = 0
        debug('Enter mount_all snapshot')
        mount_info_list = self.get_mount_list(args.vol_id)
        if len(mount_info_list) == 0:
            debug("No exported snapshots to mount.")
            return 0
        for entry in mount_info_list:
            debug("mount_info list entry %s %s %s" % (entry.snapshot_id, entry.mount_point, entry.snapshot_name))
            if entry.snapshot_id not in self.list_snapshot_internal():
                errormsg('snapshot %s does not exist, can not mount!' % entry.snapshot_id)
                return 1
            mnt_dir = self.snapshot2mntdir(entry.snapshot_id)
            if mnt_dir is not None:
                errormsg('snapshot %s already mounted to %s!' % (entry.snapshot_id, mnt_dir))
                return 1
            try:
                os.mkdir(entry.mount_point)
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise e
                pass
            rc = self.mount_snapshot_readonly(entry.snapshot_id, entry.mount_point)
            if rc != 0:
                errormsg('mount dir %s error' % entry.mount_point)
                os.rmdir(entry.mount_point)
                return rc
            rc = self.add_export(entry.snapshot_name, entry.snapshot_id, entry.mount_point, entry.volume_number)
            if rc != 0:
                ret = 1
        return ret

    def add_all_export_base(self, args):
        debug('Enter add all export snapshot')
        mount_list = self.get_mount_list(args.vol_id)
        if len(mount_list) == 0:
            debug("No exported snapshots to mount.")
            return 0
        for entry in mount_list:
            debug("mount_info list entry %s %s %s" % (entry.snapshot_id, entry.mount_point, entry.snapshot_name))
            self.mount_snapshot_readonly(entry.snapshot_id, entry.mount_point)
        debug('add exprots for %s' % self.export_type)
        if self.export_type == "iSCSI":
            self.add_ss_exprots_iscsi_conf(self.dedup_mount_point, mount_list)
        elif self.export_type == "NFS":
            self.add_ss_exports_nfs_conf(self.dedup_mount_point, mount_list)
        else:
            errormsg('invalid export type: %s' % self.export_type)
            return 1
        return 0

    def roll_back(self, args):
        ret = 0
        if args.wait:
            ret = self.rollback_snapshot_base(args)
            os.remove(ROLLBACK_LOCK_FILE)
        else:
            if args.job_id:
                if os.path.isfile(ROLLBACK_LOCK_FILE):
                    errormsg("rollback_snapshot_async: ERROR: Unable to get lock.  Rollback in progress.")
                    return ERR_VOLUME_REPLICATION_RUNNING
                else:
                    fd = open(ROLLBACK_LOCK_FILE, "w")
                    fd.close()

                cmd_str = VV_SNAPSHOT + ' rollback -w -u %s -s %s -j %s' % (args.vol_id, args.snap_id, args.job_id)
                # Must not inherit parent's stdout/stderr.
                subprocess.Popen(cmd_str.split(),
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 close_fds=True)
                ret = 100
            else:
                errormsg('need the job id argument for async')
                return 1

        return ret

    def rollback_snapshot_base(self, args):
        debug('Enter rollback snapshot')
        rollback_vm_info(args.snap_id)

        try:
            if args.snap_id not in self.list_snapshot_internal():
                raise SnapError('snapshot %s does not exist, can not mount!' % args.snap_id)

            mnt_dir = self.snapshot2mntdir(args.snap_id)
            if mnt_dir is not None:
                raise SnapError('snapshot %s already mounted to %s!' % (args.snap_id, mnt_dir))

            ha_temp_disabled = False
            if milio_config.is_ha:
                debug('Put ha in maintenance mode to umount dedupfs.')
                ha_unmanage_resources()
                ha_temp_disabled = True
            else:
                debug('HA is not enabled.')
            rc = self.stop_export_service()
            do_system('sync')
            if rc != 0:
                raise SnapError('stop export service error!')
            ret = self.rollback_snapshot(args.snap_id, args.vol_id)
        except Exception, err:
            errormsg('rollback error:')
            errormsg(traceback.format_exc())
            error_str = str(err)
            ret = 1
        finally:
            self.start_export_service()
            if ha_temp_disabled:
                debug('Put ha back to enabled mode.')
                ha_manage_resources()
        if args.job_id:
            if ret == 0:
                self.send_update_status("SNAPSHOT", 0, "Rollback snapshot", "Successfully rolled back snapshot",
                                        args.job_id, args.vol_id)
            else:
                self.rollback_status_change(args.snap_id)
                if error_str != "":
                    self.send_update_status("SNAPSHOT", 1, "Rollback snapshot", error_str, args.job_id, args.vol_id)
                else:
                    self.send_update_status("SNAPSHOT", 1, "Rollback snapshot", "Failed to roll back snapshot",
                                            args.job_id, args.vol_id)
        return ret

    def rollback_status_change(self, snapshot_id, status='false'):

        rollback_status_api = u'/usx/dataservice/snapshot/rollback/status'
        json_str = u'{\\"uuid\\":\\"%s\\",\\"rollback\\":\\"%s\\"}' % (snapshot_id, status)
        api_str = USX_LOCAL_AGENT + rollback_status_api
        cmd = u'curl -s -k -X PUT -H "Content-Type:application/json" -d "%s" %s' % (json_str, api_str)
        runcmd(cmd, print_ret=True)

    def freespace_base(self, args):
        return self._freespace()

    def _freespace(self):
        debug('Enter freespace_snapshots')
        rc = 0
        if not milio_config.is_contains_volume:
            return 0
        snap_list = self.list_snapshot_internal_sortbytime()
        if len(snap_list) <= 2:
            return 0

        if not self.reach_used_space_threshold():
            return 0

        for snap_id in snap_list:
            if snap_id in USX_INTERNAL_LV_LIST:
                # Skip the primary LV and thin pool
                continue
            debug('unmount snapshot: %s' % snap_id)
            ret = self._unmount_snapshot(snap_id)
            debug('delete snapshot: %s' % snap_id)
            ret = self._delete(snap_id)
            if ret != 0:
                rc = 1
            debug('update amc DB for snapshot deletion: %s %s' % (snap_id, snap_id))
            self.amc_delete_snapshot(self.volume_uuid, snap_id)
            debug('send alert for snapshot deletion:  %s %s' % (snap_id, snap_id))
            send_volume_alert("WARN", "Deleted snapshot to free disk space")
            if not self.reach_used_space_threshold():
                break
        return rc

    def update_snap_info_base(self, args):
        return self._update_snapshot_info(args.vol_id)

    def _update_snapshot_info(self, vol_id):
        debug('Enter update info for snapshot')

        item_list = []
        data = {"count": '', "items": []}
        item = {"ctime": '', "scheduled": '', "snapshotname": '', "mountedpoint": "", "number": '',
                'volumeresourceuuid': vol_id}
        snap_list = self.list_snapshot_internal_sortbytime()
        i = 0
        for snap_uuid in snap_list:
            if snap_uuid in USX_INTERNAL_LV_LIST:
                continue
            t_item = item.copy()
            t_item['uuid'] = snap_uuid
            item_list.append(t_item)
            i += 1
        data['count'] = i
        data['items'] = item_list
        sys.stdout.write(json.dumps(data))
        return 0

    def replicate_volume_async(self, args):
        # Make a lock first.
        replication_lock_fd = None
        replication_lock_fd = node_trylock(REPLICATION_LOCK_FILE)
        if replication_lock_fd == None:
            errormsg(
                'A volume replication is already in progress. Only one replication operation per volume is allowed at a time')
            return ERR_VOLUME_REPLICATION_RUNNING

        if args.wait:
            ret = self.replicate_volume_base(args)
        elif args.job_id:
            if self.__class__.__name__ in ['SnapshotLvs']:
                rpid = os.popen(
                    "ps aux | grep -w rsync | grep -w delete | grep -v 'grep' | head -n 1 | awk '{print $2}'").read().strip()
            else:
                rpid = os.popen("ps aux | grep 'zfs send' |grep -v 'grep'|head -n 1 | awk '{print $2}'").read().strip()
            if len(rpid) > 0:
                errormsg(
                    'A volume replication is already in progress. Only one replication operation per volume is allowed at a time.')
                ret = ERR_VOLUME_REPLICATION_RUNNING
            else:
                cmd_str = VV_SNAPSHOT + ' replicate -w -u %s -r %s -s %s -i %s -n %s -j %s' % (args.vol_id,
                                                                                               args.replicate_id,
                                                                                               args.snap_id,
                                                                                               args.target_volume_ip,
                                                                                               args.target_volume_name,
                                                                                               args.job_id)
                # Must not inherit parent's stdout/stderr.
                subprocess.Popen(cmd_str.split(),
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 close_fds=True)
                ret = RET_ASYNC
        else:
            errormsg('need the job id argument for async')
            ret = 1

        # Release lock.
        node_unlock(replication_lock_fd)

        return ret

    def replicate_volume_base(self, args):
        debug('replicate not fully implemented yet.')
        rep_target_dir = '/exports/' + args.target_volume_name
        is_snapshot_created = False
        cleanup_remote_snapshot = False
        is_mounted = False
        error_str = ""
        ret = 1
        try:
            rc = do_system('python /opt/milio/atlas/system/sshw.pyc -i ' + args.target_volume_ip)
            if rc != 0:
                debug('Create Established trust relationships failed')
                error_msg = "Create Established trust relationships error"
                raise SnapError(error_msg)
            if args.snap_id not in self.list_snapshot_internal():
                debug('create internal snapshot %s for replication.' % args.snap_id)
                try:
                    rc = self._create_snapshot(args.snap_id, args.snap_name)
                except:
                    raise SnapError('Can not create temporary snapshot %s for replication' % args.snap_id)
                if rc != 0:
                    raise SnapError('Create snapshot failed, please check log for details.')
                is_snapshot_created = True
            mnt_dir = self.snapshot2mntdir(args.snap_id)
            if mnt_dir is None:
                mnt_dir = '/exports/%s' % args.snap_id
                try:
                    debug('make dir %s' % mnt_dir)
                    os.mkdir(mnt_dir)
                except OSError, e:
                    if e.errno != errno.EEXIST:
                        raise e
                    pass
                rc = self.mount_snapshot_readonly(args.snap_id, mnt_dir)
                if rc != 0:
                    os.rmdir(mnt_dir)
                    raise SnapError('mount snapshot error!')
                is_mounted = True
            vol_size = self.resources['volumesize']
            rc = self.ssh_exec(args.target_volume_ip, 'python \
                      /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc \
                      type_check -n %s' % args.target_volume_name)
            if (rc == 0 and self.__class__.__name__ in ['SnapshotLvs']) or (
                            rc == 1 and self.__class__.__name__ in ['SnapshotZfs']):
                debug('base and target are same snapshot type')
            else:
                raise SnapError('Snapshot is not supported by target volume layout!')
            rc = self.ssh_exec(args.target_volume_ip, 'python \
                      /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc \
                      replicate_target -n %s -s %s -ts %d'
                               % (args.target_volume_name, args.snap_id, vol_size))
            if rc != 0:
                if ERR_VOLUME_LAYOUT_WRONG == rc:
                    error_msg = "Target: Snapshot is not supported by this volume layout!"
                elif ERR_VOLUME_ID_MISMATCH == rc:
                    error_msg = "Target volume id mismatch"
                elif ERR_VOLUME_NAME_MISMATCH == rc:
                    error_msg = "Target volume name mismatch"
                elif rc == ERR_TARGET_VOLUME_TOO_SMALL:
                    error_msg = "Target volume too small"
                elif rc == ERR_INFRA_VOLUME:
                    error_msg = "Target volume can't be infrastructure volume"
                elif rc == ERR_TARGET_VOLUME_MOUNT:
                    error_msg = "Target volume export path was mounted"
                else:
                    error_msg = "Target volume error"
                raise SnapError(error_msg)
            for i in range(5):
                # Check target mount status again.
                cmd_mount_status = 'ssh %s python /opt/milio/scripts/usx_mount_umount_adm.pyc status' \
                                   % args.target_volume_ip
                out = ['']
                do_system(cmd_mount_status, out)
                if out[0].strip() in ['mount']:
                    debug('target volume export path was mounted.')
                    rc = 1
                    break
                rc = do_system('rsync -P -av --exclude="*.vswp" --delete --log-file=/tmp/rsync-status.txt %s %s:/%s' % (
                    mnt_dir + '/', args.target_volume_ip, rep_target_dir))
                if rc == 0:
                    break
            if rc != 0:
                # Data transfer error, keep remote snapshot for possible manually recovery.
                raise SnapError('rsync to target volume failed!')
            cleanup_remote_snapshot = True
            ret = 0
        except Exception, err:
            errormsg('replication error:')
            errormsg(traceback.format_exc())
            error_str = str(err)
        finally:
            debug('replication cleanup...')
            if cleanup_remote_snapshot:
                self.ssh_exec(args.target_volume_ip, 'python \
                          /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc \
                          replicate_target -c -n %s -s %s'
                              % (args.target_volume_name, args.snap_id))
            else:
                # Remove rsync running flag.
                self.ssh_exec(args.target_volume_ip, 'rm -rf %s' % self.rsync_running)
            if is_mounted:
                self.do_umount(mnt_dir, args.snap_id)
                os.rmdir(mnt_dir)
            if is_snapshot_created:
                rc = self._delete(args.snap_id)
                if rc != 0:
                    errormsg('Cannot remove tmp snapshot %s!' % args.snap_id)
                    ret = rc
            if ret == 0:
                self.amc_create_replicate(args)
        if args.job_id:
            if ret == 0:
                self.send_update_status("REPLICATION", 0, "Replicate volume", "Successfully replicated volume",
                                        args.job_id, args.vol_id)
            else:
                if error_str != "":
                    self.send_update_status("REPLICATION", 1, "Replicate volume", error_str, args.job_id,
                                            args.vol_id)
                else:
                    self.send_update_status("REPLICATION", 1, "Replicate volume", "Failed to replicate volume",
                                            args.job_id, args.vol_id)

        return ret

    def replicate_volume_target_base(self, args):
        debug('Enter replicate target volume snapshot.')
        debug('check the target volume mount status!')
        cmd_mount_status = 'python /opt/milio/scripts/usx_mount_umount_adm.pyc status'
        rc, msg = runcmd(cmd_mount_status, print_ret=True)
        if msg.strip() in ['mount']:
            return ERR_TARGET_VOLUME_MOUNT
        snap_id = args.snap_id + '_replicate'
        if self.vol_name != args.target_volume_name:
            return ERR_VOLUME_NAME_MISMATCH
        if is_infra_volume(milio_config.atltis_conf):
            return ERR_INFRA_VOLUME
        dedupfs_found = False
        the_mountpoint = self.search_device(self.vol_name)
        if the_mountpoint is None:
            errormsg('cannot load the export devcie name %s ' % self.vol_name)
            return 1
        if the_mountpoint in self.dedup_mount_point:
            dedupfs_found = True
        if not dedupfs_found:
            debug("ERR: cannot locate dedupFS mount point")
            return 1
        if args.target_volume_size:
            this_volume_size = self.resources['volumesize']
            if args.target_volume_size > this_volume_size:
                errormsg(
                    'target volume too small, required:%d, actual:%d' % (args.target_volume_size, this_volume_size))
                return ERR_TARGET_VOLUME_TOO_SMALL
            usedspace, totalspace, used_space_percent, GBflag = self.lvs_snapshot_version.used_space_threshold_lvs()
            rc = self.used_space_threshold(used_space_percent)
            if rc != 0:
                return ERR_TARGET_VOLUME_TOO_SMALL
        if args.cleanup:
            rc = self._delete(snap_id)
            if rc == 0:
                self.amc_delete_snapshot(self.volume_uuid, snap_id)
            cmd_str = 'rm -rf %s' % self.rsync_running
            rc = do_system(cmd_str, log=True)
            if rc != 0:
                debug('Remove file % failed.' % self.rsync_running)
        else:
            try:
                # Add rsync running flag.
                cmd_str = 'touch %s; sync' % self.rsync_running
                rc = do_system(cmd_str, log=True)
                if rc != 0:
                    debug('Create file %s failed.' % self.rsync_running)
                self._create_snapshot_flush(snap_id)
            except:
                if self.__class__.__name__ in ['SnapshotLvs']:
                    rpid = os.popen(
                        "ps aux | grep -w rsync | grep -w delete | grep -v 'grep' | head -n 1 | awk '{print $2}'").read().strip()
                    if len(rpid) > 0:
                        print 'A volume replication is already in progress. Only one replication operation per volume is allowed at a time.'
                        rc = 1
                    else:
                        print 'reuse an existing snapshot!'
                        rc = 0
            else:
                self.amc_create_snapshot(self.volume_uuid, snap_id, snap_id)
                rc = 0

        return rc

    def create_golden_image(self, args):
        raise SnapError('create golden image just lvs has this function!')

    def send_update_status(self, task_type, status, task_name, message, job_id, vol_id):
        debug("Entering send_update_status")
        status_str = str(status)
        if status == 0:
            percent_complete = 100
        else:
            percent_complete = 80
        message = urllib.quote_plus(message)
        task_name_enc = make_urlencoded(task_name)
        url_to_send = USX_LOCAL_AGENT + AMC_URL_JOBSTATUS_SUFFIX + "/" + job_id + "?tasktype=" + task_type + "&percentcomplete=" + str(
            percent_complete) + "&task=" + task_name_enc + "&status=" + status_str + "&message=" + message + "&api_key=" + vol_id

        cmd = CURL_PREFIX_SEND_STATUS + "'" + url_to_send + "'"
        status, ret = runcmd(cmd, print_ret=True, block=False)

        if status != 0:
            debug(
                "ERROR : got non-zero status when running command to send Job status for Job ID " + job_id + ", Status WAS NOT send to AMC/Grid.")
            return False

    def amc_create_replicate(self, args):
        amcpost_str = '/usx/dataservice/volumeextensions/replica?api_key=%s' % args.vol_id
        apiurl_str = USX_LOCAL_AGENT + amcpost_str  # actual API url

        timestamp = int(time.time() * 1000)
        json_str = '{\\"uuid\\":\\"%s\\",\\"targetvolumeip\\":\\"%s\\",\\"targetvolumename\\":\\"%s\\",\\"snapshotuuid\\":\\"%s\\",\\"volumeresourceuuid\\":\\"%s\\",\\"description\\":\\"\\",\\"ctime\\":%d}' % (
            args.replicate_id, args.target_volume_ip, args.target_volume_name, args.snap_id, args.vol_id, timestamp)
        # actual curl command to send a JSON formatted body
        cmd = r'curl -s -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, apiurl_str)
        rc = do_system(cmd, log=True)
        if rc != 0:  # curl system call failed, return error
            return False
        return True

    def search_device(self, devname):
        mnt_dir = None
        cmd_mount = 'mount'
        out = ['']
        rc = do_system(cmd_mount, out)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')
        for snap in list_out:
            m = re.search('\S*%s' % devname, snap)
            if m is not None:
                mnt_dir = m.group()
        return mnt_dir

    def ssh_exec(self, host, cmd):

        import pexpect
        ssh_newkey = 'Are you sure you want to continue connecting'
        p = pexpect.spawn('ssh %s %s' % (host, cmd))
        i = p.expect([ssh_newkey, 'password:', pexpect.EOF], timeout=600)
        if i == 0:
            p.sendline('yes')
            i = p.expect([ssh_newkey, 'password:', pexpect.EOF], timeout=600)
        if i == 1:
            debug('password--')
        if i == 2:
            debug('end--')
        debug(p.before)
        p.close()
        debug(str(p.exitstatus))
        return p.exitstatus

    def del_export(self, export_dir, snap_name=None):
        if 'iSCSI' in self.export_type:
            return self.iscsi_unexport(snap_name, export_dir)
        elif 'NFS' in self.export_type:
            return self.nfs_unexport(export_dir)
        else:
            errormsg('invalid export type: %s' % self.export_type)

    def add_ss_exprots_iscsi_conf(self, vol_export, mount_info):
        debug("Entering open_iscsi_conf()")
        f = open(SCST_CONF, "r+")
        data_str = f.read()

        lines = data_str.splitlines(True)
        curr_device_line = 0
        vol_export_id = ""
        # first search for the mount_dir in one of the device entries
        done = False
        while not done:
            i = 0
            popped_line = False
            debug("begin for loop")
            for line in lines:
                mount_dir = ""
                if "DEVICE" in line:
                    curr_device_line = i
                    curr_device = line.split()[1]
                if "filename" in line:
                    mount_dir = line.split()[1]
                    debug("mount_dir is " + mount_dir)
                    if vol_export in mount_dir:
                        vol_export_id = curr_device
                        debug("vol_export_id is %s" % vol_export_id)
                    else:
                        if (mount_dir == "") or (mount_dir is None):
                            pass
                        else:
                            debug("clean_config - mount_dir = %s" % mount_dir)
                            found_close_brace = False

                            while not found_close_brace:
                                if '}' in lines[curr_device_line]:
                                    found_close_brace = True
                                debug("popping out line - %s", lines[curr_device_line])
                                lines.pop(curr_device_line)
                            popped_line = True
                            break
                i += 1
            if not popped_line:
                done = True
        done = False

        while not done:
            i = 0
            found_target_driver = False
            popped_line = False
            for line in lines:
                if 'TARGET_DRIVER' in line:
                    found_target_driver = True
                elif (vol_export_id in line) and found_target_driver:
                    debug("found vol_export_id in the lines")
                    i += 1
                    continue
                elif ('LUN' in line) and found_target_driver:
                    # need to delete the line with the LUN
                    debug("popping line %s" % (lines[i]))
                    lines.pop(i)
                    popped_line = True
                    break
                i += 1
            if not popped_line:
                done = True

        new_conf = ''.join(lines)

        f.seek(0)
        f.truncate()
        f.write(new_conf)

        debug("Entering add_exports_iscsi_conf ")
        f.seek(0)
        data_str = f.read()

        debug("add_ss_exports_iscsi_conf - data = " + data_str)
        lines = data_str.splitlines(True)

        found_handler = False
        i = 0
        for line in lines:
            if 'HANDLER' in line:
                device_insert_index = i + 1
                found_handler = True
                debug("\nfound handler\n")
                break
            i += 1
        if not found_handler:
            debug("Error: badly formed scst.conf file")
            return 1

        # found entry point - now add the DEVICE entries
        debug("Adding the lines to the new conf")
        for entry in mount_info:
            line_device_filename = "    filename " + entry.mount_point + '/LUN1\n'
            entry.device_id = scsi.get_unique_device_id(lines)
            line_device_heading = "  DEVICE " + entry.device_id + " {\n"
            line_device_read_only = "    read_only 1\n"
            line_device_close_brace = "  }\n"
            lines.insert(device_insert_index, line_device_heading)
            device_insert_index += 1
            lines.insert(device_insert_index, line_device_filename)
            device_insert_index += 1
            lines.insert(device_insert_index, line_device_read_only)
            device_insert_index += 1
            lines.insert(device_insert_index, line_device_close_brace)
            device_insert_index += 1

        new_conf = ''.join(lines)
        debug("new conf file is " + new_conf)

        i = 0
        found_target_driver = False
        found_target = False
        for line in lines:
            if 'TARGET_DRIVER' in line:
                # print "found line with TARGET_DRIVER"
                found_target_driver = True
                i += 1
                continue
            if found_target_driver and (not found_target) and ('TARGET ' in line):
                target_insert_index = i + 1
                break
            i += 1

        for entry in mount_info:
            unique_lun_num = scsi.find_unique_lun_id(lines)
            line_target_lun = "    LUN %s %s\n" % (entry.volume_number, entry.device_id)
            lines.insert(target_insert_index, line_target_lun)

        new_conf = ''.join(lines)
        debug("new conf is " + new_conf)

        f.seek(0)
        f.write(new_conf)
        f.close()
        self.iscsi_stop_service()
        self.iscsi_start_service()

    def add_ss_exports_nfs_conf(self, vol_export, mount_info):
        debug("Entering open_nfs_conf()")
        fd = open(NFS_CONF, "r+")
        debug("Entering clean_nfs_conf")
        fd.seek(0)
        data_str = fd.read()
        debug("clean_nfs_conf - data = %s" + data_str)

        lines = data_str.splitlines(True)

        # first search for the mount_dir in one of the device entries
        # This will iterate through lines, popping a line each time, but then breaking out and starting over
        #    with the altered lines - so the iterator does not become invalid
        done = False
        while not done:
            popped_line = False
            i = 0
            for line in lines:
                if "exports" in line:
                    mount_dir = line.split()[0]
                    if vol_export in line:
                        i += 1
                        debug("clean_nfs_conf() - found vol_export")
                        continue
                    lines.pop(i)
                    popped_line = True
                    break
                i += 1
            if not popped_line:
                done = True

        new_conf = ''.join(lines)

        fd.seek(0)
        fd.truncate()
        fd.write(new_conf)

        fd.seek(0)
        data_str = fd.read()
        exports_str = ""
        for entry in mount_info:
            export_line = "\"" + entry.mount_point + "\" " + NFS_EXPORTS_OPT + "fsid=" + entry.volume_number + "," + "async" + ")\n"
            exports_str += export_line

        exports_str += data_str
        debug("exports_str = " + exports_str)

        fd.seek(0)
        fd.truncate()
        fd.write(exports_str)
        fd.close()
        self.nfs_stop_service()
        self.nfs_start_service()

    def add_export(self, snap_name, snap_id, mount_dir, export_id):
        if 'iSCSI' in self.export_type:
            return self.iscsi_export(snap_name, snap_id, mount_dir, export_id)
        elif 'NFS' in self.export_type:
            return self.nfs_export(mount_dir, export_id)
        else:
            errormsg('invalid export type: %s' % self.export_type)

    def stop_export_service(self):
        debug("Entering stop_export_service() ")
        if 'iSCSI' in self.export_type:
            return self.iscsi_stop_service()
        elif 'NFS' in self.export_type:
            return self.nfs_stop_service()
        else:
            errormsg('invalid export type: %s' % self.export_type)

    def start_export_service(self):
        debug("entering start_export_service()")
        if 'iSCSI' in self.export_type:
            return self.iscsi_start_service()
        elif 'NFS' in self.export_type:
            return self.nfs_start_service()
        else:
            errormsg('invalid export type: %s' % self.export_type)

    def iscsi_start_service(self):
        rc = do_system("/etc/init.d/scst start")
        return rc

    def iscsi_stop_service(self):
        rc = do_system("/etc/init.d/scst stop")
        return rc

    def nfs_start_service(self):
        rc = do_system("/etc/init.d/nfs-kernel-server start")
        return rc

    def nfs_stop_service(self):
        for time_status in range(1, 3):
            rc = do_system("/etc/init.d/nfs-kernel-server stop")
            if rc == 0:
                rc = 1
                deadtime = time.time() + 180
                while time.time() < deadtime:
                    status = self.get_nfs_service_status()
                    if status in ['nfsd not running']:
                        rc = 0
                        break
                    else:
                        time.sleep(1)
                if rc == 0:
                    break
        else:
            rc = 1
        return rc

    def get_nfs_service_status(self):
        out = ['']
        do_system("/etc/init.d/nfs-kernel-server status", out)
        return out[0].replace('\n', '')

    def iscsi_export(self, snap_name, snap_id, mount_dir, export_id):
        debug("exporting iscsi...")

        ret = scsi.add_export_to_scst(snap_name, snap_id, mount_dir, export_id)
        if ret != 0:
            debug("error in adding iscsi export")
            return 1
        ret = scsi.notify_scst()
        if ret != 0:
            debug("error in notifying scst")
            return 1
        return 0

    def nfs_export(self, mount_dir, export_id):
        debug('exporting nfs...')
        export_dir = mount_dir
        sync_option = 'async'
        try:
            f = open("/etc/exports", "a+")
            f.write(
                "\"" + export_dir + "\" " + NFS_EXPORTS_OPT + "fsid=" + export_id + "," + sync_option + ")" + "\n")
            f.close()
        except:
            errormsg('write nfs exports error!')
            return 1
        do_system("exportfs -r")
        return 0

    def iscsi_unexport(self, snap_name, export_dir):
        ret = scsi.remove_export_from_scst(snap_name, export_dir)
        if ret != 0:
            debug("error in removing iscsi export")
            return 1
        ret = scsi.notify_scst(force=True)
        if ret != 0:
            debug("error in notifying scst")
            return 1
        return 0

    def nfs_unexport(self, export_dir):
        try:
            f = open("/etc/exports", "r+")
            d = f.readlines()
            f.seek(0)
            for i in d:
                if i.split(' ')[0] != '"' + export_dir + '"':
                    f.write(i)
            f.truncate()
            f.close()
        except:
            errormsg('update nfs exports error!')
            return 1
        rc = do_system("exportfs -r")
        return rc

    def kill_fsuser(self, export_path):
        out = ['']
        do_system('lsof -t -x +D ' + export_path, out)
        fsusers = out[0].split()
        for pid in fsusers:
            do_system('kill -9 ' + pid)
            time.sleep(2)
            if os.path.exists('/proc/' + pid):
                debug('Cannot kill ' + pid)
                return 1
            debug('Killed ' + pid)
        return 0

    def get_mntopts_from_resource(self):
        mnt_opts = self.resources['volumemountoption']
        if not mnt_opts:
            mnt_opts = DEFAULT_MOUNT_OPTIONS
        is_journaled = self.resources.get("directio")  # USX 2.0

        type_str = self.resources["volumetype"]
        if type_str.upper() in ["SIMPLE_MEMORY"]:
            is_inmem = True
        else:
            is_inmem = False

        if is_journaled:
            mnt_opts += ",journaled"
        if is_inmem:
            mnt_opts += ",inmem"
        return mnt_opts

    def used_space_threshold(self, used_space_percent):
        debug("Entering used_space_threshold")
        debug("vv-snapshot.py used_space_percent = %d" % used_space_percent)

        if used_space_percent > USED_SPACE_THRESHOLD:
            send_alert = False
            alert_type = 'WARN'
            curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # debug( "num_alerts = %d" %(num_alerts))
            alert_str = "Volume snapshot pool space usage has reached %d%%" % USED_SPACE_THRESHOLD
            if not os.path.exists(ALERT_TIMESTAMP_FILE):
                send_alert = True
                self.write_timestamp_to_file(curr_time)
            else:
                rc, last_alert_used_space_time = self.get_timestamp_from_file()
                if rc > 0:
                    return rc
                curr_time_date_time = datetime.datetime.strptime(curr_time, '%Y-%m-%d %H:%M:%S')
                last_alert_date_time = datetime.datetime.strptime(last_alert_used_space_time, '%Y-%m-%d %H:%M:%S')
                if int((curr_time_date_time - last_alert_date_time).total_seconds() / 60) > 20:
                    send_alert = True
                    self.write_timestamp_to_file(curr_time)

            if used_space_percent >= VOLUME_REBOOT_THRESHOLD:
                alert_type = 'FATAL'
                alert_str = "Volume snapshot pool space usage has reached %d%%, please remove some snapshot(s) to save space!" % VOLUME_REBOOT_THRESHOLD
                # alert_str = 'Out of data space mode, may need to reboot the volume!'
            if send_alert:
                send_volume_alert(alert_type, alert_str)
            if used_space_percent >= 90.00:
                # stop replicate function if used space more than 90%
                return 1
        return 0

    def write_timestamp_to_file(self, timestamp):
        debug("write_timestamp_to_file()")
        try:
            with open(ALERT_TIMESTAMP_FILE, 'w') as time_tmp:
                time_tmp.write(timestamp)
        except:
            errormsg('write timestamp file error!')
            errormsg(traceback.format_exc())
            return 1

    def get_timestamp_from_file(self):
        try:
            with open(ALERT_TIMESTAMP_FILE, 'r') as read_time:
                timestamp = read_time.read()
        except:
            errormsg('error on opening timestamp file!')
            errormsg(traceback.format_exc())
            return 1, []

        return 0, timestamp

    def amc_delete_snapshot(self, vol_id, snap_id):
        amcpost_str = '/usx/dataservice/volumeextension/snapshots/%s?api_key=%s' % (snap_id, vol_id)
        apiurl_str = USX_LOCAL_AGENT + amcpost_str  # actual API url

        timestamp = int(time.time() * 1000)
        # actual curl command to send a JSON formatted body
        cmd = r'curl -s -k -X DELETE -H "Content-Type:application/json" %s' % (apiurl_str)
        rc = do_system(cmd, log=True)
        if rc != 0:  # curl system call failed, return error
            return False
        return True

    def amc_create_snapshot(self, vol_id, snap_id, snap_name, scheduled_snapshot=False):
        amcpost_str = '/usx/dataservice/volumeextensions/snapshot?api_key=%s' % vol_id
        apiurl_str = USX_LOCAL_AGENT + amcpost_str  # actual API url

        timestamp = int(time.time() * 1000)
        json_str = '{\\"uuid\\":\\"%s\\",\\"snapshotname\\":\\"%s\\",\\"volumeresourceuuid\\":\\"%s\\",\\"ctime\\":%d,\\"mountedpoint\\":\\"\\",\\"description\\":\\"\\",\\"scheduled\\":\\"%s\\"}' % (
            snap_id, snap_name, vol_id, timestamp, scheduled_snapshot)
        # actual curl command to send a JSON formatted body
        cmd = r'curl -s -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, apiurl_str)
        rc = do_system(cmd, log=True)
        if rc != 0:  # curl system call failed, return error
            return False
        return True


class MountInfo():
    snapshot_name = ''
    snapshot_id = ''
    mount_point = ''
    device_id = ''


class TimeStamp:
    def __init__(self):
        tm_year = 0
        tm_mon = 0
        tm_mday = 0
        tm_hour = 0


class SnapshotLvs(SnapshotBase):
    def __init__(self):
        super(SnapshotLvs, self).__init__()
        self.dedupvg = 'dedupvg'
        self.deduplv = 'deduplv'
        self.lvpath = 'dedupvg/deduplv'
        self.rsync_running = '/tmp/rsync_running'
        self._check_snap_status()
        self.is_enable_snapshot = SnapshotBase._is_enabled_snap()
        self.lvs_snapshot_version = LvsSnapshotManager().get_version(milio_config.is_fastfailover,
                                                                     milio_settings.enable_new_ibdserver)

    def _create(self, snapshot_id):
        self.lvs_snapshot_version.create_snapshot_lv(snapshot_id)

    def bg_sync(self, snapshot_id):
        debug('No background sync for lvs snapshot!')

    def _delete(self, snap_id):
        try:
            self.lvs_snapshot_version.delete_snapshot_lv(snap_id)
        except Exception as e:
            errormsg('{error_msg}'.format(error_msg=e))
            if 'failed: Device or resource busy' in str(e):
                rc = 0
            else:
                rc = 1
        else:
            rc = 0
        return rc

    def snapshot2mntdir(self, snap_id):
        mnt_dir = None
        cmd_mount = 'mount'
        out = ['']
        rc = do_system(cmd_mount, out)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')
        snap_id_replace = snap_id.replace('-', '--')
        for snap in list_out:
            m = re.match('(\S+%s)\s\S+\s(\S+)\s' % snap_id_replace, snap)
            if m is not None:
                mnt_dir = m.group(2)
        return mnt_dir

    def list_snapshot_internal(self, list_hidden=False):
        return self.lvs_snapshot_version._list_snapshot_internal(list_hidden)

    def _list_snapshot(self):
        list_s = self.lvs_snapshot_version.snapshot_list()
        if list_s:
            return 0
        return 1

    def do_umount(self, export_path, snapshot_id=None):
        cmd_umount = 'umount %s' % export_path
        retry = 1
        while retry <= 10:
            rc = do_system(cmd_umount)
            if rc != 0:
                errormsg('umount busy: %s, retry: %d' % (export_path, retry))
                errormsg('snapshot id was %s' % snapshot_id)
                retry += 1
                cmd_umount = 'umount -l %s' % export_path
                time.sleep(2)
            else:
                break
        if rc != 0:
            errormsg('umount error: %s' % export_path)
        return rc

    def mount_snapshot_readonly(self, snap_id, mount_dir):
        out = ['']
        rc = 0
        try:
            os.makedirs(mount_dir)
        except OSError, e:
            if e.errno == errno.EEXIST and os.path.isdir(mount_dir):
                pass
            else:
                raise e
        try:
            self.lvs_snapshot_version.mount_snapshot(snap_id, mount_dir)
        except Exception as e:
            rc = 1
            raise e

        return rc

    def rollback_snapshot(self, snapshot_id, vol_id):
        rc = 0
        try:
            self.lvs_snapshot_version.rollback_snapshot_lvs(snapshot_id, vol_id)
        except Exception as e:
            rc = 1
            raise e
        return rc

    def reach_used_space_threshold(self):

        if not self.volume_uuid:
            return False
        usedspace, totalspace, used_space_percent, GBflag = self.lvs_snapshot_version.used_space_threshold_lvs()
        self.used_space_threshold(used_space_percent)
        if totalspace > 0 and GBflag and (
                            usedspace / totalspace > MIN_SPACE_RATIO or totalspace - usedspace < MIN_SNAPSHOT_SIZE):
            # disable auto delete snapshot function
            return False
        else:
            return False

    def list_snapshot_internal_sortbytime(self):
        snapshot_list = self.lvs_snapshot_version.snapshot_inter_bytime()
        return snapshot_list

    def _check_snap_status(self):
        try:
            # Try two times.
            for i in range(2):
                rc = do_system_timeout('lvs', 60)
                if rc == 0:
                    break
        except:
            raise SnapError('failed to check lvs, maybe the dedup is hang!')
        return True

    def check_process_for_golden_image(self, vol_id):
        # check golden images process
        process_cmd = 'ps -ax | grep "vv-snapshot.pyc create_golden_image -u %s" | grep -v grep | grep -v %s' % (
            vol_id, os.getpid())
        rc, msg = runcmd(process_cmd, print_ret=True)
        debug(rc)
        debug('%s' % msg)
        if rc == 0:
            raise SnapError(
                'Another process to create gold image for vol_id %s is already in progress, quitting..' % vol_id)

    def create_golden_image_snapshot(self, golden_snap_id, mnt_opts, mnt_point):
        # Cleanup any possible leftover merge operation by re-activate golden image.
        do_system('lvchange -an dedupvg/%s' % golden_snap_id)
        lvchange_active_sync('dedupvg/{}'.format(golden_snap_id))
        do_system('lvs -a dedupvg')
        while '[deduplv]' in self.list_snapshot_internal(True):
            debug('Waiting for merge to complete...')
            time.sleep(5)

        if self._is_enabled_snap():
            cmd_str = 'lvcreate -s -p r -n deduplv dedupvg/%s' % golden_snap_id
        else:
            cmd_str = 'lvcreate -s -l 100%%FREE -n deduplv dedupvg/%s' % golden_snap_id
        rc = do_system(cmd_str)
        if rc != 0:
            raise SnapError('create snapshot error!')
        do_system('lvchange -p rw %s/%s' % ('dedupvg', 'deduplv'))

        cmd_str = 'mount -t dedup -o %s /dev/dedupvg/deduplv %s' % (mnt_opts, mnt_point)
        rc = do_system(cmd_str)
        if rc != 0:
            raise SnapError('mount snapshot error!')

    def create_golden_image(self, args):
        return self._create_golden_image(args.vol_id)

    def _create_golden_image(self, vol_id):
        ret = 0
        self.check_process_for_golden_image(vol_id)
        golden_snap_id = 'dedup_golden_image_lv'
        need_repair_vg = False
        mnt_opt = self.get_mntopts_from_resource()
        snap_list = self.list_snapshot_internal()
        if golden_snap_id in snap_list:
            golden_image_exist = True
        else:
            golden_image_exist = False
        if not golden_image_exist and not self._is_enabled_snap():
            # Thick volume must have enough free space (1GB) to create golden image.
            vgname = 'dedupvg'
            out = ['']
            cmd_str = 'vgs -o vg_free_count --noheadings %s' % vgname
            rc = do_system(cmd_str, out)
            if rc != 0:
                raise SnapError('get vg free count failed!')
            free_extents = int(out[0].split(' ')[-1])
            cmd_str = 'vgs -o vg_extent_size --noheadings --units=k --nosuffix %s' % vgname
            rc = do_system(cmd_str, out)
            if rc != 0:
                raise SnapError('get vg extent size failed!')
            extent_size = int(float(out[0].split(' ')[-1]))

            free_space_k = free_extents * extent_size
            if free_space_k < 1024 * 1024:
                raise SnapError('Not enough free space for golden image!')
        try:
            ha_temp_disabled = False
            if milio_config.is_ha:
                debug('Put ha in maintenance mode to umount dedupfs.')
                ha_unmanage_resources()
                ha_temp_disabled = True

            rc = self.stop_export_service()
            if rc != 0:
                raise SnapError('stop export service error!')

            mnt_dir = self.snapshot2mntdir("deduplv")
            if mnt_dir is not None:
                rc = self.do_umount(mnt_dir)
                if rc != 0:
                    raise SnapError('umount error!')

            # Generate the command to create golden image.
            if self._is_enabled_snap():
                if golden_image_exist:
                    debug('Golden image already exist, delete old golden image.')
                    cmd_str = 'lvremove -f dedupvg/%s' % golden_snap_id
                    rc = do_system(cmd_str)
                    if rc != 0:
                        raise RuntimeError('Delete old golden image error!')

                cmd_str = 'lvrename dedupvg/deduplv %s' % golden_snap_id
            else:
                if golden_image_exist:
                    # deduplv must be umounted to perform the merge.
                    debug('Golden image already exist, merge deduplv back to it.')
                    cmd_str = 'lvconvert --merge dedupvg/deduplv'
                else:
                    cmd_str = 'lvrename dedupvg/deduplv %s' % (golden_snap_id)

            # Take action to create the golden image.
            rc = do_system(cmd_str)
            if rc != 0:
                need_repair_vg = True
                raise RuntimeError('rename/merge deduplv error!')
            self.create_golden_image_snapshot(golden_snap_id, mnt_opt, self.dedup_mount_point)
        except:
            errormsg('create golden image error:')
            errormsg(traceback.format_exc())
            ret = 1
        finally:
            if need_repair_vg:
                debug('ERROR: create golden image failed, trying to rescue the LV!')
                try:
                    self.create_golden_image_snapshot(golden_snap_id, mng_opt, self.dedup_mount_point)
                except:
                    pass
            self.start_export_service()
            if ha_temp_disabled:
                debug('Put ha back to enabled mode.')
                ha_manage_resources()
        return ret


class ParseIbdserver(object):
    def __init__(self, ibdserver_conf):
        self._uuid = None
        self._write_cache_uuid = None
        self._wcache_path = None
        self._drw_uuid = None
        self._exp_path = None
        self._read_cache_uuid = None
        self._read_path = None
        try:
            self.CF = ConfigParser.ConfigParser()
            self.CF.read(ibdserver_conf)
        except Exception as e:
            errormsg('{excp}'.format(excp=e))
            raise SnapError('get ibdserver configuration failed')
        self._load_config(milio_config.volume_uuid)

    def _load_config(self, channel_uuid):
        debug('%s' % self.CF.sections())
        if channel_uuid not in self.CF.sections():
            debug('')
            debug('channel uuid %s does not in config' % channel_uuid)
            raise SnapError('channel uuid does not exist!')
        self._uuid = channel_uuid
        self._write_cache_uuid = '%s-wcache' % self._uuid
        try:
            self._drw_uuid = self.CF.get(self._uuid, 'dev_name')
            self._exp_path = self.CF.get(self._drw_uuid, 'exportname')
            self._wcache_path = self.CF.get(self._write_cache_uuid, 'cache_device')
        except Exception as e:
            raise e

    @property
    def channel_uuid(self):
        return self._uuid

    @property
    def exp_path(self):
        return self._exp_path

    @property
    def write_cache(self):
        return self._wcache_path

    @property
    def read_cache(self):
        return self._read_path


class SnapshotZfs(SnapshotBase):
    def __init__(self):

        super(SnapshotZfs, self).__init__()
        self._check_snap_status()
        self.is_enable_snapshot = SnapshotBase._is_enabled_snap_from_json()

    def _create(self, snapshot_id):
        debug('SnapshotZfs create snapshot!')

        out = ['']
        # pre_snapshot = snapshot_id + '-pre'
        # src_buf_file = '/cachefs/usx-default-wc'
        # dst_buf_file = src_buf_file + '-' + pre_snapshot

        cmd_seq = [
            # fast clone
            # 'file.ilio.clone -s %s -d %s -F' % (src_buf_file, dst_buf_file),
            # create zfs snapshot
            '%s snapshot %s%s' % (self.zfs, self.snap_name, snapshot_id),
        ]

        for cmd in cmd_seq:
            rc = do_system(cmd, out)
            if rc != 0:
                errormsg('%s Error: %s' % (cmd, out[0]))
                raise SnapError('create snapshot failed with zfs!')

    def bg_sync(self, snapshot_id):
        debug('SnapshotZfs background sync!')

        out = ['']
        pre_snapshot = snapshot_id + '-pre'
        pre_clone = pre_snapshot + '-clone'
        src_buf_file = '/cachefs/usx-default-wc'
        dst_buf_file = src_buf_file + '-' + pre_snapshot
        ibdserver_conf = ParseIbdserver(IBDSERVER_CONF).CF
        io_bufdevicesz = ibdserver_conf.get('global', 'io_bufdevicesz')
        io_poolsz = ibdserver_conf.get('global', 'io_poolsz')
        io_pagesz = ibdserver_conf.get('global', 'io_pagesz')

        cmd_seq = [
            # clone and mount the snapshot
            "%s clone %s%s %s/%s" % (self.zfs, self.snap_name, pre_snapshot, self.zvol_name, pre_clone),
            # sync the write_cache to zfs snapshot.
            'ibdcacheflush %s %s %s %s /dev/zvol/%s/%s' % (
                dst_buf_file, io_bufdevicesz, io_poolsz, io_pagesz, self.zvol_name, pre_clone),
            # cleanup
            'rm -rf %s' % dst_buf_file,
            # create second zfs snapshot
            '%s snapshot %s%s' % (self.zfs, self.snap_name, snapshot_id),
        ]

        for cmd in cmd_seq:
            w = 1
            for i in range(7):
                rc = do_system(cmd, out)
                if rc == 0:
                    break;
                time.sleep(w)
                w += w
            if rc != 0:
                errormsg('%s Error: %s' % (cmd, out[0]))
                raise SnapError('create snapshot failed with zfs!')

    def _delete(self, uuid):
        out = ['']
        cmd_delete = '%s destroy -R %s%s' % (self.zfs, self.snap_name, uuid)
        rc = do_system(cmd_delete, out)
        if rc != 0:
            errormsg('delete snapshot error: %s' % out[0])
        return rc

    def list_snapshot_internal(self):
        mnt_dir = []
        out = ['']
        cmd_lsit = '%s list -t snapshot' % self.zfs
        rc = do_system(cmd_lsit, out, log=False)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')

        for snap in list_out:
            m = re.match('%s(\S+)\s' % self.snap_name, snap)
            if m is not None:
                mnt_dir.append(m.group(1))
        return mnt_dir

    def _list_snapshot(self):
        out = ['']
        cmd_lsit = '%s list -t snapshot' % self.zfs
        rc = do_system(cmd_lsit, out)
        debug('%s' % out[0])
        return rc

    def snapshot2mntdir(self, snap_id):
        mnt_dir = None
        cmd_mount = 'mount'
        out = ['']
        rc = do_system(cmd_mount, out)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')
        zvol_path = '/dev/zvol/usx-zpool/%s' % snap_id
        if not (os.path.islink(zvol_path) and os.access(zvol_path, os.W_OK)):
            return mnt_dir
        zd_path = os.readlink('/dev/zvol/usx-zpool/%s' % snap_id)
        m = re.match('.*/(\w+)$', zd_path)
        if m is None:
            return mnt_dir
        zd_path = m.group(1)
        for snap in list_out:
            m = re.match('(/dev/%s)\s\S+\s(\S+)\s' % zd_path, snap)
            if m is not None:
                mnt_dir = m.group(2)
        return mnt_dir

    def do_umount(self, export_path, snapshot_id):
        # zfs_name = self.mntdir2zfs(export_path)
        umount_cmd = 'umount %s' % export_path
        retry = 1
        while retry <= 10:
            rc = do_system(umount_cmd)
            if rc != 0:
                errormsg('umount busy: %s, retry: %d' % (export_path, retry))
                retry += 1
                time.sleep(2)
            else:
                break
        if rc != 0:
            errormsg('umount error: %s' % export_path)
        zfvol_name = '%s/%s ' % (self.zvol_name, snapshot_id)
        cmd_destroy_zfvol = '%s destroy %s' % (self.zfs, zfvol_name)
        rc, msg = runcmd(cmd_destroy_zfvol, print_ret=True)
        if rc != 0:
            errormsg('failed to destory zvol!')
            errormsg('%s' % msg)
        return rc

    def mntdir2zfs(self, export_path):
        mnt_dir = None
        cmd_mount = 'mount'
        out = ['']
        rc = do_system(cmd_mount, out)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')
        for snap in list_out:
            m = re.match('(\S+)\s\S+\s(%s)\s' % export_path, snap)
            if m is not None:
                mnt_dir = m.group(1)
        return mnt_dir

    def is_mounted(self, snapshot_uuid):
        out = self.zfs_get_status('mounted', snapshot_uuid)
        if 'yes' in out['vaule']:
            return True
        return False

    def mount_snapshot_readonly(self, snap_id, mount_dir):
        try:
            os.makedirs(mount_dir)
        except OSError, e:
            if e.errno == errno.EEXIST and os.path.isdir(mount_dir):
                pass
            else:
                raise e
        out = ['']
        snap_name = '%s%s' % (self.snap_name, snap_id)
        zvol_name = '%s/%s' % (self.zvol_name, snap_id)
        zfs_clone = '%s clone %s %s' % (self.zfs, snap_name, zvol_name)
        rc, msg = runcmd(zfs_clone, print_ret=True)
        if rc != 0:
            msg = msg.replace('\n', '')
            if not msg.endswith('exists'):
                return 1
            debug('the zvol name  %s was exists.' % zvol_name)

        mount_cmd = 'mount -t ext4 -o ro /dev/%s %s' % (zvol_name, mount_dir)
        rc, msg = runcmd(mount_cmd, print_ret=True)
        if rc != 0:
            os.rmdir(mount_dir)
        return rc

    def zfs_get_status(self, key, zfs_name):
        out = ['']
        out_dir = {'name': '', 'property': '', 'vaule': '', 'source': ''}
        cmd_get = '%s get %s %s' % (self.zfs, key, zfs_name)
        rc = do_system(cmd_get, out)
        if rc != 0:
            return out_dir
        out_list = out[0].split('\n')
        for val in out_list:
            m = re.match('(%s)\s+(\S+)\s+(\S+)\s+(\S+)' % zfs_name, val)
            if m is not None:
                out_dir['name'] = m.group(1)
                out_dir['property'] = m.group(2)
                out_dir['vaule'] = m.group(3)
                out_dir['source'] = m.group(4)
        return out_dir

    def is_mounted_path(self, mnt_path):
        mnt_dir = False
        cmd_mount = 'mount'
        out = ['']
        rc = do_system(cmd_mount, out)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')
        for snap in list_out:
            m = re.search('%s' % mnt_path, snap)
            if m is not None:
                mnt_dir = True
        return mnt_dir

    def get_export_ibd_dev(self, export_name=None):
        try:
            if not export_name:
                export_name = self.volume_uuid
            # (ret, ibd_dev) = IBDAgent().get_ibd_devname_by_uuid(export_name)
            # if ret != 0:
            #     debug('get ibd device by uuid %s failed.' % export_name)
            #     return None
            # debug('got the ibd device %s' % ibd_dev)
            # return ibd_dev
            ibd_dev = '/dev/usx-%s' % export_name
            with open('/tmp/%s.devlist' % export_name, 'w') as fd:
                fd.write(ibd_dev)
                fd.write('\n')
            debug('got the ibd device %s' % ibd_dev)
            return ibd_dev
        except Exception, e:
            debug('ERROR: got exception when export ibd device %s' % e)
            raise e

    def rollback_snapshot(self, snap_id, vol_id):
        debug('Enter rollback with ZFS')
        debug('stop ibdserver server on volume')
        # get mount
        # rollback_snap_id = args.snap_id + '_rb_tmp'
        # rollback_backup_id = self.vol_name + '_rollback_backup'
        # is_rollback_backup_removed = False
        # cmd_str = '%s rename'
        # self._create(rollback_snap_id)
        try:
            # self.ibd_version_snapshot.freeze()
            debug('freeze')
        except Exception as e:
            errormsg('%s' % e)
            raise e
        finally:
            try:
                # self.ibd_version_snapshot.unfreeze()
                debug('unfreeze')
            except Exception as e:
                raise e

        cmd_mount = 'mount|grep %s|grep -v grep' % self.dedup_mount_point
        rc, msg = runcmd(cmd_mount, print_ret=True)
        if len(msg) <= 0:
            errormsg('can\'t get the file system mount ponit')
            raise SnapError('failed to load mount point!')
        self.kill_fsuser(self.dedup_mount_point)
        cmd_umount = 'umount %s' % self.dedup_mount_point
        retry = 1
        while retry <= 10:
            rc = do_system(cmd_umount)
            if rc != 0:
                errormsg('umount busy: %s, retry: %d' % (self.dedup_mount_point, retry))
                retry += 1
                time.sleep(2)
            else:
                break
        if rc != 0:
            raise SnapError('umount error: %s' % self.dedup_mount_point)
        # get all snapshot list with set function

        ibd_stop = '/bin/ibdmanager -r s -S'
        do_system_timeout(ibd_stop)
        # and force stop about ibdserver.
        ibd_force_stop = 'killall -9 ibdserver'
        do_system(ibd_force_stop)

        rollback_backup_id = self.vol_name + '_rollback_backup'
        latest_list = set(self.list_snapshot_internal())
        cmd_roll = '%s rollback -r %s%s' % (self.zfs, self.snap_name, snap_id)
        rc = do_system(cmd_roll)
        if rc != 0:
            raise SnapError('zfs roll back failed')
        # if rollback_backup_id in self.list_snapshot_internal():
        #     self._delete(rollback_backup_id)
        #     is_rollback_backup_removed = True
        # self._rename(rollback_snap_id, rollback_backup_id)
        snapshot_list_after_rename = set(self.list_snapshot_internal())
        removed_snapshot_list = latest_list - snapshot_list_after_rename
        # self.ibd_version_snapshot.roll_back_apply_channel()
        deadtime = time.time() + 30
        while True:
            if self.volume_uuid in IBDManager.find_working_ibd():
                break
            if time.time() > deadtime:
                debug('ERROR: TIMEOUT, ibdserver is not working!')
                return 1
            time.sleep(0.75)
        ibd_dev = self.get_export_ibd_dev()
        if not ibd_dev:
            return 1

        for snapshot_id in removed_snapshot_list:
            self.amc_delete_snapshot(vol_id, snapshot_id)
        self.amc_create_snapshot(vol_id, snap_id, rollback_backup_id)
        fs_dev = FsManager().get_dev(milio_settings.export_fs_mode, ibd_dev)
        rc = fs_dev.start_with_mount(self.dedup_mount_point)
        if rc != 0:
            return rc
        fs_dev.setup_mnttable_for_ha()
        # # Start NFS/iSCSI.
        # UsxServiceManager.start_service(self.dedup_mount_point)
        return rc

    def _rename(self, snap_name, changed_name):
        cmd_rename = '%s rename %s%s %s%s' % (self.zfs, self.snap_name, snap_name, self.snap_name, changed_name)
        rc = do_system(cmd_rename)
        if rc != 0:
            raise SnapError('rename snapshot error!')

    def replicate_volume_base(self, args):
        debug('replicate not fully implemented yet.')
        is_snapshot_created = False
        cleanup_remote_snapshot = False
        is_mounted = False
        error_str = ""
        try:
            rc = do_system('python /opt/milio/atlas/system/sshw.pyc -i ' + args.target_volume_ip)
            if rc != 0:
                debug('Create Established trust relationships failed')
                error_msg = "Create Established trust relationships error"
                raise SnapError(error_msg)
            if args.snap_id not in self.list_snapshot_internal():
                debug('create internal snapshot %s for replication.' % args.snap_id)
                rc = self.create_base(args)
                if rc != 0:
                    raise SnapError('Can not create temporary snapshot %s for replicatio' % args.snap_id)
                is_snapshot_created = True
            vol_size = self.resources['volumesize']
            # Get the REFER value for the snapshot.
            out = ['']
            cmd_str = "%s list -p -t snapshot %s/%s@%s | grep -v NAME |  awk {'print $4'}" % (ZFS_CMD,
                                                                                              USX_ZPOOL,
                                                                                              USX_BLOCK_DEVICE,
                                                                                              args.snap_id)
            do_system(cmd_str, out)
            source_snapshot_refer_size = int(out[0].strip())
            rc = self.ssh_exec(args.target_volume_ip, 'python \
                      /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc \
                      type_check -n %s' % args.target_volume_name)
            if (rc == 1 and self.__class__.__name__ in ['SnapshotZfs']):
                debug('base and target are same snapshot type')
            else:
                raise SnapError('snapshot type not match')
            rc = self.ssh_exec(args.target_volume_ip, 'python \
                      /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc \
                      replicate_target -n %s -s %s -ts %d'
                               % (args.target_volume_name, args.snap_id, source_snapshot_refer_size))
            if rc != 0:
                if ERR_VOLUME_LAYOUT_WRONG == rc:
                    error_msg = "Target: Snapshot is not supported by this volume layout!"
                elif ERR_VOLUME_ID_MISMATCH == rc:
                    error_msg = "Target volume id mismatch"
                elif ERR_VOLUME_NAME_MISMATCH == rc:
                    error_msg = "Target volume name mismatch"
                elif rc == ERR_TARGET_VOLUME_TOO_SMALL:
                    error_msg = "Target volume too small"
                elif rc == ERR_INFRA_VOLUME:
                    error_msg = "Target volume can't be infrastructure volume"
                else:
                    error_msg = "Target volume error"
                raise SnapError(error_msg)
            for i in range(5):
                rc = self.remote_replicate_zfs(args.target_volume_ip, args.snap_id)
                if rc == 0:
                    break
            if rc != 0:
                raise SnapError('Replication to target volume failed!')
            ret = 0
        except Exception, err:
            errormsg('replication error:')
            errormsg(traceback.format_exc())
            error_str = str(err)
            ret = 1
        finally:
            debug('replication cleanup...')
            if is_snapshot_created:
                rc = self._delete(args.snap_id)
                if rc != 0:
                    errormsg('Cannot remove tmp snapshot %s!' % args.snap_id)
                    ret = rc
            if ret == 0:
                self.amc_create_replicate(args)
        if args.job_id:
            if ret == 0:
                self.send_update_status("REPLICATION", 0, "Replicate volume", "Successfully replicated volume",
                                        args.job_id, args.vol_id)
            else:
                if error_str != "":
                    self.send_update_status("REPLICATION", 1, "Replicate volume", error_str, args.job_id,
                                            args.vol_id)
                else:
                    self.send_update_status("REPLICATION", 1, "Replicate volume", "Failed to replicate volume",
                                            args.job_id, args.vol_id)

        return ret

    def replicate_volume_target_base(self, args):
        debug('Enter replicate target volume snapshot')
        debug('check the target volume mount status!')

        snap_id = args.snap_id + '_replicate'
        if self.vol_name != args.target_volume_name:
            return ERR_VOLUME_NAME_MISMATCH
        if is_infra_volume(milio_config.atltis_conf):
            return ERR_INFRA_VOLUME
        if args.target_volume_size:
            # Get current node zpool free space.
            out = ['']
            cmd_str = "%s list -p %s | grep -v NAME | awk {'print $3'}" % (ZFS_CMD, USX_ZPOOL)
            do_system(cmd_str, out)
            this_zpool_free_size = int(out[0].strip())
            if args.target_volume_size > this_zpool_free_size:
                errormsg(
                    'target volume too small, required:%d, actual:%d' % (args.target_volume_size, this_zpool_free_size))
                return ERR_TARGET_VOLUME_TOO_SMALL

        return 0

    def remote_replicate_zfs(self, target_ip, snap_id):
        cmd_str = 'zfs send %s%s | ssh %s zfs recv -F %s/%s' % (
            self.snap_name, snap_id, target_ip, USX_ZPOOL, snap_id)
        rc = do_system(cmd_str)
        return rc

    def list_snapshot_internal_sortbytime(self):
        return self.list_snapshot_internal()

    def reach_used_space_threshold(self):
        usedspace = 0.0
        totalspace = 0.0
        GBflag = True
        if not self.volume_uuid:
            return False
        cmd = '%s list -H -o avail,used -t filesystem' % self.zfs
        used_space_percent = 0
        (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        for line in msg:
            tmp = line.split()
            tmp_avail = tmp[0]
            tmp_used = tmp[1]
            if tmp_avail[-1:] == 'G':
                tmp_avail = float(tmp_avail[:-1])
            elif tmp_avail[-1:] == 'M':
                tmp_avail = float(tmp_avail[:-1]) / 1000
            elif tmp_avail[-1:] == 'T':
                tmp_avail = float(tmp_avail[:-1]) * 1000
            else:
                GBflag = False

            if tmp_used[-1:] == 'G':
                tmp_used = float(tmp_used[:-1])
            elif tmp_used[-1:] == 'M':
                tmp_used = float(tmp_used[:-1]) / 1000
            elif tmp_used[-1:] == 'T':
                tmp_used = float(tmp_used[:-1]) * 1000
            else:
                return False
            totalspace = tmp_avail + tmp_used

            used_space_percent = tmp_used
            usedspace = totalspace * used_space_percent / 100.0
        debug('dedupvgpool total_space %.2fGB, used_space: %.2fGB' % (totalspace, tmp_used))
        self.used_space_threshold(used_space_percent)
        if totalspace > 0 and GBflag and (
                            usedspace / totalspace > MIN_SPACE_RATIO or totalspace - usedspace < MIN_SNAPSHOT_SIZE):
            return True
        else:
            return False

    def _check_snap_status(self):
        try:
            do_system_timeout('%s list -t all' % self.zfs, 10, log=False)
        except:
            raise SnapError('failed to check zfs, maybe the dedup is hang!')
        return True


class SnapshotManager(object):
    def __init__(self):
        try:
            self.atltis = milio_config.atltis_conf
        except:
            errormsg('load json failed!')
            raise OSError
        self._snapashot_map = {
            'btrfs': 'SnapshotZfs',
            'ext4': 'SnapshotLvs',
            'zfs': 'SnapshotZfs',
            'dedup': 'SnapshotLvs'
        }

    def get_type_snap(self, type_s):
        if type_s in self._snapashot_map:
            return globals()[self._snapashot_map[type_s]]()


class RollbackSnapshotSimp(object):
    def __init__(self):
        self.vscaler_ssd_path = None
        self.vscaler_disk_path = None
        self.vscaler_ssd_uuid = None
        self.vscaler_disk_uuid = None
        self.cache_mode = None
        self._load_vscaler()

    def _load_vscaler(self):
        cmd_status = 'dmsetup table'
        out = os.popen(cmd_status, 'r', 1)
        for m in out.readlines():
            search_out = re.search('\s+ssd.+\((/dev/usx/(\S+))\)\S+\sdisk.+\((/dev/usx/(\S+))\)\scache.+\((\S+)\)', m)
            if search_out is not None:
                self.vscaler_ssd_path = search_out.group(1)
                self.vscaler_ssd_uuid = search_out.group(2)
                self.vscaler_disk_path = search_out.group(3)
                self.vscaler_disk_uuid = search_out.group(4)
                self.cache_mode = search_out.group(5)

    def destroy_vscaler(self, device_name):
        cmd_destroy = 'dmsetup remove -f %s' % device_name
        rc = do_system_timeout(cmd_destroy)
        return rc

    def stop_ibd_agent(self):
        cmd_stop_ibdagent = '/bin/ibdmanager -r a -S'
        rc = do_system_timeout(cmd_stop_ibdagent)
        return rc

    def stop_ibd_server(self):
        cmd_stop_ibdserver = '/bin/ibdmanager -r s -S'
        rc = do_system_timeout(cmd_stop_ibdserver)
        return rc

    def setup_vscaler(self, device_name):

        if "WRITEBACK" in self.cache_mode:
            options = "back"
        elif "WRITETHROUGH" in self.cache_mode:
            options = "thru"
        elif "WRITEAROUND" in self.cache_mode:
            options = "around"
        else:
            debug("WARNING: unsupported hybrid cache mode, fallback to thru")
            options = "thru"
        cmd_str = "yes | /opt/milio/scripts/vscaler_create -p %s %s %s %s" % (
            options, device_name, self.vscaler_ssd_path, self.vscaler_disk_path)
        rc = do_system(cmd_str)
        return rc

    def tune_vscaler(self, key, value):
        try:
            cachedev_str = self.vscaler_ssd_uuid + "+" + self.vscaler_disk_uuid
            sysctl_var = "dev.vscaler." + cachedev_str + "." + key
            cmd_str = "/sbin/sysctl " + sysctl_var + "=" + value
            rc = do_system(cmd_str)
        except:
            debug(traceback.format_exc())
            debug("WARNING: vscaler tunning failed! cmd:%s" % cmd_str)
            rc = 1

        if rc != 0:
            debug("WARNING: vscaler tunning failed with rc: %s" % str(rc))

        return rc

    def restart_ibd(self):
        cmd_ibdserver = '/bin/ibdserver'
        do_system(cmd_ibdserver)
        cmd_ibdagent = '/sbin/ibdagent'
        do_system(cmd_ibdagent)
        is_working = False
        while not is_working:
            cmd = '/bin/ibdmanager -r a -s get'
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
            debug("check the ibd working state: " + str(msg))
            for line in msg:
                if line.find("state:working") >= 0:
                    is_working = True
                    break
            time.sleep(3)
        return 0

    def list_snapshot_internal(self):
        out = ['']
        cmd_str = 'lvs --noheadings dedupvg -o lv_name'
        rc = do_system(cmd_str, out)
        if rc != 0:
            errormsg('get snapshot list error!')
            return []
        snapshot_list = out[0].split()
        return snapshot_list


class IBDSnapshotVersion(object):
    def __init__(self):
        self._version_map = {
            True: 'IBDSnasphotNew',
            False: 'IBDSnaspshotOld'
        }
        self._volume_type = milio_config.volume_type

    def get_version(self, version):
        if milio_config.is_fastfailover or (
                    is_new_simple_hybrid() and 'SIMPLE_HYBRID' in self._volume_type):
            if version in self._version_map:
                return globals()[self._version_map[version]](milio_settings.export_fs_mode)
            else:
                raise SnapError('IBD version does not exist ')
        return globals()['IBDSnasphotBase'](milio_settings.export_fs_mode)


class IBDSnasphotBase(object):
    def __init__(self, fs_mode):
        self._fs_mode = fs_mode
        self._volume_type = milio_config.volume_type
        self._volume_uuid = milio_config.volume_uuid
        if 'SIMPLE_HYBRID' in self._volume_type:
            self._load_exports_for_simple()

    def _load_exports_for_simple(self):
        if not os.path.isfile(SVM_EXPORTS):
            self.all_exports = {}
            raise SnapError('There is no %s file in the volume ' % SVM_EXPORTS)
        try:
            svm_exports_file = open(SVM_EXPORTS, 'r')
            data = svm_exports_file.read()
            svm_exports_file.close()
            self.all_exports = json.loads(data)
        except:
            debug(traceback.format_exc())

    def _statr_ibd_server(self):
        start_ibd = '/bin/ibdserver'
        rc, msg = runcmd(start_ibd, print_ret=True)
        if rc != 0:
            raise SnapError('start ibdserver failed')

    def freeze(self):
        debug('has no ibd server on volume!')

    def unfreeze(self):
        debug('has no ibd server on volume!')

    def roll_back_apply_channel(self):
        debug('has no ibd server on volume!')

    def flush_cache(self):
        debug('has no ibd server on volume!')


class IBDSnasphotNew(IBDSnasphotBase):
    def __init__(self, fs_mode):
        super(IBDSnasphotNew, self).__init__(fs_mode)
        self.snapconf_info = {}

    def _get_disk_uuid(self):
        for top_uuid in self.all_exports:
            for subdevices in self.all_exports[top_uuid]['subdevices']:
                if 'WHOLEDISK' in subdevices['storagetype']:
                    dev_uuid = subdevices['uuid']
                    self.snapconf_info = SrvSnapConfig(dev_uuid).snapconf
        if not self.snapconf_info:
            raise SnapError('formation dir failed!')

    def flush_cache(self):
        pass

    def freeze(self):
        try:
            if 'SIMPLE_HYBRID' in self.volume_type:
                self._get_disk_uuid()
            else:
                self.snapconf_info = SrvSnapConfig(self._volume_uuid).snapconf
            rc = IBDManager.freeze_snapshot(self.snapconf_info)
            if rc != 0:
                raise SnapError('try to freeze data failed!')
        except Exception as e:
            raise e

    def unfreeze(self):
        rc = IBDManager.unfreeze_snapshot(self.snapconf_info)
        if rc != 0:
            raise SnapError('try to unfreeze failed!')

    def roll_back_apply_channel(self):
        ibdserver_config = ParseIbdserver(IBDSERVER_TMP)
        ibdserver_config._load_config(self._volume_uuid)
        rc = apply_new_drw_channel(ibdserver_config.channel_uuid, ibdserver_config.exp_path,
                                   ibdserver_config.write_cache,
                                   ibdserver_config.read_cache)
        if rc != 0:
            raise SnapError('apply new channel failed!')


class IBDSnaspshotOld(IBDSnasphotBase):
    def __init__(self, fs_mode):
        super(IBDSnaspshotOld, self).__init__(fs_mode)
        self.ibd_flush_manger = IBDFlushCache().get_fs_mode(fs_mode)

    def _load_exports_ponit(self):
        self.fs_name = milio_config.volume_dedup_mount_point
        cmd_mount = 'mount|grep %s|grep -v grep' % self.fs_name
        rc, msg = runcmd(cmd_mount, print_ret=True)
        if len(msg) <= 0:
            errormsg('can\'t get the file system mount ponit')
            raise SnapError('failed to load mount point!')

    def freeze(self):

        try:
            self._load_exports_ponit()
            cmd_str = 'fsfreeze -f %s' % self.fs_name
            rc, msg = runcmd(cmd_str, print_ret=True)
            if rc != 0:
                if 'Device or resource busy' in msg:
                    debug('FS already freezed')
                else:
                    errormsg('ERROR: Failed to freeze %s' % self.fs_name)
                    errormsg('%s' % msg)
                    raise SnapError('%s' % msg)
            self.ibd_flush_manger.flush_cache_data()
        except Exception as e:
            raise e

    def flush_cache(self):
        self.ibd_flush_manger.flush_cache_data()

    def unfreeze(self):
        self.ibd_flush_manger.stop_flush_cache()
        cmd_unfreeze = 'fsfreeze -u %s' % self.fs_name
        rc, msg = runcmd(cmd_unfreeze, print_ret=True)
        if rc != 0:
            if 'Invalid argument' in msg:
                debug('fs already unfreezed')
            elif 'no filename specified' in msg:
                debug('fs not mounted. Skipping...')
            else:
                raise SnapError('try to unfreeze failed!')

    def roll_back_apply_channel(self):
        self._statr_ibd_server()


class SnapError(Exception):
    def __init__(self, desc):
        self.desc = desc

    def __str__(self):
        return repr(self.desc)


class IBDFlushCache(object):
    def __init__(self):
        self._map_fs_mode = {
            'dedup': 'DedupFlushCache'
        }

    def get_fs_mode(self, fs_mode):
        if fs_mode in self._map_fs_mode:
            return globals()[self._map_fs_mode[fs_mode]]()
        return globals()['ZfsFlushCache']()


class FlushCacheBase(object):
    def flush_cache_data(self):
        try:
            self._flush()
        except Exception as e:
            raise e

    def stop_flush_cache(self):
        try:
            self._stop()
        except Exception as e:
            raise e

    def _flush(self):
        raise SnapError('Flush cache failed!')

    def _stop(self):
        raise SnapError('Stop flush cache failed!')


class DedupFlushCache(FlushCacheBase):
    def _flush(self):
        try:
            self.flush_dedup_cache()
            self.stop_flush_dedup_cache()
        except Exception as e:
            raise e
        debug('flush cache data successfully! ')

    def flush_dedup_cache(self):
        debug('start flush cache data on ibdserver!')
        rc, msg = runcmd('/bin/ibdmanager -r s -b ff', print_ret=True)
        if rc != 0:
            raise SnapError('start flush data failed with %s' % msg)

    def stop_flush_dedup_cache(self):
        need_flush = True
        while need_flush:
            cmd_status = '/bin/ibdmanager -r s -s get'
            rc, msg = runcmd(cmd_status, print_ret=True, lines=True)
            if rc != 0:
                raise SnapError('get ibdserver status failed with %s' % msg)
            for detail_msg in msg:
                m = re.search('\s*block_occupied:(\d*)', detail_msg)
                if m is not None:
                    if int(m.group(1)) == 0:
                        need_flush = False
                        break
            else:
                time.sleep(2)
        cmd_stop_flush = 'ibdmanager -r s -b stop_ff'
        rc, msg = runcmd(cmd_stop_flush, print_ret=True)
        if rc != 0:
            errormsg('flush failed %s' % msg)
            raise SnapError('stop flush cache data failed!')

    def _stop(self):
        debug('Dedup do not need unfreeze it with ibd commands')


class ZfsFlushCache(FlushCacheBase):
    def _flush(self):
        # rc = IBDManager.write_freeze()
        # if rc != 0:
        #     raise SnapError('freeze ibdserver failed!')
        debug('freeze ibdserver successfully! ')

    def _stop(self):
        # rc = IBDManager.write_unfreeze()
        # if rc != 0:
        #     raise SnapError('unfreeze ibdserver failed!')
        debug('unfreeze ibdserver successfully! ')


class IBDFlushWriteCache(object):
    def __init__(self):
        self.dedup_export_name = None
        self.dedup_export_name = milio_config.volume_dedup_mount_point

    def _check_mount_status(self):
        cmd_mount = 'mount|grep {device_export}|grep -v grep'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_mount, print_ret=True)
        if len(msg) <= 0:
            errormsg('can\'t get the file system mount point')
            raise SnapError('failed to load mount point!')

    def _freeze_file_system(self):
        raise SnapError('Invalid freeze file system!')

    def _unfreeze_file_systme(self):
        raise SnapError('Invalid unfreeze file system!')

    def _freeze(self):
        raise SnapError('Invalid freeze function')

    def _unfreeze(self):
        raise SnapError('Invalid unfreeze function')

    def _flush(self, device_path, wc_path):
        raise SnapError('Invalid unfreeze function')

    def stop_ibd_server(self):
        raise SnapError('Invalid stop ibd server')

    def reset_vscaler(self):
        raise SnapError('Invalid reset vscaler function')


class IBDOldVersion(IBDFlushWriteCache):
    def __init__(self):
        super(IBDOldVersion, self).__init__()
        self.volume_uuid = milio_config.volume_uuid

    def _freeze_file_system(self):
        pass

    def _unfreeze_file_systme(self):
        pass

    def _freeze(self):
        pass

    def _unfreeze(self):
        pass

    def _flush(self, device_path, wc_path):
        pass

    def stop_ibd_server(self):
        pass

    def reset_vscaler(self):
        pass


class IBDOldVersionHybrid(IBDOldVersion):
    def __init__(self):
        super(IBDOldVersionHybrid, self).__init__()

    def _freeze(self):
        rc = IBDManager.write_freeze()
        if rc != 0:
            raise SnapError('freeze ibdserver failed!')
        debug('freeze ibdserver successfully! ')

    def _unfreeze(self):
        rc = IBDManager.write_unfreeze()
        if rc != 0:
            raise SnapError('unfreeze ibdserver failed!')
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
            raise SnapError('flush cache failed!')
        debug('successfully ibdserver flush cache with \'wc_devcie = {wc_name} export_device = {export_name}\''.format(
            wc_name=wc_path, export_name=device_path))

    def stop_ibd_server(self):
        cmd_stop_ibdserver = '/bin/ibdmanager -r s -S'
        rc = do_system_timeout(cmd_stop_ibdserver)
        if rc != 0:
            raise SnapError('stop ibdserver failed!')
        debug('stop ibdserver successfully!')

    def reset_vscaler(self):
        cmd_ibdserver = '/bin/ibdserver'
        do_system(cmd_ibdserver)
        is_working = False
        while not is_working:
            cmd = '/bin/ibdmanager -r a -s get_wud'
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
            debug("check the ibd working state: " + str(msg))
            for line in msg:
                if line.find(self.volume_uuid) >= 0:
                    is_working = True
                    break
            time.sleep(3)
        return 0


class IBDOldVersionSimple(IBDOldVersion):
    def __init__(self):
        super(IBDOldVersionSimple, self).__init__()
        self.sim_roll = RollbackSnapshotSimp()
        self.all_exports = {}
        self._load_exports()
        self.export_device = self._get_simple_export_uuid()

    def _load_exports(self):
        if not os.path.isfile(SVM_EXPORTS):
            raise SnapError('load svm config failed!')
        try:
            svm_exports_file = open(SVM_EXPORTS, 'r')
            data = svm_exports_file.read()
            svm_exports_file.close()
            self.all_exports = json.loads(data)
        except:
            debug(traceback.format_exc())

    def _get_simple_export_uuid(self):
        for uuid in self.all_exports:
            vscaler_uuid = uuid
        return vscaler_uuid

    def _freeze(self):
        cmd_str = 'fsfreeze -f {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_str, print_ret=True)
        if rc != 0:
            if 'Device or resource busy' in msg:
                debug('FS already freezed')
            else:
                errormsg('ERROR: Failed to freeze {device_export}'.format(device_export=self.dedup_export_name))
                raise SnapError('{msg}'.format(msg=msg))
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
                raise SnapError('try to unfreeze failed!')
        debug('unfreeze file system successfully!')

    def _get_seq_assigend_size(self):
        assigned_cmd = '/bin/ibdmanager -r s -s get | grep seq_assigned | cut -d ":" -f2'
        rc, msg = runcmd(assigned_cmd, print_ret=True, lines=True)
        if rc != 0:
            raise SnapError('get ibdserver seq_assigned size failed!')
        return msg[0]

    def _get_seq_flushed_size(self):
        flushed_cmd = 'ibdmanager -r s -s get | grep seq_flushed | cut -d ":" -f2'
        rc, msg = runcmd(flushed_cmd, print_ret=True, lines=True)
        if rc != 0:
            raise SnapError('get ibdserver seq_flushed size failed!')
        return msg[0]

    def _flush(self, device_path, wc_path):
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
            raise SnapError('stop flush cache data failed!')

    def stop_ibd_server(self):
        rc = self.sim_roll.stop_ibd_server()
        if rc != 0:
            raise SnapError('stop ibdserver failed!')
        debug('stop ibdserver successfully!')
        debug('destroy vscaler with old ibd!')
        self.sim_roll.destroy_vscaler(self.export_device)
        self.sim_roll.stop_ibd_agent()

    def reset_vscaler(self):
        self.sim_roll.restart_ibd()
        rc = self.sim_roll.setup_vscaler(self.export_device)
        if rc == 0:
            self.sim_roll.tune_vscaler("dirty_thresh_pct", "80")
            self.sim_roll.tune_vscaler("max_clean_ios_set", "2")
            self.sim_roll.tune_vscaler("max_clean_ios_total", "4")
            self.sim_roll.tune_vscaler("reclaim_policy", "1")
        else:
            raise OSError('set vscaler failed')


class IBDNewVersion(IBDFlushWriteCache):
    def __init__(self):
        super(IBDNewVersion, self).__init__()

    def _freeze_file_system(self):
        pass

    def _freeze(self):
        pass

    def _unfreeze(self):
        pass

    def _flush(self, device_path, wc_path):
        pass

    def stop_ibd_server(self):
        pass

    def reset_vscaler(self):
        pass

    def _unfreeze_file_systme(self):
        pass


class IBDNewVersionHybrid(IBDOldVersionHybrid):
    def __init__(self):
        super(IBDNewVersionHybrid, self).__init__()
        self.snapshot_channel_info = SrvSnapConfig(milio_config.ibdserver_resources_uuid).snap_config

    def _freeze_file_system(self):
        cmd_str = 'fsfreeze -f {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_str, print_ret=True)
        if rc != 0:
            if 'Device or resource busy' in msg:
                debug('FS already freezed')
            else:
                errormsg('ERROR: Failed to freeze {device_export}'.format(device_export=self.dedup_export_name))
                raise SnapError('{msg}'.format(msg=msg))
        debug('fsfreeze file system successfully!')

    def _unfreeze_file_systme(self):
        cmd_unfreeze = 'fsfreeze -u {device_export}'.format(device_export=self.dedup_export_name)
        rc, msg = runcmd(cmd_unfreeze, print_ret=True)
        if rc != 0:
            if 'Invalid argument' in msg:
                debug('fs already unfreezed')
            elif 'no filename specified' in msg:
                debug('fs not mounted. Skipping...')
            else:
                raise SnapError('try to unfreeze failed!')
        debug('unfreeze file system successfully!')

    def _freeze(self):
        rc = IBDManager.freeze_snapshot(self.snapshot_channel_info)
        if rc != 0:
            raise SnapError('freeze ibdserver failed!')
        debug('freeze ibdserver successfully! ')

    def _unfreeze(self):
        rc = IBDManager.unfreeze_snapshot(self.snapshot_channel_info)
        if rc != 0:
            raise SnapError('unfreeze ibdserver failed!')
        debug('unfreeze ibdserver successfully! ')

    def stop_ibd_server(self):
        cmd_stop_ibdserver = '/bin/ibdmanager -r s -S'
        rc = do_system_timeout(cmd_stop_ibdserver)
        if rc != 0:
            raise SnapError('stop ibdserver failed!')
        debug('stop ibdserver successfully!')

    def _flush(self, device_path, wc_path):
        rc = IBDManager.flush_snapshot(self.snapshot_channel_info)
        if rc != 0:
            raise SnapError('flush ibdserver failed!')
        debug('flush ibdserver successfully! ')

    def reset_vscaler(self):
        channel_info = ParseIbdserver(IBDSERVER_TMP)
        rc = apply_new_drw_channel(channel_info.channel_uuid, channel_info.exp_path, channel_info.write_cache,
                                   channel_info.read_cache)
        if rc != 0:
            raise SnapError('New ibdserver apply channel failed!')


class IBDNewVersionSimple(IBDNewVersionHybrid):
    def __init__(self):
        super(IBDNewVersionSimple, self).__init__()
        self.sim_roll = RollbackSnapshotSimp()
        self.all_exports = {}
        self._load_exports()
        self.export_device = self._get_simple_export_uuid()

    def _load_exports(self):
        if not os.path.isfile(SVM_EXPORTS):
            raise SnapError('load svm config failed!')
        try:
            svm_exports_file = open(SVM_EXPORTS, 'r')
            data = svm_exports_file.read()
            svm_exports_file.close()
            self.all_exports = json.loads(data)
        except:
            debug(traceback.format_exc())

    def _get_simple_export_uuid(self):
        for uuid in self.all_exports:
            vscaler_uuid = uuid
        return vscaler_uuid

    def stop_ibd_server(self):
        rc = self.sim_roll.stop_ibd_server()
        if rc != 0:
            raise SnapError('stop ibdserver failed!')
        debug('stop ibdserver successfully!')
        debug('destroy vscaler with old ibd!')
        self.sim_roll.destroy_vscaler(self.export_device)
        self.sim_roll.stop_ibd_agent()

    def reset_vscaler(self):
        self.sim_roll.restart_ibd()
        rc = self.sim_roll.setup_vscaler(self.export_device)
        if rc == 0:
            self.sim_roll.tune_vscaler("dirty_thresh_pct", "80")
            self.sim_roll.tune_vscaler("max_clean_ios_set", "2")
            self.sim_roll.tune_vscaler("max_clean_ios_total", "4")
            self.sim_roll.tune_vscaler("reclaim_policy", "1")
        else:
            raise OSError('set vscaler failed')


class LvsSnapshotBaseSetup(object):
    def __init__(self, is_new_ibd):
        self._map_ibd = {}
        self.list_snapshot = []
        self.is_new_ibd_version = is_new_ibd
        self.volume_uuid = milio_config.volume_uuid
        self.export_device = 'deduplv'
        self.vol_name = milio_config.volume_server_name
        self.ibd_version = None
        self.back_up_removed = False

    def create_snapshot_lv(self, snapshot_id, is_need_freeze_file_system=True):
        raise SnapError('create snapshot failed!')

    def _ibd_manager(self):
        if self.is_new_ibd_version in self._map_ibd:
            return globals()[self._map_ibd[self.is_new_ibd_version]]()
        else:
            raise SnapError('get ibd version mananger failed')

    def _configuration_snapshot_lv_list(self):
        dedup_dir = {'dedupvg': 'deduplv'}
        self.list_snapshot.append(dedup_dir)

    def _lvs_create(self, snapshot_id, lvname, vgname):
        out = ['']

        cmd_str = 'lvcreate -s -p r -n %s %s/%s' % (snapshot_id, vgname, lvname)
        rc = do_system(cmd_str)
        if rc != 0:
            errormsg('create snapshot error:%s' % out[0])
            raise SnapError('create snapshot failed with lvs!')

        cmd_lvchange = 'lvchange -p rw %s/%s' % (vgname, snapshot_id)
        rc = do_system(cmd_lvchange)
        if rc != 0:
            raise SnapError('lvchange failed !')

    def _lvs_delete(self, snapshot_id, vgname):
        out = ['']
        cmd_activate = 'lvchange -an %s/%s' % (vgname, snapshot_id)
        do_system(cmd_activate)
        cmd_remove = 'lvremove -f %s/%s' % (vgname, snapshot_id)
        rc = do_system(cmd_remove, out)
        if rc != 0:
            raise SnapError('delete snapshot error: %s!' % out[0])

    def _get_mount_dir(self, snapshot_id):
        mnt_dir = None
        cmd_mount = 'mount'
        out = ['']
        rc = do_system(cmd_mount, out)
        if rc != 0:
            return mnt_dir
        list_out = out[0].split('\n')
        snap_id_replace = snapshot_id.replace('-', '--')
        for snap in list_out:
            m = re.match('(\S*%s)\s\S+\s(\S+)\s' % snap_id_replace, snap)
            if m is not None:
                mnt_dir = m.group(2)
        return mnt_dir

    def delete_snapshot_lv(self, snapshot_id):
        for snapshot_level in self.list_snapshot:
            for vg in snapshot_level:
                if 'ibd-wc' in vg:
                    continue
                self._lvs_delete(snapshot_id, vg)

    def mount_snapshot(self, snapshot_id, mount_dir):
        for snapshot_level in self.list_snapshot:
            for vg, lv in snapshot_level.items():
                if vg in ['ibd-wc-vg']:
                    continue
                out = ['']
                mount_type = 'ext3' if milio_settings.export_fs_mode.lower() == 'dedup' else milio_settings.export_fs_mode.lower()
                cmd_mount = 'mount -t {} -o ro /dev/{}/{} {}'.format(mount_type, vg, snapshot_id, mount_dir)
                rc = do_system(cmd_mount, out)
                if rc != 0:
                    errormsg('mount dir %s error: %s' % (mount_dir, out[0]))
                    os.rmdir(mount_dir)
                    raise SnapError('mount snapshout failed!')

    def kill_fsuser(self, export_path):
        out = ['']
        do_system('lsof -t -x +D ' + export_path, out)
        fsusers = out[0].split()
        for pid in fsusers:
            do_system('kill -9 ' + pid)
            time.sleep(2)
            if os.path.exists('/proc/' + pid):
                debug('Cannot kill ' + pid)
                return 1
            debug('Killed ' + pid)
        return 0

    def snapshot_list(self):
        snapshot_list = []
        for snapshot_level in self.list_snapshot:
            for vg in snapshot_level:
                out = ['']
                cmd_str = 'lvs --noheadings %s -o +lv_time 2>/dev/null' % vg
                rc = do_system(cmd_str, out, log=False)
                if rc != 0:
                    errormsg('get snapshot list error!')
                    return []
                snapshot_list += out[0].split()
        return snapshot_list

    def do_umount(self, export_path):
        cmd_umount = 'umount %s' % export_path
        retry = 1
        while retry <= 10:
            rc = do_system(cmd_umount)
            if rc != 0:
                errormsg('umount busy: %s, retry: %d' % (export_path, retry))
                retry += 1
                cmd_umount = 'umount -l %s' % export_path
                time.sleep(2)
            else:
                break
        if rc != 0:
            errormsg('umount error: %s' % export_path)
        return rc

    def _list_snapshot_internal(self, list_hidden=False):
        snapshot_list = []
        for snapshot_level in self.list_snapshot:
            for vg in snapshot_level:
                cmd_str = 'lvs --noheadings %s -o lv_name 2> /dev/null' % vg
                if list_hidden:
                    cmd_str += ' --all'
                rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
                if rc != 0:
                    errormsg('get snapshot list error!')
                    return []
                snapshot_list += [line.replace(' ', '') for line in msg if line != '']
        return snapshot_list

    def amc_delete_snapshot(self, vol_id, snap_id):
        amcpost_str = '/usx/dataservice/volumeextension/snapshots/%s?api_key=%s' % (snap_id, vol_id)
        apiurl_str = USX_LOCAL_AGENT + amcpost_str  # actual API url

        timestamp = int(time.time() * 1000)
        # actual curl command to send a JSON formatted body
        cmd = r'curl -s -k -X DELETE -H "Content-Type:application/json" %s' % (apiurl_str)
        rc = do_system(cmd, log=True)
        if rc != 0:  # curl system call failed, return error
            return False
        return True

    def amc_create_snapshot(self, vol_id, snap_id, snap_name):
        amcpost_str = '/usx/dataservice/volumeextensions/snapshot?api_key=%s' % vol_id
        apiurl_str = USX_LOCAL_AGENT + amcpost_str  # actual API url

        timestamp = int(time.time() * 1000)
        json_str = '{\\"uuid\\":\\"%s\\",\\"snapshotname\\":\\"%s\\",\\"volumeresourceuuid\\":\\"%s\\",\\"ctime\\":%d,\\"mountedpoint\\":\\"\\",\\"description\\":\\"\\"}' % (
            snap_id, snap_name, vol_id, timestamp)
        # actual curl command to send a JSON formatted body
        cmd = r'curl -s -k -X POST -H "Content-Type:application/json" -d "%s" %s' % (json_str, apiurl_str)
        rc = do_system(cmd, log=True)
        if rc != 0:  # curl system call failed, return error
            return False
        return True

    def zero_wc_device_cache(self):
        pass

    def _list_snapshot_for_rollback(self, vgname):
        cmd_str = 'lvs --noheadings %s -o lv_name 2> /dev/null' % vgname
        rc, msg = runcmd(cmd_str, print_ret=True, lines=True)
        if rc != 0:
            errormsg('get snapshot list error!')
            return []
        snapshot_list = [line.replace(' ', '') for line in msg if line != '']
        return snapshot_list

    def lv_rename_for_rollback(self, snapshot_id, rollback_sap_id, rollback_backup_id, vgname, lvname):
        # delte snapshot of base lvs
        self._lvs_delete(lvname, vgname)
        cmd_back_up_to_base = 'lvrename %s/%s %s' % (vgname, snapshot_id, lvname)
        rc = do_system(cmd_back_up_to_base)
        if rc != 0:
            raise SnapError('rename %s error!' % lvname)
        if rollback_backup_id in self._list_snapshot_for_rollback(vgname):
            self.delete_snapshot_lv(rollback_backup_id)
            self.back_up_removed = True
        cmd_tmp_backup = 'lvrename {vgname}/{snaphsot_tmp} {snapshot_backup}'.format(vgname=vgname,
                                                                                     snaphsot_tmp=rollback_sap_id,
                                                                                     snapshot_backup=rollback_backup_id)
        rc = do_system(cmd_tmp_backup)
        if rc != 0:
            raise SnapError('rename {snapshot_tmp} error!'.format(snapshot_tmp=rollback_sap_id))

    def get_mntopts_from_resource(self):
        mnt_opts = self.resources['volumemountoption']
        if not mnt_opts:
            mnt_opts = DEFAULT_MOUNT_OPTIONS
        is_journaled = self.resources.get("directio")  # USX 2.0

        type_str = self.resources["volumetype"]
        if type_str.upper() in ["SIMPLE_MEMORY"]:
            is_inmem = True
        else:
            is_inmem = False

        if is_journaled:
            mnt_opts += ",journaled"
        if is_inmem:
            mnt_opts += ",inmem"
        return mnt_opts

    def rollback_snapshot_lvs(self, snapshot_id, vol_id):
        self.resources = milio_config.volume_resources
        mnt_dir = self._get_mount_dir(self.export_device)
        if mnt_dir is not None:
            self.kill_fsuser(mnt_dir)
            rc = self.do_umount(mnt_dir)
            if rc != 0:
                raise SnapError('umount error!')
        rollback_snap_id = snapshot_id + '_roll_tmp'
        rollback_backup_id = self.vol_name + '_rollback_backup'
        latest_list = set(self._list_snapshot_internal())
        self.create_snapshot_lv(rollback_snap_id, False)
        self.ibd_version.stop_ibd_server()
        self.zero_wc_device_cache()
        for snapshot_level in self.list_snapshot:
            for vg, lv in snapshot_level.items():
                if 'ibd-wc' in vg:
                    continue
                self.lv_rename_for_rollback(snapshot_id, rollback_snap_id, rollback_backup_id, vg, lv)
        self.ibd_version.reset_vscaler()
        snapshot_list_after_rename = set(self._list_snapshot_internal())
        removed_snapshot_list = latest_list - snapshot_list_after_rename
        for snap_id in removed_snapshot_list:
            self.amc_delete_snapshot(vol_id, snap_id)
        if self.back_up_removed:
            self.amc_delete_snapshot(vol_id, rollback_backup_id)
        self.amc_create_snapshot(vol_id, rollback_backup_id, rollback_backup_id)
        mnt_opt = self.get_mntopts_from_resource()
        self._mount_device(mnt_dir, mnt_opt)
        debug('rollback snapshot successflly!')

    def used_space_threshold_lvs(self):
        GBflag = True
        usedspace = 0.0
        totalspace = 0.0
        used_space_percent = 0
        for snapshot_level in self.list_snapshot:
            for vg in snapshot_level:
                if vg in ['ibd-wc-vg']:
                    continue
                cmd = 'lvs --noheadings %s -o +lv_time 2>/dev/null' % vg
                (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                for line in msg:
                    tmp = line.split()
                    if len(tmp) > 3 and (tmp[0] == '%spool' % vg):
                        tmp_str = tmp[3]
                        tmp_space = float(tmp_str[:-1])
                        if tmp_str[-1:] == 'g':
                            tmp_space = tmp_space
                        elif tmp_str[-1:] == 'm':
                            tmp_space /= 1000
                        elif tmp_str[-1:] == 't':
                            tmp_space *= 1000
                        else:
                            GBflag = False
                        used_space_percent = float(tmp[4])
                        totalspace = tmp_space
                        usedspace = totalspace * used_space_percent / 100.0
                        debug('dedupvgpool total_space %.2fGB, used_space: %.2fGB' % (totalspace, usedspace))
                        break
        return usedspace, totalspace, used_space_percent, GBflag

    def snapshot_inter_bytime(self):
        debug('enter snapshot bytime')
        snapshot_list = []
        for snapshot_level in self.list_snapshot:
            for vg in snapshot_level:
                snapshot_dir = {}
                out = ['']
                if 'ibd-wc' in vg:
                    continue
                # Ignore WARNING output.
                cmd_str = 'lvs --noheadings %s -o lv_name,lv_time 2>/dev/null' % vg
                rc = do_system(cmd_str, out, log=False)
                if rc != 0:
                    errormsg('get snapshot list error!')
                    return []
                snapshot_name_time = out[0].split('\n')
                for lv in snapshot_name_time:
                    m = re.search('\s*(\S*)\s+(\S*)\s*(\S*)\s*', lv)
                    if m is not None:
                        snapshot_dir.setdefault(m.group(1),
                                                '{y_m_d} {h_m_s}'.format(y_m_d=m.group(2), h_m_s=m.group(3)))
                if not snapshot_dir:
                    return []
                new_dir = sorted(snapshot_dir.items(), key=lambda x: x[1], reverse=False)
                for snapshot_name in new_dir:
                    snapshot_list.append(snapshot_name[0])
        return snapshot_list


class FastFailoverSnapshot(LvsSnapshotBaseSetup):
    def __init__(self, is_new_ibd):
        super(FastFailoverSnapshot, self).__init__(is_new_ibd)
        self._map_ibd = {
            True: 'IBDNewVersionHybrid',
            False: 'IBDOldVersionHybrid'
        }
        self._configuration_snapshot_lv_list()
        self.export_device_snapshot = None
        self.wc_device_snapshot = None
        self.ibd_version = self._ibd_manager()
        self.export_device = self.get_export_ibd_dev()

    def _mount_device(self, mount_dir, mount_opt):
        # Ext3 may clear the needs_recovery bit during export operation, set it back.
        rc = do_system("echo feature needs_recovery | /opt/milio/sbin/debugfs -w /dev/usx-%s" % self.volume_uuid)
        if rc != 0:
            raise SnapError('mark export device needs_recovery error!')
        cmd_mount = 'mount -t dedup -o %s /dev/usx-%s %s' % (mount_opt, self.volume_uuid, mount_dir)
        rc = do_system(cmd_mount)
        if rc != 0:
            raise SnapError('mount export device  error!')
        debug('mount export device successfully!')

    def get_export_ibd_dev(self, export_name=None):
        try:
            if export_name is None:
                export_name = self.volume_uuid
            ibd_dev = '/dev/usx-{export}'.format(export=export_name)
            ibd_dev_name = os.readlink(ibd_dev)
            debug('got the ibd device %s' % ibd_dev_name)
            return ibd_dev_name
        except Exception, e:
            debug('ERROR: got exception when export ibd device %s' % e)
            raise e

    def zero_wc_device_cache(self):
        wc_dev = '/dev/ibd-wc-vg/ibd-wc-lv'
        debug('WARNING: start to zero the cache device header...')
        if wc_dev is None:
            debug('Cannot find write cache device.')
            return 1
        out = ['']
        cmd_str = 'blockdev --getsz %s' % wc_dev
        do_system(cmd_str, out)
        # dd zero on wc header and tail
        # header:size_in_byte/4096*32 + 10M
        # tail: last 40M.
        dd_tail_size = 40
        count_num = int(out[0].strip()) / 2 / (1024 * 1024) * 8 + 10
        cmd_str = 'dd if=/dev/zero of=%s bs=1M count=%s oflag=sync conv=notrunc' % (wc_dev, count_num)
        try:
            # Set the timeout as 300s.
            do_system_timeout(cmd_str, 300)
            debug('Zero on cache device header finished.')
        except:
            # Ignore the timeout error.
            debug('WARNNING: cannot zero the header of %s' % wc_dev)
        seek_num = int(out[0].strip()) / 2 / 1024 - int(dd_tail_size)
        cmd_str = 'dd if=/dev/zero of=%s bs=1M seek=%s count=%s oflag=sync conv=notrunc' % (
            wc_dev, seek_num, dd_tail_size)
        try:
            # Set the timeout as 300s
            do_system_timeout(cmd_str, 300)
            debug('Zero on cache device tail finished.')
        except:
            # Ignore the timeout error.
            debug('WARNNING: cannot zero the tail of %s' % wc_dev)

    def _configuration_snapshot_lv_list(self):
        target_dir = {'ibd-target-vg': 'ibd-target-lv'}
        self.list_snapshot.append(target_dir)

    def _get_ibdcacheflush_device(self, snapshot_id, vg):
        if 'dedup' in vg:
            self.export_device_snapshot = '/dev/%s/%s' % (vg, snapshot_id)
        if 'ibd-wc' in vg:
            self.wc_device_snapshot = '/dev/%s/%s' % (vg, snapshot_id)
        if 'ibd-target' in vg:
            self.export_device_snapshot = '/dev/%s/%s' % (vg, snapshot_id)

    def delete_wc_snapshot(self, snapshot_id):
        for snapshot_level in self.list_snapshot:
            for vg in snapshot_level:
                if vg in ['ibd-wc-vg']:
                    self._lvs_delete(snapshot_id, vg)

    def create_snapshot_lv(self, snapshot_id, is_need_freeze_file_system=True):
        try:
            if is_need_freeze_file_system:
                self.ibd_version._check_mount_status()
                self.ibd_version._freeze_file_system()
            self.ibd_version._freeze()
            if is_need_freeze_file_system:
                self.ibd_version._unfreeze_file_systme()
            self.ibd_version._flush(self.wc_device_snapshot, self.export_device_snapshot)
            for snapshot_level in self.list_snapshot:
                for vg, lv in snapshot_level.items():
                    self._lvs_create(snapshot_id, lv, vg)
                    self._get_ibdcacheflush_device(snapshot_id, vg)
        except Exception as e:
            errormsg('{e}'.format(e=e))
            raise e
        finally:
            try:
                self.ibd_version._unfreeze()
            except Exception as e:
                errormsg('{excp}'.format(excp=e))
                raise e


class FastFailoverSnapshotOldIBD(FastFailoverSnapshot):
    def __init__(self, is_new_ibd):
        super(FastFailoverSnapshotOldIBD, self).__init__(is_new_ibd)
        self._configuration_snapshot_lv_list()

    def _configuration_snapshot_lv_list(self):
        target_dir = {'ibd-target-vg': 'ibd-target-lv'}
        wc_dir = {'ibd-wc-vg': 'ibd-wc-lv'}
        self.list_snapshot.append(target_dir)
        self.list_snapshot.append(wc_dir)

    def create_snapshot_lv(self, snapshot_id, is_need_freeze_file_system=True):
        try:
            if is_need_freeze_file_system:
                self.ibd_version._freeze()
            for snapshot_level in self.list_snapshot:
                for vg, lv in snapshot_level.items():
                    self._lvs_create(snapshot_id, lv, vg)
                    self._get_ibdcacheflush_device(snapshot_id, vg)
        except Exception as e:
            errormsg('{e}'.format(e=e))
            raise e
        finally:
            try:
                if is_need_freeze_file_system:
                    self.ibd_version._unfreeze()
                self.ibd_version._flush(self.wc_device_snapshot, self.export_device_snapshot)
            except:
                errormsg('{excp}'.format(excp=e))
                raise e
            finally:
                self.delete_wc_snapshot(snapshot_id)


class UnFastFailoverSnapshot(LvsSnapshotBaseSetup):
    def __init__(self, is_new_ibd):
        super(UnFastFailoverSnapshot, self).__init__(is_new_ibd)
        self._map_ibd = {
            True: 'IBDNewVersion',
            False: 'IBDOldVersion'
        }
        self._configuration_snapshot_lv_list()
        self.ibd_version = self._ibd_manager()

    def _mount_device(self, mount_dir, mount_opt):
        # Ext3 may clear the needs_recovery bit during export operation, set it back.
        rc = do_system("echo feature needs_recovery | /opt/milio/sbin/debugfs -w /dev/dedupvg/deduplv")
        if rc != 0:
            raise SnapError('mark export device needs_recovery error!')
        cmd_mount = 'mount -t dedup -o %s /dev/dedupvg/deduplv %s' % (mount_opt, mount_dir)
        rc = do_system(cmd_mount)
        if rc != 0:
            raise SnapError('mount export device  error!')
        debug('mount export device successfully!')

    def create_snapshot_lv(self, snapshot_id, is_need_freeze_file_system=True):
        for snapshot_level in self.list_snapshot:
            for vg, lv in snapshot_level.items():
                self._lvs_create(snapshot_id, lv, vg)


class SimpleSnapshot(FastFailoverSnapshot):
    def __init__(self, is_new_ibd):
        self.all_exports = {}
        self._load_exports()
        super(SimpleSnapshot, self).__init__(is_new_ibd)
        self._map_ibd = {
            True: 'IBDNewVersionSimple',
            False: 'IBDOldVersionSimple'
        }
        self.sim_roll = RollbackSnapshotSimp()
        self._configuration_snapshot_lv_list()
        self.ibd_version = self._ibd_manager()
        self.export_device = self.get_export_ibd_dev()

    def _configuration_snapshot_lv_list(self):
        self.list_snapshot = []
        dedup_dir = {'dedupvg': 'deduplv'}
        self.list_snapshot.append(dedup_dir)

    def get_export_ibd_dev(self):
        for uuid in self.all_exports:
            vscaler_uuid = uuid
        return vscaler_uuid

    def _load_exports(self):
        if not os.path.isfile(SVM_EXPORTS):
            raise SnapError('load svm config failed!')
        try:
            svm_exports_file = open(SVM_EXPORTS, 'r')
            data = svm_exports_file.read()
            svm_exports_file.close()
            self.all_exports = json.loads(data)
        except:
            debug(traceback.format_exc())

    def zero_wc_device_cache(self):
        zero_dd = 'dd if=/dev/zero of=/bufdevice bs=1M count=4096 oflag=sync conv=notrunc'
        try:
            # Set the timeout as 300s.
            do_system_timeout(zero_dd, 300)
            debug('Zero on cache device finished.')
        except:
            # Ignore the timeout error.
            debug('WARNNING: cannot zero the header!')

    def _mount_device(self, mount_dir, mount_opt):
        # Ext3 may clear the needs_recovery bit during export operation, set it back.
        rc = do_system("echo feature needs_recovery | /opt/milio/sbin/debugfs -w /dev/mapper/%s" % self.export_device)
        if rc != 0:
            raise SnapError('mark export device needs_recovery error!')
        cmd_mount = 'mount -t dedup -o %s /dev/mapper/%s %s' % (mount_opt, self.export_device, mount_dir)
        rc = do_system(cmd_mount)
        if rc != 0:
            raise SnapError('mount export device  error!')
        debug('mount export device successfully!')


class SimpleSnapshotOldIBD(SimpleSnapshot):
    def __init__(self, is_new_ibd):
        super(SimpleSnapshotOldIBD, self).__init__(is_new_ibd)

    def create_snapshot_lv(self, snapshot_id, is_need_freeze_file_system=True):
        try:
            if is_need_freeze_file_system:
                self.ibd_version._freeze()
            self.ibd_version._flush(self.wc_device_snapshot, self.export_device_snapshot)
            for snapshot_level in self.list_snapshot:
                for vg, lv in snapshot_level.items():
                    self._lvs_create(snapshot_id, lv, vg)
        except Exception as e:
            errormsg('{excp}'.format(excp=e))
            raise e
        finally:
            if is_need_freeze_file_system:
                self.ibd_version._unfreeze()


class LvsSnapshotManager(object):
    def __init__(self):
        self.volume_type = milio_config.volume_type
        self._map_snapshot = {
            True: {
                True: 'FastFailoverSnapshot',
                False: 'FastFailoverSnapshotOldIBD'
            },
            False: {
                True: 'SimpleSnapshot',
                False: 'SimpleSnapshotOldIBD'
            }
        }

    def get_version(self, is_fast_faileover, ibd_version):
        """ lvs snapshot manager class
        Args:
            is_fast_faileover: bool
            ibd_version: bool

        Returns:
            Correspondingly snapshot class
        """
        if not (isinstance(is_fast_faileover, bool) and isinstance(ibd_version, bool)):
            raise SnapError(self.get_version.__doc__)
        if is_fast_faileover:
            return globals()[self._map_snapshot[is_fast_faileover][ibd_version]](ibd_version)
        else:
            if self.volume_type in ['SIMPLE_HYBRID'] and is_new_simple_hybrid():
                return globals()[self._map_snapshot[is_fast_faileover][ibd_version]](ibd_version)
            else:
                return globals()['UnFastFailoverSnapshot'](ibd_version)


def setup_cmdline_parser():
    manager = SnapshotManager().get_type_snap(milio_settings.export_fs_mode.lower())
    parser = argparse.ArgumentParser(description='USX Volume snapshot and replication management API.')
    subparsers = parser.add_subparsers()
    # create the parser for the "create" command
    parser_create = subparsers.add_parser('create')
    parser_create.add_argument('-u', '--vol-id', type=str, required=True)
    parser_create.add_argument('-s', '--snap-id', type=str, required=True)
    parser_create.add_argument('-m', '--snap-name', type=str, required=False)
    parser_create.add_argument('-j', '--job-id', type=str, required=False)
    parser_create.add_argument('-w', '--wait', action='store_true', default=False, required=False)
    parser_create.add_argument('-c', '--scheduled-snapshot', action='store_true', default=False, required=False)
    parser_create.set_defaults(func=manager.create_base)

    # create the parser for the "delete" command
    parser_delete = subparsers.add_parser('delete')
    parser_delete.add_argument('-u', '--vol-id', type=str, required=True)
    parser_delete.add_argument('-s', '--snap-id', type=str, required=True)
    parser_delete.add_argument('-m', '--snap-name', type=str, required=False)
    parser_delete.add_argument('-j', '--job-id', type=str, required=False)
    parser_delete.set_defaults(func=manager.delete_base)

    # create the parser for the "deleteall" command
    parser_deleteall = subparsers.add_parser('deleteall')
    parser_deleteall.add_argument('-u', '--vol-id', type=str, required=True)
    parser_deleteall.add_argument('-j', '--job-id', type=str, required=False)
    parser_deleteall.add_argument('-m', '--snap-name', type=str, required=False)
    parser_deleteall.set_defaults(func=manager.delete_all_base)

    # create the parser for the "list" command
    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('-u', '--vol-id', type=str, required=True)
    parser_list.set_defaults(func=manager.list_snap_base)

    # create the parser for the "unexport" command
    parser_umount = subparsers.add_parser('unexport')
    parser_umount.add_argument('-u', '--vol-id', type=str, required=True)
    parser_umount.add_argument('-s', '--snap-id', type=str, required=True)
    parser_umount.add_argument('-m', '--snap-name', type=str, required=False)
    parser_umount.add_argument('-j', '--job-id', type=str, required=False)
    parser_umount.set_defaults(func=manager.unmount_base)

    # create the parser for the "unmountall" command
    parser_unmountall = subparsers.add_parser('unmountall')
    parser_unmountall.add_argument('-u', '--vol-id', type=str, required=False)
    parser_unmountall.add_argument('-j', '--job-id', type=str, required=False)
    parser_unmountall.add_argument('-s', '--snap-id', type=str, required=False)
    parser_unmountall.set_defaults(func=manager.unmount_all_base)

    # create the parser for the "export" command
    parser_mount_readonly = subparsers.add_parser('export')
    parser_mount_readonly.add_argument('-u', '--vol-id', type=str, required=True)
    parser_mount_readonly.add_argument('-s', '--snap-id', type=str, required=True)
    parser_mount_readonly.add_argument('-i', '--export-id', type=str, required=True)
    parser_mount_readonly.add_argument('-m', '--snap-name', type=str, required=False)
    parser_mount_readonly.add_argument('-j', '--job-id', type=str, required=False)
    parser_mount_readonly.add_argument('-p', '--mount-dir', type=str, required=True)
    parser_mount_readonly.add_argument('-w', '--wait', action='store_true', default=False, required=False)
    parser_mount_readonly.set_defaults(func=manager.mount_snapshot_base)

    # create the parser for the "export_all" command
    parser_mount_readonly_all = subparsers.add_parser('export_all')
    parser_mount_readonly_all.add_argument('-u', '--vol-id', type=str, required=True)
    parser_mount_readonly_all.add_argument('-j', '--job-id', type=str, required=False)
    parser_mount_readonly_all.set_defaults(func=manager.mount_all_snapshot_base)

    # create the parser for the "rollback" command
    parser_mount = subparsers.add_parser('rollback')
    parser_mount.add_argument('-u', '--vol-id', type=str, required=True)
    parser_mount.add_argument('-s', '--snap-id', type=str, required=True)
    parser_mount.add_argument('-m', '--snap-name', type=str, required=False)
    parser_mount.add_argument('-j', '--job-id', type=str, required=False)
    parser_mount.add_argument('-w', '--wait', action='store_true', default=False, required=False)
    parser_mount.set_defaults(func=manager.roll_back)

    parser_freespace = subparsers.add_parser('freespace')
    parser_freespace.add_argument('-u', '--vol-id', type=str, required=False)
    parser_freespace.add_argument('-j', '--job-id', type=str, required=False)
    parser_freespace.add_argument('-s', '--snap-id', type=str, required=False)
    parser_freespace.set_defaults(func=manager.freespace_base)

    parser_list = subparsers.add_parser('update')
    parser_list.add_argument('-u', '--vol-id', type=str, required=True)
    parser_list.add_argument("-j", "--job_id", default='', help='Job ID', type=str, required=False)
    parser_list.set_defaults(func=manager.update_snap_info_base)

    # create the parser for the "replicate" command
    parser_replicate = subparsers.add_parser('replicate')
    parser_replicate.add_argument('-u', '--vol-id', type=str, required=True)
    parser_replicate.add_argument('-r', '--replicate-id', type=str, required=False)
    parser_replicate.add_argument('-s', '--snap-id', type=str, required=True)
    parser_replicate.add_argument('-m', '--snap-name', type=str, required=False)
    parser_replicate.add_argument('-i', '--target-volume-ip', type=str, required=True)
    parser_replicate.add_argument('-n', '--target-volume-name', type=str, required=True)
    parser_replicate.add_argument('-j', '--job-id', type=str, required=False)
    # parser_replicate.add_argument('-d', '--delete-replicate', action='store_true', default=False, required=False)
    parser_replicate.add_argument('-w', '--wait', action='store_true', default=False, required=False)
    parser_replicate.set_defaults(func=manager.replicate_volume_async)

    # create the parser for the "replicate_target" command
    parser_replicate_target = subparsers.add_parser('replicate_target')
    parser_replicate_target.add_argument('-n', '--target-volume-name', type=str, required=True)
    parser_replicate_target.add_argument('-ts', '--target-volume-size', type=int, required=False)
    parser_replicate_target.add_argument('-s', '--snap-id', type=str, required=True)
    parser_replicate_target.add_argument('-j', '--job-id', type=str, required=False)
    parser_replicate_target.add_argument('-c', '--cleanup', action='store_true', default=False, required=False)
    parser_replicate_target.set_defaults(func=manager.replicate_volume_target_base)

    parser_add_all_exports = subparsers.add_parser('add_all_exports')
    parser_add_all_exports.add_argument('-u', '--vol-id', type=str, required=True)
    parser_add_all_exports.set_defaults(func=manager.add_all_export_base)

    parser_check_type = subparsers.add_parser('type_check')
    parser_check_type.add_argument('-n', '--target-volume-name', type=str, required=True)
    parser_check_type.set_defaults(func=manager.type_check)

    # create the parser for the "create_golden_image" command
    parser_gold_image = subparsers.add_parser('create_golden_image')
    parser_gold_image.add_argument('-u', '--vol-id', type=str, required=True)
    parser_gold_image.add_argument('-j', '--job-id', type=str, required=False)
    parser_gold_image.set_defaults(func=manager.create_golden_image)
    args = parser.parse_args()

    #
    debug('((%s))' % args)
    try:
        if ((args.func == manager.add_all_export_base) or (args.func == manager.unmount_all_base)) and (
                not manager.is_enable_snapshot):

            raise SnapError("old version - not supporting snapshot!")
        elif args.func != manager.freespace_base and args.func != manager.create_golden_image and \
                        args.func != manager.update_snap_info_base and (not manager.is_enable_snapshot):
            raise SnapError("Snapshot is not supported by this volume layout!")
        elif args.func != manager.unmount_all_base and args.func != manager.freespace_base and \
                        args.func != manager.type_check and not manager.verify_volume_id(args):
            raise SnapError("volume id mismatch!")
        rc = args.func(args)
    except Exception as e:
        errormsg('snapshot API error: %s' % e)
        errormsg(traceback.format_exc())
        if 'supported by this volume layout' in str(e):
            rc = ERR_VOLUME_LAYOUT_WRONG
        elif 'volume id mismatch' in str(e):
            rc = ERR_VOLUME_ID_MISMATCH
        elif 'old version - not' in str(e):
            rc = RC_SUCCESS
        elif 'hang' in str(e):
            rc = ERR_LVS_HANG
        else:
            rc = ERR_UNKNOWN
    debug('vv-snapshot rc: %s' % str(rc))
    sys.exit(rc)


if __name__ == "__main__":
    set_log_file(SNAPSHOT_LOG)
    setup_cmdline_parser()
