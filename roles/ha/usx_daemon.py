#!/usr/bin/python
from daemon import Daemon
from subprocess import *
import tempfile
import sys
import time
import os
import signal
import logging
import traceback
import json
from ha_util import *
sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *
from atl_alerts import *
from atl_util import node_trylock, node_unlock
sys.path.insert(0, "/opt/milio/atlas/roles")
from utils import *

USX_DAEMON_PIDFILE     = '/var/run/usx_daemon.pid'
USX_DAEMON_LOGFILE     = '/var/log/usx-daemon.log'
USX_DAEMON_ROTATE_CONF = '/etc/logrotate.d/usx_daemon'
USX_DAEMON_IS_STARTED  = '/tmp/usx_deamon_started'
USX_DAEMON_HA_LOCK     = '/tmp/usx_daemon_ha_lock'
IO_WAIT_TIMEOUT        = 20    # IO wait timeout
LIST_SIZE              = 12          # IO list size
SMALL_CACHE_SIZE       = 128  # Good for latency
LARGE_CACHE_SIZE       = 4096 # Good for throughput
IO_THRESHOLD           = 400
LOW_THRESHOLD          = 0.9
ATLAS_CONF             = '/etc/ilio/atlas.json'
LOCAL_AGENT            = 'http://127.0.0.1:8080'
ALERT_API              = '/usxmanager/alerts'
global_list_size       = LIST_SIZE
global_threshold       = IO_THRESHOLD


def usx_daemon_version_record():
    with open(USX_DAEMON_VERSION, 'w') as fd:
        fd.write(ha_modules_version())
        fd.flush()
        os.fsync(fd)


def usx_daemon_logrotate_conf():
    debug('Enter usx_daemon_logrotate_conf ')
    tmp_fname = "/tmp/usx_daemon"
    cfile = open(tmp_fname, "w")

    title = USX_DAEMON_LOGFILE
    cfile.write(title + " {\n")
    cfile.write("       daily\n")
    cfile.write("       missingok\n")
    cfile.write("       rotate 14\n")
    cfile.write("       size 50M\n")
    cfile.write("       compress\n")
    cfile.write("       delaycompress\n")
    cfile.write("       notifempty\n")
    cfile.write("}\n\n")

    cfile.close()
    os.rename(tmp_fname, USX_DAEMON_ROTATE_CONF)

    return 0


def ha_start_up_prepartion():
    ha_remove_orphan(True)
    #ha_check_config_consistency()
    ha_remove_start_up_preparation()
    ha_cleanup_failed_ds_resource_after_preparation()


def monitor_io(raid5_dev):
    debug("Enter monitor_io")
    if raid5_dev == None:
        return 0

    cmd = 'nfsstat | grep -A 1 write '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'cat /sys/block/' + raid5_dev + '/md/stripe_cache_active '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'cat /proc/meminfo | grep Dirty | grep -v grep '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    cmd = 'cat /sys/block/' + raid5_dev + '/md/stripe_cache_size '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

    return 0


def set_cache_size(cache_size, raid5_dev):
    debug('Enter set_cache_size %d' % cache_size)
    if raid5_dev != None:
        spath = '/sys/block/' + raid5_dev + '/md/stripe_cache_size'
        while True:
            cmd = 'echo ' + str(cache_size)  + ' > ' + spath
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)

            cmd = 'cat ' + spath
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
            if ret == 0 and len(msg) == 1:
                the_size = int(msg[0])
                if the_size == cache_size:
                    debug('INFO: done with set_cache_size %d' % cache_size)
                    break
            time.sleep(10)
    return cache_size


def find_raid5_dev():
    debug('Enter find_raid5_dev')
    raid5_dev = None
    cmd = 'cat /proc/mdstat  | grep "active raid5" | grep -v grep '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) == 1:
        the_line = msg[0]
        tmp = the_line.split()
        if len(tmp) < 2:
            debug('ERROR: wrong /proc/mdstat output')
            return None
        raid5_dev = tmp[0]

    return raid5_dev

def find_zram_dev():
    debug('Enter find_zram_dev')
    zram_dev = None
    cmd = 'cat /proc/mdstat | grep zram '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    if ret == 0 and len(msg) > 0:
        the_line = msg[0]
        tmp = the_line.split()
        if len(tmp) < 4:
            debug('ERROR: wrong /proc/mdstat output')
            return None
        zram_dev = tmp[4].split('[')[0]
    return zram_dev


def check_snapshot_used_space():
    try:
        if milio_config.is_contains_volume and milio_config.is_snapshotenabled:
            cmd = 'ps -ef | grep "/opt/milio/atlas/roles/virtvol/vv-snapshot.pyc" | grep -v grep || python /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc freespace & '
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        else:
            debug("No need to free snapshot used space")
    except AttributeError:
        debug("No need to free snapshot used space")
    except Exception as e:
        debug(e)
'''
def _send_alert_stretchcluster(uuid, display_name, old_status, status, description):
    debug('send_alert %s %s %s %s %s' % (uuid, display_name, old_status, status, description))
    cmd = 'date +%s'
    (ret, epoch_time) = runcmd(cmd, print_ret=True)
    epoch_time = epoch_time.rstrip('\n')

    ad = {
        "value"         :0.0,
        "warn"          :0.0,
        "error"         :0.0,
        "service"       :"MONITORING",
        "usxtype"       :"VOLUME"
    }

    ad["uuid"]           = uuid + '-stretchcluster-alert-' + str(epoch_time)
    ad["checkId"]        = uuid + '-stretchcluster'
    ad["usxuuid"]        = uuid
    ad["displayname"]    = display_name
    ad["target"]         = "servers." + uuid + ".stretchcluster"
    ad["alertTimestamp"] = epoch_time
    ad['status']         = status
    ad['oldStatus']      = old_status
    ad['description']    = description

    data = json.dumps(ad)
    cmd = 'curl -X POST -H "Content-type:application/json" %s%s -d \'%s\'' % (LOCAL_AGENT, ALERT_API,data)
    runcmd_nonblock(cmd, print_ret=True, wait_time=0)

'''

class UsxDaemon(Daemon):

    def run(self):
        cnt = 0
        tiebreakerip = []
        stretchcluster_flag = False
        availability_flag = False
        (stretchcluster_flag, availability_flag, tiebreakerip) = ha_stretchcluster_config()
        scl_timeout = ha_get_scl_timeout()
        nodename = ha_get_local_node_name()
        alert_s_flag = True
        alert_flag = True
        zram_dev = find_zram_dev()
        previous_volume_uuid = None
        if zram_dev != None:
            debug('Found zram device: %s' % (zram_dev))
        zram_full_flag = False
        zram_100full_flag = False

        if ha_check_start_up_preparation():
            ha_start_up_prepartion()

        try:
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)
            while True:
                # Sometimes usx_deamon is not stopped immediately in teardown
                if os.path.exists(TEARDOWN_FLAG) == True:
                    debug('Teardowning, do nothing.')
                    time.sleep(5)
                    continue

                # Each 1 minute
                if cnt % 12 == 11:
                    # check corosync, pacemaker, storage network, etc.
                    cmd = 'ps -ef | grep restart_corosync_pacemaker | grep -v grep || python /opt/milio/atlas/roles/ha/restart_corosync_pacemaker.pyc & '
                    runcmd_nonblock(cmd, print_ret=True, wait_time=0)

                    # Push raid1PriamaryInfo to remote if it failed before
                    if ha_file_flag_operation(PUSH_RAID1_PRIMARY_INFO_FLAG, "check"):
                        try:
                            pid = os.fork()
                        except:
                            debug("Unable to create new process")
                        else:
                            if pid != 0:
                                ha_file_flag_operation(PUSH_RAID1_PRIMARY_INFO_FLAG, "remove")
                                milio_config.ha_reload(get_curr_volume())
                                MdStatMgr.push_local_to_remote()
                                os._exit(0)

                    # Mount snpashot when USX Manager is online if skip in failover
                    if check_skip_mount_snapshot_flag() and check_usxmanager_alive_flag():
                        volume_uuid = get_curr_volume()
                        if ha_check_resouce_group_unmanaged(volume_uuid):
                            crc, ha_enabled_flag = check_volume_ha_status_from_usxm(volume_uuid)
                            if crc == 0 and ha_enabled_flag:
                                ha_manage_one_resouce(volume_uuid)
                        if volume_uuid != None:
                            ha_update_mount_status(volume_uuid)
                            cmd = 'ps -ef | grep exports_all | grep -v grep || python /opt/milio/atlas/roles/virtvol/vv-snapshot.pyc export_all -u ' + volume_uuid + ' &'
                            ha_retry_cmd(cmd, 3, 2)
                        remove_skip_mount_snapshot_flag()

                    # unlock the ha failover flag if the previous failover failed with timeout
                    if ha_check_volume_starting_flag() and not ha_check_volume_is_starting():
                        ha_remove_ha_lock_file()
                        ha_remove_volume_starting_flag()

                # Each 30 minutes
                #if cnt % 360 == 180:
                    # USX-75754 Disable the free space check in usx_dameon
                    # check snapshot used space
                    #if ha_check_node_used():
                    #    volume_uuid = get_curr_volume()
                    #    if volume_uuid != previous_volume_uuid:
                    #        milio_config.ha_reload(volume_uuid)
                    #        previous_volume_uuid = volume_uuid
                    #check_snapshot_used_space()

                if zram_dev != None:
                    res = get_zram_used_space(zram_dev)
                    if(res > 98) and not zram_100full_flag:
                        zram_100full_flag = True
                        zram_full_flag = True
                        debug('ZRAM full. Changes made after last backup might be lost if volume is rebooted. To recover, restore to an earlier backup')
                        (volume_type, volume_uuid, ilio_uuid, display_name)=get_volume_info()
                        send_alert_zram(ilio_uuid, display_name, 'WARN', 'ERROR', 'ZRAM full. Changes made after last backup might be lost if volume is rebooted. To recover, restore to an earlier backup')
                    elif res > 90 and not zram_full_flag:
                        # ZRAM is almost full
                        zram_full_flag = True
                        zram_100full_flag = False
                        debug("ZRAM used %s%% of available space" % (res))
                        (volume_type, volume_uuid, ilio_uuid, display_name)=get_volume_info()
                        send_alert_zram(ilio_uuid, display_name, 'OK', 'WARN', 'ZRAM use > 90% of available memory')
                    elif res < 80 and zram_full_flag:
                        zram_full_flag = False
                        zram_100full_flag = False
                        debug("ZRAM used %s%% of available space" % (res))
                        (volume_type, volume_uuid, ilio_uuid, display_name)=get_volume_info()
                        send_alert_zram(ilio_uuid, display_name, 'WARN', 'OK', 'ZRAM use < 80% of available memory')

                # get_curr_volume should return vol info if that resource begin
                # to start
                if stretchcluster_flag == True:
                    if ha_check_enabled():
                        volume_uuid = get_curr_volume()
                        quorum_flag = ha_has_quorum()
                        starting_flag = ha_check_volume_is_starting()
                        if ( not quorum_flag ) or starting_flag:
                            # Set a default value for pid
                            pid = 31415926
                            reset_flag = 0
                            tiebreakerip = ha_get_tiebreakerip()
                            # To handle the imcompleted resource picking up
                            if not quorum_flag:
                                if volume_uuid != None and not starting_flag:
                                    # IPaddr2 is not started, incompleted
                                    if not ha_check_ipaddr2_running():
                                        time.sleep(2)
                                        debug('To finish the imcompleted failover')
                                        usx_deamon_ha_lock_fd = node_trylock(USX_DAEMON_HA_LOCK)
                                        if usx_deamon_ha_lock_fd != None:
                                            try:
                                                pid = os.fork()
                                            except:
                                                debug('Unable to create new process')
                                                continue
                                            if pid != 0:
                                                continue
                                            # Child process
                                            else:
                                                # Start resource
                                                grp = ha_stretchcluster_start_res(tiebreakerip, nodename, 60, volume_uuid)
                                                debug('Child process exit after start resource.')
                                                node_unlock(usx_deamon_ha_lock_fd)
                                                os._exit(0)
                                        else:
                                            debug('Already start child process.')
                            if volume_uuid == None:
                                usx_deamon_ha_lock_fd = node_trylock(USX_DAEMON_HA_LOCK)
                                if usx_deamon_ha_lock_fd == None:
                                    time.sleep(2)
                                    cnt += 1
                                    continue
                                else:
                                    node_unlock(usx_deamon_ha_lock_fd)
                                # Only when number of online nodes are exactly half of the total nodes
                                # the split brain will be allowed.
                                if ha_is_node_split_brain() != 0:
                                    debug('Number of online nodes are not half of total nodes, skip split brain!')
                                    time.sleep(5)
                                    cnt += 1
                                    continue
                                debug('HA VM side, try to start resource.')
                                # Stretch cluster, split brained
                                # HA standby node side:
                                # Critical sleep to let existing volume get
                                # their tiebreaker locks first.
                                time.sleep(20)
                                if not ha_has_quorum():
                                    #ha_set_no_quorum_policy('ignore', True)
                                    # Verify we still don't have volume
                                    volume_uuid = get_curr_volume()
                                    if volume_uuid != None:
                                        debug("WARN: volume %s already started." % volume_uuid)
                                        continue
                                    # We are indeed empty and no quorum,
                                    # Try to acquire lock
                                    else:
                                        usx_deamon_ha_lock_fd = node_trylock(USX_DAEMON_HA_LOCK)
                                        if usx_deamon_ha_lock_fd == None:
                                            debug('Already start child process.')
                                            time.sleep(2)
                                            cnt += 1
                                            continue
                                        else:
                                            try:
                                                pid = os.fork()
                                            except:
                                                debug('Unable to create new process')
                                                continue
                                            if pid != 0:
                                                if alert_s_flag:
                                                    #send alert
                                                    (volume_type, volume_uuid, ilio_uuid, display_name)=get_volume_info()
                                                    send_alert_stretchcluster(ilio_uuid, display_name, 'OK', 'WARN', 'A split-brain condition occurred')
                                                    alert_s_flag = False
                                                time.sleep(2)
                                                continue
                                            # Child process
                                            else:
                                                # Start resource
                                                grp = ha_stretchcluster_start_res(tiebreakerip, nodename, 30)
                                                if grp == None:
                                                    debug('Do NOT start any resource')
                                                else:
                                                    debug('Resource {} was started on this node'.format(grp))
                                                debug('Child process exit.')
                                                node_unlock(usx_deamon_ha_lock_fd)
                                                os._exit(0)
                                else:
                                    debug('Quorum resumed, do nothing.')
                                    alert_s_flag = True
                                    continue
                            else:
                                debug('Volume side, keep refresh the lock')
                                # Volume node side:
                                # Keep refresh my lock.
                                result = ha_acquire_stretchcluster_lock(tiebreakerip, volume_uuid, nodename, scl_timeout)
                                debug(result)
                                if alert_s_flag and not starting_flag:
                                    #send alert
                                    (volume_type, volume_uuid, ilio_uuid, display_name)=get_volume_info()
                                    send_alert_stretchcluster(ilio_uuid, display_name, 'OK', 'WARN', 'A split-brain condition occurred')
                                    alert_s_flag = False

                                # If we are using 2 SVMs as tiebreaker, need special logic
                                if len(tiebreakerip) == 2:
                                    if not is_vmmanager_reachable():
                                        if result[0] == 255 or result[1] == 255:
                                            reset_flag = 1
                                    else:
                                        # If Volume could not acquire lock on all live node
                                        # Result should be [1,1] [1,255] [255,1]
                                        if (result[0] == 1 and result[1] == 1) or \
                                                        (result[0] + result[1]) == 256:
                                            reset_flag = 1
                                        # If Volume acquire lock timeout on both tiebreaker
                                        # Result should be [255,255]
                                        # We need to check the power status
                                        # If not both are power off, reset the Volume
                                        if result[0] == 255 and result[1] == 255:
                                            (local_svm_power_status, remote_svm_power_status) = ha_get_local_remote_Service_VM_power_status()
                                            if local_svm_power_status + remote_svm_power_status != 0:
                                                reset_flag = 1
                                else:
                                    if result[0] != 0:
                                        debug('Volume side, failed to acquire lock from tiebreaker.')
                                        reset_flag = 1
                                if reset_flag and not starting_flag:
                                    debug('Reset flag is True and Starting flag is False: Reset the Volume')
                                    # Lost my lock in split brain mode! reboot
                                    if alert_flag:
                                        (volume_type, volume_uuid, ilio_uuid, display_name)=get_volume_info()
                                        send_alert_stretchcluster(ilio_uuid, display_name, 'OK', 'WARN', 'Reboot VM due to lost lock in split brain mode.')
                                        alert_flag = False
                                    reset_vm('STRETCH_CLUSTER_lost_tiebreaker_deamon')
                        elif quorum_flag:
                            # Quoram resumed
                            alert_s_flag = True

                time.sleep(5)
                cnt = cnt + 1
        except:
            debug(traceback.format_exc())
            debug('Exception caught on usx_daemon...')
            sys.exit(2)


    def run2(self):
        try:
            global global_list_size
            global global_threshold

            #if not os.path.exists(USX_DAEMON_ROTATE_CONF):
            usx_daemon_logrotate_conf()

            # check volume type
            (volume_type, volume_uuid, ilio_uuid) = get_volume_info()
            if volume_type == None:
                debug('ERROR: volume type is none')
                sys.exit(2)
            elif volume_type.upper() in ['SIMPLE_HYBRID', 'SIMPLE_MEMORY', 'SIMPLE_FLASH']:
                debug('WARN: exit because volume type is %s' % volume_type)
                sys.exit(0)
            else:
                debug('INFO: check volume type %s during start' % volume_type)

            write_list = [0] * global_list_size
            debug('write_list %s' % str(write_list))
            read_list = [0] * global_list_size
            debug('read_list %s' % str(read_list))

            raid5_dev = find_raid5_dev()
            debug('INFO: change to SMALL_CACHE_SIZE %d during start' % SMALL_CACHE_SIZE )
            cache_size = set_cache_size(SMALL_CACHE_SIZE, raid5_dev)

            while True:
                # TODO: support user input, reduce debug info, handle md hung
                raid5_dev = find_raid5_dev()
                if raid5_dev != None:
                    monitor_io(raid5_dev)
                    cmd = 'iostat -m 5 2 | grep ' + raid5_dev + ' | grep -v grep '
                    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
                    if ret == 0 and len(msg) == 2:
                        the_line = msg[1]
                        tmp = the_line.split()
                        debug('tmp %s ' % str(tmp))
                        if len(tmp) < 4:
                            debug('ERROR: wrong iostat output')
                            break
                        write_list.pop(0)
                        write_list.append(float(tmp[3]))
                        debug('write_list %s: avg %f' % (str(write_list), sum(write_list)/len(write_list)))
                        read_list.pop(0)
                        read_list.append(float(tmp[2]))
                        debug('read_list %s: avg %f' % (str(read_list), sum(read_list)/len(read_list)))
                else:
                    write_list = [0] * global_list_size
                    read_list = [0] * global_list_size
                    if cache_size != SMALL_CACHE_SIZE:
                        debug('INFO: no raid5 device, change to SMALL_CACHE_SIZE %d ' % SMALL_CACHE_SIZE )
                        cache_size = set_cache_size(SMALL_CACHE_SIZE, None) 
                    time.sleep(60)
                    continue

                avg_write_list = sum(write_list) / len(write_list)
                avg_read_list = sum(read_list) / len(read_list)
                if (avg_write_list > global_threshold or avg_read_list > global_threshold) and cache_size != LARGE_CACHE_SIZE:
                    debug('INFO: dynamically change to LARGE_CACHE_SIZE %d: %f %f vs %f' % (LARGE_CACHE_SIZE, avg_write_list, avg_read_list, global_threshold))
                    cache_size = set_cache_size(LARGE_CACHE_SIZE, raid5_dev)
                elif avg_write_list < global_threshold * LOW_THRESHOLD and avg_read_list < global_threshold * LOW_THRESHOLD and cache_size != SMALL_CACHE_SIZE:
                    debug('INFO: dynamically change to SMALL_CACHE_SIZE %d: %f %f vs %f' % (SMALL_CACHE_SIZE, avg_write_list, avg_read_list, global_threshold * LOW_THRESHOLD))
                    write_list = [0] * global_list_size
                    read_list = [0] * global_list_size
                    cache_size = set_cache_size(SMALL_CACHE_SIZE, raid5_dev)

                #time.sleep(5)

        except:
            debug(traceback.format_exc())
            debug('Exception caught on usx_daemon...')
            sys.exit(2)


if __name__ == "__main__":
    set_log_file(USX_DAEMON_LOGFILE)
    daemon = UsxDaemon(USX_DAEMON_PIDFILE)

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            try:
                #if len(sys.argv) == 4:
                #   global_list_size = int((float(sys.argv[2]) * 60) / 5)
                #   global_threshold = float(sys.argv[3])
                usx_daemon_version_record()
                daemon.start()
            except:
                pass
        elif 'stop' == sys.argv[1]:
            print "Stopping ..."
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            print "Restaring ..."
            usx_daemon_version_record()
            daemon.restart()
        elif 'status' == sys.argv[1]:
            try:
                pf = file(USX_DAEMON_PIDFILE,'r')
                pid = int(pf.read().strip())
                pf.close()
            except IOError:
                pid = None
            except SystemExit:
                pid = None

            if pid:
                print 'UsxDaemon is running as pid %s' % pid
            else:
                print 'UsxDaemon is not running.'

        else:
            print "Unknown command"
            sys.exit(2)
    else:
        #print "usage: %s start [period(minutes) rwIO(MB/s)]|stop|restart|status" % sys.argv[0]
        print "usage: %s start|stop|restart|status" % sys.argv[0]
        sys.exit(2)

