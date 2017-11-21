#!/usr/bin/python

from ha_util import *
import httplib
from time import sleep

sys.path.insert(0, "/opt/milio/libs/atlas")
from status_update import does_jobid_file_exist
from status_update import send_status
from cmd import *
from atl_arbitrator import arb_start

sys.path.insert(0, "/opt/milio/atlas/roles/")
from utils import *

VERSION = '1.0'

##################################################################
#  WARNING: This script is never tested for multi-thread safety  #
#  We may have problems when two nodes running together this     #
#  setup script and get one's configure overwrite the others     #
#  TODO: One solution is to move the setup as the step after all #
#        node in cluster are deployed, and only configure it     #
#        on one machine. The other solution is create a temp     #
#        resource to as a lock (with time-out) that a            #
#        re-configure is in-progress.                            #
##################################################################

set_log_file('/var/log/usx-atlas-ha.log')

ATLAS_CONF = '/etc/ilio/atlas.json'
JOBID_HA_FILE="/etc/ilio/atlas-ha-jobid"
HA_DISABLE_FILE = '/tmp/ha_disable'
LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'
CMD_IBDMANAGER = "/bin/ibdmanager"
CMD_IBDMANAGER_STAT_WD = CMD_IBDMANAGER + " -r a -s get_wd"

volresuuid = None
non_usxm_mode = False

def get_local_node_name():
    cmd = 'corosync-quorumtool -i'
    (ret, msg) = runcmd(cmd, print_ret=False, lines=True)
    for line in msg:
        if line.find('local') >= 0:
            node_id = line.split()[0]
            cmd = 'crm_node -l'
            (ret, submsg) = runcmd(cmd, print_ret=False, lines=True)
            for line1 in submsg:
                tmp = line1.split()
                if len(tmp) < 2:
                    continue
                if tmp[0] == node_id:
                    return tmp[1]
    return None


def check_node_maintenance_mode():
    node_name = get_local_node_name()
    if node_name == None:
        return False
    else:
        cmd = 'crm node status ' + node_name
        (ret, msg) = runcmd(cmd, print_ret=False, lines=True)
        for the_line in msg:
            if the_line.find('name="maintenance" value="on"') >= 0:
                return True
    return False


def set_ready_mode():
    cmd = 'crm node ready '
    (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
    return ret

def send_enable_ha_status(url, uuid, resuuid, status, cleanup):
    if resuuid is None:
        cmd = 'curl -k -X PUT ' + url + 'usx/inventory/volume/containers/' + uuid + '/ha?isha=' + status + '\&api_key=' + uuid + '\&cleanup=' + cleanup
    else:
        cmd = 'curl -k -X PUT ' + url + 'usx/inventory/volume/resources/' + resuuid + '/ha?isha=' + status + '\&api_key=' + uuid + '\&cleanup=' + cleanup
    (ret, out) = runcmd(cmd, print_ret=True)
    return (ret, out)

def send_vol_ha_status(status):
    global volresuuid
    stats = {}
    stats['HA_STATUS'] = long(status)
    if volresuuid:
        send_volume_availability_status(volresuuid, stats, "VOLUME_CONTAINER")
    else:
        debug('Add node: volresuuid is None')

def check_capacity_license(uuid):
    """
    License capacity check for enable HA
    """
    retVal = False

    apistr = LOCAL_AGENT + 'license/capacityinfo/volume/container/' + uuid +'?api_key=' + uuid
    cmd = 'curl -k -s --request GET -H "Content-Type:application/json" ' + apistr
    debug(cmd)
    (ret, out) = runcmd(cmd, print_ret=True)
    try:
        data = json.loads(out)
        if data.has_key("unusedLicensedCapacity"):
            # When volume boots, it notifies the AMC, which updates the remaining
            # capacity.
            # Therefore, when this check is made, there must be 0 or more capacity 
            # remaining to be allowed to boot.
            if data["unusedLicensedCapacity"] < 0:
                debug('ERROR : Unused Capcity: ' + str(data["unusedLicensedCapacity"]))
            else:
                debug("Unused Capacity: " + str(data['unusedLicensedCapacity']))
                debug("Proceed with enable HA...")
                retVal = True
        else:
            # Got API success but no JSON so something bad happened
            debug('Return JSON object missing capacity information.')
    except Exception as e:
        debug("Exception caught: %s" % e)

    return retVal

##################################################################
#                   START HERE                                   #
##################################################################

# remove HA_DISABLE_FILE
debug('INFO: begin to enable HA')
try:
    os.remove(HA_DISABLE_FILE)
except:
    pass

if len(sys.argv) not in [2, 3]:
    debug('Usage: add_node_to_cluster.pyc jobid (non_usxm_mode)')
    exit(1)
try:

    if len(sys.argv) == 3 and sys.argv[2] == 'non_usxm_mode':
        non_usxm_mode = True
    cfgfile = open(ATLAS_CONF, 'r')
    s = cfgfile.read()
    cfgfile.close()
    node_dict = json.loads(s)
    if node_dict is None:
        debug('Error getting node json information. HA will NOT be enabled for this node')
        sys.exit(1)
        exit(1)
    ilio_dict = node_dict.get('usx')
    if ilio_dict is None:
        debug('Error getting ilio json information. HA will NOT be enabled for this node')
        exit(1)
    uuid = ilio_dict.get('uuid') #get contaniner uuid

    # Perform license capacity check, fail the enabling HA process if license capacity violation
    #if not check_capacity_license(uuid):
    #    debug("ERROR : License capacity violation.")
    #    send_vol_ha_status(VOL_STATUS_FATAL)
    #    send_status("HA", 1, 1, "Enable HA", "Enabling HA Failed due to license violation!", does_jobid_file_need_deletion, block=False)
    #    sys.exit(1)

    # check the roles
    roles = ilio_dict.get('roles')
    if roles is None:
        debug('Error getting role information. HA will NOT be enabled for this node')
        sys.exit(1)
    role = roles[0]

    volres = node_dict.get('volumeresources')
    resuuid = None
    if volres != None and len(volres) > 0:
        resuuid = volres[0].get('uuid') #get volume resource uuid
    volresuuid = uuid
    amcurl = LOCAL_AGENT
    if amcurl is None:
        debug('Error getting USX AMC url. HA will NOT be enabled for this node')
        sys.exit(1)

    # Perform HA health check
    cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc enable_start'
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret != 0:
        time.sleep(5)
        (ret, msg) = runcmd(cmd, print_ret=True)
        if ret != 0:
            debug('ERROR: failed during HA health checking')
            send_enable_ha_status(amcurl, uuid, resuuid, "false", "true")
            sys.exit(1)

    # save job id
    cfgfile = open(JOBID_HA_FILE, 'w')
    cfgfile.write(sys.argv[1])
    cfgfile.close()

    jobid_file_exists = does_jobid_file_exist(True)
    does_jobid_file_need_deletion = not jobid_file_exists

    ret0 = check_node_maintenance_mode()
    ret1 = ha_check_enabled()
    if ret1 == True:
        debug('WARNING: HA enabling had been done')
        send_enable_ha_status(amcurl, uuid, resuuid, "true", "false")
        sys.exit(0)

    if ret0 == True:
        debug('INFO: begin to enable HA for the node disabled HA previously')
        # If the given node was disabled HA previously, then just put it back to ready
        # If non usx manager mode is selected, HA VM is considered to be ready
        if not non_usxm_mode:
            send_vol_ha_status(VOL_STATUS_WARN)
            send_status("HA", 1, 0, "Enable HA", "Enabling HA...", does_jobid_file_need_deletion, block=False)

            # Wait until there is at least one standby node
            qcnt = 240 # timeout = 240 * 5 = 1200 sec; 20 min
            HAS_QUORUM = False
            while qcnt > 0:
                if ha_has_standby_from_usxm():
                    HAS_QUORUM = True
                    break
                time.sleep(5)
                qcnt -= 1
            if qcnt == 0 and HAS_QUORUM == False:
                debug("ERROR : No quorum presented, system timed out!")
                send_vol_ha_status(VOL_STATUS_UNKNOWN)
                send_enable_ha_status(amcurl, uuid, resuuid, "false", "false")
                exit(1)

        ret2 = ha_manage_resources()
        if ret2 == 0:
            ha_enableha_postprocess()
            (pill_ibd_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
            debug("start arbitrator for: %s" % str(pill_ibd_list))
            if len(pill_ibd_list) > 0:
                arb_start(resuuid, pill_ibd_list)
            if not non_usxm_mode:
                debug('INFO: just change mode from MAINTENANCE to READY during HA enabling')
                send_status("HA",  100, ret2, "Enable HA", "Configured HA resources in maintenance mode", does_jobid_file_need_deletion, block=False)
            # change ha setting at ATLAS_CONF
        else:
            send_status("HA", 100, 1, "Enable HA", "HA resources have not been configured in a new cluster", does_jobid_file_need_deletion, block=False)
            send_vol_ha_status(VOL_STATUS_UNKNOWN)
            send_enable_ha_status(amcurl, uuid, resuuid, "false", "false")
            exit(ret2)
    else:
        debug('INFO: begin to enable HA for a fresh node.')
        if non_usxm_mode:
            debug("Could not Enable HA for a fresh node without USX Manager")
            exit(1)
        send_vol_ha_status(VOL_STATUS_WARN)
        send_status("HA", 1, 0, "Enable HA", "Starting HA configuration...", does_jobid_file_need_deletion, block=False)
        ha_enabled = node_dict.get('ha')
        ret = 0
        if ha_enabled:
            cmd = 'python /opt/milio/atlas/roles/ha/delete_node.pyc -u'
            (ret, msg) = runcmd(cmd, print_ret=True)
            send_vol_ha_status(VOL_STATUS_OK)
            send_status("HA",  100, ret, "Enable HA", "HA resources are already configured.", does_jobid_file_need_deletion, block=False)
            send_enable_ha_status(amcurl, uuid, resuuid, "true", "false")
            exit(0)

        debug('INFO: remove previous pacemaker configuration')
        cmd = 'rm -rf /var/lib/pacemaker/cib/*.*'
        (ret, msg) = runcmd(cmd, print_ret=True)
        debug('INFO: begin to configure corosync')
        cmd = 'python /opt/milio/atlas/roles/ha/corosync_config.pyc'
        (ret, msg) = runcmd(cmd, print_ret=True)
        send_status("HA", 50, ret, "Enable HA", "Configuring HA resources in a new cluster ...", does_jobid_file_need_deletion, block=False)
        if ret != 0:
            ha_disable_ha_flag()
            send_vol_ha_status(VOL_STATUS_UNKNOWN)
            send_status("HA", 100, 1, "Enable HA", "HA resources have not been configured in a new cluster", does_jobid_file_need_deletion, block=False)
            send_enable_ha_status(amcurl, uuid, resuuid, "false", "true")
            exit(ret);
        debug('INFO: begin to configure pacemaker')
        cmd = 'python /opt/milio/atlas/roles/ha/pacemaker_config.pyc'
        (ret, msg) = runcmd(cmd, print_ret=True)
        if ret == 0:
            ha_enableha_postprocess()
            (pill_ibd_list, pill_uuid_list) = IBDManager.find_ibd_status("all", True)
            debug("start arbitrator for: %s" % str(pill_ibd_list))
            if len(pill_ibd_list) > 0:
                arb_start(resuuid, pill_ibd_list)
        if ret != 0 and resuuid != None:
            send_status("HA", 100, 1, "Enable HA", "HA resources have not been configured in a new cluster", does_jobid_file_need_deletion, block=False)
            cmd = 'python /opt/milio/atlas/roles/ha/delete_node.pyc -rr ' + resuuid
            (ret, msg) = runcmd(cmd, print_ret=True)
            send_vol_ha_status(VOL_STATUS_UNKNOWN)
            send_enable_ha_status(amcurl, uuid, resuuid, "false", "true")
            exit(ret);
        send_status("HA", 100, ret, "Enable HA", "Configured HA resources in a new cluster", does_jobid_file_need_deletion, block=False)
except ValueError, e:
    debug('JSON parse exception : ' + str(e))
    send_vol_ha_status(VOL_STATUS_UNKNOWN)
    send_enable_ha_status(amcurl, uuid, resuuid, "false", "true")
    exit(1)

# Sometimes, when Enable HA for multiple nodes at the same time
# Node is still in maintenance status and resources are unmanaged after Enabled HA
# 'crm node ready' will resolve this corner case and would not affect normal case
ha_set_ready_mode()

if resuuid != None:
    # Set location for this resource
    ha_set_location(resuuid)
    # Save Raid1 Primary Info
    MdStatMgr.push_local_to_remote()

if not non_usxm_mode:
    send_vol_ha_status(VOL_STATUS_OK)

    (ret, msg) = send_enable_ha_status(amcurl, uuid, resuuid, "true", "false")
    if ret != 0:
        debug('WARN: failed to update HA flag, will try later')
        cmd = 'touch /var/log/set_ha_flag'
        (ret, msg) = runcmd(cmd, print_ret=True)

    if is_hyperconverged():
        debug("INFO: change crash dump file location for HyperConeverged Volume or HA VM")
        change_crash_file_location()

cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc enable_end'
(ret, msg) = runcmd(cmd, print_ret=True)
debug('INFO: done with enabling HA')
exit(0)
