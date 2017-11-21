#!/usr/bin/python

from ha_util import *
import httplib
from time import sleep
import hashlib
import base64
import zlib

sys.path.insert(0, '/opt/milio/atlas/roles')
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

##################################################################
# Util functions                                                 #
##################################################################

##################################################################
# TODO: This function is copied from milio/atlas/bootstrap.py to #
# speed up the coding. We need to optimize it later.             #
##################################################################

set_log_file('/var/log/usx-atlas-ha.log')
PACEMAKER_RSC_LIST = '/tmp/pacemaker_rsc.list'

def retryfunc(func, *args):
    cnt = 0
    while cnt < PCMK_DEFAULT_TRYNUM:
        confret = func(*args)
        if confret == PCMK_RES_TASK_DONE: #PCMK_RES_FOUND:
            # resource configured, happily return
            debug('configure %s done at retry count %d' % (func.__name__, cnt))
            return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
        elif confret != CLUS_SUCCESS:
            # configure failed
            return confret
        else: #confret == CLUS_SUCCESS
            cnt += 1
            # if we have multiple conflicts, try to sleep randomly longer
            time.sleep(random.randint(1, 5))
    # Now cnt == PCMK_DEFAULT_TRYNUM
    debug('configure %s retry count %d exceeds the retry limit' % (func.__name__, cnt))
    return PCMK_RETRY_EXCEED_LIMIT


##################################################################
#  Hard coded constants                                          #
##################################################################

PCMK_DEFAULT_TRYNUM = 120
# delay to wait for pacemaker is fully up.
PCMK_START_DELAY = 4

# The ads score that enables failback
#ADS_DEFAULT_STICKINESS_SCORE = 0
#ADS_LOCATION_SCORE = ADS_DEFAULT_STICKINESS_SCORE + 10   #10
# The ads score that disables failback
ADS_DEFAULT_STICKINESS_SCORE = 10000000
ADS_DEFAULT_MIGRATION_THRESHOLD = 5
#ADS_LOCATION_SCORE = ADS_DEFAULT_STICKINESS_SCORE - 10   #10
#ADS_LOCATION_SCORE = 20

##################################################################
#  Hard coded configure parameters, they will be put in to the   #
#  code after drop 1                                             #
##################################################################

ATLAS_CONF = '/etc/ilio/atlas.json'
RAID_CONF = '/etc/ilio/pool_infrastructure_*.json'
ILIO_PCMK_MON = '-E /opt/milio/atlas/roles/ha/ilio_pcmk_mon.sh'
ILIO_MON_TYPE = 'ocf:pacemaker:ClusterMon'
ILIO_MON_TMPFILE = '/tmp/iliomon.conf'

ADS_MUTUAL_EXCLUSIVE_RULE_NAME = 'single_dedup_one_node'
ADS_MUTUAL_EXCLUSIVE_SCORE = '-inf'
# make sure the exclusive score is bigger than the stickness score, so that when
# new nodes are available, the resources that are running on the same node can move
# to new available nodes.
ADS_RES_TYPE = 'ocf:heartbeat:ADS'
ADS_RES_OCF_NAME = 'ADS'
ADS_VSCALER_RES_TYPE = 'ocf:heartbeat:atl-vscaler'
ADS_VSCALER_RES_OCF_NAME = 'atl-vscaler'
ADS_NFS_RES_TYPE = 'ocf:heartbeat:nfsserver'
ADS_NFS_RES_OCF_NAME = 'nfsserver'
ADS_ISCSI_TARGET_RES_TYPE = 'ocf:heartbeat:atl-SCSTTarget'
ADS_ISCSI_TARGET_RES_OCF_NAME = 'atl-SCSTTarget'
ADS_ISCSI_LUN_RES_TYPE = 'ocf:heartbeat:atl-SCSTLun'
ADS_ISCSI_LUN_RES_OCF_NAME = 'atl-SCSTLun'
ADS_DEDUP_RES_TYPE = 'ocf:heartbeat:dedup-filesystem'
ADS_DEDUP_RES_OCF_NAME = 'dedup-filesystem'

MNT_POINT = None
vscaler_mode = None
mnttab = "/etc/ilio/mnttab"
DELIMITER = "}}##0##{{"

VSCALER_BASENAME = 'vmdata_cache'
CACHEDEV_BASENAME = '/dev/mapper/vmdata_cache'

ADS_VSCALER_RES_NAME = 'atl_vscaler'
ADS_DEDUP_RES_NAME = 'atl_dedup'
ADS_IP_RES_NAME = 'atl_shared_ip'
ADS_NFS_RES_NAME = 'atl_nfs'
NFS_SHARED_INFODIR = '/var/atl_ha/exports'
ADS_ISCSI_TARGET_RES_NAME = 'atl_iscsi_target'
ADS_ISCSI_LUN_RES_NAME = 'atl_iscsi_lun'
DEFAULT_IQN = 'iqn.com.atlantiscomputing.usx'

RARUNDIR='/run/resource-agents'

# Global variables
node_dict = None
haconf_dict = None
sharedip = None
vcip = None
privatenetmask = None
role = None

DEFAULT_MOUNT_OPTIONS = ("rw,noblocktable,noatime,nodiratime,timeout=180000,"
                         "dedupzeros,commit=30,thin_reconstruct,"
                         "data=ordered,errors=remount-ro")
DEFAULT_EXT4_MOUNT_OPTION = "noatime,nodiratime"

def is_mntopts_valid(mnt_opts):
    if mnt_opts and len(mnt_opts) and mnt_opts.strip():
        return True
    else:
        return False

def get_mntopts_from_resource(vvr_dict):
    mnt_opts = vvr_dict.get("volumemountoption")
    if not is_mntopts_valid(mnt_opts):
        mnt_opts = DEFAULT_MOUNT_OPTIONS

    is_journaled = vvr_dict.get("directio") # USX 2.0

    type_str = vvr_dict["volumetype"]
    if type_str.upper() in ["SIMPLE_MEMORY"]:
        is_inmem = True
    else:
        is_inmem = False

    if is_journaled:
        mnt_opts = mnt_opts + ",journaled"
    if is_inmem:
        mnt_opts = mnt_opts + ",inmem"

    return mnt_opts

def ha_init():
    global sharedip
    global ilio_dict
    global privatenetmask
    global role

    uuid = ilio_dict.get('uuid')
    if uuid is None:
	debug('Error getting Ilio Uid. HA will NOT be enabled for this node.')
	return(JSON_PARSE_EXCEPTION)
    if role == 'VOLUME':
    	amcfile = "/usxmanager/usx/inventory/volume/containers/" + uuid + '?composite=true'

    conn = httplib.HTTPConnection("127.0.0.1:8080")
    conn.request("GET", amcfile)
    res = conn.getresponse()
    data = res.read()
    debug(data)
    try:
	data = json.loads(data)
	if data is None:
	    debug('No Ilio DATA. HA will NOT be enabled.')
	    return(JSON_PARSE_EXCEPTION)
    except ValueError, e:
	debug('Exception checking network info. HA will NOT be enabled. Exception was: ' + str(e))
	return (JSON_PARSE_EXCEPTION)

    sharedip = virtualvol_res_dict[0].get('serviceip')
    if sharedip is None:
        debug('Error getting shared IP info. HA will NOT be enabled for this node.')
        return (JSON_PARSE_EXCEPTION)

    for nic in ilio_dict.get('nics'):
	if nic.get("storagenetwork") is True:
    	    privatenetmask = nic.get('netmask')
	    break
    if privatenetmask is None:
        debug('Error getting privatenetmask. HA will NOT be enabled for this node.')
        return (JSON_PARSE_EXCEPTION)

    debug('sharedip=' + sharedip + ' privatenetmask=' + privatenetmask)
    return (CLUS_SUCCESS)

# all the configure functions are called at least twice. The reason is in crm, we cannot take
# the pacemaker config lock. If when we commit the configure change, there is another commit
# transaction running at the same time, crm only gives a warning that this commit may not be
# written, and return a success code. Thus, all the configure functions have the check(crm
# configure show) to verify the resource exists, then configure it. And we call these functions
# multiple times until we the configure show displays the resource.

def configure_quorum_policy():

# TODO: hostlist only contain the nodes that pacemaker can detect, we may have
#       bugs when there are offline nodes during configuration.
# Configure quorum policy

    (ret, hostlist) = fetch_hostlist()
    if ret == PCMK_HOSTLIST_EMPTY:
        # give the cluster a chance to retry, especially for the first time
        # that pacemaker starts up.
        return CLUS_SUCCESS
    elif ret != CLUS_SUCCESS:
        return ret

    if is_stretchcluster_or_robo_raw():
        #TODO: should handle preferavailability
        quorum_policy = 'freeze'
    else:
        quorum_policy = 'ignore'
    '''
    quorum_policy = 'freeze'
    curr_hostnum = len(hostlist)
    if curr_hostnum <= 2:
        quorum_policy = 'ignore'
    '''

    # TODO: when the cluster only has one node, manually set the no-quorum-policy=ignore always
    #       succeed, however, when bootstrap calls this script, we always get error like following.
    #       We need to fix this bug.
    #       2013-12-02 16:51:50,873 Running: crm configure property no-quorum-policy=ignore
    #       2013-12-02 16:52:21,192  -> 0: None: Call cib_replace failed (-41): Remote node did not respond
    #       <null>
    #       ERROR: could not replace cib
    #       INFO: offending xml: <configuration>
           #       <crm_config>
                    #       <cluster_property_set id="cib-bootstrap-options">
                            #       <nvpair id="cib-bootstrap-options-no-quorum-policy" name="no-quorum-policy" value="ignore"/>
                    #       </cluster_property_set>
            #       </crm_config>
            #       <nodes/>
            #       <resources/>
            #       <constraints/>
    #       </configuration>

    needset = False
    cmd = 'crm_attribute --type crm_config --name no-quorum-policy --query'
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 6:
        needset = True
    else:
        policy_str = 'name=no-quorum-policy value='+quorum_policy
        msgindex = msg.find(policy_str)
        if msgindex >= 0:
            # the policy has been set correctly
            #debug('node number is %d, configure quorum policy %s done' %(curr_hostnum, quorum_policy))
            return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
        else:
            # the policy is different
            needset = True

    if needset:
        remove_watchdog()
        cmd = 'crm_attribute --type crm_config --name no-quorum-policy --update ' + quorum_policy
        (ret, msg) = ha_retry_cmd2(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

        if ret != 0:
	    if msg.find('Update was older than existing configuration') >= 0:
		return CLUS_SUCCESS
            debug('ERROR : fail to run %s, err=%s, msg=%s' % (cmd, str(ret),
                  msg))
            return (PCMK_SET_QUORUM_FAIL)
    return PCMK_RES_TASK_DONE

def resource_is_started(res):
	cmd = 'crm resource status ' + res
	(ret, msg) = runcmd(cmd, print_ret=True, lines=True)
	for line in msg:
		if line.find(res) >= 0 and line.find('is running') >= 0:
			return True
	return False


def remove_watchdog():
    cmd = 'crm_attribute --type crm_config --name have-watchdog --delete'
    (ret, msg) = ha_retry_cmd(cmd, 2, 2)
    return ret


def disable_stonith_device():
    cmd = 'crm configure property stonith-enabled=false'
    (ret, msg) = ha_retry_cmd2(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)
    return ret


def fetch_res_group():

    res_group_list = []

    # get the resource group names in the current cluster
    cmd = 'crm_resource --list'
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret != 0:
        # Fail to get resource list
        return (PCMK_LIST_RESOURCE_FAIL, res_group_list)

    # For resources from all the existing node, if its resource is in the
    # resource list, we add it to the mutual exclusive list.
    print cmd+'return:'+msg

    msglines = msg.split('\n')
    res_group_list = []
    for line in msglines:
        index = line.find('Resource Group:')
        if index >= 0:
            sublines = line.split(':')
            res_group_name = sublines[1].strip()
            res_group_list.append(res_group_name)
    debug('res_group_list='+str(res_group_list))
    return (CLUS_SUCCESS, res_group_list)

def configure_exclusive_loc(exclusive_rule_name, score):

    # get the current resource group names in the cluster
    (ret, curr_res_group) = fetch_res_group()
    if ret != 0:
       return ret

    new_res_group_list = []
    # get the resource group names in the configure
    # For ads node, the rule looks like
    #     colocation single_dedup_one_node -inf: test1_su-ads-hybrid-76_group test1_su-ads-mem-73_group

    cmd = 'crm configure show ' + exclusive_rule_name
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret != 0:
	new_res_group_list = curr_res_group
    else:
	if msg.find(exclusive_rule_name) < 0:
	    debug('no rule found for ' + exclusive_rule_name)
	    new_res_group_list = curr_res_group
        else:
	    res_groups = []
	    tmp = msg.split('xml')
	    if len(tmp) > 1:
	    	from xml.dom import minidom
		xmlfile = open('/tmp/rsc_colocation.xml', 'w')
	    	xmlfile.write(tmp[1])
		xmlfile.close()
		xmldoc = minidom.parse('/tmp/rsc_colocation.xml')
		res_groups = xmldoc.getElementsByTagName('resource_ref')[0].attributes['id'].value.split()
	    else:
		res_groups = msg.split(' ' + score + ': ')[1].split()

            missing_res_groups = []
            for res_group in curr_res_group:
                if not res_group in res_groups:
                    missing_res_groups.append(res_group)
            if len(missing_res_groups) == 0:
                debug('res_groups = (' + str(res_groups) + ') are the same as curr_res_group = (' + str(curr_res_group) +'), done configure the exclusive rule.')
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
            new_res_group_list = res_groups + missing_res_groups

            # need to remove the old one
            subcmd = 'crm configure delete ' + exclusive_rule_name
            (subret, submsg) = runcmd(subcmd, print_ret=True)
            if subret == 1:
                submsgindex = submsg.find('does not exist')
                if submsgindex < 0:
                    # if submsgindex>=0 means the resource has been deleted
                    return (PCMK_SET_CONFIGURE_FAIL)
            elif subret != 0:
                # Fail to set configure
                return (PCMK_SET_CONFIGURE_FAIL)

    # when we have no resource, no need to set the single dedup per node rule
    if len(new_res_group_list) == 0:
        debug ('resource group len = 0')
        return PCMK_RES_TASK_DONE #PCMK_RES_FOUND

    new_res_group_list_str = ' '.join(new_res_group_list)

    # now either there is no such rule, or the old rule is removed,
    # let us set the new rule

    cmd = 'crm configure colocation ' + exclusive_rule_name + \
	' ' + score + ': ' + new_res_group_list_str
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 1:
        # did not return PCMK_RES_TASK_DONE(PCMK_RES_FOUND), so that we can double check the resource
        return (CLUS_SUCCESS)
    elif ret != 0:
        # Fail to set configure
        return (PCMK_SET_CONFIGURE_FAIL)
    else: #ret == 0
        return (CLUS_SUCCESS)

# currently this resource monitor only reports the successful failover of vg resource group
# and ads resource group.
# TODO: make this a clone resource in future.
def configure_res_mon():
    iliomonname = 'iliomon'

    print 'in configure_res_mon, monitor script= ' + ILIO_PCMK_MON
    cmd = 'crm configure show ' + iliomonname
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(iliomonname)
        if msgindex >= 0:
            # the resource has been configured
            submsgindex = msg.find(ILIO_MON_TYPE)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
    else:
	# the resource has not been configured
	# this is to bypass a bug in crmsh, it cannot pass "-" as its option.
	montmpfile = open(ILIO_MON_TMPFILE, 'w')
	monconf = 'primitive ' + iliomonname + ' ' \
		+ ILIO_MON_TYPE + ' params user="root" update="30" extra_options="' \
                + ILIO_PCMK_MON + '"' \
                + ' op monitor on-fail="restart" interval="10s" timeout="60s" ' \
                + ' op start timeout="60s" ' + ' op stop timeout="60s" meta target-role=started'
	montmpfile.write(monconf)
	montmpfile.close()
	subcmd = 'crm configure load update ' + ILIO_MON_TMPFILE
	(subret, submsg) = runcmd(subcmd, print_ret=True)
	#if subret != 0:
	#	# Fail to set the monitor resource
	#	return (PCMK_CONF_MON_FAIL)
	#else: #subret == 0
	#	return (CLUS_SUCCESS)

	# return CLUS_SUCCESS to allow retry
	return (CLUS_SUCCESS)

def configure_ads_vol_ds(adsname):

    print 'in configure_ads_vol_ds, adsname = ' + adsname
    adsname_ds = adsname + '_ds'
    cmd = 'crm configure show ' + adsname_ds
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_ds)
        if msgindex >= 0:
            # the resource has been configured
            submsgindex = msg.find(ADS_RES_OCF_NAME)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND

    # Store Resource JSON and Raid JSON in pacemaker
    cfgfile = open(ATLAS_CONF, 'r')
    resource_json = cfgfile.read()
    cfgfile.close()
    resource_json_encode = base64.encodestring(zlib.compress(resource_json)).replace('\n', '')
    raidCfgfile = open(RAID_CONF.replace('*', adsname), 'r')
    raid_json = raidCfgfile.read()
    raidCfgfile.close()
    raid_json_encode = base64.encodestring(zlib.compress(raid_json)).replace('\n', '')

    # the resource has not been configured
    subcmd = 'crm configure primitive ' + adsname_ds + ' ' \
    + ADS_RES_TYPE + ' params adsname=' + adsname + ' ' \
    + ' params resourceJson="' + resource_json_encode + '" ' \
    + ' params raidJson="' + raid_json_encode + '" ' \
    + ' op monitor interval="40s" timeout="60s" ' \
    + ' op start timeout="10800s" ' + ' op stop timeout="3600s"  '
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
        return (CLUS_SUCCESS)
    elif subret != 0:
        # Fail to set the ads datastore resource
        return (PCMK_CONF_ADSDS_FAIL)
    else: #subret == 0
        return (CLUS_SUCCESS)

def configure_ads_vol_ip(adsname):

    print 'in configure_ads_vol_ip, adsname = ' + adsname
    adsname_ip = adsname + '_ip'

    cmd = 'crm configure show ' + adsname_ip
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_ip)
        if msgindex >= 0:
            # TODO: we need to check the netmask as well
            # the resource has been configured
            submsgindex = msg.find(sharedip)
            if submsgindex < 0:
                # however the resource is with another IP
                return (PCMK_RESOURCE_PARAM_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
    # the resource has not been configured
    # calculate the cidr_netmask from netmask
    # For example, netmask 255.255.252.0 will be calculated to 22
    (subret, cidrmask) = netmask2cidrmask(privatenetmask)
    if (subret != CLUS_SUCCESS):
	return subret
    subcmd = 'crm configure primitive ' + adsname_ip \
	+ ' ocf:heartbeat:IPaddr2 ' + ' params ip=' + sharedip \
	+ ' cidr_netmask=' + str(cidrmask) \
	+ ' op monitor interval="20s" timeout="60s" ' \
	+ ' op start timeout="60s" ' + ' op stop timeout="60s"  '
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	# Fail to set the shared ip
	return (PCMK_CONF_ADSIP_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)

def configure_ads_vol_vscaler(adsname, cache_dev):

    adsname_vscaler = adsname + '_' + ADS_VSCALER_RES_NAME
    print 'in configure_ads_vol_vscaler, adsname = ' + adsname + ' adsname_vscaler = ' \
        + adsname_vscaler + ' cache_dev = ' + cache_dev

    global vscaler_mode
    cmd = 'crm configure show ' + adsname_vscaler
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_vscaler)
        if msgindex >= 0:
            submsgindex = msg.find(ADS_VSCALER_RES_OCF_NAME)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
    # the resource has not been configured
    subcmd = 'crm configure primitive ' + adsname_vscaler + ' ' \
	+ ADS_VSCALER_RES_TYPE + ' params cache_dev=' + cache_dev \
	+ ' mode=' + vscaler_mode \
	+ ' op monitor interval="20s" timeout="60s" ' \
	+ ' op start timeout="120s" ' + ' op stop timeout="3600s"  '
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	# Fail to set the vscaler
	return (PCMK_CONF_ADSVSCALER_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)

def configure_ads_vol_ddp(adsname, device, directio, is_sync):
    global MNT_POINT
    adsname_ddp = adsname + '_' + ADS_DEDUP_RES_NAME
    print 'in configure_ads_vol_ddp, adsname = ' + adsname + 'adsname_ddp = ' \
          + adsname_ddp + ' device = ' + device

    cmd = 'crm configure show ' + adsname_ddp

    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_ddp)
        if msgindex >= 0:
            submsgindex = msg.find(ADS_DEDUP_RES_OCF_NAME)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
    # the resource has not been configured
    if milio_settings.export_fs_mode == 'ext4':
        mount_opt = DEFAULT_EXT4_MOUNT_OPTION
    else:
        mount_opt = get_mntopts_from_resource(virtualvol_res_dict[0])
        mount_opt = '"' + mount_opt + '"'
    if is_sync:
        nfs_sync_mode = "sync"
    else:
        nfs_sync_mode = "async"
    nfs_options = '"*(rw,no_root_squash,no_subtree_check,insecure,nohide,fsid=1,%s)"' % nfs_sync_mode
    subcmd = 'crm configure primitive ' + adsname_ddp + ' ' \
        + ' ocf:heartbeat:dedup-filesystem params device=' + device \
        + ' directory="' + MNT_POINT + '" fstype="' + milio_settings.export_fs_mode + '" run_fsck="no" ' \
        + ' nfs_options=' + nfs_options \
        + ' options=' + mount_opt + ' op monitor interval="20s" ' \
        + 'timeout="60s" op start timeout="36000s" op stop ' \
        + 'timeout="3600s"  '

    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
        return (CLUS_SUCCESS)
    elif subret != 0:
        # Fail to set the vscaler
        return (PCMK_CONF_ADSDDP_FAIL)
    else: #subret == 0
        return (CLUS_SUCCESS)

def configure_ads_vol_nfs(adsname, sharedip):

    adsname_nfs = adsname + '_' + ADS_NFS_RES_NAME
    print 'in configure_ads_vol_nfs, adsname = ' + adsname + ', adsname_nfs = ' + adsname_nfs

    cmd = 'crm configure show ' + adsname_nfs
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_nfs)
        if msgindex >= 0:
            submsgindex = msg.find(ADS_NFS_RES_OCF_NAME)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
    # the resource has not been configured
    subcmd = 'crm configure primitive ' + adsname_nfs + ' ' + ADS_NFS_RES_TYPE \
	+ ' params nfs_init_script="/etc/init.d/nfs-kernel-server" nfs_ip="' + sharedip \
	+ '" nfs_shared_infodir="' + NFS_SHARED_INFODIR + '" op monitor interval="20s" ' \
	+ ' timeout="120s" op start timeout="120s" op stop timeout="120s"  '
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	# Fail to set the nfs
	return (PCMK_CONF_ADSNFS_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)

def configure_ads_vol_iscsi_target(adsname):

    adsname_iscsi_target = adsname + '_' + ADS_ISCSI_TARGET_RES_NAME
    adsname_iscsi_iqn = DEFAULT_IQN + ':' + re.sub('[^A-Za-z0-9-]', '-', adsname)

    debug ('in configure_ads_vol_iscsi_target, adsname = ' + adsname + ', adsname_iscsi_target = ' + adsname_iscsi_target + ' adsname_iscsi_iqn = ' + adsname_iscsi_iqn)

    cmd = 'crm configure show ' + adsname_iscsi_target
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_iscsi_target)
        if msgindex >= 0:
            submsgindex = msg.find(ADS_ISCSI_TARGET_RES_OCF_NAME)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND

    # the resource has not been configured
    subcmd = 'crm configure primitive ' + adsname_iscsi_target + ' ' \
	+ ADS_ISCSI_TARGET_RES_TYPE + ' params iqn="' + str(adsname_iscsi_iqn) \
	+ '" op monitor interval="20s" timeout="120s" op start timeout="120s"' \
	+ ' op stop timeout="120s"  '
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	# Fail to set the target
	return (PCMK_CONF_ADS_ISCSI_TARGET_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)

def configure_ads_vol_iscsi_lun(adsname, mntpnt):

    adsname_iscsi_lun = adsname + '_' + ADS_ISCSI_LUN_RES_NAME
    adsname_iscsi_iqn = DEFAULT_IQN + ':' + re.sub('[^A-Za-z0-9-]', '-', adsname)

    debug('in configure_ads_vol_iscsi_lun, adsname = ' + adsname + ', adsname_iscsi_lun = ' + adsname_iscsi_lun + ' adsname_iscsi_iqn = ' + adsname_iscsi_iqn + ' mntpnt = ' + mntpnt)

    cmd = 'crm configure show ' + adsname_iscsi_lun
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        msgindex = msg.find(adsname_iscsi_lun)
        if msgindex >= 0:
            submsgindex = msg.find(ADS_ISCSI_LUN_RES_OCF_NAME)
            if submsgindex < 0:
                # however the resource is not right type
                return (PCMK_RESOURCE_TYPE_ERR)
            else:
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND

    # iSCSI Target names need to be unique per Volume Resource, so that one ESXi host
    # can mount multiple iSCSI exported USX Volumes as separate devices.
    # Create unique string with adsname([volumeresources][0][uuid]) and device basename
    devbasename = "LUN1"
    if adsname == "":
        dname = devbasename
    else:
        dname = "-".join([adsname, devbasename])
    # Get md5 hash on this unique string and use the last 16 characters
    dnamehash = hashlib.md5(dname).hexdigest()
    debug("iSCSI Target name to be registered to pacemaker: " + dnamehash[16:32])
    debug("It is derived from lower 16 char of md5sum of " + dname)

    # the resource has not been configured
    subcmd = 'crm configure primitive ' + adsname_iscsi_lun + ' ' \
	+ ADS_ISCSI_LUN_RES_TYPE + ' params device_name="' + dnamehash[16:32] + '" ' \
	+ 'target_iqn="' + adsname_iscsi_iqn + '" path="' + str(mntpnt) + '/LUN1" ' \
	+ 'handler="vdisk_fileio" lun="0" additional_parameters="nv_cache=1" ' \
	+ 'op monitor interval="20s" timeout="120s" op start timeout="120s" ' \
	+ 'op stop timeout="120s"  '

    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	# Fail to set the lun
	return (PCMK_CONF_ADS_ISCSI_LUN_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)

# generate the ads resource list based on the ads name, type, and export type.
def gen_ads_res_list(adsname, adstype, exporttype):

    adsname_ds = adsname + '_ds'
    adsname_ip = adsname + '_ip'
    adsname_vscaler = adsname + '_' + ADS_VSCALER_RES_NAME
    adsname_ddp = adsname + '_' + ADS_DEDUP_RES_NAME
    adsname_nfs = adsname + '_' + ADS_NFS_RES_NAME
    adsname_iscsc_target = adsname + '_' + ADS_ISCSI_TARGET_RES_NAME
    adsname_iscsc_lun = adsname + '_' + ADS_ISCSI_LUN_RES_NAME
    print 'in gen_ads_res_list, adsname = ' + adsname + ' adsname_ds = ' + adsname_ds + \
          ' adsname_ip = ' + adsname_ip + ' adsname_vscaler = ' + adsname_vscaler + \
          ' adsname_ddp ' + adsname_ddp + ' adsname_nfs = ' + adsname_nfs + \
          ' adstype = ' + adstype + ' exporttype = ' + exporttype + \
          ' adsname_iscsc_target = ' + adsname_iscsc_target + ' adsname_iscsc_lun = ' + adsname_iscsc_lun

    # the possible ads resource lists
    # hybrid/nfs: ads_res_list = [adsname_ds, adsname_vscaler, adsname_ddp, adsname_nfs, adsname_ip]
    # hybrid/iscsc: ads_res_list = [adsname_ds, adsname_vscaler, adsname_ddp, adsname_iscsc_target, adsname_iscsc_lun, adsname_ip]
    # other/nfs:  ads_res_list = [adsname_ds, adsname_ddp, adsname_nfs, adsname_ip]
    # other/iscsc: ads_res_list = [adsname_ds, adsname_ddp, adsname_iscsc_target, adsname_iscsc_lun, adsname_ip]
    ads_res_list = [adsname_ds]
    if adstype.lower() == 'hybrid_deprecate':
        ads_res_list.append(adsname_vscaler)

    ads_res_list.append(adsname_ddp)
    if exporttype.lower() == 'nfs':
        ads_res_list.append(adsname_nfs)
    elif exporttype.lower() == 'iscsi':
        ads_res_list.append(adsname_iscsc_target)
        ads_res_list.append(adsname_iscsc_lun)
    else:
        debug('Error in configure_ads_vol_group: export type ' + exporttype + ' is not supported')
        return (PCMK_CONF_ADS_EXPORT_NOT_SUPPORTED, [])

    ads_res_list.append(adsname_ip)
    debug('In gen_ads_res_list, ads_res_list=' + str(ads_res_list))
    return (CLUS_SUCCESS, ads_res_list)


def configure_ads_vol_group(adsname, adstype, exporttype):

    ads_res_group = adsname + '_group'
    (subret, ads_res_list) = gen_ads_res_list(adsname, adstype, exporttype)
    if subret != CLUS_SUCCESS:
	return subret
    cmd = 'crm configure show ' + ads_res_group
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
    	msgindex = msg.find(ads_res_group)
	if msgindex >= 0:
	    # the resource has been configured
	    # TODO: need to add the check for the resource order
	    for res in ads_res_list:
		submsgindex = msg.find(res)
		if submsgindex < 0:
		    debug('Error in finding ADS resource %s' % res)
		    return (PCMK_RESOURCE_PARAM_ERR)
	    # all resources are there
	    return PCMK_RES_TASK_DONE #PCMK_RES_FOUND

    group_res = ' '.join(ads_res_list)
    subcmd = 'crm configure group ' + ads_res_group + ' ' + group_res + ' meta target-role=started'
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	return (PCMK_CONF_ADSRES_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)


def configure_ads_vol_location(adsname):

    ads_res_group = adsname + '_group'
    ads_res_group_loc = ads_res_group + '_loc'
    print 'in configure_ads_vol_location, ads_res_group = ' + ads_res_group \
        + ' ads_res_group_loc = ' + ads_res_group_loc
    cmd = 'crm configure show ' + ads_res_group_loc
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
    	msgindex = msg.find(ads_res_group)
	if msgindex >= 0:
	    # the resource has been configured
	    # TODO: need to add the check for the resource order
	    submsgindex = msg.find(ads_res_group)
	    if submsgindex < 0:
		# however the resource is with another ads
		return (PCMK_RESOURCE_PARAM_ERR)
	    # all resources are there
	    return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
    subcmd = 'crm configure location ' + ads_res_group_loc + ' ' + ads_res_group \
	+ ' ' + str(ADS_LOCATION_SCORE) + ': ' + socket.gethostname()
    (subret, submsg) = runcmd(subcmd, print_ret=True)
    if subret == 1:
	return (CLUS_SUCCESS)
    elif subret != 0:
	# Fail to set the ads group
	return (PCMK_CONF_POOLLOC_FAIL)
    else: #subret == 0
	return (CLUS_SUCCESS)

def start_ads_vol_resources(adsname, adstype, exporttype):
    (ret, ads_res_list) = gen_ads_res_list(adsname, adstype, exporttype)
    if ret != CLUS_SUCCESS:
        return ret
    for res in ads_res_list:
        (ret, submsg) = runcmd('touch /var/run/resource-agents/' + res, print_ret=True)
        if resource_is_started(res):
            debug('INFO: first check volume resources, already up, skip start %s resource' % res)
            continue

        cnt = 2
        res_running = False
        (ret, submsg) = runcmd('touch /var/run/resource-agents/' + res, print_ret=True)
        while cnt > 0:
            debug('INFO: cleanup %s resource' % res)
            (ret, submsg) = runcmd('crm resource cleanup ' + res, print_ret=True)
            time.sleep(5)
            (ret, submsg) = runcmd('touch /var/run/resource-agents/' + res, print_ret=True)
            # skip crm resource stop during init if the resource is already up
            if resource_is_started(res):
                debug('INFO: check volume resources again, already up, skip start %s resource' % res)
                res_running = True
                break
            cnt = cnt - 1
        if res_running == True:
            continue

	cmd = 'crm resource start ' + res
	(ret, submsg) = runcmd(cmd, print_ret=True)
	cnt = 10
	while cnt > 0 and ret != 0:
	    (ret, submsg) = runcmd('crm resource cleanup ' + res, print_ret=True)
	    time.sleep(5)
	    cnt = cnt - 1
	    (ret, submsg) = runcmd(cmd, print_ret=True)
	if cnt == 0 and ret != 0:
	    # Fail to start a resource
	    debug('Error starting %s resource' % res)
	    return (PCMK_START_RESOURCE_FAIL)
	cnt = 6*125
	while cnt > 0 and not resource_is_started(res):
	    time.sleep(5)
	    cnt = cnt - 1
	    if cnt % 10 == 0:
		(ret, submsg) = runcmd('crm resource cleanup ' + res, print_ret=True)
		(ret, submsg) = runcmd(cmd, print_ret=True)
	if not resource_is_started(res):
            debug('Error starting %s resource' % res)
            return (PCMK_START_RESOURCE_FAIL)
    return PCMK_RES_TASK_DONE #PCMK_RES_FOUND


def configure_virtual_vol_resource():
    # TODO: Ask PM if we need to do failback

    global sharedip
    global vcip
    global privatenetmask
    global MNT_POINT
    global ilio_dict

    current_index = 0
    mnttab_content = []
    (ret, submsg) = runcmd('crm node maintenance', print_ret=True)

    adstype = virtualvol_res_dict[0].get('volumetype')
    if adstype is None:
        debug('Error getting ADS type. HA will NOT be enabled for this node.')
        return (JSON_PARSE_EXCEPTION)

    exporttype = virtualvol_res_dict[0].get('exporttype')
    if exporttype is None:
        debug('Error getting export type. HA will NOT be enabled for this node.')
        return (JSON_PARSE_EXCEPTION)
    print 'exporttype=('+exporttype+')'

    directio = virtualvol_res_dict[0].get('directio')
    if directio is None:
        debug('Error getting ADS journaled option. HA will NOT be enabled for this node.')
        return (JSON_PARSE_EXCEPTION)

    # Infrastructure volume use *sync* NFS export.
    is_sync = virtualvol_res_dict[0].get('infrastructurevolume')
    if not is_sync:
        is_sync = ilio_dict.get('infrastructurevolume')
    # directio always implies sync export mode
    if directio or virtualvol_res_dict[0].get('fs_sync', False):
        is_sync = True

    # configure the ads resource
    adsname = virtualvol_res_dict[0].get('uuid')
    if adsname is None:
        debug('Error getting ADS name. HA will NOT be enabled for this node.')
        return (JSON_PARSE_EXCEPTION)

    (ret, ads_res_list) = gen_ads_res_list(adsname, adstype, exporttype)
    for res in ads_res_list:
        cmd = 'crm configure show ' + res
        (ret, msg) = runcmd(cmd, print_ret=True)
        if ret == 0:
            cmd = 'crm resource maintenance ' + res
            (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

            #cmd = 'crm resource cleanup ' + res
            #(ret, msg) = runcmd(cmd, print_ret=True)

            cmd = 'crm resource unmanage ' + res
            (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

            cmd = 'crm configure delete ' + res
            (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    ads_res_group = adsname + '_group'
    cmd = 'crm configure show ' + ads_res_group
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        cmd = 'crm configure delete ' + ads_res_group
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    ads_res_group_loc = ads_res_group + '_loc'
    cmd = 'crm configure show ' + ads_res_group_loc
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0:
        cmd = 'crm configure delete ' + ads_res_group_loc
        (ret, msg) = ha_retry_cmd(cmd, HA_RETRY_TIMES, HA_SLEEP_TIME)

    # fake the ha pseudo resource
    for res in ads_res_list:
        (ret, submsg) = runcmd('touch /var/run/resource-agents/' + res, print_ret=True)

    ret = retryfunc(configure_ads_vol_ds, adsname)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        return ret

    # The following code are originally from ddp_setup.py
    ddp_dev = None
    cache_dev = None

    if os.path.isfile(mnttab):
        with open(mnttab) as f:
            mnttab_content = f.readlines()
        if len(mnttab_content) > current_index:
            read_list = mnttab_content[current_index].split(DELIMITER)
            if len(read_list) >= 5:
                ddp_dev = read_list[0]
                cache_dev = read_list[1]
    if not (ddp_dev or cache_dev):
        debug("Error reading mnttab file: device not found\n")
        return PCMK_READ_MNTTAB_FAIL

    cmd = 'mkdir -p ' + NFS_SHARED_INFODIR
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret != 0:
        # cluster is not healthy, cannot get the configure info
        return (PCMK_CREATE_NFS_SHARED_INFODIR_FAIL)

    if adstype.lower() == "hybrid_deprecate":
        ret = retryfunc(configure_ads_vol_vscaler, adsname, VSCALER_BASENAME)
        if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
            return ret
        ret = retryfunc(configure_ads_vol_ddp, adsname, CACHEDEV_BASENAME, directio, is_sync)
    else:
        ret = retryfunc(configure_ads_vol_ddp, adsname, ddp_dev, directio, is_sync)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        return ret

    if exporttype.lower() == 'nfs':
        ret = retryfunc(configure_ads_vol_nfs, adsname, sharedip)
        if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
            return ret
    elif exporttype.lower() == 'iscsi':
        ret = retryfunc(configure_ads_vol_iscsi_target, adsname)
        if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
            return ret
        ret = retryfunc(configure_ads_vol_iscsi_lun, adsname, MNT_POINT)
        if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
            return ret
    else:
        debug('Error: export type ' + exporttype + ' is not supported')
        return PCMK_CONF_ADS_EXPORT_NOT_SUPPORTED

    ret = retryfunc(configure_ads_vol_ip, adsname)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        return ret

    ret = retryfunc(configure_ads_vol_group, adsname, adstype, exporttype)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        return ret

#    ret = retryfunc(configure_ads_vol_location, adsname)
#    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
#        return ret

    # We are no longer using colocation to provide exclusive resource.
    # Instead we use resource utilization, one vol will occupy all CPUs of a node.
    #ret = retryfunc(configure_exclusive_loc, ADS_MUTUAL_EXCLUSIVE_RULE_NAME, ADS_MUTUAL_EXCLUSIVE_SCORE)
    #if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
    #    return ret

    # fake the ha pseudo resource again before start volume resources
    (ret, ads_res_list) = gen_ads_res_list(adsname, adstype, exporttype)
    for res in ads_res_list:
        (ret, submsg) = runcmd('touch /var/run/resource-agents/' + res, print_ret=True)
    # write down the resoure list
    ads_res_group = adsname + '_group'
    with open(PACEMAKER_RSC_LIST, "w") as fd:
        if len(ads_res_list) > 0:
            fd.write("%s\n" % ads_res_group)
            for res in reversed(ads_res_list):
                fd.write("%s\n" % res)

    (ret, submsg) = runcmd('crm node ready', print_ret=True)
    ret = retryfunc(start_ads_vol_resources, adsname, adstype, exporttype)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        return ret

    return retryfunc(configure_res_mon)

def configure_stickiness_score(stickiness_score):

    if stickiness_score == None:
        return (PCMK_CONF_WRONG_STICKINESS_SCORE)
    print 'in configure_stickiness_score, stickiness_score = ' + str(stickiness_score)

    needsetscore = False
    cmd = 'crm_attribute --type rsc_defaults --name resource-stickiness --query'
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 6:
        # If the attribute is not set, the crm_attribute return error code 6.
        # If in future, pacemaker changes its return, we need to change this part as well.
        needsetscore = True
    else:
        msgindex = msg.find('name=resource-stickiness')
        if msgindex >= 0:
            # the resource has been configured
            submsgindex = msg.find('value='+str(stickiness_score))
            if submsgindex < 0:
                # however the score is different
                # TODO: in future, just delete this score and reconfig is with the new one?
                debug('stickiness_score exists, but different from the new one ' + str(stickiness_score))
                needsetscore = True
                #return (PCMK_RESOURCE_PARAM_ERR)
            else:
                # all resources are there
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
        else:
            needsetscore = True
            # msgindex < 0
            # the resource has not been configured

    if needsetscore:
        subcmd = 'crm configure rsc_defaults resource-stickiness=' + str(stickiness_score)
        (subret, submsg) = runcmd(subcmd, print_ret=True)
        if subret == 1:
            submsgindex = submsg.find('id is already in use')
            # did not return PCMK_RES_TASK_DONE(PCMK_RES_FOUND), so that we can double check the ip address
            return (CLUS_SUCCESS)
        elif subret != 0:
            # Fail to set the vg group
            return (PCMK_SET_PROPERTY_FAIL)
        else: #subret == 0
            return (CLUS_SUCCESS)

def configure_migration_threshold(migration_threshold):

    if migration_threshold == None:
        return (PCMK_CONF_WRONG_MIGRATION_THRESHOLD)
    print 'in configure_migration_threshold, migration_threshold = ' + str(migration_threshold)

    needsetthreshold = False
    cmd = 'crm_attribute --type rsc_defaults --name migration-threshold --query'
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 6:
        # If the attribute is not set, the crm_attribute return error code 6.
        # If in future, pacemaker changes its return, we need to change this part as well.
        needsetthreshold = True
    else:
        msgindex = msg.find('name=migration-threshold')
        if msgindex >= 0:
            # the resource has been configured
            submsgindex = msg.find('value='+str(migration_threshold))
            if submsgindex < 0:
                # however the score is different
                # reconfig is with the new one
                debug('migration_threshold exists, but different from the new one ' + str(migration_threshold))
                needsetthreshold  = True
            else:
                # all resources are there
                return PCMK_RES_TASK_DONE #PCMK_RES_FOUND
        else:
            needsetthreshold  = True
            # msgindex < 0
            # the resource has not been configured

    if needsetthreshold :
        subcmd = 'crm configure rsc_defaults migration-threshold=' + str(migration_threshold)
        (subret, submsg) = runcmd(subcmd, print_ret=True)
        if subret == 1:
            submsgindex = submsg.find('id is already in use')
            # did not return PCMK_RES_TASK_DONE(PCMK_RES_FOUND), so that we can double check the ip address
            return (CLUS_SUCCESS)
        elif subret != 0:
            # Fail to set the property
            return (PCMK_SET_PROPERTY_FAIL)
        else: #subret == 0
            return (CLUS_SUCCESS)

def configure_ha_ads():
    return

def fetch_hostlist():
    hlist = []
    # Get the current list of nodes in the cluster
    cmd = 'crm_node -l'
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret != 0:
        debug('ERROR : fail to run %s, err=%s, msg=%s' % (cmd, str(ret),
              msg))
        return (PCMK_GET_HOSTLIST_FAIL, hlist)

    # The node list output look like below.
    # np-ha-ads-66 # crm node list
    # np-ha-ads-66: normal
    # np-ads-optimized-72: normal
    # np-ads-hybrid-58: normal
    # np-ads-optimized-101: normal

    msglines = msg.split('\n')
    for line in msglines:
        nodeinfo = line.split(' ')
        if len(nodeinfo) == 2:
            hlist.append(nodeinfo[1])

    if len(hlist) <= 0:
        debug('ERROR : hostlist in this cluster is empty')
        return (PCMK_HOSTLIST_EMPTY, hlist)
    print 'hlist='+str(hlist)
    return (CLUS_SUCCESS, hlist)

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


def set_res_loc_preference(adsname):
	node_name = get_local_node_name()
	if node_name is None:
		return 1
	cmd = 'crm configure show | grep location | grep -v grep'
	(ret, msg) = runcmd(cmd, print_ret=False, lines=True)
	for line in msg:
		tmp = line.split()
		if len(tmp) < 5:
			continue
		if tmp[4] == node_name:
			cmd = 'crm configure delete ' + tmp[1]
			(ret, submsg) = runcmd(cmd, print_ret=True)
			break
	for line in msg:
		tmp = line.split()
		if len(tmp) < 5:
			continue
		if tmp[1] and tmp[1].startswith(adsname):
			cmd = 'crm configure delete ' + tmp[1]
			(ret, submsg) = runcmd(cmd, print_ret=True)
			cmd = 'crm configure location ' + tmp[1] + ' ' + tmp[2] + ' ' + tmp[3] + ' ' + node_name
			(ret, submsg) = runcmd(cmd, print_ret=True)
			break
	return 0

##################################################################
#                   START HERE                                   #
##################################################################

if len(sys.argv) == 2:
	rsc = sys.argv[1]
	exit(set_res_loc_preference(rsc))

cfgfile = open(ATLAS_CONF, 'r')
s = cfgfile.read()
cfgfile.close()

try:
    node_dict = json.loads(s)
    if node_dict is None:
        sys.exit(CLUS_SUCCESS)  # stop if this is not a ha node
except:
    debug('Exception checking whether HA or not, HA will NOT be enabled. Exception was: %s'
           % sys.exc_info()[0])
    sys.exit(JSON_PARSE_EXCEPTION)

ilio_dict = node_dict.get('usx')
if ilio_dict is None:
    debug('Error getting Ilio info')
    sys.exit(JSON_PARSE_EXCEPTION)

#ha_enabled = ilio_dict.get('ha')
#if ha_enabled is None:
#    debug('Error checking whether HA exists or HA enabled is false')
#    sys.exit(JSON_PARSE_EXCEPTION)

#if ha_enabled is False:
#    sys.exit(JSON_PARSE_EXCEPTION)

virtualvol_res_dict = node_dict.get('volumeresources')
if virtualvol_res_dict is None:
    debug('Error getting Virtual Volume Resources. HA will NOT be enabled for this node.')
    sys.exit(JSON_PARSE_EXCEPTION)

# check the roles
roles = ilio_dict.get('roles')
if roles is None or len(roles) == 0:
    debug('Error getting role information. HA will NOT be enabled for this node')
    sys.exit(JSON_PARSE_EXCEPTION)
role = roles[0]

haconf_dict = node_dict.get('haconfig')
if haconf_dict is None:
    debug('Error getting HA information. HA will NOT be enabled for this node.')
    sys.exit(JSON_PARSE_EXCEPTION)

##################################################################
#  Check whether corosync is started, and start pacemaker        #
##################################################################

# Make sure corosync has started already

count = 8
cmd = 'service corosync status'
pcmkrunning = False
while count > 0:
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0 and msg.find('is running') >= 0:
        cnt = 24
        while cnt > 0:
            # restart pacemaker in case it has been started
            (ret, msg) = runcmd('service pacemaker restart', print_ret=True)
            time.sleep(1)
            (ret, msg) = runcmd('service pacemaker status', print_ret=True)
            if ret == 0 and msg.find('is running') >= 0:
                debug('Pacemaker started')
                pcmkrunning = True
                break
            time.sleep(5)
            cnt -= 1
    else:
        # restart corosync just in case
        (ret, msg) = runcmd('service corosync restart', print_ret=True)

    if pcmkrunning is True:
        break
    time.sleep(4)
    count -= 1

if count == 0:
    debug('ERROR : corosync is not running, err=%s, msg=%s' % (str(ret), msg))
    sys.exit(CORO_NOT_RUNNING)

if pcmkrunning != True:
    debug('ERROR : fail to start pacemaker, err=%s, msg=%s' % (str(ret), msg))
    sys.exit(PCMK_START_FAIL)

# add some delay for cluster to be up and running
time.sleep(PCMK_START_DELAY)

# TODO: this imports section should not matter, remove it now, if later we need it
#       uncomment the code.
#imports_dicts = node_dict.get('imports')
#if imports_dicts is None or len(imports_dicts) == 0:
    ## Empty imports mean this is a standby HA node.
    ## We are done.
    #debug('INFO : No import section. pacemaker setup for standby node done.')
    #sys.exit(CLUS_SUCCESS)

remove_watchdog()
disable_stonith_device()

if not os.path.exists(RARUNDIR):
    os.makedirs(RARUNDIR)

# remove orphan nodes and resources
cmd = 'python /opt/milio/atlas/roles/ha/ha_health_check.pyc remove_orphan '
(ret, msg) = runcmd(cmd, print_ret=True)

# Make pacemaker wait to configure until we have quorum
# crm_node
# -q, --quorum
#    Display a 1 if our partition has quorum, 0 if not

# Wait until there is at least one standby node
qcnt = 240 # timeout = 240 * 5 = 1200 sec; 20 min
HAS_QUORUM = False
if len(virtualvol_res_dict) == 0:
    (qcnt, HAS_QUORUM) = (0, True)
while qcnt > 0:
    if ha_has_standby_from_usxm():
        HAS_QUORUM = True
        break
    time.sleep(5)
    qcnt -= 1

if qcnt == 0 and HAS_QUORUM == False:
    debug("ERROR : No quorum presented, system timed out!")
    ha_disable_ha_flag()
    ha_set_maintenance_mode()
    sys.exit(1)

stickiness_score = None
migration_threshold = None
if role == 'VOLUME':
    if len(virtualvol_res_dict) == 0:
    	# empty function reserved for future change
	configure_ha_ads()
    	stickiness_score = ADS_DEFAULT_STICKINESS_SCORE
    else:
        if virtualvol_res_dict[0].has_key('dedupfsmountpoint'):
            MNT_POINT = virtualvol_res_dict[0]['dedupfsmountpoint']
        else: # get vm name out of uuid: yj-vc_yj-hyb-1-1413398521118. vm name: yj-hyb-1
            MNT_POINT = '/exports/' + virtualvol_res_dict[0].get('uuid').split('_')[-1].rsplit('-',1)[0]
	# get the sharedip/resource ip and its private network mask
	ret = ha_init()
	if (ret != CLUS_SUCCESS):
	    sys.exit(ret)
	if virtualvol_res_dict[0].get('volumetype').lower() == 'hybrid_deprecate':
	    if virtualvol_res_dict[0].get('fastsync') == False:
		vscaler_mode = 'thru'
	    else:
		vscaler_mode = 'back'
	ret = configure_virtual_vol_resource()
	if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
	    sys.exit(ret)
	stickiness_score = ADS_DEFAULT_STICKINESS_SCORE
    migration_threshold = ADS_DEFAULT_MIGRATION_THRESHOLD
    open('/run/ha_enabled_alert', 'a').close()
else:
    ha_set_ready_mode()
    open('/run/ha_enabled_alert', 'a').close()
    sys.exit(CLUS_SUCCESS)

remove_watchdog()
ret = retryfunc(configure_quorum_policy)
if (ret != PCMK_RES_TASK_DONE and len(virtualvol_res_dict) > 0): #PCMK_RES_FOUND
    debug('virtualvol_res_dict = %s, len = %d ' % (str(virtualvol_res_dict), len(virtualvol_res_dict)))
    debug('ERROR: Failed configure_quorum_policy: ret = %d' % ret)
    cmd = 'crm configure show '
    (subret, submsg) = runcmd(cmd, print_ret=True, lines=True)
    sys.exit(ret)

# TODO: add check to see whether the cluster properties has been set. If not
#       skip the setting. To query a property has been set, use
#       crm_attribute --type rsc_defaults --name resource-stickiness --query
# This code rely on the reset the same property again will not cause error.
# set the resource stickiness to prevent auto failback

if stickiness_score != None:
    ret = retryfunc(configure_stickiness_score, stickiness_score)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        sys.exit(ret)

if migration_threshold != None:
    ret = retryfunc(configure_migration_threshold, migration_threshold)
    if (ret != PCMK_RES_TASK_DONE): #PCMK_RES_FOUND
        sys.exit(ret)

# Put this node online in case it is HA re-enabled and in standby mode when it was HA disabled
# If the node is already online, this operation is harmless.
(ret, msg) = runcmd('crm node online', print_ret=False)

# The properties that we do not set and using the defaults
# TODO: List all the variables here. Should we explicitely set everything?
# expected-quorum-votes: leave it to the number of the nodes inside the cluster,
#                        so that we do not need to reset it after node join/leave.
# migration-limit: (default=-1 unlimited). If we find that migration of multiple
#                  VGs or ADS volumes will cause data consistency issue, we will
#                  set it to 1.
# no-quorum-policy: (default=stop, stop all resources in the nodes out-of-quorum).
#                  If later we find stop may not always succeed, we can change this
#                  to suicide.
#                  change it for ADS node after drop 1.
# Since we may not be the first one in the cluster, let's check whether the
# cluster has been configured with the resource that we want to add.

open('/run/pacemaker_started', 'a').close()

time.sleep(1)
remove_watchdog()
debug('INFO : pacemaker setup done.')
sys.exit(CLUS_SUCCESS)
