#!/usr/bin/python
import os,sys
import logging
import socket
from ha_util import *
from time import sleep

LOG_FILENAME = '/var/log/usx-atlas-ha.log'
LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'

# This script is used to monitor the resources in the cluster, configure it
# with ClusterMon. It can resport the resource status to data grid.
# Currently, we only report the failover event.

# Resources:
#  crm ra meta ocf:pacemaker:ClusterMon
#  man 8 crm_mon

# Sample configuration
# ================================
# primitive ClusterMon ocf:pacemaker:ClusterMon \
#        params user="root" update="30" extra_options="-E /path/to/ilio_ha_mon.py 
#        op monitor on-fail="restart" interval="10"
#
# clone ClusterMon-clone ClusterMon \
#        meta target-role="Started"
# ================================

# The external agent is fed with environment variables allowing us to know
# what transition happened and to react accordingly:
#  http://clusterlabs.org/doc/en-US/Pacemaker/1.1-crmsh/html/Pacemaker_Explained/s-notification-external.html
'''
def _send_alert_pcmk_mon(ilio_id, ha_res):
	if not os.path.isfile('/run/start_ha_alert'):
		return
	cmd = 'date +%s'
	(ret, epoch_time) = runcmd(cmd, print_ret=True)
	epoch_time = epoch_time.rstrip('\n')
	cfgfile = open('/etc/ilio/atlas.json', 'r')
	s = cfgfile.read()
	cfgfile.close()
	node_dict = json.loads(s)
	usx = node_dict.get('usx')
	usx_displayname = usx.get('displayname')

	ad = {
		"uuid"			:"",
		"checkId"		:"",
		"usxuuid"		:"",
		"value"			:0.0,
		"target"		:"",
		"warn"			:0.0,
		"error"			:0.0,
		"oldStatus"		:"OK",
		"status"		:"OK",
		"description"		:"Volume completed failover",
		"service"		:"HA",
		"alertTimestamp"	:"",
		"iliotype"		:"VOLUME"
	}

	ad["uuid"] = ilio_id + '-ha-alert-' + str(epoch_time)
	ad["checkId"] = ilio_id + '-ha'
	#ad["usxuuid"] = ilio_id
	ad["displayname"] = usx_displayname
	ad["usxuuid"] = ha_res
	ad["target"] = "servers." + ilio_id + ".ha"
	ad["alertTimestamp"] = epoch_time

	data = json.dumps(ad)
	cmd = 'curl -X POST -H "Content-type:application/json" ' + LOCAL_AGENT + 'alerts/ -d \'' + data + '\''
	(ret, out) = runcmd(cmd, print_ret=True, block=False)
'''

set_log_file(LOG_FILENAME)

# get all the notification variable from pacemaker

CRM_notify_recipient = os.environ.get('CRM_notify_recipient')
if CRM_notify_recipient is None:
    debug('CRM_notify_recipient = None')
    CRM_notify_recipient = 'None'

CRM_notify_node = os.environ.get('CRM_notify_node')
if CRM_notify_node is None:
    debug('CRM_notify_node = None')
    CRM_notify_node = 'None'

CRM_notify_rsc = os.environ.get('CRM_notify_rsc')
if CRM_notify_rsc is None:
    debug('CRM_notify_rsc = None')
    CRM_notify_rsc = 'None'

CRM_notify_task = os.environ.get('CRM_notify_task')
if CRM_notify_task is None:
    debug('CRM_notify_task = None')
    CRM_notify_task = 'None'

CRM_notify_desc = os.environ.get('CRM_notify_desc')
if CRM_notify_desc is None:
    debug('CRM_notify_desc = None')
    CRM_notify_desc = 'None'

CRM_notify_rc = os.environ.get('CRM_notify_rc')
if CRM_notify_rc is None:
    debug('CRM_notify_rc = None')
    CRM_notify_rc = 'None'

CRM_notify_target_rc = os.environ.get('CRM_notify_target_rc')
if CRM_notify_target_rc is None:
    debug('CRM_notify_target_rc = None')
    CRM_notify_target_rc = 'None'

CRM_notify_status = os.environ.get('CRM_notify_status')
if CRM_notify_status is None:
    debug('CRM_notify_status = None')
    CRM_notify_status = 'None'

if CRM_notify_node[0].isdigit():
    cmd = 'crm_node -l | grep ' + CRM_notify_node
    (ret, out) = runcmd(cmd, print_ret=False,lines=True)
    if ret == 0 and len(out.split()) > 1:
	CRM_notify_node = out.spit()[1]

debug('A new cluster event: CRM_notify_recipient(%s) node(%s) rsc(%s) task(%s) desc(%s) rc(%s) target_rc(%s) status(%s)' %(CRM_notify_recipient, CRM_notify_node, CRM_notify_rsc, CRM_notify_task, CRM_notify_desc, CRM_notify_rc, CRM_notify_target_rc, CRM_notify_status))

# for any operations (even successful) that are not a monitor, check whether this is
# a start event we need to report
# TODO: this function relies on our resource name convention, i.e., start of the _ds
#       is the start of a group resource, and start of the _ip resource is the end of
#       the group resource.
#       When we see a successful _ds start, we log a resource location change may happen.
#       When we see a successful _ip start, we report a resource location changed happened.

(ret, ha_enabled, role, node_dict) = readHAJsonFile()
if (ret != CLUS_SUCCESS):
    debug('Fail to get the configure from local json file in this node. rc=%d' %ret )
    sys.exit(JSON_ROLE_NOT_DEFINED)

if ha_enabled is None:
    sys.exit(JSON_HA_NOT_FOUND)
elif ha_enabled is False:
    sys.exit(JSON_HA_NOT_ENABLED)
    
usx_dict = node_dict.get('usx')
if (usx_dict is None):
    debug('Could not get the USX info.')
    sys.exit(JSON_VCENTER_NOT_DEFINED)
vmmanagername = usx_dict.get('vmmanagername')
if (vmmanagername is None):
    debug('Could not get the vm manager name.')
    sys.exit(JSON_VCENTER_NOT_DEFINED)
if 'displayname' in usx_dict:
    displayname = usx_dict.get('displayname')
else:
    displayname = socket.gethostname()

targetiliouuid = usx_dict.get('uuid')

if CRM_notify_task == 'None':
    # probably invoked by IPaddr2 resource agent
    CRM_notify_task = 'start'
    cnt = 10
    while cnt > 0:
        cmd = 'crm status'
        (ret, out) = runcmd(cmd, print_ret=True, lines=True)
        if ret == 0:
            for line in out:
                tmp = line.strip().split()
                if len(tmp) == 4 and tmp[0].endswith("_ip") and "IPaddr2" in tmp[1] and "Started" == tmp[2] and displayname == tmp[3]:
                    CRM_notify_task = 'start'
                    CRM_notify_rc = '0'
                    CRM_notify_rsc = tmp[0]
                    CRM_notify_node = tmp[3]
                    cnt = 1
                    break
        cnt = cnt-1
        time.sleep(6)

if ((CRM_notify_rc != '0' and CRM_notify_task == 'monitor') or  CRM_notify_task != 'monitor'):
    ## filter out the start operation of the resource
    if (CRM_notify_task != 'start'):
        debug('Do not report %s operation yet.' % CRM_notify_task)
	sys.exit(CLUS_SUCCESS)

    if (CRM_notify_rsc.endswith('_ds')):
        if (CRM_notify_rc != '0'):
            debug('on node %s, resource %s %s failed. (msg=%s rc=%s target_rc=%s).' % (CRM_notify_node, CRM_notify_rsc, CRM_notify_task, CRM_notify_desc, CRM_notify_rc, CRM_notify_status))
	else:
            debug('on node %s, resource %s %s succeed. (msg=%s rc=%s target_rc=%s).' % (CRM_notify_node, CRM_notify_rsc, CRM_notify_task, CRM_notify_desc, CRM_notify_rc, CRM_notify_status))
    elif (CRM_notify_rsc.endswith('_ip')):
        if (CRM_notify_rc != '0'):
            debug('on node %s, resource %s %s failed. (msg=%s rc=%s target_rc=%s).' % (CRM_notify_node, CRM_notify_rsc, CRM_notify_task, CRM_notify_desc, CRM_notify_rc, CRM_notify_status))
	else:
            # only when we succeed, we update the data grid
            debug('on node %s, resource %s %s succeed. (msg=%s rc=%s target_rc=%s).' % (CRM_notify_node, CRM_notify_rsc, CRM_notify_task, CRM_notify_desc, CRM_notify_rc, CRM_notify_status))

            resname = CRM_notify_rsc[:-len("_ip")]

            (ret, msg) = runcmd("rm /var/run/updated_vol_resource_container", print_ret=True)
            cmd = 'curl -k -X PUT -H "Content-type:application/json" "http://127.0.0.1:8080/usxmanager/usx/inventory/volume/resources/' + resname + '/location?targetuuid='+targetiliouuid+'" --connect-timeout 10 --max-time 60 -v'
	    (ret, msg) = runcmd(cmd, print_ret=True)
            cnt = 10
            while ret != 0 and cnt > 0:
                time.sleep(6)
                cnt = cnt - 1
	        (ret, msg) = runcmd(cmd, print_ret=True)
            if ret != 0 and cnt == 0:
                debug('Failed to update data grid with new location %s for resource %s.' % (targetiliouuid, resname))
            else:
                debug('Attempted to update data grid with new location %s for resource %s.' % (targetiliouuid, resname))
            send_alert_pcmk_mon(targetiliouuid, CRM_notify_rsc[:-len("_ip")])
    else:
        debug('No report for resource %s yet.' % CRM_notify_rsc)
	sys.exit(CLUS_SUCCESS)

sys.exit(CLUS_SUCCESS)
