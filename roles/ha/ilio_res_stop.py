#!/usr/bin/python
from ha_util import *

LOG_FILENAME = '/var/log/usx-ads-pool.log'
set_log_file(LOG_FILENAME)

ADS_LOAD_CMD = 'python /opt/milio/atlas/roles/ads/ads-load.pyc'
VIRT_VOL_LOAD_CMD = 'python /opt/milio/atlas/roles/virtvol/vv-load.pyc'
POOL_LOAD_CMD = 'python /opt/milio/atlas/roles/pool/cp-load.pyc'


debug("Entering ilio_res_stop:", sys.argv)
if len(sys.argv) < 2:
        debug("Wrong cmd format:", sys.argv)
        exit(1)

reason = sys.argv[1]

# TODO: this function relies on our resource name convention, i.e., start of the _ds
#       is the start of a group resource, and start of the _ip resource is the end of
#       the group resource.
#       When we see a successful _ds start, we log a resource location change may happen.
#       When we see a successful _ip start, we report a resource location changed happened.

(ret, ha_enabled, role, node_dict) = readHAJsonFile()
if (ret != CLUS_SUCCESS):
	debug('Fail to get the configure from local json file in this node. rc=%d' %ret )
	sys.exit(JSON_ROLE_NOT_DEFINED)

# Stage 1: check whether we can delete the resource
# 1.1 For nodes in ha cluster, check whether this node is in quorum and capable to stop and
#      delete the resource

    
# 1.2 Check the resource is being used or not.
#     Currently we do not do any check on ads node. On a pool node, we will find where the resource
#     is running now. The code, which uses remote agent api to check whether there are still logic
#     volumes left (except the internal one) on that remote node, is inside the "cp-load destroy".
#     If the vg is used, fail the delete.
# TODO: for node with ha, we need to find which node the resource is running on, and check that node
#       now, just use the local node.

#     Currently there is no check in the ilio to prevent agg node being deleted while it is still
#     used by pool node. If UI does not validate whether this agg node can be deleted or not, it 
#     may cause damage in pool level, since pvs will be missing.

if (role == 'AGGREGATE'):
	# no check, no resource cleanup
	sys.exit(CLUS_SUCCESS)
    
elif (role == 'VOLUME'):
	# HA_ADS node can skip resource checking
	adsname = None
	if (ha_enabled):
		adsname = sys.argv[2]
	else:
		volres = node_dict.get('volumeresources')
		adsname = volres[0].get('uuid')

	if adsname is None:
		debug('Error getting ADS name.')
		sys.exit(JSON_PARSE_EXCEPTION)

	if reason == 'disable':
		debug('INFO: not stop %s resources during disabling HA' %(adsname))
	elif reason == 'delete':
		cmd = VIRT_VOL_LOAD_CMD + ' localdestroy ' + adsname
		(ret, msg) = runcmd(cmd, print_ret=True)
		if ret != 0:
			debug('Fail to do localdestroy for Volume %s: rc=%d' %(adsname, ret))
		cmd = VIRT_VOL_LOAD_CMD + ' remotedestroy ' + adsname
		(ret, msg) = runcmd(cmd, print_ret=True)
		if ret != 0:
			sys.exit(ADS_DESTROY_RES_FAIL)
else:
	sys.exit(JSON_ROLE_NOT_SUPPORTED)

sys.exit(CLUS_SUCCESS)
