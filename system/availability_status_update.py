#!/usr/bin/python

"""
Report several USX statuses on demand

Location of the script: /opt/milio/atlas/system/

### Dependencies

### Collected status

1. Dedup filesystem status
    Collecting the the dedup fs mount status;
    If it is mounted, DEDUP_FILESYSTEM_STATUS is OK; otherwise it is FATAL
2. Volume export availability status
    Collecting the volume export service status (NFS/iSCSI service status),
    as well as the service IP status (ip addr show if service IP is configured)
3. HA status
    Collecting the /usr/sbin/crm_mon status to ensure that HA cluster is able to support
    faliover
4. Bootstrap status
    Query the grid to see what is current bootstrap status, just send this status
    again

## Agent service
/usr/bin/java
/opt/amc/agent/lib/amc-agent.jar

## IBD service
/usr/local/bin/ibdmanager -r s (IBD server status)
/usr/local/bin/ibdmanager -r a (IBD client status)

"""

import httplib
import json
import os, sys
import string
import time
import urllib2
import traceback
import socket
import math
import re
import datetime
import subprocess

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_constants import *
from cmd import *
from log import *
sys.path.insert(0, "/opt/milio/atlas/roles/ha")
from ha_util import *

# Global variables
USX_DICT={}
API_KEY=''
TIME_OUT=30

# Strings
VOLUME_ROLE = 'VOLUME'
SERVICE_ROLE = 'SERVICE_VM'
USX_USER = 'admin'
USX_PASSWORD = 'poweruser'
VOLUME_TYPE_FLASH = 'ALL_FLASH'
VOLUME_TYPE_SIMPLE = 'SIMPLE'
DEFAULT_CACHE_DEV = '/dev/usx-default-wc'
LOCAL_LOOPBACK_IP = '127.0.0.1'
# Configuration files and APIs
ATLAS_CONF = '/etc/ilio/atlas.json'
LOCAL_AGENT = 'http://127.0.0.1:8080'
LOGIN_API = '/user/login?istempkey=false'
VOLUME_STATUS_API = '/usxmanager/usx/status'
VOLUME_STATUS_UPDATE_API = '/usxmanager/usx/status/update'
SVM_CONTAINER_API = '/usxmanager/usx/inventory/servicevm/containers/'
VOLUME_CONTAINER_API = '/usxmanager/usx/inventory/volume/containers/'
VOL_RESOURCE_API = '/usxmanager/usx/inventory/volume/resources'
ALERT_API = '/usxmanager/alerts'
COUNT_FILE = '/var/backups/check_raid_count.txt'
LOST_IBD_FILE = '/var/backups/lost_idb.txt'
ALERT_FLAG = '/var/backups/raid_sync_flag.txt'

# Status dictionaries
alert_status_dict = {0:'OK',1:'WARN',2:'ERROR'}
status_level_dict = {0:'OK',1:'WARN',2:'CRITICAL',3:'FATAL',4:'UNKNOWN'}
status_type_dict = {0:'USX_MANAGER',1:'SERVICE_RESOURCE',2:'SERVICE_CONTAINER',
                    3:'VOLUME_CONTAINER',4:'VOLUME_RESOURCE'}
status_dict = {0:'BOOTSTRAP_STATUS',1:'HA_STATUS',
               2:'MANAGEMENT_NETWORK_REACHABILITY',3:'STORAGE_NETWORK_',
               4:'DEDUP_FILESYSTEM_STATUS',5:'VOLUME_EXPORT_AVAILABILITY',
               6:'POWER_STATUS',7:'VOLUME_SERVICE_STATUS',
               8:'VOLUME_STORAGE_STATUS',9:'VOLUME_FILESERVICE_STATUS',
               10:'CONTAINER_STATUS',11:'HA_FAILOVER_STATUS',12:'RAID_SYNC_STATUS',13:'TIEBREAKER_STATUS'}

# Log file
LOG_FILENAME = '/var/log/usx-availability_status.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename=LOG_FILENAME,
                    level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))
'''
"""
Helper methods
"""
'''
def info(*args):
    msg = " ".join([str(x) for x in args])
    print >> sys.stderr, msg
'''

def run_cmd(cmd, timeout=None):
    rtn_dict = {}
    rtn_dict['stderr'] = ''
    start = datetime.datetime.now()
    obj_rtn = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,close_fds=False,bufsize=10000)
    while obj_rtn.poll() is None:
        time.sleep(0.1)
        now = datetime.datetime.now()
        if not timeout:
            timeout = TIME_OUT
        if (now - start).seconds> timeout:
            try:
                obj_rtn.terminate()
            except Exception,e:
                rtn_dict['stderr'] = e
                return rtn_dict
            rtn_dict['stderr'] = 'timeout'
            return rtn_dict

    out = obj_rtn.stdout.read()
    err = obj_rtn.stderr.read()
    rtn_dict['stdout'] = out
    rtn_dict['stderr'] = err

    if obj_rtn.stdin:
        obj_rtn.stdin.close()
    if obj_rtn.stdout:
        obj_rtn.stdout.close()
    if obj_rtn.stderr:
        obj_rtn.stderr.close()
    try:
        obj_rtn.kill()
    except OSError:
        pass

    return rtn_dict

def init_global_variables():
    """
    Generate USX info dictionary from atlas.json
    """
    global USX_DICT
    err=''

    try:
        fp = open(ATLAS_CONF)
        jsondata = json.load(fp)
        fp.close()
        if jsondata.has_key('usx'): # this is a volume
            USX_DICT['role'] = jsondata['usx']['roles'][0]
            USX_DICT['uuid'] = jsondata['usx']['uuid']
            USX_DICT['usxmanagerurl'] = jsondata['usx']['usxmanagerurl']
            USX_DICT['nics'] = jsondata['usx']['nics']
            USX_DICT['ha'] = jsondata['usx']['ha']
            USX_DICT['resources'] = jsondata['volumeresources']
            USX_DICT['sharestorage4capacity'] = False
            USX_DICT['sharestorage4cache'] = False
            USX_DICT['ibd'] = False
            share_dict = check_sharestorage()
            USX_DICT['sharestorage4capacity'] = share_dict['storage']
            USX_DICT['sharestorage4cache'] = share_dict['cache']
            USX_DICT['ibd'] = share_dict['ibd']
##            USX_DICT['displayname'] = jsondata['usx']['displayname']
            if USX_DICT['resources']:
                if jsondata['volumeresources'][0].has_key('raidplans'):
                    USX_DICT['volumetype'] = jsondata['volumeresources'][0]['raidplans'][0]['volumetype']
                    #if jsondata['volumeresources'][0]['raidplans'][0].has_key('raidbricks'):
                    #    if len(jsondata['volumeresources'][0]['raidplans'][0]['raidbricks'])== 0 :
                    #        USX_DICT['share'] = True
        else: # this is a service vm
            USX_DICT['role'] = jsondata['roles'][0]
            USX_DICT['uuid'] = jsondata['uuid']
            USX_DICT['usxmanagerurl'] = jsondata['usxmanagerurl']
        debug("USX Manager URL from ATLAS:%s" % USX_DICT['usxmanagerurl'])
        USX_DICT['usxmanagerurl'] = get_master_amc_api_url()
        debug("USX Manager URL from grid member:%s" % USX_DICT['usxmanagerurl'])

    except err:
        debug("ERROR : exception occurred, exiting ...")
        debug(err)
        exit(1)

def retrieve_from_usx_grid(usxmanagerurl, apistr):
    """
    Get information from grid
     Input: REST API URL, REST API query string
     Return: response data
    """
    try:
        protocol = usxmanagerurl.split(':')[0]
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        apiaddr = usxmanagerurl.split('/')[2]  # like: 10.15.107.2:8443
        #debug(usxmanagerurl)
        #debug(apiaddr)
        if use_https == True:
            conn = httplib.HTTPSConnection(apiaddr)
        else:
            conn = httplib.HTTPConnection(apiaddr)
        conn.request("GET", apistr)
        debug(usxmanagerurl)
        debug('apiaddr: '+apiaddr+' | apistr: ' + apistr)
        response = conn.getresponse()
        debug(response.status, response.reason)
        if response.status != 200 and response.reason != 'OK':
            return None
        else:
            data = response.read()
            debug('Response:')
            debug(data)
    except:
        debug("ERROR : Cannot connect to USX Manager to query")
        return None

    return data
'''
def _publish_to_usx_grid(usxmanagerurl, apistr, data, putFlag=0):
    """
    Call REST API to update availability status to grid
     Input: REST API URL, REST API query string
     Return: response data
    """
    retVal = {}
    try:
        conn = urllib2.Request(usxmanagerurl + apistr)
        debug(usxmanagerurl+apistr)
        conn.add_header('Content-type','application/json')
        if putFlag == 1:
            conn.get_method = lambda: 'PUT'
        if data != 'None':
            debug('**** data to be uploaded to AMC: ', data)
            res = urllib2.urlopen(conn, json.dumps(data))
        else:
            res = urllib2.urlopen(conn)

        debug('Returned response code: ' + str(res.code))
        retVal['code'] = str(res.code)
        if res.code == 200:
            res_data = res.read()
            try:
                res_data = eval(res_data)
            except:
                pass

            retVal['out'] = res_data
        else:
            retVal['out'] = ""
        res.close()
    except:
        debug(traceback.format_exc())
        debug("ERROR : Exception caught!")
        retVal['code'] = '500'

    return retVal
'''
def get_service_vm_uuid(ip):
    """
    get service vm container uuid
    """
    debug('START: Get service vm container uuid')
    retVal = ""

    get_apistr = (SVM_CONTAINER_API + "?query=.%5Bnics%5Bipaddress%3D'" +
              ip +"'%5D%5D&fields=uuid")
    response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
    data = json.loads(response)
    if not data.get('items'): # no service vm uuid retrieved
        debug("get_service_vm_uuid: Service VM with storage IP %s does not exist in grid" % ip)
        return False
    retVal = data.get('items')[0]['uuid']

    debug('END: Service VM (%s) container uuid is %s' % (ip, retVal))

    return retVal

def get_service_vm_ip(uuid):
    """
    get service vm ip address
    """
    debug('START: Get service vm container uuid')
    ip = ""

    get_apistr = '%s%s?composite=false' % (SVM_CONTAINER_API, uuid)
    response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
    data = json.loads(response)

    if data.get('data'):
        if len(data['data']['nics']) == 1:
            ip  = data['data']['nics'][0]['ipaddress']
        else:
            for item_nics in data['data']['nics']:
                if item_nics['storagenetwork'] == False:
                    ip = item_nics['ipaddress']

    debug('END: Service VM (%s) container IP is %s' % (uuid,ip))

    return ip

def check_ibd_freeze_status():
    """
    Check ibd freeze status
    if not found ibd freeze prossces and ibdmanager get failed, the status is fatal, otherwise is OK
    """
    debug('START: check_ibd_freeze_status')

    cmd = "ps -A -oetime,args | grep ibdmanager | grep freeze | head -1 |awk '{print $1}'"
    ret = run_cmd(cmd)
    status = status_level_dict[3]

    if ret.has_key('stdout') and ret['stdout'].strip():
        status = status_level_dict[0]
    #    count = len(ret['stdout'].strip())
    #    if count < 4:
    #        status = status_level_dict[0]
    #    elif count < 6:
    #        time_str =ret['stdout'].strip().split(':')
    #        if int(time_str[0]) < 3:
    #            status = status_level_dict[0]
    #        elif int(time_str[0]) < 5:
    #            status = status_level_dict[1]
    debug('END: ibd status is %s' % status)
    return status

def is_enable_ha():
    """
    Check current volume whether HA is enabled
    Use Atlas JSON 'ha' flag to determine. Don't use '/usr/sbin/crm_mon'
    Return:
            Enable HA: 0
            Not enable HA: 1
    """
    debug('START: Check current volume whether enable ha')
    retVal = False

    if USX_DICT.has_key('ha'):
        if USX_DICT['ha'] == True:
            debug("HA is enabled.")
            retVal = True
        else:
            debug("HA is Not enabled.")

    debug('END: Current volume ha status is ' + str(retVal))

    return retVal

def is_volume_resource():
    """
    Check volume resource whether run on current volume container when joined HA group
    Return:
            volume resource run on current volume container: volume_resource_uuid
            volume resource does not run on current volume container: 1
    """
    debug('START: Check volume resource whether run on current volume container when joined HA group')
    ret = 1

    retVal = {}
    retVal = run_cmd('/usr/sbin/crm_mon -1')
    retVal = retVal['stdout']

    try:
        hostname = socket.gethostname()
    except socket.gaierror, err:
        return ret

    #debug('val:',val )
    for line in retVal.split('\n'):
        #debug('line:'+ line +"@")
        # if the current host is online, its HA status is ok.
        p = 'Started *' + hostname + ' *$'
        m = re.search(p,line)
        if m is not None:
            p = '([\w|_|-]*)_ds'
            m = re.search(p,line)
            if m is not None:
                ret = m.group(1)
                break

    if ret == 1:
        #Get resource uuid from atlas.json
        re_uuid = get_resource_uuid()
        if re_uuid:
            #Check resource whether run on local container
            get_apistr = (VOL_RESOURCE_API + "?query=.%5Bcontaineruuid%3D'" +
            USX_DICT['uuid'] +"'%5D&fields=uuid")
            response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
            data = json.loads(response)
            if data.get('items'): # resource exist on volume
                ret = data.get('items')[0]['uuid']

    debug('END: Check volume resource %s run on current volume container' % ret)
    return ret
'''
def _send_alert(ilio_id, name, status, description):
    debug('START: Send alert')

    cmd = 'date +%s'
    ret = run_cmd(cmd)
    epoch_time = ret['stdout'].rstrip('\n')

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
    	"description"		:"",
        "service"		:"RAIDSYNC",
    	"alertTimestamp"	:"",
    	"iliotype"		:"VOLUME"
    }

    ad["uuid"] = ilio_id + '-raid-sync-alert-' + str(epoch_time)
    ad["checkId"] = ilio_id + '-raidsync'
    ad["usxuuid"] = ilio_id
##    ad["displayname"] = name
    ad["target"] = "servers." + ilio_id + ".raidsync"
    ad["alertTimestamp"] = epoch_time
    ad["iliotype"] = 'VOLUME'
    ad['status'] = status
    ad['description'] = description

    rc =publish_to_usx_grid(LOCAL_AGENT,ALERT_API, ad)
    rc = rc['code']
    if rc != '200':
        debug("ERROR : Failed to send alert.")
        ret = False
    else:
        ret = True

    debug('END: Send alert')
    return ret
'''

def is_volume_resource_without_ha():
    """
    Check volume resource whether run on current volume container after disable HA
    Return:
            volume resource run on current volume container: volume_resource_uuid
            volume resource does not run on current volume container: 1
    """
    debug('START: Check volume resource whether run on current volume container after disable HA')
    ret = 1

    get_apistr = (VOL_RESOURCE_API + "?query=.%5Bcontaineruuid%3D'" +
               USX_DICT['uuid'] +"'%5D&fields=uuid")
    response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
    data = json.loads(response)
    if not data.get('items'): # Use REST API volume resource return value to determine if container contains resource
        debug("is_volume_resource_without_ha: Volume with uuid %s does not exist in grid" % USX_DICT['uuid'])
        return 1
    if data.get('items'): # resource exist on volume
        ret = data.get('items')[0]['uuid']

    if ret != 1:
        debug("---------- Volume %s : resource uuid %s" % (USX_DICT['uuid'], ret))
    else:
        debug("++++++++++ No resource resides on this volume %s" % USX_DICT['uuid'])

    debug('END: Check volume resource %s run on current volume container after disable ha' % ret)
    return ret
def check_raid_sync():
    debug("START:check_raid_sync ***")

    # Modified per Yifeng's suggestion, only need to call this command to check raid sync
    #  the second input parameter is a dummy variable, could used anything
    cmd = 'ps -ef| grep md_monitor.pyc | grep -v grep'
    rtn = run_cmd(cmd)
    if rtn['stdout'] == "":
        cmd = 'python /opt/milio/atlas/roles/pool/md_monitor.pyc RebuildCheck /dev/md0 %s' % LOG_FILENAME
        run_cmd(cmd)

    debug("END:check_raid_sync ***")

def check_raid_sync_progress(ibd_count,raid_info):
    debug("START:check_raid_sync_progress ***")
    retVal = {}
    retVal['status'] = True
    retVal['percent'] = '0'

    j = 0

    for i in range(int(ibd_count)):
        i = i+1
        if 'ibd'+str(i)+'p' not in raid_info:
            retVal['status'] = False
            return retVal

    if ' active raid5' not in raid_info:
        retVal['status'] = False
        return retVal
    if '(F)' in raid_info:
        retVal['status'] = False
        return retVal
    if 'speed' not in raid_info or 'recovery' not in raid_info:
        retVal['status'] = False
        return retVal
    if 'speed=0K/sec' in raid_info:
        if os.path.isfile(COUNT_FILE):
            count = run_cmd('cat %s' % COUNT_FILE)
            if count['stderr'] == '':
                if int(count['stdout'].strip()) > 5:
                    retVal['status'] = False
                else:
                    j = int(count['stdout'].strip()) + 1
                    retVal['status'] = True
        else:
            retVal['status'] = False
            j = 1
        if j != 0:
            run_cmd('echo %s > %s' % (j, COUNT_FILE))

    p = '([\d|\.]+%)'
    m = re.search(p,raid_info)
    if m != None:
        retVal['percent'] = m.group(1)

    debug("END:check_raid_sync_progress ***")
    return retVal

def get_ibd_count():
    cmd = '/usr/local/bin/ibdmanager -r a -s get | grep devname | wc -l'
    ret = run_cmd(cmd)
    if ret['stderr'] != '':
        return False
    else:
        return ret['stdout']

def check_raid(count, out):
    ret = True

    if 'active raid5' not in out:
        return False

    for item in out.split(':'):
        if 'active raid5' in item:
            p = "\[(\d*)/(\d*)\]"
            m = re.search(p, item)
            if m is not None:
                info(m.group(2))
                info(m.group(1))
                if int(m.group(2)) != int(m.group(1)):
                    ret = False
                    break
    debug("Result of verifying raid5 is: %s" % ret)
    return ret

def verify_raid(mountpoint):
    debug('START: versify_raid: %s start =====')
    verify_ret = {}
    verify_ret['status'] = False
    verify_ret['sync'] = {}
    ip = ''
    uuid = ''

    #ioping disk
    ioping_cmd = '/usr/bin/ioping'
    if os.path.exists('/usr/local/bin/ioping'):
        ioping_cmd = '/usr/local/bin/ioping'
    dd_cmd = '%s -A -D -c 3 -s 512 %s' % (ioping_cmd, mountpoint)
    debug(dd_cmd)
    ret1 = run_cmd(dd_cmd)
    if ret1['stderr'] == 'timeout':
        return verify_ret

    debug(ret1['stdout'])
    time.sleep(5)

    cmd = 'cat /proc/mdstat'
    ret1 = run_cmd(cmd)
    if ret1['stderr'] == 'timeout':
        retVal = status_level_dict[3]
    elif ret1['stderr'] == '':
        count = get_ibd_count()
        debug(ret1['stdout'])
        ret = True
        verify_ret['status'] = check_raid(count, ret1['stdout'])

        ruuid=get_resource_uuid()
        get_apistr = (VOLUME_STATUS_API+"/VOLUME_RESOURCE/" +ruuid + "/?" +"api_key="+ API_KEY)
        response = retrieve_from_usx_grid(USX_DICT['usxmanagerurl'], get_apistr)
        data = json.loads(response)
        if data.get('usxstatuslist'):
            name = ''
            for item in data['usxstatuslist']:
                if status_dict[3] in item['name']:
                    if item['value'] != status_level_dict[0]:
                        name = item['name']
                    elif item['previousvalue'] != status_level_dict[0]:
                        name = item['name']
                    if name != '':
                        p = 'STORAGE_NETWORK_(.*)'
                        m = re.search(p, name)
                        if m != None:
                            uuid = m.group(1)
                        break

        if uuid != '':
            ip = get_service_vm_ip(uuid)
            run_cmd('echo %s@%s > %s' % (uuid,ip,LOST_IBD_FILE))
        elif os.path.isfile(LOST_IBD_FILE):
            ret = run_cmd('cat %s' % LOST_IBD_FILE)
            ret = ret['stdout'].strip().split('@')
            uuid = ret[0]
            ip = ret[1]

        if not verify_ret['status']:
            ret = check_raid_sync_progress(count, ret1['stdout'])
            debug('Check raid sync progress result is %s' % ret)
            if ret['status']:
                verify_ret['sync']['status'] = status_level_dict[1]
                verify_ret['sync']['details'] = '%s, The service vm lost connection is %s, It\'s IP address is %s' % (ret['percent'], uuid, ip)
                if not os.path.isfile(ALERT_FLAG):
                    run_cmd('touch %s' % ALERT_FLAG)
                    #send_alert(USX_DICT['uuid'],USX_DICT['displayname'], alert_status_dict[1], 'Volume raid sync start...')

                run_cmd('echo %s > %s' % (1, COUNT_FILE))
            else:
#                if not os.path.isfile(ALERT_FLAG):
#                    #run_cmd('touch %s' % ALERT_FLAG)
#                    send_alert(USX_DICT['uuid'],USX_DICT['displayname'], alert_status_dict[2], 'Volume raid sync failure.')
                retVal = status_level_dict[1]

        if verify_ret['status']:
            if os.path.isfile(ALERT_FLAG):
                #send_alert(USX_DICT['uuid'],USX_DICT['displayname'], alert_status_dict[0], 'Volume raid sync is successful')
                verify_ret['sync']['status'] = status_level_dict[0]
                verify_ret['sync']['details'] = '%s, The service vm lost connection is %s, It\'s IP address is %s' % ('100%',uuid,ip)
                run_cmd('rm -rf %s' % ALERT_FLAG)
                run_cmd('rm -rf %s' % LOST_IBD_FILE)

    debug('END: versify_raid: %s =====' % verify_ret)
    return verify_ret

"""
Status collection method
"""
def check_dedup_mount_status():
    """
    Check if the dedup fs is mounted
    """
    debug('START: Check if the dedup fs is mounted')
    ret = {}
    ret['status'] = False

    cmd = 'mount | grep -i -E \'%s\' | awk -F \' \' \'{print $3}\'' % STORAGE_REG_EXPRESSION
    result = os.popen(cmd).read()
    if len(result.strip()) > 0 and check_shared_storage_status() == 0:
        ret['status'] = True
        ret['value'] = result.strip()


    debug('END: Mounted status of dedup fs is ' + str(ret))
    return ret

def get_volume_export_status():
    """
    Check volume export type status
    """
    debug('START: Check volume export type status')
    ret = status_level_dict[3]
    ibd_status = True

    if USX_DICT.has_key('volumetype') and 'SIMPLE_MEMORY' not in USX_DICT['volumetype'] and 'SIMPLE_FLASH' not in USX_DICT['volumetype']:
        ibd_status = check_local_ibd_status()
    if ibd_status:
        cmd = 'service nfs-kernel-server status'
        result = os.popen(cmd).read().strip()
        if 'nfsd running' in result:
            ret = 'NFS'
        else:
            cmd = 'service scst status'
            result = os.popen(cmd).read().strip()
            if 'SCST status: OK' in result:
                ret = 'iSCSI'

    debug('END: Volume export type is ' + ret)
    return ret

def get_service_ip_status():
    """
    Check if service ip is aliased
    """
    debug('START: Check if service ip is aliased')
    ret = False

    if USX_DICT.has_key('resources'):
        if len(USX_DICT['resources']) > 0:
            if 'serviceip' in USX_DICT['resources'][0]:
                serviceip = USX_DICT['resources'][0]['serviceip']
                cmd = 'ip addr show | grep "scope global secondary" | grep -v grep'
                result = os.popen(cmd).read().strip()
                if serviceip + '/' in result:
                    ret = True
        else:
            ret = True

    debug('END: Status of service ip is ' + str(ret))

    return ret

def get_file_system_status():
    """
    Dedup FS + Volume Export Avaliability status check
    """
    debug('START: check file system status')
    retVal = {}
    retVal[status_dict[5]] = status_level_dict[3]
    flag = 0

    ret = check_dedup_mount_status()
    if not ret['status']:
        debug("Dedup file system is NOT mounted")
        retVal[status_dict[4]] = status_level_dict[3]
    else:
#        if VOLUME_TYPE_SIMPLE not in USX_DICT['volumetype'] and not USX_DICT['share']:
#            ret = verify_raid(ret['value'])
#            retVal[status_dict[4]] = ret
#            debug("Dedup file system is %s" % ret)
#        else:
        debug("Dedup file system is mounted")
        retVal[status_dict[4]] = status_level_dict[0]

    if USX_DICT.has_key('nics'):
        for nic in USX_DICT['nics']:
            if nic.has_key('storagenetwork'):
                if nic['storagenetwork']: # This nic is mapped to storage network
                    if nic['mode'].lower() == 'static': # only check service ip if NIC is not configured as DHCP
                        flag = 1

    vol_export_avail = get_volume_export_status()
    if (vol_export_avail.lower() == 'nfs' or
        vol_export_avail.lower() == 'iscsi'):
        debug("Volume export service %s is running ..." % vol_export_avail)
        if USX_DICT.has_key('volumetype') and 'SIMPLE' not in USX_DICT['volumetype']:
            if USX_DICT.has_key('nics'):
                for nic in USX_DICT['nics']:
                    if nic.has_key('storagenetwork'):
                        if nic['storagenetwork']: # This nic is mapped to storage network
                            if nic['mode'].lower() == 'dhcp':
                                flag = 1
                            if USX_DICT.has_key('resources'):
                                if USX_DICT['resources']: # there is resource in this volume
                                    if USX_DICT['resources'][0].has_key('serviceip'): # both static or dhcp could have service ip configured
                                        if get_service_ip_status():
                                            debug("Service IP is configured")
                                            retVal[status_dict[5]] = status_level_dict[0]
                                        else:
                                            debug("Service IP is NOT configured")
                                        break
                                    elif flag:
                                        retVal[status_dict[5]] = status_level_dict[0] # Storage network is dhcp and no service ip,update "Export Avaliability" OK
                                else: # volume has no resource: standby volume
                                    retVal[status_dict[5]] = status_level_dict[0]
                            else:
                                retVal[status_dict[5]] = status_level_dict[0]
        else: # for simple volumes, no need to check service ip
            retVal[status_dict[5]] = status_level_dict[0]
    else:
        debug("Volume export service is NOT running")
        retVal[status_dict[5]] = status_level_dict[3]

    debug('END: File system status is:')
    debug(retVal)
    return retVal

def get_ha_status():
    """
    get ha status using cmd: /usr/sbin/crm_mon -1
    """
    debug('START: get ha status')
    ret = status_level_dict[3]

    retVal = {}
    retVal = run_cmd('/usr/sbin/crm_mon -1')
    retVal = retVal['stdout']

    try:
        hostname = socket.gethostname()
    except socket.gaierror, err:
        return ret

    for line in retVal.split('\n'):
        # if the current host is online, its HA status is ok.
        if 'Online:' in line and hostname in line:
            ret = status_level_dict[0]
            break
    if ret == status_level_dict[0] and is_stretchcluster():
        if 'OFFLINE:' in retVal:
            ret = status_level_dict[1]
    if ret != status_level_dict[0]:
        return ret

    '''
    Being online is not enough for a volume because if no standby node is online, then the volume HA status is WARN.
    Let count be the number of containers that are running an IP resource. If the number of online containers is equal to count,
    then the HA status of container is WARN because no standby container is ready to handle failover.
    '''
    count = 0
    for line in retVal.split('\n'):
        #volume resource has been disabled skip count the number of containers that are running an IP resource
        if '(unmanaged)' not in line:
            tmp = line.split()
            if len(tmp) > 3 and tmp[0].endswith('_ip') and tmp[2] == 'Started':
                count = count + 1

    for line in retVal.split('\n'):
        if line.startswith('Online'):
            if len(line.split()) - 3 == count:
                # no standby
                ret = status_level_dict[1]
            break

    debug('END: Ha status is ' + ret)
    return ret

def get_bootstrap_status(v_type):
    """
    get bootstrap status from node
    """
    debug('START: check bootstrap status on %s' % v_type)

    retVal = status_level_dict[4]
    if 'volume' == v_type:
        #If dedup_fs is mounted on resouces volume, then bootstrap status is OK
        ret = check_dedup_mount_status()
        if ret['status']:
            retVal = status_level_dict[0]
    else:
        #If pacemakerd process existed on HA standby node, then bootstrap status is OK
        ret = run_cmd('ps -ewf | grep pacemakerd | grep -v grep')
        if ret['stdout'] != '':
            retVal = status_level_dict[0]

    debug('END: bootstrap status is ' +retVal)

    return retVal

def get_storage_network_status_for_simple_volume():
    """
    get storage network status for simple hybrid/simple in-memory volumes
     Since simple volumes do not have IBD connection, we a direct way to detect
     storage network status
    """
    debug("START: get storage network status for simple volume")
    simple_storagenet_status = ""
    storageNIC = ""

    for nic in USX_DICT['nics']:
        if nic.has_key("storagenetwork"):
            if nic['storagenetwork']:
                storageNIC = nic['devicename']
                break

    if not storageNIC:
        debug("ERROR : Unable to get device name for storage network interface!")
        simple_storagenet_status = status_level_dict[3]
    else:
        cmd = ("ip addr show | grep %s | grep state | awk '{print $9}'" % storageNIC)
        debug(cmd)
        ret = run_cmd(cmd)
        out = ret['stdout']
        if not out:
            simple_storagenet_status = status_level_dict[3]
        else:
            for item in out.split("\n"):
                item = item.strip()
                if "DOWN" in item:
                    simple_storagenet_status = status_level_dict[3]
                    break
                else:
                    simple_storagenet_status = status_level_dict[0]
                    break
    debug("END: Simple storage status %s" % simple_storagenet_status)
    return simple_storagenet_status

def get_storage_network_status():
    """
    get IBD reachable status:
    retrun: {'STORAGE_NETWORK_vc21_tis1-sv-01-1418275986105': 'OK','STORAGE_NETWORK_vc21_tis1-sv-02-1418275986106': 'OK'}
        Success: {'STORAGE_NETWORK_vc21_tis1-sv-33-001-DONT-DELETE-1418275986105': 'OK'}
        Failure: {}
    """
    debug('START: get IBD reachable status')
    ibd_status = {}

#     i = 0
    sv_uuid = ""

    cmd = "/usr/local/bin/ibdmanager -r a -s get "
    retval = run_cmd(cmd)
    retval = retval['stdout']

    for item in retval.split("Service Agent Channel"):
        item = item.strip()
        isLocal = False
        sv_uuid = False
        for line in item.split('\n'):
            if 'ip:' in line:
                if line.split(':')[1] == LOCAL_LOOPBACK_IP:
                    isLocal = True
                    break
                sv_uuid=get_service_vm_uuid(line.split(':')[1])
        if isLocal:
            continue
        if 'state' in item and sv_uuid != False:
            if 'working' in item:
                ibd_status[status_dict[3]+sv_uuid] = status_level_dict[0]
            else:
                ibd_status[status_dict[3]+sv_uuid] = status_level_dict[3]

    debug('END: IBD reachable status is: ')
    debug(ibd_status)
    return ibd_status


def check_local_ibd_status():
    #check local ibd status
    debug('START: Get local ibd status')
    rtn = True
    ibdoutput = run_cmd('ibdmanager -r a -s get')
    if not ibdoutput['stderr'] and ibdoutput['stdout']:
        for item in ibdoutput['stdout'].split('Service'):
            if LOCAL_LOOPBACK_IP in item:
                if 'state:working' not in item:
                    ret = check_ibd_freeze_status()
                    if ret != status_level_dict[0]:
                       rtn = False
                break

    debug('END: Local ibd status is %s' % rtn)
    return rtn


def get_storage_status(ibd_status):
    """
    get volume storage status:
    input:
        ibd_status: {'STORAGE_NETWORK_vc21_tis1-sv-01-1418275986105': 'OK','STORAGE_NETWORK_vc21_tis1-sv-02-1418275986106': 'OK'}
    retrun: storage status
    """

    debug('START: get volume storage status')
    ioping_status = True
    mdstat_status = True
    ret=status_level_dict[3]

    mount_status = check_dedup_mount_status()
    if mount_status.has_key('value'):
        mountpoint = mount_status['value']

        #ioping disk
        ioping_cmd = '/usr/bin/ioping'
        if os.path.exists('/usr/local/bin/ioping'):
            ioping_cmd = '/usr/local/bin/ioping'
        cmd = "ps -ef| grep ioping | grep -v grep"
        rtn = run_cmd(cmd)
        if rtn['stdout'] == "":
            dd_cmd = '%s -A -D -c 3 -s 8k %s' % (ioping_cmd, mountpoint)
            ioping_rtn = run_cmd(dd_cmd)
            if ioping_rtn["stderr"] == "timeout":
                ioping_status = False
        else:
            ioping_status= False

    #check local IBD status
    ibdoutput = run_cmd('ibdmanager -r a -s get')
    if not ibdoutput['stderr'] and ibdoutput['stdout']:
        for item in ibdoutput['stdout'].split('Service'):
            if LOCAL_LOOPBACK_IP in item and 'state:working' not in item:
                ret = check_ibd_freeze_status()
                if ret != status_level_dict[0]:
                #ret = status_level_dict[3]
                    return ret

    ibd_count = len(ibd_status)
    ibd_fatal_count = 0

    for eachKey in ibd_status.keys():
        if status_level_dict[0] != ibd_status[eachKey]:
            ibd_fatal_count += 1

    rtn = run_cmd('ps -ef | grep /proc/mdstat | grep -v grep')
    if rtn['stdout'] == "":
        mdstat_data = run_cmd('cat /proc/mdstat')
        if mdstat_data["stderr"] == "timeout":
            mdstat_status = False
    else:
        mdstat_status = False

    if ibd_fatal_count > 0 and not mdstat_status and not ioping_status:
        debug("End: md devices are broken")
        ret = status_level_dict[3]
        debug('END: Volume cache storage status is ' + ret)
        return ret

    if not USX_DICT['sharestorage4cache']:
        #check the read/write cache raid1 status
        #cmd = 'ls -al %s | awk -F \' \' \'{print $11}\'' % DEFAULT_CACHE_DEV
        if os.path.exists(DEFAULT_CACHE_DEV):
            cmd = '/sbin/pvs | grep `ls -al %s | awk -F \' \' \'{print $11}\' | awk -F \'/\' \'{print $3}\'` | awk -F \' \' \'{print $1}\'' % DEFAULT_CACHE_DEV
            debug('Get read/write cache md dev cmd:%s' % cmd)
            out = run_cmd(cmd)
            debug('Get read/write cache md dev result:%s' % out)
            if out['stdout']:
                dev_md = out['stdout'].strip('\n')
                #cmd = "/sbin/mdadm -D `mount | grep -i -P \'%s\' | awk '{print $1}'` | grep ibd* | xargs | grep -Po '/dev/ibd\d+'" % RWCACHE_REG_EXPRESSION
                cmd = "/sbin/mdadm -D %s | grep 'ibd*' | xargs | grep -Po '/dev/ibd\d+'" % dev_md
                debug('Get read/write cache ibd dev cmd:%s' % cmd)
                out = run_cmd(cmd, 10)
                debug('Get read/write cache ibd dev result:%s' % out)
                if not out['stderr'] and out['stdout']:
                    cache_ibds = []
                    fatal_cache_ibd_count = 0
                    cache_ibds = out['stdout'].split('\n')
                    debug('read/write cache ibd dev result:%s' % cache_ibds)
                    if len(cache_ibds) > 0 and not ibdoutput['stderr'] and ibdoutput['stdout']:
                        for item in ibdoutput['stdout'].split('Service'):
                            if 'state:working' not in item and LOCAL_LOOPBACK_IP not in item:
                                p = "/dev/ibd\\d+"
                                m = re.search(p, item)
                                if m is not None:
                                    if m.group(0) in cache_ibds:
                                        fatal_cache_ibd_count += 1
                                        if fatal_cache_ibd_count > 1:
                                            break
                    if fatal_cache_ibd_count == 1:
                        ret = status_level_dict[1]
                    elif fatal_cache_ibd_count > 1:
                        ret = status_level_dict[3]
                    if fatal_cache_ibd_count > 0:
                        debug('END: Volume cache storage status is ' + ret)
                        return ret
                else:
                    debug("Faild to get read/write ibd device")
                    ret = status_level_dict[3]
                    debug('END: Volume cache storage status is ' + ret)
                    return ret
            else:
                debug("Faild to get read/write md device")
                ret = status_level_dict[3]
                debug('END: Volume cache storage status is ' + ret)
                return ret

    #if none idb status is FATAL, volume storage status is OK
    #if only 1 ibd status is FATAL, volume storage status is WARN
    # #1 If ibd count <= 5:
    #   eg: ibd conut is 4
    #       ifibd FATAL status# >= 2, volume storage status is FATAL
    # #2 If ibd count > 5:
    #   eg: ibd conut is 11
    #       if 7 ibd status are FATAL, volume storage status is FATAL
    #       if 2 ibd status are FATAL, volume storage status is WARN
    #       if 2 < ibd fatal# <7, need check raid5 status useing cmd 'cat /proc/mdstat'. if inactive statue# > 2, volume storage status is FATAL. If not, volume storage status is WARN
    if ibd_fatal_count == 0:
        ret=status_level_dict[0]
    elif ibd_fatal_count <=1:
        ret=status_level_dict[1]
    elif (ibd_count > 5 and ibd_fatal_count < (3 - ibd_count%2)):
        ret = status_level_dict[1]
    elif (ibd_count > 5 and ibd_fatal_count < ibd_count//2+2+ibd_count%2):
        if mdstat_data['stderr'] == '':
            for item in mdstat_data['stdout'].split('\n\n'):
                if 'active raid5' in item:
                    p = "\[(\d*)/(\d*)\]"
                    m = re.search(p, item)
                    if m is not None:
                        print(m.group(2))
                        print(m.group(1))
                        if int(m.group(2)) >= (int(m.group(1))-1):
                            ret=status_level_dict[1]

    debug('END: Volume storage status is ' + ret)
    return ret

"""
Status prep and publish method
"""
def publish_container_status(status):
    """
    Publish service vm or voulme container statuses collected
    """
    debug('START: Publish service vm or voulme container statuses collected')

    retVal = 0
    data = {}
    data['usxstatuslist'] = []

    data['usxuuid'] = USX_DICT['uuid']
    data['usxcontaineruuid'] = USX_DICT['uuid']
    if USX_DICT['role'] == SERVICE_ROLE:
        data['usxtype'] = status_type_dict[2]
    else:
        data['usxtype'] = status_type_dict[3]

    if status.has_key(status_dict[0]):
        bootstarp_status = {}
        bootstarp_status['name'] = status_dict[0]
        bootstarp_status['value'] = status[status_dict[0]]
        data['usxstatuslist'].append(bootstarp_status)
    if status.has_key(status_dict[1]):
        ha_status = {}
        ha_status['name'] = status_dict[1]
        ha_status['value'] = status[status_dict[1]]
        data['usxstatuslist'].append(ha_status)
    if status.has_key(status_dict[13]):
        tb_status = {}
        tb_status['name'] = status_dict[13]
        tb_status['value'] = status[status_dict[13]]
        data['usxstatuslist'].append(tb_status)

    if len(data['usxstatuslist']) == 0:
        return 1
    debug(data)
    post_apistr = VOLUME_STATUS_UPDATE_API
    rc = publish_to_usx_grid(LOCAL_AGENT, post_apistr, data)
    #rc = rc['code']
    #if rc != '200':
    if rc != 0:
        debug("ERROR : publish status to grid REST API call failed ")
        retVal = rc

    debug('END: publish status to grid REST API')
    return retVal

def get_resource_uuid():
    try:
        fp = open(ATLAS_CONF)
        jsondata = json.load(fp)
        fp.close()
        if jsondata.has_key('usx'): # this is a volume
            #if jsondata['usx'].has_key('volumeresourceuuids'):
            #    if len(jsondata['usx']['volumeresourceuuids']) > 0:
            #        return jsondata['usx']['volumeresourceuuids'][0]
            if len(jsondata['volumeresources']) > 0:
                return jsondata['volumeresources'][0]['uuid']

        if jsondata.has_key('usx'):
            if jsondata['usx'].has_key('uuid'):
                return get_resource_uuid_from_amc(jsondata['usx']['uuid'])
    except err:
        pass
    return None

def get_resource_uuid_from_amc(uuid):
    """
        get volume resource uuid if this volume container has resource
    """
    debug('START: Get volume resource uuid')
    retVal = None

    get_apistr = '%s%s?composite=false' % (VOLUME_CONTAINER_API, uuid)
    response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
    data = json.loads(response)
    if data.has_key('data'): # this is a volume
        if data['data'].has_key('volumeresourceuuids'):
            if len(data['data']['volumeresourceuuids']) > 0:
                retVal = data['data']['volumeresourceuuids'][0]

    debug('END: Volume container VM %s has resource uuid %s' % (uuid, retVal))

    return retVal

def publish_resource_status(status, uuid=None):
    """
    Publish voulme resource statuses collected
    """
    debug('START: Publish voulme resource statuses collected')

    retVal = 0
    data = {}
    data['usxstatuslist'] = []

    data['usxuuid'] = get_resource_uuid()
    if data['usxuuid'] is None:
        return 1

    data['usxcontaineruuid'] = USX_DICT['uuid']
    data['usxtype'] = status_type_dict[4]

    if status.has_key(status_dict[4]):
        dedup_fs_status = {}
        dedup_fs_status['name'] = status_dict[4]
        dedup_fs_status['value'] = status[status_dict[4]]
        data['usxstatuslist'].append(dedup_fs_status)
    if status.has_key(status_dict[5]):
        volume_export_status = {}
        volume_export_status['name'] = status_dict[5]
        volume_export_status['value'] = status[status_dict[5]]
        data['usxstatuslist'].append(volume_export_status)
    if status.has_key(status_dict[1]):
        ha_status = {}
        ha_status['name'] = status_dict[1]
        ha_status['value'] = status[status_dict[1]]
        data['usxstatuslist'].append(ha_status)
    if status.has_key(status_dict[8]):
        volume_storage_status = {}
        volume_storage_status['name'] = status_dict[8]
        volume_storage_status['value'] = status[status_dict[8]]
        data['usxstatuslist'].append(volume_storage_status)

    if status.has_key(status_dict[3]):
        for key in status[status_dict[3]].keys():
            storage_network_status = {}
            storage_network_status['name'] = key
            storage_network_status['value'] = status[status_dict[3]][key]
            data['usxstatuslist'].append(storage_network_status)
    if status.has_key(status_dict[12]):
            raid_sync_status = {}
            raid_sync_status['name'] = status_dict[12]
            raid_sync_status['value'] = status[status_dict[12]]['value']
            raid_sync_status['details'] = status[status_dict[12]]['details']

            data['usxstatuslist'].append(raid_sync_status)

    if len(data['usxstatuslist']) == 0:
        return 1
    debug(data)
    post_apistr = VOLUME_STATUS_UPDATE_API
    rc = publish_to_usx_grid(LOCAL_AGENT, post_apistr, data)
    #rc = rc['code']
    #if rc != '200':
    if rc != 0:
        debug("ERROR : publish status to grid REST API call failed ")
        retVal = rc

    debug('END: Publish service vm or voulme container statuses collected')
    return retVal

def update_container_uuid():
    debug('START: Update container uuid')
    ret = True

    resname = is_volume_resource()
    targetuuid = USX_DICT['uuid']
    debug('resname:'+str(resname))
    debug('targetuuid:'+targetuuid)

    if resname != 1 and targetuuid != resname:
        targetuuid = USX_DICT['uuid']
        post_apistr = VOL_RESOURCE_API + '/'+ str(resname) + '/location?targetuuid=' + targetuuid
        rc = publish_to_usx_grid(LOCAL_AGENT, post_apistr, None, 1)
        #rc = rc['code']
        #if rc != '200':
        if rc != 0:
            debug("ERROR : Update container uuid failed ")
            ret = False
    return ret

def check_log_files():
    path = "/var/log/"
    if os.path.isdir(path) == False:
        return 1
    log_files = os.listdir(path)
    for file in log_files:
        fs = os.path.getsize(path + file)
        if fs > 256 * 1024 * 1024: # 256M
            if file.split(".")[-1] == "gz":
                debug("check_log_files() remove file: %s, %d" % (path + file, fs))
                os.remove(path + file)
            else:
                debug("check_log_files() truncate file: %s, %d" % (path + file, fs))
                fd = open(path + file, "rw+")
                fd.truncate(256 * 1024 * 1024)
                fd.close()
    return 0

"""
Main
"""

if len(sys.argv)!= 2:
    info('USAGE: python availability-status.py <statue type>')
    sys.exit(1)

debug("=== AVAILABILITY STATUS UPDATE ===")
init_global_variables()
availability_statuses = {}
update_status = {}
update_container_status={}
CONTAINER_STATUS_LIST = [status_dict[0]]
RESOURCE_STATUS_LIST = [status_dict[1], status_dict[3], status_dict[4], status_dict[5], status_dict[8]]
VERIFYCATION_LIST_SV = [status_dict[0], 'ALL']
VERIFYCATION_LIST_VOLUME = [status_dict[0],status_dict[1], status_dict[3], status_dict[4], status_dict[5], status_dict[8], 'ALL']

API_KEY=USX_DICT['uuid']

if USX_DICT['role'] == VOLUME_ROLE:
    if sys.argv[1].upper() not in VERIFYCATION_LIST_VOLUME:
        debug('ERROR: Current ' + USX_DICT['role'] + ' doses not support for updating status '+ sys.argv[1])
        sys.exit(1)
else:
    if sys.argv[1].upper() not in VERIFYCATION_LIST_SV:
        debug('ERROR: Current ' + USX_DICT['role'] +' doses not support for updating status '+ sys.argv[1])
        sys.exit(1)

debug("=== Check USX Manager is alive or not")
if is_usxmanager_alive():
    debug("USX Manager is alive")
else:
    debug("USX Manager is disconnected")

debug("=== Check Status Start===")
if sys.argv[1].upper() in CONTAINER_STATUS_LIST:
    #update_status[status_dict[0]] = get_bootstrap_status()
    debug("=== Update Volume Container Status===")
    publish_container_status(update_status)

elif sys.argv[1].upper() in RESOURCE_STATUS_LIST:
    ha_flag = is_enable_ha() and os.path.exists('/run/pacemaker_started')
    if ha_flag:
        # Joined ha group
        if status_dict[1] == sys.argv[1].upper():
            ret_ha_status = get_ha_status()
            update_status[status_dict[1]] = ret_ha_status
            update_container_status[status_dict[1]] = ret_ha_status
            debug("=== Update Volume Container Status===")
            publish_container_status(update_container_status)

        resource_flag = is_volume_resource()
        if resource_flag != 1:
            resource_uuid = resource_flag
    else:
        resource_flag = is_volume_resource_without_ha()
        resource_uuid = resource_flag

    if resource_flag != 1:
        # Volume resource running on current volume container
        simple_flag = 0
        if status_dict[4] == sys.argv[1].upper():
            file_status = get_file_system_status()
            update_status[status_dict[4]] = file_status[status_dict[4]]

        if ha_flag:
            if USX_DICT.has_key('volumetype') and 'SIMPLE' in USX_DICT['volumetype']:
                simple_flag = 1
                if status_dict[5] == sys.argv[1].upper():
                    file_status = get_file_system_status()
                    update_status[status_dict[8]] = get_storage_network_status_for_simple_volume()
                    if update_status[status_dict[8]] == status_level_dict[3]:
                        update_status[status_dict[5]] = status_level_dict[3]
                    else:
                        update_status[status_dict[5]] = file_status[status_dict[5]]

        if not simple_flag:
            if status_dict[5] == sys.argv[1].upper():
                file_status = get_file_system_status()
                update_status[status_dict[5]] = file_status[status_dict[5]]
            if status_dict[3] == sys.argv[1].upper():
                update_status[status_dict[3]] = get_storage_network_status()
            if status_dict[8] == sys.argv[1].upper():
                update_status[status_dict[3]] = get_storage_network_status()
                if update_status[status_dict[3]] != {}:
                    status=get_storage_status(update_status[status_dict[3]])
                else:
                    status=status_level_dict[3]
                update_status[status_dict[8]] = status
                # Skip it if volume only using share storage

                if USX_DICT['ibd'] is True:

                    if update_status[status_dict[8]] == status_level_dict[3]: # if storage network is disconnected, dedup fs then is not operational
                        update_status[status_dict[4]] = status_level_dict[3]
                    ret = check_dedup_mount_status()
                    if ret.has_key('value'):
                        ret = verify_raid(ret['value'])
                        if ret.has_key('sync') and ret['sync'].has_key('status') and ret['sync'].has_key('details'):
                            update_status[status_dict[12]]['value']= ret['sync']['status']
                            update_status[status_dict[12]]['details']= ret['sync']['details']

    if update_status != {}:
        debug("=== Update Volume Resource Status===")
        update_container_uuid()
        publish_resource_status(update_status, resource_uuid)
    else:
        debug('ERROR: Current ' + USX_DICT['role'] +' doses not support for updating status '+ sys.argv[1] + ' due to volume resource not run on')
        sys.exit(1)
elif sys.argv[1].upper() == 'ALL':

        if USX_DICT['role'] == VOLUME_ROLE:
            ha_flag = is_enable_ha() and os.path.exists('/run/pacemaker_started')
            simple_flag = 0
            if ha_flag:
                robo_flag = is_robo()
                stretchcluster_flag = is_stretchcluster()

                # Get Tiebreaker
                if robo_flag or stretchcluster_flag:
                    ha_get_tiebreakerip()

                # Check User-set Tiebreaker status
                if stretchcluster_flag:
                    tiebreaker_status = ha_check_tiebreaker()
                    if tiebreaker_status == 1: #connected, send OK
                        update_container_status[status_dict[13]] = status_level_dict[0]
                    elif tiebreaker_status == 0: #disconnected, send FATAL
                        update_container_status[status_dict[13]] = status_level_dict[3]
                    elif tiebreaker_status == 2: #tiebreaker is 0.0.0.0, send UNKNOWN
                        update_container_status[status_dict[13]] = status_level_dict[4]

                # Joined ha group
                ret_ha_status = get_ha_status()
                update_status[status_dict[1]] = ret_ha_status
                update_container_status[status_dict[1]] = ret_ha_status

                resource_flag = is_volume_resource()
                if resource_flag != 1:
                    resource_uuid = resource_flag
            else:
                resource_flag = is_volume_resource_without_ha()
                resource_uuid = resource_flag

            if resource_flag != 1:
                #Updated bootstrap status when bootstrap status is OK
                bootstrap_status = get_bootstrap_status('volume')
                if bootstrap_status == status_level_dict[0]:
                    update_container_status[status_dict[0]] = status_level_dict[0]

                simple_flag = 0
                # Volume resource run on current volume container
                file_status = get_file_system_status()
                update_status[status_dict[4]] = file_status[status_dict[4]]

                if not ha_flag:
                    if USX_DICT.has_key('volumetype') and 'SIMPLE' in USX_DICT['volumetype']:
                        simple_flag = 1
                        update_status[status_dict[8]] = get_storage_network_status_for_simple_volume()
                        if update_status[status_dict[8]] == status_level_dict[3]:
                            update_status[status_dict[5]] = status_level_dict[3]
                        else:
                            update_status[status_dict[5]] = file_status[status_dict[5]]

                if not simple_flag:
                    #update raid sync status
                    check_raid_sync()
                    update_status[status_dict[5]] = file_status[status_dict[5]]
                    update_status[status_dict[3]] = get_storage_network_status()
                    if update_status[status_dict[3]] != {}:
                        status=get_storage_status(update_status[status_dict[3]])
                    else:
                        status=status_level_dict[3]
                    update_status[status_dict[8]] = status
                    # Skip it if volume only using share storage
                    if USX_DICT['ibd'] is True:
                        if update_status[status_dict[8]] == status_level_dict[3]: # if storage network is disconnected, dedup fs then is not operational
                            update_status[status_dict[4]] = status_level_dict[3]
#                        ret = check_dedup_mount_status()
#                        if ret.has_key('value'):
#                            ret = verify_raid(ret['value'])
#
#                            if ret['sync'] != {}:
#                                update_status[status_dict[12]] = {}
#                                update_status[status_dict[12]]['value']= ret['sync']['status']
#                                update_status[status_dict[12]]['details']= ret['sync']['details']
#                            elif not ret['status']:
#                                update_status[status_dict[12]] = {}
#                                update_status[status_dict[12]]['value']= status_level_dict[3]
#                                update_status[status_dict[12]]['details']= 'Raid is blocked'
#                            elif ret['status']:
#                                update_status[status_dict[12]] = {}
#                                update_status[status_dict[12]]['value']= status_level_dict[0]
#                                update_status[status_dict[12]]['details']= ''
                    else:
                        update_status[status_dict[8]] = update_status[status_dict[4]]

                debug("=== Update Volume Resource Status===")
                update_container_uuid()
                publish_resource_status(update_status, resource_uuid)
            else:
                #Updated bootstrap status when bootstrap status is OK
                bootstrap_status = get_bootstrap_status('ha_standby')
                if bootstrap_status == status_level_dict[0]:
                    update_container_status[status_dict[0]] = status_level_dict[0]

        debug("=== Update Volume Container Status===")
        publish_container_status(update_container_status)

        check_log_files()
else:
    debug("ERROR : Unknown command option %s" % sys.argv[1])
    sys.exit(1)
debug("===       END OF UPDATE        ===")
