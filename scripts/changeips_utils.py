# -*- coding:utf-8 -*-
"""USX IP change tool base function module

This module offer some basic functions to change ip address, netmask, dns, gateway and so on.

"""

import os
import sys
import json
import re
import subprocess
import socket
import fcntl
import struct
import time
import xml.etree.ElementTree as ET
sys.path.append('/opt/milio/atlas/system')
sys.path.append('/opt/milio/libs/atlas')
sys.path.append('/opt/milio/scripts')
import sshw
from sshw import run_cmd, call_rest_api
from log import set_log_file, debug, info, errormsg, warn
from usx_mount_umount_adm import umount_adm, mount_adm
from status_update import create_job, send_status, get_data_from_json, USE_LOCALHOST
from atl_alerts import send_alert_change_ip

HYPERVISOR_TYPE_VMWARE = 'VMware'
HYPERVISOR_TYPE_XEN = 'Xen'
VOLUME_BOOTSTRAP_CONFIG_FLAG = '/usr/share/ilio/configured'
NETWORK_CONFIG = '/etc/network/interfaces'
VOLUME_ATLAS_JSON = '/etc/ilio/atlas.json'
VOLUME_GRID_MEMBER_JSON = '/opt/amc/agent/config/grid_members.json'
JOBID_FILE_BAK = '/etc/ilio/atlas-jobid.bak'
JOBID_FILE = '/etc/ilio/atlas-jobid'
OVF_ENV_XML_FILE = '/opt/milio/atlas/scripts/ovfenv.xml'
CHANGEIP_LOG_FILE = '/var/log/usx-change-ip.log'
DNS_RESOLVE_FILE = '/etc/resolv.conf'
VMWARE_OVF_ENV_OE = 'http://schemas.dmtf.org/ovf/environment/1'
VMWARE_OVF_ENV_NS2 = 'http://www.vmware.com/schema/ovfenv'

API_URL = 'https://%s:8443/usxmanager%s'
VOLUME_CONTAINERS_COMMITCHANGEIP_SUBURL = '/usx/inventory/volume/containers/commitchangeip?api_key=%s'

XEN_WRITE_CMD = '/usr/bin/xenstore-write'
XEN_READ_CMD = '/usr/bin/xenstore-read'
VMWARE_CMD = '/usr/bin/vmtoolsd'
# Each value length should be shorter than the limit
XEN_OVF_ENV_VAL_LENGTH_LIMIT = 1025

TASK_NAME = 'Changed IP address'
TASK_DESCRIPTION = 'Notifying remote USX_VOLUME to change the IP configuration'
TASK_TYPE = 'CHANGE_IP'


"""
Run cmd with big pipe
Params:
    cmd:      command
    timeout   time out

Return:
   Success: return a dictionary with 2 key stdout and stderr
   Failure: False
"""
def run_cmd_big_pipe(cmd, timeout=1800):
    rtn_dict = {}
    obj_rtn = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    start_time = time.time()
    run_time = 0
    rtn_dict['stdout'] = ''
    rtn_dict['stderr'] = ''
    while True:
        if obj_rtn.poll() != None:
            break
        out, err = obj_rtn.communicate()
        rtn_dict['stdout'] += out
        rtn_dict['stderr'] += err
        end_time = time.time()

        run_time = end_time - start_time

        if run_time > timeout:
            obj_rtn.terminate()
            raise Exception('Run command timeout: %s' % cmd)

        time.sleep(0.1)

    rtn_dict['returncode'] = obj_rtn.returncode
    return rtn_dict

def check_hypervisor_type():
    """
    Parameters:
        None.
    Returns:
        Success:    A string of hypervisor type.
        Failure:    Empty string.
    Description:
        check hypervisor type.
    """
    hypervisor_type=""
    cmd_res = run_cmd('dmidecode -s system-manufacturer')
    ret = cmd_res['returncode']
    output = cmd_res['stdout']
    if (ret != 0) or (output is None) or (not output) or (len(output) <= 0):
        debug('WARNING could not get hypervisor_type from dmidecode. Checking for Xen...')
        if os.path.exists('/dev/xvda') == True:
            hypervisor_type='Xen'
        elif os.path.exists('/dev/sda') == True:
            hypervisor_type='VMware'
    else:
        output=output.strip()
        if 'Microsoft' in output:
            hypervisor_type='hyper-v'
        elif 'VMware' in output:
            hypervisor_type='VMware'
        elif 'Xen' in output:
            hypervisor_type='Xen'
        else:
            debug('WARNING do not support hypervisor_type %s' % output)

    return hypervisor_type

def _get_properties(ovf_env_xml):
    """
    Parameters:
        ovf_env_xml:    the xml object of ovf environment.
    Returns:
        Success:    A List of properties
        Failure:    Empty list
    Description:
        get properties from ovf environment.
    """
    properties = ovf_env_xml.findall('./{%s}PropertySection/{%s}Property' \
        % (VMWARE_OVF_ENV_OE, VMWARE_OVF_ENV_OE)) if ovf_env_xml is not None else []
    if properties == []:
        errormsg('Failed to get properties from VMWARE ovf env.')
    return properties

def _get_ethadapter(ovf_env_xml):
    """
    Parameters:
        ovf_env_xml:    the xml object of ovf environment.
    Returns:
        Success:    A List of ethernet adapters
        Failure:    Empty list
    Description:
        get ethernet adapters from ovf environment.
    """
    ethadapters = ovf_env_xml.findall('./{%s}EthernetAdapterSection/{%s}Adapter' \
            % (VMWARE_OVF_ENV_NS2, VMWARE_OVF_ENV_NS2)) if ovf_env_xml is not None else []
    if ethadapters == []:
        errormsg('Failed to get ethernet adapters from VMWARE ovf env.')
    return ethadapters

def _get_device_mac():
    """
    Parameters:
        None.
    Returns:
        Success:    A dictionary of network device and hw address map.
        Failure:    Empty dictionary.
    Description:
        get netowrk device and hw address map.
    """
    dev_mac_dict = {}
    dev_name_list = get_sys_net_ifaces()
    for dev_name in dev_name_list:
        res = run_cmd('ip addr show %s | grep -o -P "(?<=link/ether )([\w|\d]{2}:){5}[\w|\d]{2}"' % (dev_name))
        if res['returncode'] == 0:
            dev_mac_dict[dev_name] = res['stdout'].strip()
        else:
            errormsg('Failed to get hw address of %s' % (dev_name))
            dev_mac_dict = {}
            break
    return dev_mac_dict

def _get_ovf_env_vmware():
    """
    Parameters:
        None.
    Returns:
        Success:    A XML object of ovf environment.
        Failure:    None.
    Description:
        get ovf environment on vmware.
    """
    try:
        ovf_env_str = ""
        vmtools_cmd = '%s --cmd "info-get guestinfo.ovfEnv"' % (VMWARE_CMD)
        res = run_cmd_big_pipe(vmtools_cmd)
        ovf_env_str = res['stdout']
        ovf_root = ET.fromstring(ovf_env_str) if ovf_env_str else None
        if ovf_root is None:
            errormsg('Failed to get xml from ovfEnv.')
        return ovf_root
    except Exception as ex:
        errormsg('Exception while getting ovfEnv.')
        raise ex

def _set_ovf_env_vmware(ovf_env_xml, env_dict):
    """
    Parameters:
        ovf_env_xml:    the xml object of ovf environment.
        env_dict:   the dict of environment.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set ovf environment on vmware.
    """
    ret = False
    if ovf_env_xml is not None:
        properties = _get_properties(ovf_env_xml)
        ethernetadapters = _get_ethadapter(ovf_env_xml)
        eth_mac_dict = _get_device_mac()
        for key_name, key_value in env_dict.items():
            for property in properties:
                key_name_ovf = property.attrib['{%s}key' % (VMWARE_OVF_ENV_OE)]
                if key_name_ovf == key_name:
                    property.set('{%s}value' % (VMWARE_OVF_ENV_OE), key_value)

        for eth_adapter in ethernetadapters:
            mac = eth_adapter.attrib['{%s}mac' % (VMWARE_OVF_ENV_NS2)]
            network = eth_adapter.attrib['{%s}network' % (VMWARE_OVF_ENV_NS2)]
            for dev_name, hw_mac in eth_mac_dict.items():
                vmnetwork = '%s_vmnetwork' % (dev_name)
                if hw_mac == mac and vmnetwork in env_dict\
                                    and env_dict[vmnetwork]:
                    eth_adapter.set('{%s}network' % (VMWARE_OVF_ENV_NS2),\
                                                    env_dict[vmnetwork])

        vmtools_cmd = "%s --cmd 'info-set guestinfo.ovfEnv %s'" \
            % (VMWARE_CMD, ET.tostring(ovf_env_xml))
        res = run_cmd(vmtools_cmd)
        ret = True if res['returncode'] == 0 else False
        if not ret:
            errormsg('Failed to set ovf environment on vmware, details as below: %s %s.' \
                % (res['stdout'], res['stderr']))
    else:
        errormsg('Failed to set ovf environment on vmware, details as below: Parameters ovf environment xml is None.')
    return ret

def _set_ovf_env_xen(env_dict):
    """
    Parameters: <env_dict>
    Returns: data
    Description: set ovf property file
    """
    rtn = True
    try:
        cmd = '''%s vm-data/%s '%s' '''
        for key, value in env_dict.items():
            if key == 'Atlas-JSON':
                count = 0
                # debug(value)
                p = re.compile(r'\\(?=")')
                n_val = p.sub(r'\\\\', value)
                # debug(n_val)
                value = n_val
                while len(value) > XEN_OVF_ENV_VAL_LENGTH_LIMIT:
                    value_write = value[0 : XEN_OVF_ENV_VAL_LENGTH_LIMIT]
                    offline = 1
                    while value_write[XEN_OVF_ENV_VAL_LENGTH_LIMIT - offline] == '\\':
                        offline += 1
                    value_write = value[0 : XEN_OVF_ENV_VAL_LENGTH_LIMIT - (offline - 1)]
                    value = value[XEN_OVF_ENV_VAL_LENGTH_LIMIT - (offline - 1) :]
                    key = "AtlasJSON-%d" % (count)
                    # debug(cmd % (XEN_WRITE_CMD, key, value_write))
                    res = run_cmd(cmd % (XEN_WRITE_CMD, key, value_write))
                    if res['returncode'] != 0:
                        errormsg('Failed to set atlas json to ovf environment, details as below: %s %s' \
                                    % (res['stdout'], res['stderr']))
                        rtn = False
                        break
                    else:
                        count += 1
                else:
                    key = "AtlasJSON-%d" % (count)
                    # debug(cmd % (XEN_WRITE_CMD, key, value))
                    res = run_cmd(cmd % (XEN_WRITE_CMD, key, value))
                    if res['returncode'] != 0:
                        errormsg('Failed to set atlas json to ovf environment, details as below: %s %s' \
                                    % (res['stdout'], res['stderr']))
                        rtn = False
            else:
                res = run_cmd(cmd % (XEN_WRITE_CMD, key, value))
                if res['returncode'] != 0:
                    errormsg('Failed to set %s to ovf environment, details as below: %s %s' \
                                % (key, res['stdout'], res['stderr']))
                    rtn = False
            if not rtn:
                errormsg('Failed to set %s to ovf environment, the work will be stopped.')
                break
    except Exception as err:
        methodName = _set_ovf_env_xen.__name__
        errormsg('[%s] Failed to set ovf propery file. Details as below: %s' % (methodName, err))
        rtn = False
    return rtn

def check_backup_ovf_env():
    """
    Parameters:
        None.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        check and backup ovf environment.
    """
    ret = True
    hypervisor_type = check_hypervisor_type()
    cur_ovf_env_xml = get_ovf_env()
    if cur_ovf_env_xml is None and hypervisor_type == HYPERVISOR_TYPE_VMWARE:
        if os.path.isfile(OVF_ENV_XML_FILE):
            # get ovf environment from file
            bak_ovf_env_xml = ET.parse(OVF_ENV_XML_FILE)
            set_ovf_env(bak_ovf_env_xml.getroot(), {})
        else:
            errormsg('Failed to get ovf environment from file and system. Settings can not be recoveried.')
            ret = False
    else:
        if cur_ovf_env_xml is not None:
            # backup ovf environment
            ET.ElementTree(cur_ovf_env_xml).write(OVF_ENV_XML_FILE, encoding="utf-8", xml_declaration=True)
        else:
            if hypervisor_type == HYPERVISOR_TYPE_XEN:
                info('Not support for Xen.')
            else:
                errormsg('Failed to get ovf environment from file. Settings can not back up.')
                ret = False
    return ret

def get_ovf_env():
    """
    Parameters:
        None.
    Returns:
        Success:    A XML object of ovf environment.
        Failure:    None.
    Description:
        get ovf environment.
    """
    ovf_env_xml = None
    hypervisor_type = check_hypervisor_type()
    if hypervisor_type == HYPERVISOR_TYPE_VMWARE:
        info('Get ovf environment in vmware.')
        ovf_env_xml = _get_ovf_env_vmware()
    elif hypervisor_type == HYPERVISOR_TYPE_XEN:
        info('Hypervisor type is Xen.')
    else:
        errormsg('Unsupport hypervisor type %s' % (hypervisor_type))
    return ovf_env_xml

def set_ovf_env(ovf_env_xml, env_dict):
    """
    Parameters:
        ovf_env_xml:    the xml object of ovf environment (leave this None for using this in Xen).
        env_dict:   the dict of environment.
        { "hostname": xxxx,
          "dns: xxxx,
          "timezone": xxxx,
          "eth0_ip": xxxx,
          "eth0_netmask": xxxx,
          "eth0_gateway": xxxx,
          "eth0_storagenetwork": xxxx,
          "eth0_vmnetwork": xxxx,
          "eth1_ip": xxxx,
          "eth1_netmask": xxxx,
          "eth1_gateway": xxxx,
          "eth1_storagenetwork": xxxx,
          "eth1_vmnetwork": xxxx,
          "eth2_ip": xxxx,
          "eth2_netmask": xxxx,
          "eth2_gateway": xxxx,
          "eth2_storagenetwork": xxxx,
          "eth2_vmnetwork": xxxx,
          "eth3_ip": xxxx,
          "eth3_netmask": xxxx,
          "eth3_gateway": xxxx,
          "eth3_storagenetwork": xxxx,
          "eth3_vmnetwork": xxxx,
          "otk": xxx,
          "peerip": xxx,
          "orig_ip": xxx,
          "login": xxx,
          "pwd": xxx,
          "poweruserp": xxx,
          "Atlas-JSON": xxx
        }
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set ovf environment.
    """
    ret = False
    hypervisor_type = check_hypervisor_type()
    if hypervisor_type == HYPERVISOR_TYPE_VMWARE:
        info('Set ovf environment in vmware.')
        vmware_ovf_info = {}
        for key, value in env_dict.items():
            if key == 'eth0_ip':
                vmware_ovf_info['guestinfo.ilio.eth0_ipaddress'] = value
            elif key == 'eth0_netmask':
                vmware_ovf_info['guestinfo.ilio.eth0_netmask'] = value
            elif key == 'eth0_gateway':
                vmware_ovf_info['guestinfo.ilio.eth0_gateway'] = value
            elif key == 'eth0_storagenetwork':
                vmware_ovf_info['guestinfo.ilio.eth0_storagenetwork'] = value
            elif key == 'eth1_ip':
                vmware_ovf_info['guestinfo.ilio.eth1_ipaddress'] = value
            elif key == 'eth1_netmask':
                vmware_ovf_info['guestinfo.ilio.eth1_netmask'] = value
            elif key == 'eth1_gateway':
                vmware_ovf_info['guestinfo.ilio.eth1_gateway'] = value
            elif key == 'eth1_storagenetwork':
                vmware_ovf_info['guestinfo.ilio.eth1_storagenetwork'] = value
            elif key == 'eth2_ip':
                vmware_ovf_info['guestinfo.ilio.eth2_ipaddress'] = value
            elif key == 'eth2_netmask':
                vmware_ovf_info['guestinfo.ilio.eth2_netmask'] = value
            elif key == 'eth2_gateway':
                vmware_ovf_info['guestinfo.ilio.eth2_gateway'] = value
            elif key == 'eth2_storagenetwork':
                vmware_ovf_info['guestinfo.ilio.eth2_storagenetwork'] = value
            elif key == 'eth3_ip':
                vmware_ovf_info['guestinfo.ilio.eth3_ipaddress'] = value
            elif key == 'eth3_netmask':
                vmware_ovf_info['guestinfo.ilio.eth3_netmask'] = value
            elif key == 'eth3_gateway':
                vmware_ovf_info['guestinfo.ilio.eth3_gateway'] = value
            elif key == 'eth3_storagenetwork':
                vmware_ovf_info['guestinfo.ilio.eth3_storagenetwork'] = value
            elif key == 'dns':
                vmware_ovf_info['dns'] = value
            elif key == 'hostname':
                vmware_ovf_info['guestinfo.ilio.hostname'] = value
            elif key == 'poweruserp':
                vmware_ovf_info['guestinfo.ilio.poweruserp'] = value
            elif key == 'timezone':
                vmware_ovf_info['guestinfo.ilio.timezone'] = value
            else:
                vmware_ovf_info[key] = value

        ret = _set_ovf_env_vmware(ovf_env_xml, vmware_ovf_info)
    elif hypervisor_type == HYPERVISOR_TYPE_XEN:
        info('Set ovf environment in Xen.')
        xen_ovf_info = {}
        for key, value in env_dict.items():
            if key == 'eth0_ip':
                xen_ovf_info['eth0Ipaddress'] = value
            elif key == 'eth0_netmask':
                xen_ovf_info['eth0Netmask'] = value
            elif key == 'eth0_gateway':
                xen_ovf_info['eth0Gateway'] = value
            elif key == 'eth0_storagenetwork':
                xen_ovf_info['eth0Storagenetwork'] = value
            elif key == 'eth1_ip':
                xen_ovf_info['eth1Ipaddress'] = value
            elif key == 'eth1_netmask':
                xen_ovf_info['eth1Netmask'] = value
            elif key == 'eth1_storagenetwork':
                xen_ovf_info['eth1Storagenetwork'] = value
            elif key == 'eth2_ip':
                xen_ovf_info['eth2Ipaddress'] = value
            elif key == 'eth2_netmask':
                xen_ovf_info['eth2Netmask'] = value
            elif key == 'eth2_storagenetwork':
                xen_ovf_info['eth2Storagenetwork'] = value
            elif key == 'eth3_ip':
                xen_ovf_info['eth3Ipaddress'] = value
            elif key == 'eth3_netmask':
                xen_ovf_info['eth3Netmask'] = value
            elif key == 'eth3_storagenetwork':
                xen_ovf_info['eth3Storagenetwork'] = value
            elif key == 'eth0_vmnetwork' or key == 'eth1_vmnetwork' \
                or key == 'eth2_vmnetwork' or key == 'eth3_vmnetwork':
                # xen have no ovf environment of vmnetwork
                continue
            else:
                xen_ovf_info[key] = value

        ret = _set_ovf_env_xen(xen_ovf_info)
    else:
        errormsg('Unsupport hypervisor type %s' % (hypervisor_type))

    return ret

def _get_ovf_env_vmware_properties_val(ovfenv_xml, key_name):
    """
    Parameters:
        ovfenv_xml:    Xml object of ovf environment.
        key_name:   Key name.
    Returns:
        Success:    value of key_name.
        Failure:    None.
    Description:
        get properties val from ovf environment in vmware.
    """
    value = None
    properties = _get_properties(ovfenv_xml)
    for property in properties:
        key_name_ovf = property.attrib['{%s}key' % (VMWARE_OVF_ENV_OE)]
        if key_name_ovf == key_name:
            value = property.attrib['{%s}value' % (VMWARE_OVF_ENV_OE)]
            break
    eth_mac_dict = _get_device_mac()
    eth_adapters = _get_ethadapter(ovfenv_xml)
    for eth_adapter in eth_adapters:
        mac = eth_adapter.attrib['{%s}mac' % (VMWARE_OVF_ENV_NS2)]
        for dev_name, hw_mac in eth_mac_dict.items():
            if hw_mac == mac and '%s_vmnetwork' % (dev_name) == key_name:
                value = eth_adapter.attrib['{%s}network' % (VMWARE_OVF_ENV_NS2)]
                break
    if value is None:
        errormsg('There is no key %s.' % (key_name))
    return value

def _get_ovf_env_xen_properties_val(key_name):
    """
    Parameters:
        key_name:   Key name.
    Returns:
        Success:    value of key_name.
        Failure:    None.
    Description:
        get properties val from ovf environment in xen.
    """
    value = None
    if key_name == 'Atlas-JSON':
        # In xen, atlas json maybe split with multiple section to be stored
        # in the ovf environment. we need to connect them to a completed json.
        cmd = "%s vm-data/AtlasJSONisSplit" % (XEN_READ_CMD)
        res = run_cmd(cmd)
        if res['returncode'] == 0:
            if res['stdout'].strip() == 'true':
                # need split
                count = 0
                while True:
                    cmd = "%s vm-data/AtlasJSON-%d" % (XEN_READ_CMD, count)
                    res = run_cmd(cmd)
                    if res['returncode'] == 0:
                        if value is None:
                            value = res['stdout'].strip()
                        else:
                            value += res['stdout'].strip()
                        count += 1
                    else:
                        errormsg("Failed to get ovf environment on Xen with key %s, \
                            details as below: %s %s" % (key_name, res['stdout'], res['stderr']))
                        break
                if value is not None:
                    # take out // in the string
                    p = re.compile(r'\\(?=")')
                    n_val = p.sub('', value)
                    # debug(n_val)
                    value = n_val
            else:
                cmd = "%s vm-data/AtlasJSON" % (XEN_READ_CMD)
                res = run_cmd(cmd)
                if res['returncode'] == 0:
                    value = res['stdout'].strip()
                else:
                    errormsg("Failed to get ovf environment on Xen with key AtlasJSON, \
                        details as below: %s %s" % (res['stdout'], res['stderr']))
                    value = None
        else:
            errormsg("Failed to get ovf environment on Xen with key AtlasJSONisSplit, \
                        details as below: %s %s" % (res['stdout'], res['stderr']))
            value = None
    else:
        res = run_cmd("""%s vm-data/%s""" % (XEN_READ_CMD, key_name))
        if res['returncode'] == 0:
            value = res['stdout'].strip()
        else:
            errormsg("Failed to get ovf environment on Xen with key %s, \
                details as below: %s %s" % (key_name, res['stdout'], res['stderr']))
            value = None
    return value

def get_ovf_env_properties_val(ovfenv_xml, key_name):
    """
    Parameters:
        ovfenv_xml:    Xml object of ovf environment(leave this None if in Xen).
        key_name:   Key name.
        { "hostname": xxxx,
          "dns: xxxx,
          "timezone": xxxx,
          "eth0_ip": xxxx,
          "eth0_netmask": xxxx,
          "eth0_gateway": xxxx,
          "eth0_storagenetwork": xxxx,
          "eth0_vmnetwork": xxxx,
          "eth1_ip": xxxx,
          "eth1_netmask": xxxx,
          "eth1_gateway": xxxx,
          "eth1_storagenetwork": xxxx,
          "eth1_vmnetwork": xxxx,
          "eth2_ip": xxxx,
          "eth2_netmask": xxxx,
          "eth2_gateway": xxxx,
          "eth2_storagenetwork": xxxx,
          "eth2_vmnetwork": xxxx,
          "eth3_ip": xxxx,
          "eth3_netmask": xxxx,
          "eth3_gateway": xxxx,
          "eth3_storagenetwork": xxxx,
          "eth3_vmnetwork": xxxx,
          "otk": xxx,
          "peerip": xxx,
          "orig_ip": xxx,
          "login": xxx,
          "pwd": xxx,
          "poweruserp": xxx,
          "Atlas-JSON": xxx,
          "usxm_server_ip": xxxx
        }
    Returns:
        Success:    value of key_name.
        Failure:    None.
    Description:
        get properties val from ovf environment.
    """
    ret = None
    hypervisor_type = check_hypervisor_type()
    if hypervisor_type == HYPERVISOR_TYPE_VMWARE and ovfenv_xml is not None:
        info('Get ovf environment properties on vmware.')
        if key_name == 'eth0_ip':
            key_name = 'guestinfo.ilio.eth0_ipaddress'
        elif key_name == 'eth0_netmask':
            key_name = 'guestinfo.ilio.eth0_netmask'
        elif key_name == 'eth0_gateway':
            key_name = 'guestinfo.ilio.eth0_gateway'
        elif key_name == 'eth0_storagenetwork':
            key_name = 'guestinfo.ilio.eth0_storagenetwork'
        elif key_name == 'eth1_ip':
            key_name = 'guestinfo.ilio.eth1_ipaddress'
        elif key_name == 'eth1_netmask':
            key_name = 'guestinfo.ilio.eth1_netmask'
        elif key_name == 'eth1_gateway':
            key_name = 'guestinfo.ilio.eth1_gateway'
        elif key_name == 'eth1_storagenetwork':
            key_name = 'guestinfo.ilio.eth1_storagenetwork'
        elif key_name == 'eth2_ip':
            key_name = 'guestinfo.ilio.eth2_ipaddress'
        elif key_name == 'eth2_netmask':
            key_name = 'guestinfo.ilio.eth2_netmask'
        elif key_name == 'eth2_storagenetwork':
            key_name = 'guestinfo.ilio.eth2_storagenetwork'
        elif key_name == 'eth3_ip':
            key_name = 'guestinfo.ilio.eth3_ipaddress'
        elif key_name == 'eth3_netmask':
            key_name = 'guestinfo.ilio.eth3_netmask'
        elif key_name == 'eth3_storagenetwork':
            key_name = 'guestinfo.ilio.eth3_storagenetwork'
        elif key_name == 'hostname':
            key_name = 'guestinfo.ilio.hostname'
        elif key_name == 'poweruserp':
            key_name = 'guestinfo.ilio.poweruserp'
        elif key_name == 'timezone':
            key_name = 'guestinfo.ilio.timezone'
        elif key_name == 'usxm_server_ip':
            key_name = 'usxm_server_ip'
        ret = _get_ovf_env_vmware_properties_val(ovfenv_xml, key_name)
    elif hypervisor_type == HYPERVISOR_TYPE_XEN:
        info('Get ovf environment properties on Xen.')
        if key_name == 'eth0_ip':
            key_name = 'eth0Ipaddress'
        elif key_name == 'eth0_netmask':
            key_name = 'eth0Netmask'
        elif key_name == 'eth0_gateway':
            key_name = 'eth0Gateway'
        elif key_name == 'eth0_storagenetwork':
            key_name = 'eth0Storagenetwork'
        elif key_name == 'eth1_ip':
            key_name = 'eth1Ipaddress'
        elif key_name == 'eth1_netmask':
            key_name = 'eth1Netmask'
        elif key_name == 'eth1_gateway':
            key_name = 'eth1Gateway'
        elif key_name == 'eth1_storagenetwork':
            key_name = 'eth1Storagenetwork'
        elif key_name == 'eth2_ip':
            key_name = 'eth2Ipaddress'
        elif key_name == 'eth2_netmask':
            key_name = 'eth2Netmask'
        elif key_name == 'eth2_storagenetwork':
            key_name = 'eth2Storagenetwork'
        elif key_name == 'eth3_ip':
            key_name = 'eth3Ipaddress'
        elif key_name == 'eth3_netmask':
            key_name = 'eth3Netmask'
        elif key_name == 'eth3_storagenetwork':
            key_name = 'eth3Storagenetwork'
        elif key_name == 'usxm_server_ip':
            key_name = 'usxm_server_ip'
        elif key_name == 'eth0_vmnetwork' or \
            key_name == 'eth1_vmnetwork' or \
            key_name == 'eth2_vmnetwork' or \
            key_name == 'eth3_vmnetwork':
			# xen have no ovf environment of vmnetwork
            key_name = None
        if key_name is not None:
            ret = _get_ovf_env_xen_properties_val(key_name)
    else:
        if hypervisor_type == HYPERVISOR_TYPE_VMWARE and ovfenv_xml is None:
            errormsg("OVF environment should not be none in vmware.")
        else:
            errormsg('Unsupport hypervisor type %s' % (hypervisor_type))
    return ret

def reconfig_bootstrap_prepare():
    """
    Parameters:
        None.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set environment to let bootstrap reconfig system when reboot.
    """
    ret = True
    vmtools_cmd = '%s --cmd "info-set guestinfo.ilio.configured False"' % (VMWARE_CMD)
    res = run_cmd(vmtools_cmd)
    if res['returncode'] != 0:
        errormsg('Failed to set ovf environment "guestinfo.ilio.configured" on vmware, details as below: %s %s.' \
            % (res['stdout'], res['stderr']))
        ret = False
    # for volume
    if os.path.isfile(VOLUME_BOOTSTRAP_CONFIG_FLAG):
        os.remove(VOLUME_BOOTSTRAP_CONFIG_FLAG)
    return ret

def change_network_config_by_ovfenv(dev_name, new_ip=None, new_netmask=None, new_dns=None, new_gateway=None):
    """
    Parameters:
        dev_name:   network device name.
        new_ip: new ip address.
        new_netmask:   new netmask.
        new_dns:    new dns.
        new_gateway:    new gateway. 
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        change network config by setting ovf environment.
    """
    ovf_env_dict = {}
    ret = False
    if new_ip is not None:
        ovf_env_dict["guestinfo.ilio.%s_ipaddress" % (dev_name)] = new_ip
    if new_netmask is not None:
        ovf_env_dict["guestinfo.ilio.%s_netmask" % (dev_name)] = new_netmask
    if new_gateway is not None:
        ovf_env_dict["guestinfo.ilio.%s_gateway" % (dev_name)] = new_gateway
    if new_dns is not None:
        ret = _change_network_dns(new_dns)
    if ovf_env_dict != {}:
        ovfenv_xml = get_ovf_env()
        ret = set_ovf_env(ovfenv_xml, ovf_env_dict)
    return ret

def reboot_volume():
    """
    Parameters:
        None
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        reboot system.
    """
    res = run_cmd('reboot')
    ret = True if res['returncode'] == 0 else False
    return ret

def _change_network_dns(new_dns):
    """
    Parameters:
        new_dns:    new dns.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        change dns by file.
    """
    res = run_cmd("""[ -z "$(grep -o 'nameserver %s' %s)" ] && echo "nameserver %s" >> %s""" \
        % (new_dns, DNS_RESOLVE_FILE, new_dns, DNS_RESOLVE_FILE))
    ret = True if res['returncode'] == 0 else False
    return ret

def change_network_config_by_ilio(dev_name, new_ip=None, new_netmask=None,\
                                     new_dns=None, new_gateway=None):
    """
    Parameters:
        dev_name:    network device name.
        new_ip: new ip address.
        new_netmask:    new netmask.
        new_dns:    new dns.
        new_gateway:    new gateway.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        change network config by using ilio command.
    """
    ret = False
    # now volume is not support dns
    if dev_name is not None and dev_name:
        # set new
        cmd_arguments_str = " --interface=%s" % (dev_name)
        change_ip_flag = False
        if new_ip is not None and new_ip:
            cmd_arguments_str += " --address='%s'" % (new_ip)
            change_ip_flag = True
            if new_netmask is not None and new_netmask:
                cmd_arguments_str += " --netmask='%s'" % (new_netmask)
                change_ip_flag = True
                if new_gateway is not None and new_gateway:
                    cmd_arguments_str += " --gateway='%s'" % (new_gateway)
                    change_ip_flag = True
            else:
                errormsg("Netmask is a required option.")
                change_ip_flag = False
        else:
            errormsg("Ip address is a required option.")
            change_ip_flag = False

        if new_dns is not None and new_dns:
            ret = _change_network_dns(new_dns)
        if change_ip_flag:
            cmd = 'ilio net remove --interface=%s' % (dev_name)
            debug(cmd)
            res = run_cmd(cmd)
            ret = True if res['returncode'] == 0 else False
            if ret:
                cmd = "ilio net add_static%s" % (cmd_arguments_str)
                debug(cmd)
                res = run_cmd(cmd)
                ret = True if res['returncode'] == 0 else False
                if not ret:
                    errormsg("Failed to add new network config of interface %s, details as below: %s %s" \
                        % (dev_name, res['stdout'], res['stderr']))
            else:
                errormsg("Failed to remove network config of interface %s, details as below: %s %s" \
                    % (dev_name, res['stdout'], res['stderr']))
        else:
            errormsg("Failed to configure %s network, see the log above." % (dev_name))
            ret = False
        if not ret:
            errormsg("There is no network config changed, see the log above.")
    else:
        errormsg("Device name must be specified.")

    return ret

def _get_old_network_config_from_file(dev_name):
    """
    Parameters:
        dev_name:    network device name.
    Returns:
        A dictionary with old network config, if some members have no config of the
         device, the value of them will be None.
    Description:
        get old network config from file.
    """
    old_conf = {'interface':None, 'ip':None, 'netmask':None, 'gateway':None, 'export_ips':None}
    with open(NETWORK_CONFIG, 'r') as fp:
        network_cfg_list = fp.readlines()
        cfg_re = re.compile('((\d+\.){3}\d+)')
        begin = False
        for line in network_cfg_list:
            if begin:
                res = cfg_re.search(line)
                if 'address' in line and res:
                    old_conf['ip'] = res.group(1)
                elif 'netmask' in line and res:
                    old_conf['netmask'] = res.group(1)
                elif 'gateway' in line and res:
                    old_conf['gateway'] = res.group(1)
                elif 'iface' in line:
                    break

            if 'iface %s' % (dev_name) in line:
                old_conf['interface'] = dev_name
                begin = True

        return old_conf

def _get_old_network_config_from_command(dev_name):
    """
    Parameters:
        dev_name:    network device name.
    Returns:
        A dictionary with old network config, if some members have no config of the
         device, the value of them will be None.
        {
            'interface' : <network device name>,
            'ip' : <ip address>,
            'netmask' : <netmask>,
            'gateway' : <gateway>,
            'export_ips' : <virtual network ip addresses>,
        }
    Description:
        get old network config from command.
    """
    cur_conf = {'interface':None, 'ip':None, 'netmask':None, 'gateway':None, 'export_ips':None}
    # set interface
    cur_conf['interface'] = dev_name
    # get ip address
    res = run_cmd('ip addr show %s | grep -o -P "(?<=inet )(\d+\.){3}\d+" | xargs' % (dev_name))
    if res['returncode'] == 0:
        ip_addr_str = res['stdout'].strip()
        ip_addr_list = ip_addr_str.split(' ')
        if len(ip_addr_list) > 1:
            cur_conf['ip'] = ip_addr_list[0]
            ip_addr_list.remove(cur_conf['ip'])
            cur_conf['export_ips'] = ip_addr_list
        else:
            cur_conf['ip'] = ip_addr_str
    # get netmask
    res = run_cmd("ifconfig %s | grep -o -P '(?<=Mask:)(\d+\.){3}\d+'" % (dev_name))
    if res['returncode'] == 0:
        cur_conf['netmask'] = res['stdout'].strip()
    # get gateway
    res = run_cmd("""route -n | awk '$4~/UG/{if($8=="%s"){print $2;}}'""" % (dev_name))
    if res['returncode'] == 0:
        cur_gateway = res['stdout'].strip()
        if cur_gateway:
            cur_conf['gateway'] = cur_gateway

    return cur_conf

def _get_old_network_config_from_atlas(dev_name):
    pass

def _update_network_config_atlas(atlas_json, dev_name, new_ip, new_netmask, new_gateway, new_networkname):
    """
    Parameters:
        atlas_json: atlas json
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set network config to atlas json.
    """
    ret = True
    try:
        for nic in atlas_json['usx']['nics']:
            if nic['devicename'] == dev_name:
                if new_ip:
                    nic['ipaddress'] = new_ip
                if new_netmask:
                    nic['netmask'] = new_netmask
                if new_gateway:
                    nic['gateway'] = new_gateway
                if new_networkname:
                    nic['networkname'] = new_networkname
                break
        else:
            errormsg('Can not find %s config.' % (dev_name))
            ret = False
    except Exception as ex:
        errormsg("Parse atlas json error, details as below: %s" % (ex))
        ret = False
    return ret

def set_vmnetwork_ovf_env(vmnetwork):
    """
    Parameters:
        vmnetwork:  virtual machine network name.
    Returns:
        Success:    True
        Failure:    False
    Description:
        set vmnetwork to ovf environment.
    """

def set_net_cfg_ovf_env(dev_name, new_ip, new_netmask,\
                        new_dns, new_gateway, new_networkname):
    """
    Parameters:
        dev_name:    network device name.
        new_ip: new ip address.
        new_netmask:    new netmask.
        new_dns:    new dns.
        new_gateway:    new gateway.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set network config to ovf environment.
    """
    ret = False
    ovfenv_xml = get_ovf_env()
    # atlas_json_str = get_ovf_env_properties_val(ovfenv_xml, 'Atlas-JSON')
    # if atlas_json_str:
    try:
        # atlas_json = json.loads(atlas_json_str)
        # _update_network_config_atlas(atlas_json, dev_name, new_ip, \
        #                 new_netmask, new_gateway, new_networkname)
        # ovf_env_dict = {'Atlas-JSON' : json.dumps(atlas_json)}
        ovf_env_dict = {}
        if new_ip is not None and new_ip:
            ovf_env_dict['%s_ip' % (dev_name)] = new_ip
        if new_netmask is not None and new_netmask:
            ovf_env_dict['%s_netmask' % (dev_name)] = new_netmask
        if new_gateway is not None and new_gateway:
            ovf_env_dict['%s_gateway' % (dev_name)] = new_gateway
        if new_networkname is not None and new_networkname:
            ovf_env_dict['%s_vmnetwork' % (dev_name)] = new_networkname
        ret = set_ovf_env(ovfenv_xml, ovf_env_dict)
    except Exception as ex:
        errormsg("Some exception happend while parse atlas json : %s" % (ex))
        ret = False
    # else:
    #     errormsg('Failed to get atlas json from ovf environment.')
    return ret

def set_net_cfg_atlas_file(dev_name, new_ip, new_netmask, \
                        new_dns, new_gateway, new_networkname):
    """
    Parameters:
        dev_name:    network device name.
        new_ip: new ip address.
        new_netmask:    new netmask.
        new_dns:    new dns.
        new_gateway:    new gateway.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set network config to atlas json in file.
    """
    ret = False
    # update /etc/ilio/atlas.json
    atlas_json = _get_atlas_json_from_file()
    if atlas_json is not None:
        ret = _update_network_config_atlas(atlas_json, dev_name, new_ip,\
                             new_netmask, new_gateway, new_networkname)
        if ret:
            with open(VOLUME_ATLAS_JSON, 'w') as fp:
                atlas_json_str = json.dumps(atlas_json)
                fp.write(atlas_json_str)
                fp.flush()
        else:
            errormsg("Failed to set new config to atlas json, see the log above.")
    else:
        errormsg('Failed to get atlas json from file, see the log above.')
    return ret

def get_export_ip(dev_name, storage_ip):
    """
    Parameters:
        dev_name:    network device name.
    Returns:
        A list of export ip information.
    Description:
        get export ip config by using ip command.
    """
    export_ip = None
    netmask = None
    broadcast = None
    res = run_cmd('ip addr show %s | grep -P "(?<=inet )(\d+\.){3}\d+" | grep -v "%s"' % (dev_name, storage_ip))
    if res['returncode'] == 0:
        export_ip_info = res['stdout'].strip()
        export_ip_info_list = export_ip_info.split(' ')
        export_ip_netmask_list = export_ip_info_list[1].split('/')
        debug(export_ip_netmask_list)
        export_ip = export_ip_netmask_list[0]
        netmask = export_ip_netmask_list[1]
        broadcast = export_ip_info_list[3]
    return (export_ip, netmask, broadcast)

def remove_export_ip_config(dev_name, export_ip, netmask, brd):
    """
    Parameters:
        dev_name:    network device name.
        export_ip: export ip address.
        netmask:    netmask.
        brd:    broadcast.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        remove export ip by using ip command.
    """
    ret = False
    # now volume is not support dns
    if dev_name is not None and export_ip is not None and netmask is not None\
                                                         and brd is not None:
        # remove ip config 
        res = run_cmd('ip -f inet addr delete %s/%s brd %s dev %s' \
                    % (export_ip, netmask, brd, dev_name))
        debug(res['stdout'])
        debug(res['stderr'])
        ret = True if res['returncode'] == 0 else False
    return ret

def add_export_ip_config(dev_name, export_ip, netmask, brd):
    """
    Parameters:
        dev_name:    network device name.
        export_ip: export ip address.
        netmask:    netmask.
        brd:    broadcast.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        set export ip by using ip command.
    """
    ret = False
    # now volume is not support dns
    if dev_name is not None and export_ip is not None and netmask is not None\
                                                         and brd is not None:
        # remove ip config 
        res = run_cmd('ip -f inet addr add %s/%s brd %s dev %s' \
                    % (export_ip, netmask, brd, dev_name))
        ret = True if res['returncode'] == 0 else False
    return ret

def change_network_config(network_config):
    """
    Parameters:
        network_config:    network config dictionary.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        change network config.
    """
    ret = False
    try:
        ret = change_network_config_by_ilio(network_config['interface'], \
                network_config['ip'], network_config['netmask'], \
                network_config['dns'], network_config['gateway'])
    except Exception as ex:
        errormsg('Some exceptions happend in change_network_config, detailes as below: %s' % (str(ex)))
        ret = False
    finally:
        return ret

def update_net_cfg_atlas_file(network_config):
    """
    Parameters:
        network_config:    network config dictionary.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update network config to atlas file.
    """
    ret = False
    try:
        ret = set_net_cfg_atlas_file(network_config['interface'], \
                network_config['ip'], network_config['netmask'], \
                network_config['dns'], network_config['gateway'], \
                network_config['vmnetwork'])
    except Exception as ex:
        errormsg('Some exceptions happend in update_net_cfg_atlas_file, detailes as below: %s' % (str(ex)))
        ret = False
    finally:
        return ret

def update_net_cfg_ovf_env(network_config):
    """
    Parameters:
        network_config:    network config dictionary.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update network config to ovf environment.
    """
    ret = False
    try:
        ret = set_net_cfg_ovf_env(network_config['interface'], \
                network_config['ip'], network_config['netmask'], \
                network_config['dns'], network_config['gateway'], \
                network_config['vmnetwork'])
    except Exception as ex:
        errormsg('Some exceptions happend in update_net_cfg_ovf_env, detailes as below: %s' % (str(ex)))
        ret = False
    finally:
        return ret

def change_ip(network_config_list):
    """
    Parameters:
        network_config_list:    network config list.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        change network config.
    """
    debug('Network config list : %s' % (network_config_list))
    rflag = True
    for nc in network_config_list:
        ret = change_network_config(nc)
        if not ret:
            rflag = ret
        else:
            # update config to /etc/ilio/atlas.json
            ret = update_net_cfg_atlas_file(nc)
            if not ret:
                rflag = ret
            else:
                # update config to ovf environment
                ret = update_net_cfg_ovf_env(nc)
                if not ret:
                    rflag = ret
        if not rflag:
            break

    return rflag

def _update_raid_plan(atlas_json, key_name, key_val):
    """
    Parameters:
        atlas_json:    Json object of atlas.json.
        key_name:   key name.
        key_val:    key val.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update raid plan sub function.
    """
    ret = True
    try:
        raid_plan_json = atlas_json['raidplans'][0]
        if key_name not in raid_plan_json:
            errormsg('Failed to get %s in raid plans, please check %s' % (VOLUME_ATLAS_JSON))
            ret = False
        raid_plan_json[key_name] = key_val
    except Exception as ex:
        errormsg('Some exception happend while parse raid plan of atlas json : %s' % (ex))
        ret = False
    finally:
        return ret

def update_raid_plan_from_ovfenv(raid_plan_dict):
    """
    Parameters:
        raid_plan_dict:    Raid plan dictionary.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update raid plan from ovf environment.
    """
    ret = False
    ovfenv_xml = get_ovf_env()
    if ovfenv_xml:
        try:
            atlas_json_str = get_ovf_env_properties_val(ovfenv_xml, 'Atlas-JSON')
            if atlas_json_str:
                atlas_json = json.loads(atlas_json_str)
                ret = True
                for key_name, key_val in raid_plan_dict:
                    sret = _update_raid_plan(atlas_json, key_name, key_val)
                    if not sret:
                        ret = sret
                if ret:
                    ret = set_ovf_env(ovf_env_xml, {'Atlas-JSON' : json.dumps(atlas_json)})
            else:
                errormsg('Failed to get atlas json from ovf environment, see the log above.')
        except Exception as ex:
            errormsg('Some exception happend, details as below: %s' % (ex))
    else:
        errormsg('Failed to get ovf environment xml, see the log above.')
    return ret

def _get_atlas_json_from_file():
    """
    Parameters:
        None.
    Returns:
        Success:    A dictionary of atlas json.
        Failure:    None.
    Description:
        get atlas json from file.
    """
    atlas_json = None
    if os.path.isfile(VOLUME_ATLAS_JSON):
        with open(VOLUME_ATLAS_JSON, 'r') as fp:
            atlas_json = json.load(fp)
    else:
        errormsg('File %s not existed!' % (VOLUME_ATLAS_JSON))
    return atlas_json
    
def update_raid_plan_from_file(raid_plan_dict):
    """
    Parameters:
        raid_plan_dict: A dictionary of raid plan.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update raid plan of atlas json from file.
    """
    ret = False
    # update /etc/ilio/atlas.json
    atlas_json = _get_atlas_json_from_file()
    if atlas_json is not None:
        with open(VOLUME_ATLAS_JSON, 'w') as fp:
            for key_name, key_val in raid_plan_dict:
                sret = _update_raid_plan(atlas_json, key_name, key_val)
                if not sret:
                    ret = sret
            atlas_json_str = json.dumps(atlas_json)
            fp.write(atlas_json_str)
            fp.flush()
            ret = True
    else:
        errormsg('Failed to get atlas json from file, see log above.')
    return ret

def update_raid_plan(raid_plan_dict):
    """
    Parameters:
        raid_plan_dict: A dictionary of raid plan.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update raid plan of atlas json.
    """
    # update it to ovfenv
    ret = update_raid_plan_from_ovfenv(raid_plan_dict)
    # update it to file
    ret =update_raid_plan_from_file(raid_plan_dict)
    # update it from rest api
    return ret

def shutdown_dedupfs_ibd():
    pass

def start_dedupfs_ibd():
    pass

def update_volume_container_grid(amc_ip, volume_uuid, grid_json_str):
    """
    Parameters:
        amc_ip: AMC server ip address.
        volume_uuid:    Uuid of the volume.
        grid_json_str:  A string of grid with json format.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update volume container grid.
    """
    # call rest api /usx/inventory/volume/container/commitchangeip?api_key=uuid
    rest_url = VOLUME_CONTAINERS_COMMITCHANGEIP_SUBURL % (volume_uuid)
    debug(API_URL % (amc_ip, rest_url))
    debug(grid_json_str)
    sshw.TIME_OUT = 120
    ret = call_rest_api(API_URL % (amc_ip, rest_url), 'POST', grid_json_str)
    debug(ret)
    return ret

def get_all_amc_info(amc_ip='127.0.0.1', api_key=''):
    error_rtn = 'Get all amc info fail'

    API_URL = 'https://' + amc_ip + ':8443/usxmanager/usxmanager'
    if api_key:
        API_URL += "?api_key=%s" % api_key
    req_type = 'GET'
    debug(API_URL)
    rtn = call_rest_api(API_URL, req_type)
    if not rtn or 'USXManagerDataVO' not in rtn:
        errormsg(rtn)
        return error_rtn
    try:
        rtn_dict = json.loads(rtn)
        return rtn_dict
    except:
        return error_rtn

def get_volume_info(amc_ip='127.0.0.1', api_key='', target_uuid=''):
    # Get volume and ha info
    error_rtn = "Get volume node info fail"
    API_URL = 'https://' + amc_ip + ':8443/usxmanager/usx/inventory/volume/containers/%s?composite=false'%(target_uuid)
    if api_key:
        API_URL += "&api_key=%s" % api_key
    req_type = 'GET'
    rtn = call_rest_api(API_URL, req_type)
    if not rtn or 'VolumeContainerDataVO' not in rtn:
        errormsg(rtn)
        return error_rtn
    rtn_dict = json.loads(rtn)
    item = rtn_dict['data']
    volume_dict = {}
    volume_dict['name'] = item['usxvm']['vmname']
    volume_dict['uuid'] = item['uuid']
    if len(item['nics']) == 1:
        volume_dict['eth0'] = item['nics'][0]['ipaddress']
    else:
        for item_nics in item['nics']:
            if item_nics['storagenetwork'] == False:
                volume_dict['eth0'] = item_nics['ipaddress']
    return volume_dict

def _get_amc_master_ip(amc_ip, volume_uuid):
    master_amc_ip = ''
    rtn = get_all_amc_info(amc_ip, volume_uuid)
    if rtn != 'Get all amc info fail':
        amc_info = rtn
        for item in amc_info["items"]:
            if item["isdbserver"]:
                master_amc_ip = item["ipaddress"]
                break
    else:
        debug("Failed to get all USX Manager information.")
    return master_amc_ip

def _check_if_volume_in_amc(amc_ip, vol_ip, volume_uuid):
    ret = False
    rtn = get_volume_info(amc_ip, volume_uuid, volume_uuid)
    if rtn != 'Get volume node info fail' and rtn['eth0'] == vol_ip:
        ret = True
    else:
        debug("Failed to get volume information from %s" % (amc_ip))
    return ret

def _get_amc_ip_if_available():
    # for USX-76995
    # if we can get amc ip from ovf environment usxm_server_ip
    # we need to check if it available, we also need to check
    # the amc ip in the grid_member.json
    amc_ip = ''
    ovfenv_xml = get_ovf_env()
    ovf_amc_ip = get_ovf_env_properties_val(ovfenv_xml, 'usxm_server_ip')
    grid_amc_ip = _get_volume_amc_ip()
    local_ip = get_local_ip()
    if ovf_amc_ip:
        ovf_amc_ip_reachable = is_reachable(ovf_amc_ip)
        grid_amc_ip_reachable = is_reachable(grid_amc_ip)
        if grid_amc_ip_reachable and not ovf_amc_ip_reachable:
            # If usx manager ip of grid_member is reachable and usxm_server_ip
            # of ovf is not reachable, script do not update grid_member.json file
            debug("The IP address of usxm_server_ip is not reachable, the USX Manager IP update will be ignored.")
        elif not grid_amc_ip_reachable and ovf_amc_ip_reachable:
            amc_ip = ovf_amc_ip
        elif not grid_amc_ip_reachable and not ovf_amc_ip_reachable:
            errormsg("Both USX Manager IP address of OVF and grid_member.json is not reachable.")
        elif grid_amc_ip_reachable and ovf_amc_ip_reachable:
            # 1. get USX Manager information from each IP and check the master
            # 2. get Volume information from each IP and check if volume in it
            volume_uuid = _get_volume_uuid_from_atlas_by_ip()
            ovf_amc_ip_master = _get_amc_master_ip(ovf_amc_ip, volume_uuid)
            grid_amc_ip_master = _get_amc_master_ip(grid_amc_ip, volume_uuid)
            if ovf_amc_ip_master == grid_amc_ip_master :
                # in the same cluster
                amc_ip = ovf_amc_ip_master
            else:
                vol_in_ovf_flag = _check_if_volume_in_amc(ovf_amc_ip_master, local_ip, volume_uuid)
                if vol_in_ovf_flag:
                    amc_ip = ovf_amc_ip_master
                else:
                    vol_in_grid_flag = _check_if_volume_in_amc(grid_amc_ip_master, local_ip, volume_uuid)
                    if vol_in_grid_flag:
                        amc_ip = grid_amc_ip_master
                    else:
                        errormsg("Can not find any information of this volume in both two USX Managers.")
    else:
        debug("No USX Manager IP found in the ovf environment, the USX Manager check will be skipped.")
    return amc_ip

def _check_amc_ip_changed_ovf():
    """
    Parameters:
        None
    Returns:
        Success: True
        Failure: False
    Description:
        check if amc ip changed on ovf environment.
    """
    ret = False
    try:
        # ovfenv_xml = get_ovf_env()
        # amc_ip = get_ovf_env_properties_val(ovfenv_xml, 'usxm_server_ip')
        amc_ip = _get_amc_ip_if_available()
        if amc_ip:
            with open(VOLUME_GRID_MEMBER_JSON, 'r+') as fp:
                grid_mem = json.load(fp)
                # if not same, the grid_member.json will be updated.
                if amc_ip != grid_mem['members'][0]['ipaddress']:
                    grid_mem['members'][0]['ipaddress'] = amc_ip
                    fp.seek(0, 0)
                    fp.write(json.dumps(grid_mem))
                    fp.flush()
                    # update diamond config
                    res = run_cmd('/usr/bin/python /opt/milio/atlas/system/diamondcfgsetup.pyc')
                    if res['returncode'] != 0:
                        errormsg('Failed to update diamond config%s.' % (' due to %s' % (res['stderr']) if res['stderr'] else ''))
                    else:
                        debug('USX Manager IP changed from ovf environment.')
                        ret = True
                else:
                    debug('USX Manager IP not changed from ovf environment.')
        else:
            debug('No USX Manager IP found in the ovf environment, the check will be skipped.')
    except Exception as ex:
        errormsg('Some exception happend while checking USX Manager IP in the ovf environment, details as below: %s' % (ex))
    finally:
        return ret

def _get_volume_amc_ip():
    """
    Parameters:
        None
    Returns:
        Success:    A string of amc ip address.
        Failure:    Empty string.
    Description:
        get amc manager ip address of the volume.
    """
    amc_ip = ''
    try:
        with open(VOLUME_GRID_MEMBER_JSON, 'r') as fp:
            grid_mem = json.load(fp)
            amc_ip = grid_mem['members'][0]['ipaddress']
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return amc_ip

def _get_volume_uuid_from_atlas_by_ip():
    """
    Parameters:
        None
    Returns:
        Success:    A string of volume uuid.
        Failure:    Empty string.
    Description:
        get amc manager ip address of the volume.
    """
    uuid = ''
    try:
        atlas_json = _get_atlas_json_from_file()
        if atlas_json is not None:
            uuid = atlas_json['usx']['uuid']
        else:
            errormsg('Failed to get atlas json from file, see log above.')
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return uuid

def _get_storage_ip_from_atlas():
    """
    Parameters:
        None
    Returns:
        Success:    A string of storage ip address.
        Failure:    Empty string.
    Description:
        get storage ip address of the volume.
    """
    storage_ip = ''
    try:
        atlas_json = _get_atlas_json_from_file()
        if atlas_json is not None:
            for nic in atlas_json['usx']['nics']:
                if nic['storagenetwork']:
                    storage_ip = nic['ipaddress']
        else:
            errormsg('Failed to get atlas json from file, see log above.')
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return storage_ip

def _get_storage_ip_devicename_from_atlas():
    """
    Parameters:
        None
    Returns:
        Success:    A string of storage ip address.
        Failure:    Empty string.
    Description:
        get storage ip address of the volume.
    """
    storage_ip_devicename = ''
    try:
        atlas_json = _get_atlas_json_from_file()
        if atlas_json is not None:
            for nic in atlas_json['usx']['nics']:
                if nic['storagenetwork']:
                    storage_ip_devicename = nic['devicename']
        else:
            errormsg('Failed to get atlas json from file, see log above.')
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return storage_ip_devicename

def _get_management_ip_from_atlas():
    """
    Parameters:
        None
    Returns:
        Success:    A string of management ip address.
        Failure:    Empty string.
    Description:
        get management ip address of the volume.
    """
    management_ip = ''
    try:
        atlas_json = _get_atlas_json_from_file()
        if atlas_json is not None:
            for nic in atlas_json['usx']['nics']:
                if not nic['storagenetwork']:
                    management_ip = nic['ipaddress']
        else:
            errormsg('Failed to get atlas json from file, see log above.')
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return management_ip

def _get_management_ip_devicename_from_atlas():
    """
    Parameters:
        None
    Returns:
        Success:    A string of management ip address.
        Failure:    Empty string.
    Description:
        get management ip address of the volume.
    """
    management_ip_devicename = ''
    try:
        atlas_json = _get_atlas_json_from_file()
        if atlas_json is not None:
            for nic in atlas_json['usx']['nics']:
                if not nic['storagenetwork']:
                    management_ip_devicename = nic['devicename']
        else:
            errormsg('Failed to get atlas json from file, see log above.')
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return management_ip_devicename

def get_volume_type_from_atlas():
    """
    Parameters:
        None
    Returns:
        Success:    A string of volume uuid.
        Failure:    Empty string.
    Description:
        get volume type.
    """
    volume_type = ''
    try:
        atlas_json = _get_atlas_json_from_file()
        if atlas_json is not None:
            volume_type = atlas_json['volumeresources'][0]['volumetype']
        else:
            errormsg('Failed to get atlas json from file, see log above.')
    except Exception as ex:
        errormsg('Some exception happend while parsing the json, details as below: %s' % (ex))
    finally:
        return volume_type

def update_volume_resource_grid():
    pass

def update_svm_container_grid():
    pass

def check_netmask_available(netmask):
    """
    Parameters:
        netmask: Netmask.
    Returns:
        Success: True
        Failure: False
    Description:
        Check netmask if is available.
    """
    ret = False
    sret = validate_netmask(netmask)
    if sret == 0:
        ret = True
    return ret

def validate_netmask(netmask):
    '''
    Parameters:
        netmask : netmask to be checked, in extended (x.y.z.a) format
    Returns:
        0       :       Given netmask is a valid netmask
        !=0     :       Given netmask is invalid, or there was an error checking given
                        netmask.
    Description:
        Check whether a given IPv4 extended netmask is a valid netmask.

        This fix was put in for TISILIO-3738.

        NOTE: This function DOES NOT check whether a given combination of IP address
        and netmask is a valid combination; it only checks for a valid netmask.

        For more info on what constitues a valid netmask, please read:
                http://www.gadgetwiz.com/network/netmask.html
    '''
    if netmask is None or not netmask:
        debug("ERROR : Check netmask : Null or empty netmask received. Cannot check netmask.")
        return 1
    try:
        # Split the given netmask into octets
        octets = netmask.split('.')
        if len(octets) != 4:
            debug("ERROR : Check netmask : Decomposing given netmask "+netmask+" into octets yielded "+str(len(octets)) + " octets, but we expect exactly 4 octets.")
            return 2

        # OK, we have the expected number of octets. Now convert the given
        # netmask into a single integer
        addr = 0
        for octet in octets:
            addr = addr * 256 + int(octet)

        # addr is now a single integer representing the given netmask.
        # We now convert addr into binary, and discard the leading "0b"
        binaddr = bin(addr)[2:]

        # This is the key: Now we check if the binary representation of addr
        # contains the string "01". A valid netmask will ONLY have 0's on the
        # right hand side; there is never a 0 followed by 1 in a valid netmask
        strpos = binaddr.find("01")

        if strpos >= 0:
            debug("ERROR : Check netmask : Netmask "+netmask+" is INVALID!")
            return 3

        # If we got here, we have a valid netmask.
        debug("INFO : Check netmask : Netmask "+netmask+" is a valid netmask, all OK.")
        return 0

    except:
        debug("ERROR : Check netmask : There was an exception validating the netmask.")
        return 4

def check_gateway_available(ip, gateway, netmask):
    """
    Parameters:
        ip: Ip address.
        gateway: Gateway.
        netmask: Netmask.
    Returns:
        Success: True
        Failure: False
    Description:
        Check gateway and ip address if in the same network segment.
    """
    ret = False
    if ip is not None and ip and gateway is not None and gateway and\
         netmask is not None and netmask:
        ip_net_raw = socket.inet_aton(ip)
        gateway_net_raw = socket.inet_aton(gateway)
        netmask_net_raw = socket.inet_aton(netmask)
        ip_net_int = struct.unpack('>L', ip_net_raw)
        gateway_net_int = struct.unpack('>L', gateway_net_raw)
        netmask_net_int = struct.unpack('>L', netmask_net_raw)
        network_seg_ip_int = ip_net_int[0] & netmask_net_int[0]
        network_seg_gateway_int = gateway_net_int[0] & netmask_net_int[0]

        if network_seg_ip_int == network_seg_gateway_int:
            ret = True
    else:
        errormsg('Failed to check gateway, need to specify 3 arguments : ip address, netmask, gateway.')
    return ret

def _get_old_network_config_from_ovf_env(dev_name):
    """
    Parameters:
        dev_name:    network device name.
    Returns:
        A dictionary with old network config, if some members have no config of the
         device, the value of them will be None.
    Description:
        get old network config from command.
    """
    cur_conf = {'interface':None, 'ip':None, 'netmask':None, 'gateway':None, 'export_ips':None, 'storagenetwork':None, 'dns':None, 'vmnetwork':None}
    # set interface
    cur_conf['interface'] = dev_name
    ovfenv_xml = get_ovf_env()
    hypervisor_type = check_hypervisor_type()
    if hypervisor_type == HYPERVISOR_TYPE_VMWARE:
        # wait for get the ovf env
        total_try_count = 5
        try_count = 0
        while os.system("ps aux | grep 'ovf_check' | grep -v 'grep'") == 0 and try_count < total_try_count:
            warn('Check if ovf environment has already been set in system.')
            time.sleep(5)
            ovfenv_xml = get_ovf_env()
            try_count += 1
    # get ip address
    key_name_ip = '%s_ip' % (dev_name)
    cur_conf['ip'] = get_ovf_env_properties_val(ovfenv_xml, key_name_ip)
    # get netmask
    key_name_netmask = '%s_netmask' % (dev_name)
    cur_conf['netmask'] = get_ovf_env_properties_val(ovfenv_xml, key_name_netmask)
    # get gateway
    key_name_gateway = '%s_gateway' % (dev_name)
    cur_conf['gateway'] = get_ovf_env_properties_val(ovfenv_xml, key_name_gateway)
    # get storagenetwork
    key_name_storagenetwork = '%s_storagenetwork' % (dev_name)
    val = get_ovf_env_properties_val(ovfenv_xml, key_name_storagenetwork)
    if val is not None and val:
        cur_conf['storagenetwork'] = True if val == '1' else False
    # get vmnetwork name
    key_name_vmnetwork = '%s_vmnetwork' % (dev_name)
    cur_conf['vmnetwork'] = get_ovf_env_properties_val(ovfenv_xml, key_name_vmnetwork)

    return cur_conf

def check_net_cfg_available(net_conf):
    """
    Parameters:
        net_conf:    network config dictionary.
    Returns:
        Success:    True
        Failure:    False
    Description:
        check ovf network config if available.
    """
    # check the netmask is available.
    debug(net_conf)
    err_msg = ''
    ret = check_netmask_available(net_conf['netmask'])
    if ret:
        if net_conf['gateway'] is not None and net_conf['gateway'] and \
                                        net_conf['gateway'] != '0.0.0.0':
            # check the gateway is in the same network segment
            ret = check_gateway_available(net_conf['ip'], net_conf['gateway'],\
                                             net_conf['netmask'])
            if not ret:
                err_msg = 'Ip address %s and gateway %s are not in the same network segment.'\
                                 % (net_conf['ip'], net_conf['gateway'])
                errormsg(err_msg)
                ret = False
        else:
            errormsg('Failed to find gateway of %s, the gateway check will be skipped.' % (net_conf['interface']))
    else:
        err_msg = 'Inavilable netmask %s' % (net_conf['netmask'])
        errormsg(err_msg)
        ret = False
    return (ret, err_msg)

def check_net_cfg_changed(cur_conf, ovf_conf):
    """
    Parameters:
        cur_conf:   current network config dictionary.
        ovf_conf:   ovf environment network config dictionary.
    Returns:
        Success:    True
        Failure:    False
    Description:
        check ovf network config if changed.
    """
    ret = True
    ip_changed = False
    netmask_changed = False
    gateway_changed = False

    if cur_conf['ip'] != ovf_conf['ip']:
        ip_changed = True

    if cur_conf['netmask'] != ovf_conf['netmask']:
        netmask_changed = True

    if cur_conf['gateway'] and ovf_conf['gateway'] and cur_conf['gateway'] != ovf_conf['gateway']:
        gateway_changed = True

    if ip_changed or netmask_changed or gateway_changed:
        debug("Network changed.")
    else:
        debug("No network config will be changed.")
        ret = False

    debug("%s-%s-%s" % (cur_conf, ovf_conf, ret))
    return ret

def get_sys_net_ifaces():
    """
    Parameters:
        None
    Returns:
        Success:    A list with existed network interface names.
        Failure:    Empty list.
    Description:
        get system network interface list.
    """
    net_dev_list = []
    # get network device name
    res = run_cmd("""ip addr show | grep -P "^\d+:\s+" | awk '$2!~/lo/{gsub(/:/,"",$2);print $2}' | xargs""")
    if res['stdout']:
        net_dev_list = res['stdout'].strip().split(' ')
    else:
        errormsg("Failed to get network device list in the system.")
    return net_dev_list

def update_ip_if_changed():
    """
    Parameters:
        None
    Returns:
        Success:    True and a changed ip information with json format.
        Failure:    False and a empty string.
    Description:
        update network config if changed.
    """
    ret = False
    grid_json_str = ''
    volume_uuid = _get_volume_uuid_from_atlas_by_ip()
    # get network device name
    net_dev_list = get_sys_net_ifaces()
    if net_dev_list:
        net_conf_list = []
        for net_dev in net_dev_list:
            # get current network config
            cur_conf = _get_old_network_config_from_command(net_dev)
            # get network config from ovf environmen
            ovf_conf = _get_old_network_config_from_ovf_env(net_dev)
            ret, err_msg = check_net_cfg_available(ovf_conf)
            if ret and check_net_cfg_changed(cur_conf, ovf_conf):
                # check the ovf conf is available.
                # only the new ip is not same with old ip we need to check
                # it if is reachable here.
                if cur_conf['ip'] != ovf_conf['ip'] and is_reachable(ovf_conf['ip']):
                    err_msg = 'This IP address %s is already in used. Please choose an unused one.' % (ovf_conf['ip'])
                    send_alert_change_ip(volume_uuid, err_msg, 'ERROR')
                    errormsg(err_msg)
                    ret = False
                    net_conf_list = []
                    break
                net_conf_list.append(ovf_conf)
                sret = change_ip([ovf_conf])
                if not sret:
                    err_msg = "Failed to change device %s network config." % (net_dev)
                    send_alert_change_ip(volume_uuid, err_msg, 'ERROR')
                    errormsg(err_msg)
                    ret = sret
                    net_conf_list = []
                    break
        # generage grid json
        if net_conf_list:
            grid_json = {"nics":[], "type":"volume_container", "uuid":"", "jobId":""}
            grid_json["uuid"] = _get_volume_uuid_from_atlas_by_ip()
            for net_conf in net_conf_list:
                grid_net_conf = {'ip':'', 'netmask':'', 'gateway':'', 'type':'', 'vmnetwork':''}
                if net_conf['ip'] is not None:
                    grid_net_conf['ip'] = net_conf['ip']
                if net_conf['netmask'] is not None:
                    grid_net_conf['netmask'] = net_conf['netmask']
                if net_conf['gateway'] is not None:
                    grid_net_conf['gateway'] = net_conf['gateway']
                if net_conf['vmnetwork'] is not None:
                    grid_net_conf['vmnetwork'] = net_conf['vmnetwork']
                if net_conf['storagenetwork'] is not None:
                    grid_net_conf['type'] = 'storage' if net_conf['storagenetwork'] else 'management'
                grid_json['nics'].append(grid_net_conf)
            else:
                grid_json_str = json.dumps(grid_json)
                ret = True
        else:
            info("No network will be config.")
            ret = False
    else:
        err_msg = "Failed to get network device list in the system."
        send_alert_change_ip(volume_uuid, err_msg, 'ERROR')
        errormsg(err_msg)
    return (ret, grid_json_str)

def is_reachable(ip_address):
    """
    verify remote node status
    Params: <ip_address>
    Return: <boolean>
    Ip address whether is reachable
    """
    rtn = True
    cmd = "ping %s -c 3 | grep -c '100%% packet loss'" % ip_address
    ret = run_cmd(cmd)
    if ret.has_key('returncode') and ret['returncode'] == 0:
        if ret['stdout'].replace('\n', '') != '0':
            rtn = False
    return rtn

def get_local_ip(ifname='eth0'):
    """
    Parameters: <ifname>
    Returns: IP address
    Description: Get local ip address
    """
    #Uses the Linux SIOCGIFADDR ioctl to find the IP address associated with a
    #network interface, given the name of that interface, e.g. ?eth0?.
    #The address is returned as a string containing a dotted quad.
    skt_obj = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        #Get 32-bit packed binary format
        pktString = fcntl.ioctl(skt_obj.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
        #Convert packed binary format string to string
        ip = socket.inet_ntoa(pktString[20:24])
    except Exception as err:
        raise Exception(err)
    return ip

def send_job_status(start_percent=0, status=0, message="", close_commit=False):
    res = close_commit
    if not close_commit:
        if start_percent == 0 and os.path.isfile(JOBID_FILE):
            os.rename(JOBID_FILE, JOBID_FILE_BAK)
        res = send_status(TASK_TYPE, start_percent, status, TASK_NAME, message)
        debug("Send job status message %s (%s)" % (message, res))
        if start_percent == 100 and os.path.isfile(JOBID_FILE):
            os.remove(JOBID_FILE)
            if os.path.isfile(JOBID_FILE_BAK):
                os.rename(JOBID_FILE_BAK, JOBID_FILE)
    return res

def update_main_task_process(start_percent=0, step=5, status=0, message="", close_commit=False):
    """
    Parameters:
        start_percent: task start percent value.
        step: task steps per each call.
        status: task status.
        message: task message.
    Returns:
        Success: True
        Failure: False
    Description:
        Update task process.
    """
    ret = True
    try:
        if not globals().has_key('total_percent_complete'):
            global total_percent_complete
            total_percent_complete = start_percent
        if total_percent_complete >= 70 and total_percent_complete < 80:
            step = int(step * 0.8)
        elif total_percent_complete >= 80 and total_percent_complete < 90:
            step = int(step * 0.6)
        elif total_percent_complete >= 90 and total_percent_complete < 99:
            step = int(step * 0.4)
        elif total_percent_complete >= 99:
            total_percent_complete = 99
        send_job_status(total_percent_complete, status, message, close_commit)
        total_percent_complete += 1 if step < 1 else step
    except Exception as ex:
        errormsg('Failed to update task status message: %s exception: %s.' % (message, ex))
        ret = False
    finally:
        return ret

def update_mount_status(status="umount"):
    """
    Parameters:
        status: mount or unmount
    Returns:
        Success: 0
        Failure: 1
    Description:
        Update mount status to /etc/ilito/usx_mount_umount_status .
    """
    ret = 1
    if status == "umount":
        ret = umount_adm(status)
    if status == "mount":
        ret = mount_adm(status)

    return ret


# if __name__ == '__main__':
    # The following are unit test
    # set_log_file(CHANGEIP_LOG_FILE)
    # init_vmware_ovf_env_properties_attrib_dict()
    # _set_ovf_env_vmware_properties_attrib_dict('guestinfo.ilio.eth0_ipaddress', '10.16.170.21')
    # _set_ovf_env_vmware_properties_attrib_dict('guestinfo.ilio.eth1_ipaddress', '10.116.170.21')
    # res = _get_ovf_env_vmware_xml_string()
    # debug(res)
    # change_ip_by_dev_name('eth0', '10.16.170.21')
    # change_ip_by_dev_name('eth1', '10.116.170.21')
    # change_ip_by_dev_name('eth0', '10.16.170.12')
    # update_raid_plan_to_ovfenv()
    # grid_json_str = sys.stdin.read()
    # volume_uuid = _get_volume_uuid_from_atlas_by_ip()
    # debug(volume_uuid)
    # # update_volume_container_grid('10.16.170.14', volume_uuid, grid_json_str)
    # old_conf = _get_old_network_config('eth0')
    # debug(old_conf)
    # old_conf = _get_old_network_config('eth1')
    # debug(old_conf)
    # volume_type = get_volume_type_from_atlas()
    # debug(volume_type)
    # dev_name = 'eth1'
    # (export_ip, netmask, brd) = get_export_ip(dev_name, '10.116.159.11')
    # debug(export_ip)
    # debug(netmask)
    # debug(brd)
    # # remove_export_ip_config(dev_name, export_ip, netmask, brd)
    # export_ip = '10.116.159.151'
    # netmask = '16'
    # brd = '10.116.255.255'
    # add_export_ip_config(dev_name, export_ip, netmask, brd)
    # man_ip = _get_management_ip_from_atlas()
    # debug(man_ip)
    # man_dev = _get_management_ip_devicename_from_atlas()
    # debug(man_dev)
    # storage_ip = _get_storage_ip_from_atlas()
    # debug(storage_ip)
    # storage_ip_dev = _get_storage_ip_devicename_from_atlas()
    # debug(storage_ip_dev)
    # conf = _get_old_network_config_from_command('eth1')
    # debug(conf)
    # conf = _get_old_network_config_from_file('eth1')
    # debug(conf)
    # env_dict = {'eth0_ip' : '10.16.145.201', 'eth0_netmask' : '255.255.0.0', 'eth0_gateway' : '10.16.0.1'}
    # set_ovf_env(None, env_dict)
    # change_network_config_by_ilio('eth0', None, None, None, None)
    # change_network_config_by_ilio('eth0', '10.16.145.202', None, None, None)
    # change_network_config_by_ilio('eth0', '10.16.145.202', '255.0.0.0', None, None)
    # change_network_config_by_ilio('eth0', '10.16.145.202', '255.0.0.0', '8.8.8.8', None)
    # change_network_config_by_ilio('eth0', '10.16.145.202', '255.0.0.0', '8.8.8.8', '10.116.0.1')
    # change_network_config_by_ilio('eth1', None, None, None, None)
    # change_network_config_by_ilio('eth1', '10.16.145.202', None, None, None)
    # change_network_config_by_ilio('eth1', '10.16.145.202', '255.0.0.0', None, None)
    # change_network_config_by_ilio('eth1', '10.16.145.202', '255.0.0.0', '8.8.8.8', None)
    # change_network_config_by_ilio('eth1', '10.16.145.202', '255.0.0.0', '8.8.8.8', '10.116.0.1')
    # set_net_cfg_atlas_file('eth1', '10.16.145.202', '255.0.0.0', '8.8.8.8', '10.116.0.1')
    # set_net_cfg_atlas_file('eth1', '10.121.159.51', '255.255.0.0', '8.8.8.8', '10.21.0.1')
    # ret = validate_netmask('255.0.0.0')
    # debug(ret)
    # ret = validate_netmask('255.255.0.0')
    # debug(ret)
    # ret = validate_netmask('255.255.255.0')
    # debug(ret)
    # ret = validate_netmask('255.255.255.255')
    # debug(ret)
    # ret = check_gateway_available('10.16.170.10', '10.16.0.1', '255.255.0.0')
    # debug(ret)
    # ret = check_gateway_available('10.16.170.10', '10.16.0.1', '255.255.0.0')
    # debug(ret)
    # ret = check_gateway_available('10.16.159.254', '10.16.0.1', '255.255.0.0')
    # debug(ret)
    # ret = check_gateway_available('10.16.170.10', '0.0.0.0', '255.255.0.0')
    # debug(ret)
    # ret = check_gateway_available('10.16.159.254', '0.0.0.0', '255.255.0.0')
    # debug(ret)
    # ret, grid_json_str = update_ip_if_changed()
    # debug(ret)
    # debug(grid_json_str)
    # debug("test check_net_cfg_available")
    # ovf_conf = _get_old_network_config_from_ovf_env('eth0')
    # debug(ovf_conf)
    # ret = check_net_cfg_available(ovf_conf)
    # debug(ret)
    # ovf_conf = _get_old_network_config_from_ovf_env('eth1')
    # debug(ovf_conf)
    # ret = check_net_cfg_available(ovf_conf)
    # debug(ret)
    # nc = {'interface': 'eth0', 'ip': '10.121.159.53', 'netmask': '255.255.0.0', 'gateway': '', 'dns': None}
    # rflag = update_net_cfg_ovf_env(nc)
    # if not rflag:
    #     debug('update_net_cfg_ovf_env failed.')
    # else:
    #     debug('update_net_cfg_ovf_env successful.')
    # res = job_status_init()
    # debug(res)
    # res = update_main_task_process(message="test4")
    # debug(res)
    # update_main_task_process(message="test5")
    # res = send_job_status(100, 0, message="test6")
    # debug(res)
