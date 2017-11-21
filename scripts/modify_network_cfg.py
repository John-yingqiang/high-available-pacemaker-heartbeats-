#!/usr/bin/python
"""USX IP change tool chain

This module is used for change localhost network config of ip address, netmask, dns, gateway in the USX.

Example:
    To begin change the ip, you can call the script like this:

    $ python changeips.py change_ip

Note that the script will be blocked to accept parameter formatted as json from stdout with "change_ip", "change_raid_plan". Please pass the parameter through stdin.

"""

import sys
import os
import base64
import json
import argparse
import traceback
import daemon
from changeips_utils import change_ip, update_ip_if_changed, _get_volume_amc_ip, _get_storage_ip_devicename_from_atlas, _get_volume_uuid_from_atlas_by_ip, update_volume_container_grid, get_volume_type_from_atlas, get_export_ip, remove_export_ip_config, add_export_ip_config, change_network_config, update_net_cfg_atlas_file, update_net_cfg_ovf_env, _get_storage_ip_from_atlas, _get_storage_ip_devicename_from_atlas, check_net_cfg_available, set_log_file, is_reachable, get_local_ip, check_backup_ovf_env, get_sys_net_ifaces, send_job_status, update_main_task_process, CHANGEIP_LOG_FILE, debug, info, errormsg
from changeips_utils import update_mount_status, _get_old_network_config_from_command, _get_old_network_config_from_ovf_env, _check_amc_ip_changed_ovf
from change_ip_task_msg import *

REST_API_CHANGEIP_VOLUME_FLAG = 'volume_container'
VOLUME_TYPE_HYBRID = 'HYBRID'
VOLUME_TYPE_SIMPLE_HYBRID = 'SIMPLE_HYBRID'
VOLUME_TYPE_SIMPLE_FLASH = 'SIMPLE_FLASH'
VOLUME_TYPE_SIMPLE_MEMORY = 'SIMPLE_MEMORY'

def argument_parser():
    parser = argparse.ArgumentParser(description='Atlantis USX network config modify')
    parser.add_argument('-ej', '--encode_json', nargs='?', help='Decode input parameter with base64 and read as json format.', type=str, default='', const='')
    parser.add_argument('-f', '--file', nargs='?', help='Get IP change policy from file with json format.', type=str, default='', const='')
    parser.add_argument('-update_nfg', '--update_network_config', nargs='?', help='Update volume container grid.', type=bool, default=False, const=True)
    parser.add_argument('-ovf_check', '--ovf_check', nargs='?', help='Check and backup ovf environment.', type=bool, default=False, const=True)
    parser.add_argument('-no-commit', '--not_commit', nargs='?', help='Not commit information of IP change.', type=bool, default=False, const=True)
    return parser.parse_args()

def ERR_MSG(err_msg):
    errormsg(err_msg)
    err_msg = err_msg + os.linesep
    sys.stderr.write(err_msg)

def ERR_EXIT(err_msg):
    ERR_MSG(err_msg)
    sys.exit(1)

def check_if_config_changed(p_json):
    """
    Parameters:
        p_json: policy json.
    Returns:
        Success:    New network config list.
        Failure:    Empty list.
    Description:
        Check the network config list if it is changed.
    """
    new_nics = []
    for nic in p_json['nics']:
        if nic['netmask'] == '' and nic['gateway'] == '':
            errormsg('Network device %s use DHCP mode.' % (nic['ifaceName']))
        else:
            # get current network config
            ovf_conf = _get_old_network_config_from_ovf_env(nic['ifaceName'])
            cur_conf = _get_old_network_config_from_command(nic['ifaceName'])
            if nic['ip'] != cur_conf['ip'] or nic['netmask'] != cur_conf['netmask'] \
                or (nic['gateway'] != cur_conf['gateway'] and nic['gateway'] != '0.0.0.0' and nic['gateway'] != ''):
                new_nics.append(nic)
            else:
                errormsg('Network device %s configuration not changed.' % (nic['ifaceName']))
    return new_nics

def check_if_shared_interface(nics):
    """
    Parameters:
        nics : network config list.
    Returns:
        Success:    New network config list.
        Failure:    Empty list.
    Description:
        Check the network config list if it specified
         a shared network interface.
    """
    check_list = []
    new_nics = []
    for nic in nics:
        # check_str = '%s %s %s' % (nic['ip'], nic['netmask'], nic['gateway'])
        # if it is shared network, the value of vmnetwork must be same.
        if 'vmnetwork' not in nic or nic['vmnetwork'] == '':
            errormsg('Vmnetwork is a required option of the policy. please check the policy')
            new_nics = []
            break
        else:
            check_str = nic['vmnetwork']
            if check_str not in check_list:
                check_list.append(check_str)
                new_nics.append(nic)

    return new_nics

def parse_nic_config(nic):
    """
    Parameters:
        nic: network config.
        {
            'ifaceName' : <network interface name>,
            'ip' : <network ip address>,
            'netmask' : <network netmask>,
            'dns' : <dns>,
            'gateway' : <network gateway>
            'vmnetwork' : <vmnetwork name>
        }
    Returns:
        Success:    new network config dictionary.
                    {
                        'interface' : <network interface>,
                        'ip' : <ip address>,
                        'netmask' : <netmask>,
                        'dns' : <dns>,
                        'gateway' : <gateway>
                        'vmnetwork' : <vmnetwork name>
                    }
        Failure:    empty dictionary.
    Description:
        simple volume ip address change.
    """
    net_config = {}
    err_msg = ''
    if 'ifaceName' in nic and nic['ifaceName']:
        net_config['interface'] = nic['ifaceName']
    else:
        err_msg = 'Network device type is a required option, the config %s will not take effect.' % (nic)
        errormsg(err_msg)

    if net_config != {}:
        # check new ip address if reachable
        net_dev_list = get_sys_net_ifaces()
        for net_dev in net_dev_list:
            if nic['ifaceName'] == net_dev and nic['ip'] != get_local_ip(net_dev) and is_reachable(nic['ip']):
                err_msg = 'This ip address %s is already in use. Please choose an unused one.' % (nic['ip'])
                errormsg(err_msg)
                net_config = {}
                break

    # ip address and netmask are required options.
    if net_config != {}:
        if 'ip' not in nic or nic['ip'] == '':
            err_msg = 'Failed to get ip address from the policy, it is required, please check the policy %s' % (nic)
            errormsg(err_msg)
            net_config = {}
        else:
            net_config['ip'] = nic['ip']
            if 'netmask' not in nic or nic['netmask'] == '':
                err_msg = 'Failed to get netmask from the policy, it is required, please check the policy %s' % (nic)
                errormsg(err_msg)
                net_config = {}
            else:
                net_config['netmask'] = nic['netmask']

    if net_config != {}:
        net_config['dns'] = None if 'dns' not in nic else nic['dns']
        net_config['gateway'] = None if 'gateway' not in nic else nic['gateway']
        net_config['vmnetwork'] = nic['vmnetwork']
        ret, err_msg = check_net_cfg_available(net_config)
        if not ret:
            errormsg("Failed to check network config, please check the policy.")
            net_config = {}

    return (net_config, err_msg)

def restart_amc_agent():
    """
    Parameters:
        None.
    Returns:
        Success: True.
        Failure: False.
    Description:
        Restart volume agent.
    """
    ret = True
    res = os.system('/opt/amc/agent/bin/amc_agent_stop.sh')
    if res == 0:
        res = os.system('/opt/amc/agent/bin/amc_agent_start.sh')
        if res != 0:
            errormsg('Failed to start agent.')
            ret = False
    else:
        errormsg('Failed to stop agent.')
        ret = False
    return ret

def simple_volume_ip_change(p_json):
    """
    Parameters:
        p_json: policy json.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        simple volume ip address change.
    """
    ret = True
    close_job = False
    close_chgip = False


    #update mount status to umount
    update_mount_status("umount")

    # close job status commit if needed, it will be closed
    # when running the script under command line.
    if 'close_job_commit' in p_json:
        close_job = p_json['close_job_commit']

    # close changeips commit if needed, it will be closed
    # when running the script under command line.
    if 'close_chgip_commit' in p_json:
        close_chgip = p_json['close_chgip_commit']

    net_config_list = []
    send_job_status(0, 0, message=CHANGE_IP_START, close_commit=close_job)
    # check the IP if need to change
    update_main_task_process(start_percent=5, message=CHECK_NETWORK_IF_CHANGE_START, close_commit=close_job)
    chg_nics = check_if_config_changed(p_json)
    if chg_nics == []:
        update_main_task_process(status=1, message=CHECK_NETWORK_IF_CHANGE_FAILURE, close_commit=close_job)
    else:
        update_main_task_process(message=CHECK_NETWORK_IF_CHANGE_SUCCESSFUL, close_commit=close_job)
        # check if use shared network interface
        update_main_task_process(message=CHECK_SHARED_NET_START, close_commit=close_job)
        sig_nics = check_if_shared_interface(chg_nics)
        if sig_nics:
            update_main_task_process(message=CHECK_SHARED_NET_SUCCESSFUL, close_commit=close_job)
        else:
            update_main_task_process(status=1, message=CHECK_SHARED_NET_FAILURE, close_commit=close_job)
        update_main_task_process(message=CHECK_NETWORK_POLICY_START, close_commit=close_job)
        for nic in sig_nics:
            if nic['type'] != 'service':
                net_config, err_msg = parse_nic_config(nic)
                if net_config != {}:
                    net_config_list.append(net_config)
                else:
                    errormsg("This config %s is invalidate, it will be skipped." % (nic))
                    net_config_list = []
                    update_main_task_process(status=1, message=CHECK_NETWORK_POLICY_FAILURE % (err_msg), close_commit=close_job)
                    ret = False
                    break
            else:
                errormsg('Simple Hybrid need not to config service ip (export ip), the config will be skipped.')
                ret = False
        else:
            update_main_task_process(message=CHECK_NETWORK_POLICY_SUCCESSFUL, close_commit=close_job)

    if net_config_list:
        # ret = change_ip(net_config_list)
        rflag = True
        for nc in net_config_list:
            # change network config by command
            update_main_task_process(message=CHANGE_SYS_NETCFG_START % (nc['interface']), close_commit=close_job)
            rflag = change_network_config(nc)
            if not rflag:
                update_main_task_process(status=1, message=CHANGE_SYS_NETCFG_FAILURE % (nc['interface']), close_commit=close_job)
                debug('change_network_config failed.')
                ret = rflag
            else:
                update_main_task_process(message=CHANGE_SYS_NETCFG_SUCCESSFUL % (nc['interface']), close_commit=close_job)

                # restart amc agent
                update_main_task_process(message=RESTART_AMC_AGENT_START, close_commit=close_job)
                rflag = restart_amc_agent()
                if not rflag:
                    update_main_task_process(status=1, message=RESTART_AMC_AGENT_FAILURE, close_commit=close_job)
                    debug('restart_amc_agent failed.')
                    ret = rflag
                else:
                    update_main_task_process(message=RESTART_AMC_AGENT_SUCCESSFUL, close_commit=close_job)

                    # update config to /etc/ilio/atlas.json
                    update_main_task_process(message=UPDATE_NETCFG_ATLAS_START % (nc['interface']), close_commit=close_job)
                    rflag = update_net_cfg_atlas_file(nc)
                    if not rflag:
                        update_main_task_process(status=1, message=UPDATE_NETCFG_ATLAS_FAILURE % (nc['interface']), close_commit=close_job)
                        debug('update_net_cfg_atlas_file failed.')
                        ret = rflag
                    else:
                        update_main_task_process(message=UPDATE_NETCFG_ATLAS_SUCCESSFUL % (nc['interface']), close_commit=close_job)
                        # update config to ovf environment
                        update_main_task_process(message=UPDATE_NETCFG_OVFENV_START % (nc['interface']), close_commit=close_job)
                        rflag = update_net_cfg_ovf_env(nc)
                        if not rflag:
                            update_main_task_process(status=1, message=UPDATE_NETCFG_OVFENV_FAILURE % (nc['interface']), close_commit=close_job)
                            debug('update_net_cfg_ovf_env failed.')
                            ret = rflag
                        else:
                            update_main_task_process(message=UPDATE_NETCFG_OVFENV_SUCCESSFUL % (nc['interface']), close_commit=close_job)
            if not ret:
                errormsg('One of nic network configure failed, the work will be stopped.')
                break
        else:
            if not close_chgip:
                # commit volume container information
                update_main_task_process(message=UPDATE_VOL_CONTAINER_CHG_INFO_START, close_commit=close_job)
                ret = update_volume_container(json.dumps(p_json))
                if ret:
                    update_main_task_process(message=UPDATE_VOL_CONTAINER_CHG_INFO_SUCCESSFUL, close_commit=close_job)
                else:
                    update_main_task_process(status=1, message=UPDATE_VOL_CONTAINER_CHG_INFO_FAILURE, close_commit=close_job)
    else:
        errormsg('There is no validate config can be used to change ip, please check the policy.')
        ret = False
    if ret:
        send_job_status(100, 0, message=CHANGE_IP_SUCCESSFUL, close_commit=close_job)
    else:
        send_job_status(100, 1, message=CHANGE_IP_FAILURE, close_commit=close_job)
    return ret

def hybrid_volume_ip_change(p_json):
    """
    Parameters:
        p_json: policy json.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        hybrid volume ip address change.
    """
    net_config_list = []
    storage_ip = None
    storage_dev = None
    new_export_ip = None
    ret = False
    sig_nics = check_if_shared_interface(p_json['nics'])
    for nic in sig_nics:
        if nic['type'] != 'service':
            net_config = parse_nic_config(nic)
            if net_config != {}:
                net_config_list.append(net_config)
        if nic['type'] == 'storage' and net_config != {}:
            storage_ip = net_config['ip']
            storage_dev = net_config['interface']
        if nic['type'] == 'service' and net_config != {}:
            new_export_ip = None if 'ip' not in nic else nic['ip']
    if net_config_list:
        ret = change_ip(net_config_list)
        for nic in p_json['nics']:
            if nic['type'] == 'service':
                if storage_ip is None:
                    storage_ip = _get_storage_ip_from_atlas()
                if storage_dev is None:
                    storage_dev = _get_storage_ip_devicename_from_atlas()
                if storage_ip and storage_dev:
                    (export_ip, netmask, brd) = get_export_ip(storage_dev, storage_ip)
                    remove_export_ip_config(storage_dev, export_ip, netmask, brd)
                    if new_export_ip is not None and new_export_ip:
                        export_ip = new_export_ip
                    add_export_ip_config(storage_dev, export_ip, netmask, brd)

    return ret

def update_volume_container(vol_container_json_str, offline=False):
    """
    Parameters:
        vol_container_json_str: volume container information string.
    Returns:
        Success:    True.
        Failure:    False.
    Description:
        update volume container information to AMC.
    """
    ret = False
    if offline:
        # only in offline status, script need to check ovf environment
        # to get if USX Manager IP changed.
        _check_amc_ip_changed_ovf()
    amc_ip = _get_volume_amc_ip()
    volume_uuid = _get_volume_uuid_from_atlas_by_ip()
    if amc_ip and volume_uuid:
        ret = update_volume_container_grid(amc_ip, volume_uuid, vol_container_json_str)
    else:
        errormsg('Failed to get amc ip address or volume uuid, see log above.')
    return ret

def main(args):
    set_log_file(CHANGEIP_LOG_FILE)
    ret = True
    try:
        p_json = None
        if args.encode_json:
            p_json_str = args.encode_json
            p_json = json.loads(base64.b64decode(p_json_str))
        elif args.file:
            with open(args.file, 'r') as fp:
                p_json_str = fp.read()
                p_json = json.loads(p_json_str)
                p_json['close_job_commit'] = True
        debug(p_json)
        if p_json is not None:
            if args.not_commit:
                p_json['close_chgip_commit'] = True
            if REST_API_CHANGEIP_VOLUME_FLAG == p_json['type']:
                volume_container_json = p_json
                # check volume type
                volume_type = get_volume_type_from_atlas()
                # Simple hybrid and simple flash have the same workflow of changing ip address.
                if volume_type == VOLUME_TYPE_SIMPLE_HYBRID or \
                    volume_type == VOLUME_TYPE_SIMPLE_FLASH or \
                    volume_type == VOLUME_TYPE_SIMPLE_MEMORY:
                    ret = simple_volume_ip_change(volume_container_json)
                elif volume_type == VOLUME_TYPE_HYBRID:
                    ret = hybrid_volume_ip_change(volume_container_json)
                    if ret:
                        ret = update_volume_container(json.dumps(volume_container_json))
                else:
                    ret = False

        if args.update_network_config:
            volume_type = get_volume_type_from_atlas()
            if volume_type == VOLUME_TYPE_SIMPLE_HYBRID or \
                volume_type == VOLUME_TYPE_SIMPLE_FLASH or \
                volume_type == VOLUME_TYPE_SIMPLE_MEMORY:
                ret ,grid_json_str = update_ip_if_changed()
                if ret:
                    debug('Found network changed by ovf environment, new config will be updated.')
                    debug(grid_json_str)
                    ret = update_volume_container(grid_json_str, offline=True)
                else:
                    debug('No change of the network config.')
            else:
                debug("Change IP address update skipped.")

        if args.ovf_check:
            ret = check_backup_ovf_env()
            if ret:
                debug('Check and back up ovf environment Successful.')
            else:
                debug('Failed to check and back up ovf environment.')
    except Exception as ex:
        traceback.print_exc(file=open(CHANGEIP_LOG_FILE, "a"))
        err_msg = 'Failed to change the IP address due to %s, details refer to %s.' % (ex, CHANGEIP_LOG_FILE)
        errormsg(err_msg)
        send_job_status(100, 1, message=err_msg, close_commit=args.not_commit)
        ret = False
    finally:
        return ret

if __name__ =='__main__':
    args = argument_parser()
    ret = True

    with daemon.DaemonContext():
        ret = main(args)

    if ret:
        sys.exit(0)
    else:
        sys.exit(1)
