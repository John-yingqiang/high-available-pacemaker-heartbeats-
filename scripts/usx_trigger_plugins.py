#!/usr/bin/python
import sys
import os
import json
import urllib
sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *
from atl_util import run_cmd, scp_folder_to_local_with_pwd, get_all_plugins, \
    get_amc_ips, check_reachable_trust, check_is_volume, get_node_uuid, call_rest_api
from atl_constants import *
#Define some path
LOG_FILE = '/var/log/usx-plugin.log'
TARGET_PLUGIN = "usx-boot-hooks-plugin"
PLUGIN_EXECUTE_URL = "https://%s:8443/usxmanager/plugins/execute/%s?api_key=%s"

def trigger_plugins():
    hook_plugins = []
    rest_api_post_data = {}
    all_plugins = get_all_plugins()
    amc_ips = get_amc_ips()
    target_amc_ip = ""
    local_uuid = get_node_uuid()
    for ip in amc_ips:
        ret = check_reachable_trust(ip, USX_MGR_USERNAME)
        if UNREACHABLE != ret:
            target_amc_ip = ip
    if not target_amc_ip:
        debug("All USX Managers are unreachable")
        return False
    for plugin in all_plugins:
        target_group_valid = False
        target_type_valid = False
        has_target_type_limit = False
        if plugin.has_key("pluginname") and plugin.has_key("enabled") and \
                plugin["pluginname"] == TARGET_PLUGIN and plugin["enabled"]:
            if plugin.has_key("pluginconditions"):
                for plugincondition in plugin["pluginconditions"]:
                    if plugincondition["name"] == "TARGET_GROUP" and plugincondition["allow"] \
                    and ((plugincondition["value"] == "ALL_USX") \
                        or (plugincondition["value"] == "ALL_SERVICE_VMS" and not check_is_volume()) \
                        or (plugincondition["value"] == "ALL_USX_VOLUMES" and check_is_volume())):
                        target_group_valid = True
                    if plugincondition["name"] == "TARGET_TYPE" and plugincondition["allow"] \
                    and ((plugincondition["value"] == "SERVICE_VM" and not check_is_volume()) \
                        or (plugincondition["value"] == "USX_VOLUME" and check_is_volume())):
                        has_target_type_limit = True
                if has_target_type_limit:
                    for plugincondition in plugin["pluginconditions"]:
                        if plugincondition["name"] == "TARGET_UUIDS" and \
                        ((plugincondition["value"] is list and local_uuid in plugincondition["value"]) \
                            or (isinstance(plugincondition["value"], basestring) and local_uuid in plugincondition["value"].split(','))):
                            target_type_valid = True
            if (target_group_valid and not has_target_type_limit) or (has_target_type_limit and target_type_valid):
                rest_api_post_data["targetgroup"] = "ALL_USX"
                if check_is_volume():
                    rest_api_post_data["targettype"] = "USX_VOLUME"
                else:
                    rest_api_post_data["targettype"] = "SERVICE_VM"
                rest_api_post_data["targetuuids"] = [local_uuid]
    if rest_api_post_data:
        url = PLUGIN_EXECUTE_URL % (target_amc_ip, TARGET_PLUGIN, local_uuid)
        data = json.dumps(rest_api_post_data).replace('"', '\\"')
        cmd = 'curl -s -k -X POST -H "Content-Type:application/json" %s -d "%s"' % (url, data)
        debug("trigger hook plugin cmd is %s" % cmd)
        ret = run_cmd(cmd)
        debug("trigger hook plugin result is %s " % ret)
        if ret and ret["stdout"] and local_uuid in ret["stdout"]:
            return True
        else:
            return False
    return True

def main():
    """
    Excuting
    """
    rtn = True
    try:
        rtn = trigger_plugins()
    except Exception as err:
        debug("exception details as below: %s when trigger plugin boot" % err)
        rtn = False

    return rtn

if __name__ == '__main__':
    set_log_file(LOG_FILE)
    rtn = main()
    if rtn:
        sys.exit(0)
    else:
        sys.exit(1)



