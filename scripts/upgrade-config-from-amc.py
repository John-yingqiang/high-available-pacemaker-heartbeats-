#!/usr/bin/env python
import urllib2
import time
import json
import os
JSON_FILE_LOCATION = "/etc/ilio/atlas.json"


def update_local_json_from_amc():
    """
    update the /etc/ilio/atlas.json file when the VM be updated successfully.
    """
    ret = 1
    try:
        with open(JSON_FILE_LOCATION, "r") as config:
            cfg_str = config.read()
            usx_setup_info = json.loads(cfg_str)
            if 'usx' in usx_setup_info:
                if 'ha' in usx_setup_info['usx'] and usx_setup_info['usx']['ha']:
                    print('HA node, does not need to be updated.')
                    return 0
                usxmanagerurl = usx_setup_info['usx']['usxmanagerurl']
                usxuuid = usx_setup_info['usx']['uuid']
                role = usx_setup_info['usx']['roles'][0]
            else:
                print("Service VM does not need to be updated.")
                return 0
    except Exception, e:
        print e
        return 1

    # force to use the local agent.
    usxmanagerurl = 'http://127.0.0.1:8080/usxmanager'
    if role.upper() == 'SERVICE_VM':
        apistr = "/usx/inventory/servicevm/containers/" + usxuuid + "?composite=true&api_key=" + usxuuid
    if role.upper() == 'VOLUME':
        apistr = "/usx/inventory/volume/containers/" + usxuuid + "?composite=true&api_key=" + usxuuid
    print(usxmanagerurl + apistr)

    retry_num = 5
    retry_interval_time = 10
    cnt = 0
    while cnt < retry_num:
        conn = urllib2.Request(usxmanagerurl + apistr)
        try:
            res = urllib2.urlopen(conn, timeout=10)
        except:
            cnt += 1
            print("Exception caught: retry count: %d" % cnt)
            time.sleep(retry_interval_time)
            continue
        # API invocation did not return correctly, retry
        if res.code != 200:
            cnt += 1
            print("ERROR : REST API invocation failed, retry count: %d" % cnt)
            time.sleep(retry_interval_time)
            continue
        else:
            retJson = json.load(res)
            if 'data' in retJson:
                tmpjson = retJson['data']
                myjson = json.dumps(tmpjson, sort_keys=True, indent=4, separators=(',', ': '))
                print(myjson)

                # overwrite the local atlas json since it has incorrect data that got us here
                try:
                    # Make a backup for atlas.json.
                    os.system('cp -f %s %s' % (JSON_FILE_LOCATION, JSON_FILE_LOCATION + '.bak'))
                    with open(JSON_FILE_LOCATION, 'w') as f:
                        f.write(myjson)
                        f.flush()
                        os.fsync(f.fileno())
                    ret = 0
                    print('INFO : Write JSON file : SUCCESSFULLY wrote JSON received from USX Manager to' \
                            '%s and will use this local data in subsequent calls to load/get the JSON config.' % JSON_FILE_LOCATION)
                except:
                    print('WARNING : Write JSON file : EXCEPTION writing json data to %s' \
                            'JSON data might not have been saved properly on the local system!' % JSON_FILE_LOCATION)

            break
    return ret


if __name__ == "__main__":
    print("Only called by amc when updated successfully.")
    ret = update_local_json_from_amc()
    if ret != 0:
        print("update json from amc failed.")
    exit(ret)
