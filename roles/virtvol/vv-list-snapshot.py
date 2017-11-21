import sys
import json
import argparse

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *

def list_snapshot_sortbytime():
    out = ['']
    cmd_str = 'lvs --noheadings dedupvg -o lv_name -O -Time 2>/dev/null'
    rc = do_system(cmd_str,out,log=False)
    if rc != 0:
        errormsg('get snapshot list error!')
        return []
    snapshot_list = out[0].split()
    return snapshot_list

def update_snapshot_info(args):
    uuid = args.volume_id
    snap_list = []
    item_list = []
    data = {"count": '', "items": []}
    item = { "ctime": '', "scheduled": '', "snapshotname": '', "mountedpoint": "", "number": ''}
    item['volumeresourceuuid'] = uuid

    snap_list = list_snapshot_sortbytime()
    i = 0
    for snap_uuid in snap_list:
        if "dedup" in snap_uuid:
            continue
        t_item = item.copy()
        t_item['uuid'] = snap_uuid
        item_list.append(t_item)
        i = i + 1

    data['count'] = i
    data['items'] = item_list
    debug(json.dumps(data))
    return 0

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='vv-list-snapshot.py', usage='%(prog)s [-u] [-j]')
    parser.add_argument("-u", "--volume_id", default='', help='Volume uuid', type=str)
    parser.add_argument("-j", "--job_id", default='', help='Job ID', type=str)

    args = parser.parse_args()
    update_snapshot_info(args)
