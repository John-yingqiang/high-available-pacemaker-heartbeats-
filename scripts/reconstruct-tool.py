#!/usr/bin/python

import os, sys
import argparse
import json
#import logging

sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *

VV_CFG = "/etc/ilio/atlas.json"
list_ip = []
LOG_FILENAME = '/var/log/usx-milio-change-mount-option.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))
'''

def usage():
    debug('Wrong command: %s' % str(sys.argv))
    print('Usage: python /opt/milio/atlas/scripts/ReconstructTool.py reconstruct|thin_reconstruct|force_reconstruct\n')

debug("Entering ReconstructTool:", sys.argv)

if len(sys.argv) < 2:
    usage()
    exit(1)

validIP = ['reconstruct','thin_reconstruct','force_reconstruct']
parser = argparse.ArgumentParser()
parser.add_argument("word", help="word that reconstruct will be replaced with", type=str )
args = parser.parse_args()
newStr = args.word

if newStr not in validIP:
    debug("ERROR : Incorrect argument '%s'" % newStr)
    usage()
    exit(1)

f7 = open('/etc/ilio/atlas.json', 'r')
a = json.load(f7)
f7.close()

#if non resource, exit.
if len(a[u'volumeresources']) == 0 : 
    exit(0)

# If non ha,update atlas.json mount option
if a[u'usx'][u'ha'] == False :
    original = a[u'volumeresources'][0].get(u'volumemountoption')
    if not original or len(original) == 0:
        a[u'volumeresources'][0][u'volumemountoption'] = u'rw,noblocktable,noatime,nodiratime,timeout=180000,dedupzeros,commit=180,' + newStr + ',data=ordered,errors=remount-ro'
    else:
        currLine = a[u'volumeresources'][0][u'volumemountoption']
        start = currLine.find('reconstruct')
        end = start
        while( currLine[start] != ',' ):
            start = start - 1
        while( currLine[end] != ',' ):
            end = end + 1
        replaceStr = currLine[start + 1:end]
        if ( replaceStr == newStr ) :
            debug("INFO :Same mount option, don't need update.")
            exit(0)
        # Note: this depends on there being only a single reconstruct in the string
        a[u'volumeresources'][0][u'volumemountoption'] = currLine.replace(replaceStr, newStr)
    
    # Update atlas.json
    tmp_fname = '/tmp/new_atlas.json'
    cfgfile = open(tmp_fname, "w")
    json.dump(a, cfgfile, sort_keys=True, indent=4, separators=(',', ': '))
    cfgfile.close()
    os.rename(tmp_fname, VV_CFG)

# If ha true, do crm configure update
if a[u'usx'][u'ha'] == True : 
    cmd_get_ip="corosync-quorumtool -li | corosync-quorumtool -li | awk '{print $3}' | grep -v Name"
    list_ip=os.popen(cmd_get_ip).read().strip().split()
    os.system("crm configure show > /tmp/a.cli")
    # Now we want to change thin_reconstruct to newStr in a.cli
    f = open("/tmp/a.cli")
    f2 = open( "/tmp/b.cli", 'w' )
    flag = False

    for line in f:
        if 'reconstruct' in line:
            flag = True
            currInd = line.find('reconstruct')
            end = currInd + len('reconstruct')
            while( line[currInd] != ',' ):
                currInd = currInd - 1
            entireWord = line[currInd + 1 :end]
            if entireWord == newStr:
                print("INFO :Same mount option, don't need update.")
                f2.close()
                f.close()
                os.system("rm /tmp/a.cli")
                os.system("rm /tmp/b.cli")
                exit(0)
            newline = line.replace(entireWord, newStr)
            f2.write(newline)
        elif flag == True and 'start timeout' in line:
            newline = line.replace('3600s', '36000s')
            f2.write(newline)
            flag = False
        else:
            f2.write(line)
    f2.close()
    f.close()
    
    os.system("crm configure load update /tmp/b.cli")
    os.system("rm /tmp/a.cli")
    os.system("rm /tmp/b.cli")

    # Check for crm configure update success or not
    f3 = open("/var/lib/pacemaker/cib/cib.xml")
    if newStr in f3.read():
        print("INFO:crm configure load update successfully.")
        f3.close()
    else:
        print("ERROR:crm configure load update failed.")
        f3.close()
        exit(1)

# Set Kernel and sysctl
cmd_change_script='sed -i s/kernel.hung_task_panic=./kernel.hung_task_panic=0/ /opt/milio/scripts/enable_debug.sh'
cmd_enable_sysclt='sysctl -w kernel.hung_task_panic=0 > /dev/null'

# Check whether it is NON HA or HA. If NON HA, only run on this volume. If HA, run related command on every volume. 
if list_ip ==[]:
    ssh_cmd=cmd_change_script+";"+cmd_enable_sysclt
    rc1=os.system(ssh_cmd)
    if rc1 != 0:
        print("ERROR: setup kernel.hung_task_panic failed.")
        exit(1)
    print("INFO: %s update successfully." % newStr)
else:
    rc3 = 0
    for each_ip in list_ip:
        ssh_cmd1 = "ssh -A -q " + each_ip + " -t " + cmd_change_script
        ssh_cmd2 = "ssh -A -q " + each_ip + " -t " + cmd_enable_sysclt
        ssh_cmd3 = ssh_cmd1 +";"+ ssh_cmd2
        rc2=os.system(ssh_cmd3)
        if rc2 != 0:
            print("ERROR: setup kernel.hung_task_panic failed on %s." % each_ip)
            rc3 = 1
            continue	
    if rc3 == 0:
        print("INFO: %s update successfully." % newStr)

