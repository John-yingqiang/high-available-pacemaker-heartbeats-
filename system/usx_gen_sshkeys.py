#!/usr/bin/python

"""
USX node ssh generation and distribution

1. If input flag is true, generate SSH key pair (i.e., ssh-keygen -t rsa)
2. Get public key from USX Manager, i.e., GET /usxmanager/usxm/sshkeys/public

   curl -k -X GET https://10.15.200.13:8443/usxmanager/usxmanager/usxm/sshkeys/public?api_key=USX_1f0d08bd-2b78-3150-960d-084119119eb5

3. Copy the public key to authorized keys, i.e., /root/.ssh/authroized_keys
4. Push current USX node public key to USX Manager, i.e., POST /usxmanager/usx/sshkeys/public

   curl -k --form file=@/root/.ssh/id_rsa.pub --form press=OK https://10.15.115.101:8443/usxmanager/usxmanager/usx/sshkeys/public/USX_1f0d08bd-2b78-3150-960d-084119119eb5?api_key=USX_1f0d08bd-2b78-3150-960d-084119119eb5

5. Return success
"""

import argparse
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
from __builtin__ import True

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_constants import *
from cmd import *
from log import *

# Global variables
USX_DICT={}
API_KEY=''
TIME_OUT=30

# Configuration files and APIs
ATLAS_CONF = '/etc/ilio/atlas.json'
RSA_FILE = '/root/.ssh/id_rsa'
RSA_PUB_FILE = '/root/.ssh/id_rsa.pub'
AUTHORIZED_FILE = '/root/.ssh/authorized_keys'
AUTHORIZED_NEW_FILE = '/root/.ssh.new/authorized_keys'
SSHKEY_FOLDER = '/root/.ssh/'
SSHKEY_FOLDER_BAKUP = '/root/.ssh.old/'
SSHKEY_FOLDER_NEW = '/root/.ssh.new/'
SSH_KEYGEN = '/usr/bin/ssh-keygen'
USX_GEN_SSHKEY_SCRIPT = '/opt/amc/server/scripts/usx-ssh-keygen.sh'

USXM_GET_PUB_KEY_API = '/usxmanager/usxm/sshkeys/public?api_key='
USXM_GET_MEMBER_API = '/grid/member/memberips?api_key='
USXM_PUT_REPLICATIE_API = '/usxmanager/sshkeys/replicate'
USX_POST_PUB_KEY_API = '/usxmanager/usx/sshkeys/public'
# Log file
LOG_FILENAME = '/var/log/usx-sshkey.log'

set_log_file(LOG_FILENAME)


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
            USX_DICT['share'] = False
            if USX_DICT['resources']:
                if jsondata['volumeresources'][0].has_key('raidplans'):
                    USX_DICT['volumetype'] = jsondata['volumeresources'][0]['raidplans'][0]['volumetype']
                    if jsondata['volumeresources'][0]['raidplans'][0].has_key('raidbricks'):
                        if len(jsondata['volumeresources'][0]['raidplans'][0]['raidbricks'])== 0 :
                            USX_DICT['share'] = True
        else: # this is a service vm
            USX_DICT['role'] = jsondata['roles'][0]
            USX_DICT['uuid'] = jsondata['uuid']
            USX_DICT['usxmanagerurl'] = jsondata['usxmanagerurl']
        USX_DICT['usxmanagerurl'] = get_master_amc_api_url()

    except err:
        debug("ERROR : exception occurred, exiting ...")
        debug(err)
        exit(1)

def run_cmd(cmd, timeout=300):
    ret_dict = {}
    obj_ret = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    
    # Get start time
    start_time = time.time()
    run_time = 0
    
    # use popen.communicate() to avoid deadlock when subprocess PIPE is enabled for stdout, stderr
    out_stdout, out_stderr = obj_ret.communicate()
    
    # Get end time
    end_time = time.time()
    run_time = end_time - start_time
    
    if run_time > timeout:
        obj_ret.terminate()
        raise Exception("run_cmd timeout: %s" % cmd)
    
    ret_dict['stdout'] = out_stdout
    ret_dict['stderr'] = out_stderr
    ret_dict['status'] = obj_ret.returncode
    ret_dict['time'] = run_time

    return ret_dict

def get_usxm_pub_key(apikey, apiurl="http://127.0.0.1:8080/usxmanager"):
    
    result = ""
    get_api_url = apiurl + USXM_GET_PUB_KEY_API + apikey
    cmd = ('curl -s -k -X GET "%s"' % (get_api_url))
    ret = run_cmd(cmd)
    if ret['status'] == 0 and not ret['stderr']:
        result = ret['stdout'].strip('"')
        
    return result

def get_usx_manager_member(apikey, apiurl="http://127.0.0.1:8080/usxmanager"):
    result = ""
    get_api_url = apiurl + USXM_GET_MEMBER_API + apikey
    cmd = ('curl -s -k -X GET "%s"' % (get_api_url))
    ret = run_cmd(cmd)
    if ret['status'] == 0 and not ret['stderr']:
        result = ret['stdout']

    return result

def put_usxm_replicate(apikey, apiurl="http://127.0.0.1:8080/usxmanager"):

    result = False
    put_api_url = apiurl + USXM_PUT_REPLICATIE_API + '?api_key=' + apikey
    cmd = ('curl -s -k -X PUT "%s"' % put_api_url)
    ret = run_cmd(cmd)
    if ret['status'] == 0 and ret['stdout'] == 'true':
        result = True

    return result

def post_usx_pub_key(usxuuid, apikey, apiurl="http://127.0.0.1:8080/usxmanager"):
    
    result = False
    post_api_url = apiurl + USX_POST_PUB_KEY_API + '?api_key=' + apikey
    cmd = ('curl -s -k --form file=@%s --form press=OK "%s"' % (RSA_PUB_FILE, post_api_url))
    ret = run_cmd(cmd)
    if ret['status'] == 0 and ret['stdout'] == 'true':
        result = True
    
    return result

"""
Main logic
"""
os.chdir(os.path.dirname(os.path.abspath(__file__)))
parser = argparse.ArgumentParser(description='USX SSH keygen utility')

parser.add_argument('-t', '--true', action='store_true', dest='keygen_flag',
                    help='keygen: True ? False', default=False)

args = parser.parse_args()

init_global_variables()

success = True
# Generate SSH key pair
if args.keygen_flag:
    run_cmd("rm -rf %s" % SSHKEY_FOLDER)
    cmd1 = ["/bin/echo", "-e", "y\\n"]
    cmd2 = [SSH_KEYGEN, "-q", "-t", "rsa", "-N", "", "-f", RSA_FILE]
    p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    output, err = p2.communicate()
    if err:
        debug("ERROR : SSH keygen failed!")
        success = False

if success:
    ret = run_cmd("cat %s" % RSA_PUB_FILE)
    if ret['stdout'] != "":
        usx_sshkey = ret['stdout']
        file_flag = 0
        if os.path.exists(AUTHORIZED_FILE):
            file_flag = 1
            ret = run_cmd("cat %s | grep \"%s\"; echo $?" % (AUTHORIZED_FILE, usx_sshkey.replace('\n', '')))
        if file_flag == 0 or (ret['status'] == 0 and ret['stdout'].replace('\n', '') == '1'):
            with open(AUTHORIZED_FILE, "a") as keyfile:
                keyfile.write(usx_sshkey)
            keyfile.close()

    # Get USX Manager public key
    usxm_pubkey = get_usxm_pub_key(USX_DICT['uuid'], USX_DICT['usxmanagerurl'])

    # Copy USXM pub key to authorized key file
    if not usxm_pubkey:
        debug("ERROR : Unable to get the USX Manager public key via REST API")
        success = False
    else:
        ret = run_cmd("cat %s | grep \"%s\"; echo $?" % (AUTHORIZED_FILE, usxm_pubkey))
        if ret['status'] == 0 and ret['stdout'].replace('\n', '') == '1':
            with open(AUTHORIZED_FILE, "a") as keyfile:
                keyfile.write(usxm_pubkey + "\n")
            keyfile.close()

if success:
    # Push current USX pub key to USX Manager
    ret = post_usx_pub_key(USX_DICT['uuid'], USX_DICT['uuid'], USX_DICT['usxmanagerurl'])
    if not ret:
        debug("ERROR : Failed to post USX public key to USX Manager via REST API")
        success = False

if success:
    # trigger USX manager replicate
    ret = put_usxm_replicate(USX_DICT['uuid'], USX_DICT['usxmanagerurl'])
    if not ret:
        debug("ERROR : Failed to replicate USX Manager ssh key via REST API")
        success = False

if success:
    debug("USX keygen completed SUCCESSFULLY")
    sys.exit(0)
else:
    debug("Issue occurred in USX keygen utility, please refer to log file for more details")
    sys.exit(1)
