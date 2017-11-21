#!/usr/bin/python
#from __future__ import print_function
import time
import sys
import os
import socket
import fcntl
import struct
import logging
import logging.handlers
import json
import subprocess
import paramiko
import argparse
import urllib2

API_URL_BAK="http://%s:8080/usxmanager"
API_PUBLIC="/usx/sshkeys/public"
AUTH_KEY="/root/.ssh/authorized_keys"
KEYFILE="/root/.ssh/id_rsa.pub"
ATLAS_FILE="/etc/ilio/atlas.json"
TIME_OUT=60

def ssh_cmd(ip, cmd=None, username='poweruser', TIMEOUT=7):
    """
    Parameters:
        <ip>
        <cmd>
        <username>
        <timeout>
    Returns: dict
    Description: run commond on local
    """
    rtn_dict = {}
    rtn_dict['error'] = None
    rtn_dict['stderr'] = None
    rtn_dict['stdout'] = None

    pkey='/root/.ssh/id_rsa'
    key=paramiko.RSAKey.from_private_key_file(pkey)
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(ip, 22, username, pkey=key, timeout=7)
    except Exception as e:
        rtn_dict['stderr'] = e
        return rtn_dict
    stdin, stdout, stderr = ssh.exec_command(cmd, bufsize=-1, timeout=TIMEOUT)
    try:
       rtn_dict['stderr'] = stderr.read()
    except Exception as e:
       return rtn_dict
    try:
       rtn_dict['stdout'] = stdout.read()
    except Exception as e:
       return rtn_dict
    ssh.close()
    return rtn_dict

def run_cmd(cmd, timeout=1800):
    """
    Parameters: <cmd>
                <timeout>
    Returns: dict
    Description: run commond on local
    """
    rtn_dict = {}
    rtn_dict['stderr'] = ''
    obj_rtn = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    start_time = time.time()
    run_time = 0
    while True:
        if obj_rtn.poll() != None:
            break
        end_time = time.time()
        run_time = end_time - start_time
        if run_time > timeout:
            obj_rtn.terminate()
            rtn_dict['stderr'] = 'Run command timeout: %s' % cmd
            return rtn_dict
        time.sleep(0.1)

    out = obj_rtn.stdout.read()
    err = obj_rtn.stderr.read()

    rtn_dict['stdout'] = out
    rtn_dict['stderr'] = err
    rtn_dict['returncode'] = obj_rtn.returncode
    return rtn_dict

def call_rest_api(url, req_type, data=None, cookies=None, header=True, content_type='application/json'):
    """
    Calling REST API
    Params:
        url:      The url of the agent REST API
        req_type: Request type of REST API. Such as PUT, POST, DELETE, GET. Default is GET
        data:     Data of Agent REST API. Default is None

    Return:
       Success: Result of REST API
       Failure: False
    """
    ret = False
    retry_num = 1
    cnt = 0
    while cnt < retry_num:
        conn = urllib2.Request(url)
        if header == True:
            conn.add_header('Content-type',content_type)
        if cookies != None:
            tmp_header = 'JSESSIONID=' + cookies
            conn.add_header('Cookie', tmp_header)

        conn.get_method = lambda: req_type
        try:
            if data != None:
                res = urllib2.urlopen(conn, data, timeout=TIME_OUT)
            else:
                res = urllib2.urlopen(conn, timeout=TIME_OUT)

        except Exception as e:
            cnt += 1
            time.sleep(TIME_OUT)
            continue

        if str(res.code) == "200":
            ret = res.read()
            res.close()
            return ret
        else:
            time.sleep(retry_interval_time)
            cnt += 1
            res.close()

    return ret

def main(ip_address):
    ret_val = False
    ret = ssh_cmd(ip_address, 'uname')
    if ret['stderr']:
        API_URL = API_URL_BAK % ip_address

        #Get target node pubilc key
        usx_sshkey = call_rest_api("%s%s" % (API_URL, API_PUBLIC), "GET")
        if usx_sshkey != False:
            usx_sshkey_1 = usx_sshkey.replace('"', '')
            with open(AUTH_KEY, "a") as keyfile:
                keyfile.write('%s\n' % usx_sshkey_1)
            keyfile.close()

            cmd = "curl -k --form \"file=@%s\" %s%s" % \
                (KEYFILE, API_URL, API_PUBLIC)
            ret = run_cmd(cmd)
            if "true" in ret['stdout']:
                ret = ssh_cmd(ip_address, 'uname')
                if not ret['stderr']:
                    ret_val = True
    else:
        ret_val = True
    return ret_val

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sshw.py')
    parser.add_argument('-i', '--ip', action='store', dest='ip_address',
                        help='Target node IP adress', required=True)
    args = parser.parse_args()
    ret = main(args.ip_address)
    if ret:
        print('Established trust relationships successful!')
        sys.exit(0)
    else:
        print('Failed to establish trust relationships')
        sys.exit(1)
