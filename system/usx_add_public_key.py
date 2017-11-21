#!/usr/bin/python

import argparse
import os, sys

sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *
from cmd import *

AUTH_KEY = '/root/.ssh/authorized_keys'
LOG_FILENAME = '/var/log/usx-sshkey.log'

"""
Main logic
"""
os.chdir(os.path.dirname(os.path.abspath(__file__)))
parser = argparse.ArgumentParser(description='usx add public keys')

parser.add_argument('-f', '--file', action='store', dest='input_file',
                    help='Input file location', required=True)

args = parser.parse_args()

set_log_file(LOG_FILENAME)
ret = 1
if args.input_file:
    try:
        if os.access(args.input_file, os.R_OK):
            (status, out) = runcmd('cat %s' % args.input_file, False)
            if status != 0:
                info("ERROR : failed to get USX public key from id_rsa.pub")
            else:
                usx_sshkey = out
                file_flag = 0
                if os.path.exists(AUTH_KEY):
                    file_flag = 1
                    (status, out) = runcmd("cat %s | grep \"%s\"; echo $?" % (AUTH_KEY, usx_sshkey.replace('\n', '')))

                if file_flag == 0 or (status == 0 and out.replace('\n', '') == '1'):
                    with open(AUTH_KEY, "a") as keyfile:
                        keyfile.write(usx_sshkey)
                    keyfile.close()
                ret = 0
        else: # Check if it is URL connection
            info("USX id_rsa.pub temporary file: %s not found!" % args.input_file)
    except:
        traceback.print_exc(file = open(LOG_FILENAME, "a"))

print ret

