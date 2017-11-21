#!/usr/bin/python

import sys
import logging
sys.path.insert(0, "/opt/milio/libs/atlas")
from cmd import *

CMD = 'cat /root/.ssh/id_rsa.pub'
# Log files
LOG_FILENAME = '/var/log/usx-sshkey.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,
                    format='%(asctime)s %(message)s')

def debug(*args):
    logging.debug("".join([str(x) for x in args]))

def info(*args):
    logging.info("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))

"""
Get SSH public key

Return:
    id_rsa.pub string
"""
def get_public_key():

    (status, out) = runcmd(CMD,False)
    if status != 0:
        debug("Error: %s" % out)
        out = ""

    return out

if __name__ == '__main__':
    set_log_file(LOG_FILENAME)
    try:
        result = get_public_key()
        info(result)
    except Exception, ex:
        traceback.print_exc(file = open(LOG_FILENAME, "a"))
