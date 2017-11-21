# coding=utf-8

import os
import sys
import time
import datetime
#import logging
import traceback
import configobj
import json

sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *
from atl_util import get_master_amc_ip
atlas_conf = '/etc/ilio/atlas.json'
grid_members_json = '/opt/amc/agent/config/grid_members.json'
diamond_conf = '/etc/diamond/diamond.conf'

# Log files
LOG_FILENAME = '/var/log/diamond/diamond.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug("".join([str(x) for x in args]))
    print("".join([str(x) for x in args]))
'''

def info(*args):
    msg = " ".join([str(x) for x in args])
    print >> sys.stderr, msg

def get_host_type():
    """
    Returns node type from JSON
    """

    host_type = 'ERROR'

    try:
        with open(atlas_conf) as fp:
            jsondata = json.load(fp)    
            if jsondata.has_key('usx'):
                host_type = jsondata['usx']['roles'][0] 
            else:
                host_type = jsondata['roles'][0]

            return host_type

    except:
        scriptName = os.path.basename(__file__)
        methodName = get_host_type.__name__
        debug("[%s]  [%s] ERROR when processing %s.  Metrics reporting will be incomplete!" % (scriptName, methodName, atlas_conf))
        sys.exit(1)
    finally:
        fp.close()

def process_conf():
    """
    Read diamond conf file, add amc host IP to the correct locations
    """
    content = ''
    graphite_handler = '[[GraphiteHandler]]'
    graphite_pickle_handler = '[[GraphitePickleHandler]]'
    graphite_handler_flag = False
    graphite_pickle_handler_flag = False
    kairosdb_handler = '[[KairosDBHandler]]'
    kairosdb_handler_flag = False
    kairosdb_handler_server = False
    kairosdb_handler_host_type = False
    master_amc_ip = get_master_amc_ip()
    try:
        with open(diamond_conf) as fp:
            for line in fp.readlines():
                if graphite_handler in line:
                    graphite_handler_flag = True
                    
                if graphite_pickle_handler in line:
                    graphite_pickle_handler_flag = True

                if kairosdb_handler in line:
                    # entering kairosdb config area
                    kairosdb_handler_flag = True
                    
                if 'host =' in line and graphite_handler_flag == True:
                    line = 'host = ' + master_amc_ip + '\n'
                    graphite_handler_flag = False
                
                if 'host =' in line and graphite_pickle_handler_flag == True:
                    line = 'host = ' + master_amc_ip + '\n'
                    graphite_pickle_handler_flag = False

                if 'server =' in line and kairosdb_handler_flag == True:
                    line = 'server = ' + master_amc_ip + '\n'
                    kairosdb_handler_server = True 

                if 'host_type =' in line and kairosdb_handler_flag == True:
                    line = 'host_type = ' + get_host_type() + '\n'
                    kairosdb_handler_host_type = True

                if kairosdb_handler_server and kairosdb_handler_host_type:
                    # both kairosdb entries completed so done with kairosdb config
                    kairos_handler_flag = False
                
                content += line
    except Exception as e:
        debug("Error: %s", e)
        scriptName = os.path.basename(__file__)
        methodName = process_conf.__name__
        debug("[%s] [%s] ERROR when processing %s, Metrics reporting will not work!" % (scriptName, methodName, diamond_conf))
        sys.exit(1)
    finally:
        fp.close()

    return content

def generate_diamond_conf():
    """
    Overwrite diamond.conf with intended AMC host IP
    """
    file_content = process_conf()
    fp = open(diamond_conf, "w+")
    fp.write(file_content)
    fp.close()

master_amc_ip = get_master_amc_ip()
cmd = 'cat %s | grep -P \'(%s)$\'' % (diamond_conf, master_amc_ip)
result = os.popen(cmd).read()
if result and master_amc_ip in result:
    info("diamond is already point to primary USX Manager.")
    sys.exit(0)

generate_diamond_conf()
os.system('service diamond restart')
info('diamond service restarted')
