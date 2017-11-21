#!/usr/bin/python

"""
Collect statuses via ibdagent hook
### Dependencies

https://10.21.115.105:8443/usxmanager/usx/inventory/servicevm/containers?query=.%5Bnics%5Bipaddress%3D'10.121.115.22'%5D%5D&fields=uuid
"""

import httplib
#import ConfigParser
import json
#import operator
import os, sys, threading
import string 
import time
#import copy 
import urllib2
import traceback
import socket
import math
#import base64 
#import errno

sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import * 
#from status_update import does_jobid_file_exist
#from status_update import send_status
from cmd import *
from log import *

LOCAL_CFG = "/etc/ilio/atlas.json"
LOCAL_AGENT = "http://127.0.0.1:8080"
VOLUME_STATUS_API = "/usxmanager/usx/status/update"
SVM_CONTAINER_API = "/usxmanager/usx/inventory/servicevm/containers/"
VOLUME_RESOURCE_API = "/usxmanager/usx/inventory/volume/resources/"

USX_DICT={}

LOG_FILENAME = '/var/log/usx-status-update.log'

set_log_file(LOG_FILENAME)

'''
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s %(message)s')

def _debug(*args):
        logging.debug("".join([str(x) for x in args]))
        print("".join([str(x) for x in args]))
'''

'''
def info(*args):
  msg = " ".join([str(x) for x in args])
  print >> sys.stderr, msg
'''

def init_global_variables():
    """
    Generate USX info dictionary from atlas.json
    """
    global USX_DICT
    
    try:
        fp = open(LOCAL_CFG)
        jsondata = json.load(fp)
        fp.close()
        if jsondata.has_key('usx'): # this is a volume
            USX_DICT['role'] = jsondata['usx']['roles'][0]
            USX_DICT['uuid'] = jsondata['usx']['uuid']
            USX_DICT['usxmanagerurl'] = jsondata['usx']['usxmanagerurl']
            USX_DICT['resources'] = jsondata['volumeresources']
        else: # this is a service vm
            USX_DICT['role'] = jsondata['roles'][0]
            USX_DICT['uuid'] = jsondata['uuid']
            USX_DICT['usxmanagerurl'] = jsondata['usxmanagerurl']
        USX_DICT['usxmanagerurl'] = get_master_amc_api_url()
    except:
        debug("ERROR : exception occured, exiting ...")
        exit(1)     


def retrieve_from_usx_grid(apiurl, apistr):
    """
    Get information from grid
     Input: query string
     Return: response data
    """
    try:
        protocol = apiurl.split(':')[0]
        if protocol == 'https':
            use_https = True
        else:
            use_https = False
        apiaddr = apiurl.split('/')[2]  # like: 10.15.107.2:8443
        debug(apistr)
        debug(apiaddr)
        if use_https == True:
            conn = httplib.HTTPSConnection(apiaddr)
        else:
            conn = httplib.HTTPConnection(apiaddr)
        conn.request("GET", apistr)
        response = conn.getresponse()
        debug(response.status, response.reason)
        if response.status != 200 and response.reason != 'OK':
            return None
        else:
            data = response.read()
    except:
        debug("ERROR : Cannot connect to USX Manager to query")
        return None
    
    return data


'''
def _publish_to_usx_grid(usxmanagerurl, apistr, data, putFlag=0):
    """
    Call REST API to update availability status to grid
    """
    retVal = 0
    conn = urllib2.Request(usxmanagerurl + apistr)
    debug(usxmanagerurl+apistr)
    conn.add_header('Content-type','application/json')
    if putFlag == 1:
        conn.get_method = lambda: 'PUT'
    debug('**** data to be uploaded to AMC: ', data)
    res = urllib2.urlopen(conn, json.dumps(data))
    debug('Returned response code: ' + str(res.code))
    if res.code != 200:
        retVal = 1
    res.close()
    return retVal
'''


# def storage_network_reachable(argv):
#     """
#     Update IBD reachable status:
#      input: service vm storage IP, connection status
#     """
#     retVal = 0
#     debug("Sending storage network reachable status... ")
#     if len(argv) != 4:
#         debug("ERROR : Incorrect number of arguments")
#         exit(1)
#     
#     try:
#         get_apistr = (SVM_CONTAINER_API + "?query=.%5Bnics%5Bipaddress%3D'" + 
#                   argv[2] +"'%5D%5D&fields=uuid")
#         response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
#         data = json.loads(response)
#         if not data.get('items'): # no service vm uuid retrieved
#             debug("Service VM with storage IP %s does not exist in grid" % argv[2])
#             return 1
#         svm_uuid = data.get('items')[0]['uuid']
#         debug(svm_uuid)
#         
#         get_apistr = (VOLUME_RESOURCE_API + USX_DICT['resources'][0]['uuid'])
#         response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
#         data = json.loads(response)
#         if not data.get('data'): # no volume
#             debug("No resource associated with this uuid: %s" % USX_DICT['resources'][0]['uuid'])
#             return 1
#         current_container_uuid = data.get('data')['containeruuid']
#     except:
#         debug("Exception caught:")
#         debug(traceback.format_exc())
#         return 1
#     
#     #debug(USX_DICT['uuid'])
#     if current_container_uuid == USX_DICT['uuid']: # resource is on this container
#         data = {}
#         data['usxuuid'] = USX_DICT['resources'][0]['uuid']
#         data['usxcontaineruuid'] = USX_DICT['uuid']
#         data['usxtype'] = 'VOLUME_RESOURCE'
#          
#         storagenetwork_status = {}
#         storagenetwork_status['name'] = 'STORAGE_NETWORK_' + svm_uuid
#         if argv[3].lower() == 'ok':
#             storagenetwork_status['value'] = 'OK'
#         else:
#             storagenetwork_status['value'] = 'FATAL'
#         data['usxstatuslist'] = []
#         data['usxstatuslist'].append(storagenetwork_status)
#      
#         post_apistr = VOLUME_STATUS_API
#         rc = publish_to_usx_grid(LOCAL_AGENT, post_apistr, data)
#         if rc != 0:
#             debug("ERROR : publish status to grid REST API call failed ")
#             retVal = rc
#     else:
#         debug("resource: %s is not on this container: %s. Skip update storage network status to USX manager" % 
#               (USX_DICT['resources'][0]['uuid'], current_container_uuid))
# 
#     return retVal

def storage_network_reachable(servicevm_ip, status):
    """
    Update IBD reachable status:
     input: service vm storage IP, connection status
    """
    retVal = 0
    
    try:
        get_apistr = (SVM_CONTAINER_API + "?query=.%5Bnics%5Bipaddress%3D'" + 
                  servicevm_ip +"'%5D%5D&fields=uuid")
        response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
        data = json.loads(response)
        if not data.get('items'): # no service vm uuid retrieved
            debug("Service VM with storage IP %s does not exist in grid" % servicevm_ip)
            return 1
        svm_uuid = data.get('items')[0]['uuid']
        debug(svm_uuid)
        
        get_apistr = (VOLUME_RESOURCE_API + USX_DICT['resources'][0]['uuid'])
        response = retrieve_from_usx_grid(LOCAL_AGENT, get_apistr)
        data = json.loads(response)
        if not data.get('data'): # no volume
            debug("No resource associated with this uuid: %s" % USX_DICT['resources'][0]['uuid'])
            return 1
        current_container_uuid = data.get('data')['containeruuid']
    except:
        debug("Exception caught:")
        debug(traceback.format_exc())
        return 1
    
    #debug(USX_DICT['uuid'])
    if current_container_uuid == USX_DICT['uuid']: # resource is on this container
        data = {}
        data['usxuuid'] = USX_DICT['resources'][0]['uuid']
        data['usxcontaineruuid'] = USX_DICT['uuid']
        data['usxtype'] = 'VOLUME_RESOURCE'
         
        storagenetwork_status = {}
        storagenetwork_status['name'] = 'STORAGE_NETWORK_' + svm_uuid
        if status.lower() == 'ok':
            storagenetwork_status['value'] = 'OK'
        else:
            storagenetwork_status['value'] = 'FATAL'
        data['usxstatuslist'] = []
        data['usxstatuslist'].append(storagenetwork_status)
     
        post_apistr = VOLUME_STATUS_API
        rc = publish_to_usx_grid(LOCAL_AGENT, post_apistr, data)
        if rc != 0:
            debug("ERROR : publish status to grid REST API call failed ")
            retVal = rc
    else:
        debug("resource: %s is not on this container: %s. Skip update storage network status to USX manager" % 
              (USX_DICT['resources'][0]['uuid'], current_container_uuid))

    return retVal

def update_storage_network_statuses(argv):
    """
    Update IBD reachable status:
        using threads
    """
    retVal = 0
    debug("Sending storage network reachable status... ")
    if len(argv) != 4:
        debug("ERROR : Incorrect number of arguments")
        exit(1)
    
    rc = multi_thread_update(argv[2], argv[3])
    return rc

class Multi_storage_network_update(threading.Thread):
    def __init__(self, ip, status):
        threading.Thread.__init__(self)
        self.ip = ip
        self.status = status
        self.rtn = ''
    def run(self):
        self.rtn = storage_network_reachable(self.ip, self.status)
    def get_return(self):
        return self.rtn

def multi_thread_update(svm_ip, status):
    thread_list = []
        
    t = Multi_storage_network_update(svm_ip, status)
    thread_list.append(t)
         
    for thread in thread_list:
        thread.start()
         
#     for thread in thread_list:
#         thread.join()
#          
#     for thread in thread_list:
#         rtn = thread.get_return()
#         if rtn == False:
#             return rtn
#         else:
#             continue
         
    return 0



cmd_options =  {
#     "reachable"          : storage_network_reachable,
    "reachable"          : update_storage_network_statuses,
#     "ha"         : ha_status,
}


debug("Entering availability_status_check:", sys.argv)

init_global_variables()

if len(sys.argv) < 2:
    debug("ERROR : Incorrect number of arguments!")
    debug("Usage: " + sys.argv[0] + " reachable")
    exit(1)

cmd_type = sys.argv[1]

if cmd_type in cmd_options:
    try:
        rc = cmd_options[cmd_type](sys.argv)
    except:
        debug(traceback.format_exc())
        debug("Exception exit...")
        rc = 1 
    if rc != 0:
        debug("%s Failed with: %s" % (sys.argv, rc))
        exit(rc)
    else:
        exit(0)
else:
    debug("ERROR : Incorrect argument '%s'" % cmd_type)
    debug("Usage: " + sys.argv[0] + " reachable|")
    exit(1)
