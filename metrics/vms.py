# coding=utf-8

"""
The VMsCollector collects count of VMs under /exports/...

"""

import os, sys
import json
import string
#import logging
import diamond.collector
import diamond.service_collector
import time
from diamond.collector import str_to_bool
import httplib
import tempfile
from subprocess import *

ATLAS_CONF = "/etc/ilio/atlas.json"
LOCAL_AGENT = 'http://127.0.0.1:8080/usxmanager/'
USX_DICT = {}

class VMsCollector(diamond.service_collector.ServiceCollector):
    LastCollectCnt = 0

    def runcmd(
        self,
        cmd,
        print_ret=False,
        lines=False,
        input_string=None,
        ):
        if print_ret:
            self.log.info('Running: %s' % cmd)
        try:
            tmpfile = tempfile.TemporaryFile()
            p = Popen(
                [cmd],
                stdin=PIPE,
                stdout=tmpfile,
                stderr=STDOUT,
                shell=True,
                close_fds=False,
                )
            (out, err) = p.communicate(input_string)
            status = p.returncode
            tmpfile.flush()
            tmpfile.seek(0)
            out = tmpfile.read()
            tmpfile.close()

            if lines and out:
                out = [line for line in out.split('\n') if line != '']

            if print_ret:
                self.log.debug(' -> %s: %s: %s' % (status, err, out))
            return (status, out)
        except OSError:
            return (127, 'OSError')

    def init_global_variables(self):
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
                USX_DICT['resources'] = jsondata['volumeresources']
                if USX_DICT['resources']:
                    if len(USX_DICT['resources']) > 0:
                        USX_DICT['resourcesuuid'] = jsondata['volumeresources'][0]['uuid']
                        USX_DICT['dedupfsmountpoint'] = jsondata['volumeresources'][0]['dedupfsmountpoint']

            else: # this is a service vm
                USX_DICT['role'] = jsondata['roles'][0]

        except err:
            self.log.debug("ERROR : exception occurred, exiting ...")
            self.log.debug(err)
            return

    def check_hypervisor_type(self):
            hypervisor_type=""
            ret, output = self.runcmd('dmidecode -s system-manufacturer', print_ret = False)

            if (ret != 0) or (output is None) or (not output) or (len(output) <= 0):
                    self.log.debug('WARNING could not get hypervisor_type from dmidecode. Checking for Xen...')
                    if os.path.exists('/dev/xvda') == True:
                            hypervisor_type='Xen'
                    elif os.path.exists('/dev/sda') == True:
                            hypervisor_type='VMware'
            else:
                    output=output.strip()
                    if 'Microsoft' in output:
                            hypervisor_type='hyper-v'
                    elif 'VMware' in output:
                            hypervisor_type='VMware'
                    elif 'Xen' in output:
                            hypervisor_type='Xen'
                    else:
                            self.log.debug('WARNING do not support hypervisor_type %s' % output)

            return hypervisor_type

    def get_default_config_help(self):
        return super(VMsCollector, self).get_default_config_help()

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        return super(VMsCollector, self).get_default_config()

    def collect(self):
        """
        Collector VMs count under /exports/...
         """
        str_key = ''
        cnt = 0
        self.init_global_variables()
        hy_type = self.check_hypervisor_type()
        if hy_type == 'VMware':
            str_key = '*.vmx'
        else:
            str_key = '*.vhd'

        if USX_DICT['role'] != 'VOLUME':
            return

        cmd = 'find %s -name "%s" | sort | uniq | wc -l' % (USX_DICT['dedupfsmountpoint'], str_key)
        (ret, msg) = self.runcmd(cmd, print_ret=True, lines=True)
        if ret == 0 and len(msg) > 0 and msg[0][0].isdigit():
            cnt = int(msg[0])

        mypath = string.replace(self.get_metric_path('VMsCount'), USX_DICT['uuid'], USX_DICT['resourcesuuid'], 1) # replace ILIO UUID with resource ID
        self.publish('VMsCount', cnt, mypath, None, 2)
