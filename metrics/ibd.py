# coding=utf-8

"""
The IBDCollector collects IBD queue size...
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
USX_DICT = {}

class IBDCollector(diamond.service_collector.ServiceCollector):

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

        try:
            fp = open(ATLAS_CONF)
            jsondata = json.load(fp)
            fp.close()
            if jsondata.has_key('usx'): # this is a volume
                USX_DICT['uuid'] = jsondata['usx']['uuid']
                if jsondata['volumeresources']:
                    if len(jsondata['volumeresources']) > 0:
                        USX_DICT['resourcesuuid'] = jsondata['volumeresources'][0]['uuid']
                        USX_DICT['volumetype'] = jsondata['volumeresources'][0]['volumetype']
        except err:
            self.log.debug("ERROR : exception occurred, exiting ...")
            self.log.debug(err)
            return

    def get_default_config_help(self):
        return super(IBDCollector, self).get_default_config_help()

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        return super(IBDCollector, self).get_default_config()

    def collect(self):
        '''
        Collector IBD queue size...
         '''
        self.init_global_variables()
        if (not USX_DICT.has_key('volumetype')) or (USX_DICT['volumetype'] != 'SIMPLE_HYBRID'):
            return None

        cmd = "ibdmanager -r s -s get | grep block_occupied | awk -F ':' '{print $2}'"
        (ret, msg) = self.runcmd(cmd, print_ret = True, lines = True)
        if ret == 0 and len(msg) > 0:
            ibd_queue = msg[0].split('/')
        if len(ibd_queue) == 2:
            ibd_queue_total = int(ibd_queue[1])
            ibd_queue_used = int(ibd_queue[0])
            quene_percentage_used = ibd_queue_used * 100.0 / ibd_queue_total 
            percentage_used_mypath = string.replace(self.get_metric_path('quene_percentage_used'), USX_DICT['uuid'], USX_DICT['resourcesuuid'], 1)
            self.publish('quene_percentage_used', quene_percentage_used, percentage_used_mypath, None, 2)
        else:
            self.log.error('ERROR : Failed to get queue sizi vai ibdmanager -r s -s get, exiting ...')
