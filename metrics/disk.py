 # coding=utf-8

"""
This class collects data on disk sda1 utilization

#### Dependencies

* /proc/meminfo or psutil

"""

import diamond.collector
import diamond.convertor
import os
import sys

sys.path.insert(0, "/opt/milio/libs/atlas")
from cmd import *

try:
    import psutil
    psutil  # workaround for pyflakes issue #13
except ImportError:
    psutil = None

class DiskCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(DiskCollector, self).get_default_config_help()
        config_help.update({
            'detailed': 'Set to True to Collect all the nodes',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(DiskCollector, self).get_default_config()
        config.update({
            'enabled':  'True',
            'path':     'disk',
            'method':   'Threaded',
            # Collect all the nodes or just a few standard ones?
            # Uncomment to enable
            #'detailed': 'True'
        })
        return config

    def disk_sda1_utilization(self):
        """
        Get sda1 disk utilization
        """
        cmd = '/bin/df -B M -T | grep sda1 | awk -F \' \' \'{print $6}\''

        (ret, out) = runcmd(cmd)

        ret = out.strip()[:-1]
        self.log.debug('sda1 disk utilization is %s' % ret)

        return ret

    def collect(self):
        """
        Collect disk sda1 stats
        """
        value = self.disk_sda1_utilization()
        if value == '':
            self.log.error("No sda1 disk status metrics retrieved. Will not publish anything.")
            return None
        self.publish('sda1.utilization', value)
