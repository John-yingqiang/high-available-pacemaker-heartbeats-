 # coding=utf-8

"""
This class collects data on memory utilization

Note that MemFree may report no memory free. This may not actually be the case,
as memory is allocated to Buffers and Cache as well. See
[this link](http://www.linuxatemyram.com/) for more details.

#### Dependencies

* /proc/meminfo or psutil

"""

import diamond.collector
import diamond.convertor
import os

try:
    import psutil
    psutil  # workaround for pyflakes issue #13
except ImportError:
    psutil = None

_KEY_MAPPING = [
    'MemTotal',
    'MemFree',
    'Buffers',
    'Cached',
    'Active',
    'Dirty',
    'Inactive',
    'Shmem',
    'SwapTotal',
    'SwapFree',
    'SwapCached',
    'VmallocTotal',
    'VmallocUsed',
    'VmallocChunk'
]


class MemoryCollector(diamond.collector.Collector):

    PROC = '/proc/meminfo'

    def get_default_config_help(self):
        config_help = super(MemoryCollector, self).get_default_config_help()
        config_help.update({
            'detailed': 'Set to True to Collect all the nodes',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MemoryCollector, self).get_default_config()
        config.update({
            'enabled':  'True',
            'path':     'memory',
            'method':   'Threaded',
            # Collect all the nodes or just a few standard ones?
            # Uncomment to enable
            #'detailed': 'True'
        })
        return config

    def collect(self):
        """
        Collect memory stats
        """
        if os.access(self.PROC, os.R_OK):
            file = open(self.PROC)
            data = file.read()
            file.close()
            
            memTotal=0
            memFree=0
            memUsed=0
            Buffers=0
            Cached=0
            Shmem=0
            for line in data.splitlines():
                try:
                    name, value, units = line.split()
                    name = name.rstrip(':')
                    value = int(value)

                    if (name not in _KEY_MAPPING
                            and 'detailed' not in self.config):
                        continue

                    for unit in self.config['byte_unit']:
                        value = diamond.convertor.binary.convert(value=value,
                                                                 oldUnit=units,
                                                                 newUnit=unit)
                        if name == 'MemTotal':
                            memTotal = long(value)
                        if name == "MemFree":
                            memFree = long(value)
                            self.publish('MemFree', memFree, metric_type='GAUGE')
                            memUsed = memTotal - memFree
                            self.publish('MemUsed', memUsed, metric_type='GAUGE')
#                            self.publish('naive_utilization', (float(memUsed) / memTotal) * 100, metric_type='GAUGE')
                        if name == 'Buffers':
                            Buffers = long(value)
                            self.publish('Buffers', value)
                        if name == 'Cached':
                            Cached = long(value)
                            self.publish('Cached', value)
                        if name == 'Shmem':
                            Shmem = long(value)
                            """
                            for Atlas v1.5 monitor feature, report total memory utilization
                            Calculation:
                                total memory = MemTotal - Shmem
                                free memory = (MemFree + Buffers + Cached) - Shmem
                                utilization = (total memory - free memory) / total memory
                            """
                            totalMemory = memTotal - Shmem
                            freeMemory = (memFree + Buffers + Cached) - Shmem
                            utilization = (float(totalMemory - freeMemory) / totalMemory) * 100
                            self.publish('utilization', utilization, metric_type='GAUGE')
                        # TODO: We only support one unit node here. Fix it!
                        break

                except ValueError:
                    continue
            return True
        else:
            if not psutil:
                self.log.error('Unable to import psutil')
                self.log.error('No memory metrics retrieved')
                return None

            self.log.debug('Reporting from psutil')
            phymem_usage = psutil.phymem_usage()
            virtmem_usage = psutil.virtmem_usage()
            units = 'B'

            for unit in self.config['byte_unit']:
                value = diamond.convertor.binary.convert(
                    value=phymem_usage.total, oldUnit=units, newUnit=unit)
#                self.publish('MemTotal', value, metric_type='GAUGE')

                value = diamond.convertor.binary.convert(
                    value=phymem_usage.free, oldUnit=units, newUnit=unit)
                self.publish('MemFree', value, metric_type='GAUGE')

#               value = diamond.convertor.binary.convert(
#                   value=virtmem_usage.total, oldUnit=units, newUnit=unit)
#               self.publish('SwapTotal', value, metric_type='GAUGE')

#                value = diamond.convertor.binary.convert(
#                    value=virtmem_usage.free, oldUnit=units, newUnit=unit)
#                self.publish('SwapFree', value, metric_type='GAUGE')

                # TODO: We only support one unit node here. Fix it!
                break

            return True

        return None
