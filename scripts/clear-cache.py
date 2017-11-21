#!/usr/bin/python

import os
import time
import sys
import logging
import logging.handlers

import logging
import logging.handlers

sys.path.insert(0, "/opt/milio/atlas/roles/ha/")
# sys.path.insert(0, "../roles/ha/")

from ha_util import runcmd

LOG_FILENAME = '/var/log/clear-cache.log'
# logging.basicConfig( filename=LOG_FILENAME,level=logging.DEBUG,)

cc_logger = logging.getLogger(__name__)
cc_logger.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s %(message)s')
handler   = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes= 5*1024*1024, backupCount=3)
handler.setFormatter(formatter)
cc_logger.addHandler(handler)


def check_free_mem():
    MIN_MEM_NEEDED = 5368709
    cmd_free = "free"
    #out = ['']
    ( status,out ) = runcmd( cmd_free )
    arr_out = out.split()
    free_mem = arr_out[ 9 ]
    return int(free_mem)


def run_clear_cache():
   i = 0
   os.close(0)
   os.close(1)
   os.close(2)
   while True:
       dstat_cmd = "dstat -cnmdst -D sdb,sdc 1 1"
       (status, out) = runcmd( dstat_cmd )
       cc_logger.debug( out )
       ibdm_cmd = "ibdmanager -r s -s get"
       (status, out) = runcmd( ibdm_cmd )

       mem_free = check_free_mem()
       cc_logger.debug( "memory free is %d" % mem_free )
       # this is in MB reported
       MIN_MEM_THRESHOLD = 2097152
       if ( mem_free < MIN_MEM_THRESHOLD ):
           cc_logger.error( "memory free is %d" % mem_free )
           cc_logger.error( "below the memory threshold,  dropping the caches"  )
           (status, out) = runcmd( dstat_cmd )
           cc_logger.debug( out )
           drop_caches_cmd = "echo 3 > /proc/sys/vm/drop_caches"
           (status, out) = runcmd( drop_caches_cmd )
           cc_logger.error( out )
           (status, out) = runcmd( dstat_cmd )
           cc_logger.debug( out )

       time.sleep( 2 )
       i = i + 1

   os._exit(0)  


run_clear_cache()

