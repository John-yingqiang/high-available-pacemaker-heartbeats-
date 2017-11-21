#!/usr/bin/python

import os, sys
import json
import logging
import tempfile
from time import sleep
from subprocess import *

from ha_util import *

PK_NOT_RUNNING = 12
RARUNDIR='/run/resource-agents'

set_log_file('/var/log/usx-atlas-ha.log')

if not os.path.exists(RARUNDIR):
    os.makedirs(RARUNDIR)

count = 120
cmd = 'service corosync status'
pcmkrunning = False
while count > 0:
    (ret, msg) = runcmd(cmd, print_ret=True)
    if ret == 0 and msg.find('is running') >= 0:
	cnt = 120
	while cnt > 0:
	    (ret, msg) = runcmd('service pacemaker start', print_ret=True)
	    sleep(1)
	    (ret, msg) = runcmd('service pacemaker status', print_ret=True)
	    if ret == 0 and msg.find('is running') >= 0:
	        debug('Pacemaker started')
		pcmkrunning = True
		break
	    sleep(5)
	    cnt -= 1
    if pcmkrunning is True:
	break
    sleep(5)
    count -= 1

if pcmkrunning is True:
    open('/run/pacemaker_started', 'a').close()
    sys.exit(0)
else:
    debug('Fail to start pacemaker because corosync did not start.')
    sys.exit(PK_NOT_RUNNING)

