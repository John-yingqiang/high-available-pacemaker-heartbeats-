#!/usr/bin/python

import os, sys
import json
import logging
import tempfile
from subprocess import *

from ha_util import *
set_log_file('/var/log/usx-atlas-ha.log')

CORO_NOT_RUNNING = 11


cmd = 'service corosync start'
(ret, msg) = runcmd(cmd, print_ret=True)
cmd = 'service corosync status'
(ret, msg) = runcmd(cmd, print_ret=True)
if msg.find('is running'):
    print('corosync started.')
    sys.exit(0)

print('Failed to start corosync')
sys.exit(CORO_NOT_RUNNING)
