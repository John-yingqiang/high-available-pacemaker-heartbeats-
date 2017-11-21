#!/usr/bin/python
from daemon import Daemon
from subprocess import *
import tempfile
import sys
import time
import os
import signal
import logging
import traceback
from socket import *
import thread
from ha_util import *


debug("===== BEGIN USX CLIENT OPERATION =====")

debug("Entering usx_client:", sys.argv)
if len(sys.argv) < 3:
	debug("ERROR : Incorrect number of arguments!")
	debug("Usage: " + sys.argv[0] + "server_ip port_num cmd [vol_uuid]")
	exit(1)

# e.g., python /opt/milio/atlas/roles/ha/usx_client.pyc  10.16.88.56  55567 volume_status  USX_e5dd522c-503e-3714-abff-e86f1bad6f5e
HOST = sys.argv[1]  # the remote host
PORT = int(sys.argv[2])  # the same port as used by the server
cmd = sys.argv[3]
vol_uuid = ""
if len(sys.argv) > 4:
	vol_uuid = sys.argv[4]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

#s.sendall('volume_start VOL1')
#s.sendall('volume_stop USX_e5dd522c-503e-3714-abff-e86f1bad6f5e')
cmd_str = cmd + " " + vol_uuid
s.sendall(cmd_str)
data = s.recv(1024)
debug('Received: %s' % repr(data))

s.close()

debug("===== END USX CLIENT OPERATION: SUCCESSFUL! =====")
exit(0)
