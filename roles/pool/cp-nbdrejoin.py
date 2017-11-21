#!/usr/bin/python

import os, sys, time
sys.path.insert(0, "/opt/milio/libs/atlas")
from atl_util import *
from atl_md import *

CMD_IDEL = "/sbin/mdadm -If"
CMD_IADD = "/sbin/mdadm -I --run"
CMD_MDADM = "/sbin/mdadm"

debug("Entering cp-nbdrejoin:", sys.argv)
if len(sys.argv) < 2:
	debug("need one arg")
	exit(0)

nbd_dev = sys.argv[1] 
time.sleep(1)
rejoin_raid1_manage_mode(nbd_dev)

exit(0)
