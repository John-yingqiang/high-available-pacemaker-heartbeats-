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
import datetime
from ha_util import *

##################################################################
# Global variable and configure parameters                               #
##################################################################
USX_SERVER_PIDFILE = '/var/run/usx_server.pid'
USX_SERVER_LOGFILE = '/var/log/usx-server.log'
USX_SERVER_ROTATE_CONF = '/etc/logrotate.d/usx_server'
idx = 0
ERROR = 1
SUCCESS = 0

# Configure logging
logging.basicConfig(filename=USX_SERVER_LOGFILE,
					level=logging.DEBUG,
					format='%(asctime)s %(message)s')


def usx_server_logrotate_conf():
	debug('Enter usx_server_logrotate_conf ')
	tmp_fname = "/tmp/usx_server"
	cfile = open(tmp_fname, "w")

	title = USX_SERVER_LOGFILE
	cfile.write(title + " {\n")
	cfile.write("		daily\n")
	cfile.write("		missingok\n")
	cfile.write("		rotate 14\n")
	cfile.write("		size 50M\n")
	cfile.write("		compress\n")
	cfile.write("		delaycompress\n")
	cfile.write("		notifempty\n")
	cfile.write("}\n\n")

	cfile.close()
	os.rename(tmp_fname, USX_SERVER_ROTATE_CONF)
	
	return 0


def vol_ha_start(data):
	debug('Enter vol_ha_start ')
	msg = str(data)
	items = data.split()
	resuuid = items[1]
	cmd = "python /opt/milio/atlas/roles/virtvol/vv-load.pyc usx_start " + resuuid + "  > /dev/null 2>&1"
	(ret, cmsg) = runcmd(cmd, print_ret=True)
	msg = str(ret) + " " + msg
	return msg


def vol_ha_stop(data):
	debug('Enter vol_ha_stop ')
	msg = str(data)
	items = data.split()
	resuuid = items[1]
	cmd = "python /opt/milio/atlas/roles/virtvol/vv-load.pyc usx_stop " + resuuid + "  > /dev/null 2>&1"
	(ret, cmsg) = runcmd(cmd, print_ret=True)
	msg = str(ret) + " " + msg
	return msg


def vol_ha_status(data):
	debug('Enter vol_ha_status ')
	msg = str(data)
	items = data.split()
	resuuid = items[1]
	cmd = "python /opt/milio/atlas/roles/virtvol/vv-load.pyc usx_status " + resuuid + "  > /dev/null 2>&1"
	(ret, cmsg) = runcmd(cmd, print_ret=True)
	msg = str(ret) + " " + msg
	return msg


def vol_health_check():
	global idx
	while True:
		print "vol_health_check " + str(idx)
		idx += 1
		time.sleep(2)
	return


def service_help(data):
	msg = str(SUCCESS) + " usage: help|volume_start|volume_stop|volume_status vol_uuid"
	return msg


cmd_options =  {
	"volume_start"         : vol_ha_start,
	"volume_stop"          : vol_ha_stop,
	"volume_status"        : vol_ha_status,
	"help"                 : service_help
}


def handle_task(clientsocket, clientaddr):
	debug('Enter handle_task ')
	global idx
	debug("Accepted connection from: " + str(clientaddr))
	
	data = clientsocket.recv(1024)
	if data:
		debug("received: " + data)
		items = data.split()
		cmd = items[0]
		msg = ''
		if cmd in cmd_options:
			try:
				msg = cmd_options[cmd](data)
			except:
				debug("Exception happened...")
				msg = str(ERROR) + " Exception happened"
	
			send_msg = msg
		else:
			send_msg = str(ERROR) + " Not supported service: " + data
		
		ts = time.time()
		send_msg = send_msg + " " + str(clientaddr) + " " + datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S ') 
		debug("replied: " + send_msg)
		clientsocket.send(send_msg)
		idx += 1
	clientsocket.close()


class UsxServer(Daemon):

	def run(self):
		try:
			#if not os.path.exists(USX_SERVER_ROTATE_CONF):
			usx_server_logrotate_conf()
			
			#host = 'localhost'
			port = 55567
			buf = 1024
			addr = ('', port)
			serversocket = socket.socket(AF_INET, SOCK_STREAM)
			serversocket.bind(addr)
			serversocket.listen(2)
			
			#thread.start_new_thread(vol_health_check, ())
			while 1:
				print "Server is listening for connections\n"
				clientsocket, clientaddr = serversocket.accept()
				print 'Connected by', clientaddr
				thread.start_new_thread(handle_task, (clientsocket, clientaddr))
			serversocket.close()

		except:
			debug(traceback.format_exc())
			debug('Exception caught on usx_daemon...')
			sys.exit(2)


if __name__ == "__main__":

	daemon = UsxServer(USX_SERVER_PIDFILE)
	
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
			try:
				daemon.start()
			except:
				pass
		elif 'stop' == sys.argv[1]:
			print "Stopping ..."
			daemon.stop()
		elif 'restart' == sys.argv[1]:
			print "Restaring ..."
			daemon.restart()
		elif 'status' == sys.argv[1]:
			try:
				pf = file(USX_SERVER_PIDFILE,'r')
				pid = int(pf.read().strip())
				pf.close()
			except IOError:
				pid = None
			except SystemExit:
				pid = None
			
			if pid:
				print 'UsxServer is running as pid %s' % pid
			else:
				print 'UsxServer is not running.'
		
		else:
			print "Unknown command"
			sys.exit(2)
	else:
		print "usage: %s start|stop|restart|status" % sys.argv[0]
		sys.exit(2)
