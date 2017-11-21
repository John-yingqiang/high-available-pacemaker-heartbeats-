#! /usr/bin/env python
'''
usxobjstore.py - The Object Store Service program.

Copyright (c) 2014 Atlantis Computing Inc.

Author: Kartikeya Iyer (kartik@atlantiscomputing.com)
Date: 19-Mar-2014

The Object Store feature makes available via HTTP (on port 9095, which 
according to IANA is currently unassigned) the files in the dedupFS of a 
USX ADS node which has its filesystem exported via NFS.

When this Object Store Service program is running, the files in the DedupFS 
are available for READ/Download access via an HTTP GET call using the IP 
address of the ILIO, which can be done through a browser or through a 
command line HTTP tool like cURL.

The entities in the Object Store can be accessed from a flat hierarchy 
with the following URL as the root of the Object hierarchy:
	http://<ETH0_IP_OF_ILIO>:9095/USX

The Object Store feature is only available for ADS Nodes, and not for any 
other type of USX ILIO node.

This feature is only available on ADS nodes which have their DedupFS volume
(mountpoint) exported out via NFS.

This feature is NOT available on an ADS node which has its DedupFS exported 
via iSCSI. This is because a SCSI export the DedupFS only contains the 
LUN sparse file - all the data files are present INSIDE this sparse file, and 
there is no visibility into the actual data files present inside this LUN file.

This Object Store Service program is designed to be started when the
DedupFS of an ADS node is mounted. It should be stopped/killed when
the DedupFS is unmounted on an ADS node

To stop/kill the process corresponding to this program, DO NOT USE "kill -9".
Using "kill -9" will cause this program to exit uncleanly, without giving it
a chance to clean up the sub-processes that it spawns. To kill this program
from the command line, use the following command:
	kill -SIGTERM <PID_OF_THIS_PROGRAM>

Currently, the IP address that is used to access the Object Store on the ADS 
ILIO is the IP address of eth0. In a later revision we can make configurable 
the network interface used to access the ILIO (and hence the IP address).

The access provided to the files on the ADS node's DedupFS is currently 
READ-ONLY/DOWNLOAD access. This feature does not currently enable WRITE access 
to the DedupFS using the Object Store HTTP access.

The HTTP port number used to access the Object Store on an ADS ILIO is set 
to 9095. This port is currently unassigned to any other entity by the IANA.

HTTP access provided to the Object Store entities is via standard HTTP, and 
not SSL-secured HTTPS.

For further details on the features and functionality of this program, please
read the design document for this feature: ObjectStoreDesign.doc
This document is available at the JIRA Item which tracks the design and 
development of this feature: ATLANTIS-1645
Link to JIRA item on our JIRA server as of this writing (19-Mar-2014):
http://10.15.2.93:8080/browse/ATLANTIS-1645

Revision History:
	Initial development: 19-Mar-2014, Kartikeya Iyer
'''
import os
import sys, tempfile, re
from types import *
from subprocess import *
import signal
import time
import netifaces as ni
import xml.etree.ElementTree as ET
import tempfile as TF

# milio/Atlas library functions
sys.path.insert(0, "/opt/milio")
from libs.atlas.cmd import runcmd
from libs.atlas.log import debug

######## Globals ########
# The default Object Store bucket name for the DedupFS
DEDUP_BUCKET_NAME="USX"

# Global to hold the Popen process handle to inotifywait
INOTIFYWAIT_PROCESS=None

# Dictionary to hold the IP address of each interface, e.g. {"eth0":"1.1.1.1", "eth1":"1.1.1.2"}
INTERFACE_IPS = {}

# The main IP address which we will use to make the cURL calls.
# This is set by get_first_valid_ipaddress()
MAIN_IP = None

# The Dedup FS Mountpoint
DEDUPFS_MOUNTPOINT = ""

## Object store framework-related globals 
OBJECTSTORE_MAIN_DIR="/opt/milio/atlas/objectstore/"
OBJECTSTORE_METADATA_DIR="StorageEngine"
JETTY_RUNNER_JARFILE="jetty-runner-8.1.9.v20130131.jar"
LITTLES3_WARFILE="littleS3-2.3.0.war"
OBJECTSTORE_HTTP_PORT="9095"
SERVER_RUNNING_CHECK_CMD = 'ps aux | grep java | grep -v grep | grep '+LITTLES3_WARFILE+' | grep -v "su -c" | grep -v "sh -c"'

# An enum for the operations to be conducted on the Object Store
# Python still doesn't have enums, PEP 435 notwithstanding
class StoreOps:
	ADD = 0
	DELETE = 1
	def __init__(self, Type):
		self.value = Type
	def __str__(self):
		if self.value == StoreOps.ADD:
			return 'ADD'
		if self.value == StoreOps.DELETE:
			return 'DELETE'
	def __eq__(self,y):
		return self.value==y.value

# A dictionary to map inotify Operations to Object Store operations
storeops_dict = {
	"MOVED_FROM" : StoreOps.DELETE,
	"MOVED_TO" : StoreOps.ADD,
	"CREATE" : StoreOps.ADD,
	"MODIFY" : StoreOps.ADD,
	"DELETE" : StoreOps.DELETE,
}


######## END : Globals ########


'''
Function to trap POSIX signals sent to this program.
According to http://www.gnu.org/savannah-checkouts/gnu/libc/manual/html_node/Termination-Signals.html
we cannot hook the SIGKILL function.
SIGSTOP/SIGSTP is also not explicitly handled because not all systems define
this signal.
The signals which we currently handle are:
	SIGINT
	SIGQUIT
	SIGTERM
	SIGHUP

Parameters:
	signum : Signal Number
	frame: Object referring to the current stack frame

Returns:
	This function does not return anything, but exits the program with
	a return code of 0

Reference:
	http://docs.python.org/2/library/signal.html
'''
def signal_handler(signum, frame):
	debug("WARNING : Termination signal received! Stopping USX Object Store updates. You will NOT get further Object Store updates until this program is restarted!")
	if INOTIFYWAIT_PROCESS:
		INOTIFYWAIT_PROCESS.terminate()
	debug("   Termination : Killing DedupFS monitor if required...")
	runcmd("killall -9 inotifywait")
	# NOTE: Although there is No need to explicitly kill jetty-runner, since
	# when this program terminates, it automatically kills jetty-runner
	# because the jetty-runner process is spawned as a child of this
	# process, we still kill it anyway, for safety, if it is running.
	debug("   Termination : Killing Object Store DB process if required...")
	kill_object_server()
	debug("INFO : EXITING USX Object Store update program!")
	sys.exit(0)


'''
Get the IP address assigned to eth0, or the first valid IP address assigned
to this ILIO. 
In practice, this will return the IP address assigned to eth0.
This function also sets the global variable INTERFACE_IPS which is a 
dictionary containing eth_name -> IP mappings.

Parameters:
	None

Returns:
	String containing first valid IP Address, if one was found
	Returns python 'None' object on errors
'''
def get_first_valid_ipaddress():
	global INTERFACE_IPS
	ret = None
	try:
		# Get list of all network interfaces in the system 
		ifaces = ni.interfaces()
		# remove the 'lo' interface from this list
		ifaces.remove('lo')

		debug("Getting IP Addresses...")
		for iface in ifaces:
			ifacestr = str(iface)
			ipaddr = ni.ifaddresses(iface)[2][0]['addr']
			if ipaddr is None or not ipaddr:
				continue
			INTERFACE_IPS[ifacestr] = ipaddr
			debug("   "+ifacestr+" = "+ipaddr)


		if len(INTERFACE_IPS) <= 0:
			debug("ERROR : Could not get a valid IP Address on any interface.")
			return None

		# Set ret to the first IP address in the dictionary. This is for safety
		ret = INTERFACE_IPS.values()[0]

		# Now get the IP address of eth0. If no eth0 (how did that happen??),
		# We just return the first available IP address we got above
		# Later we can modify this to get the IP address of a particular NIC
		if INTERFACE_IPS.has_key('eth0'):
			ret = INTERFACE_IPS.get('eth0')
			if ret is None or not ret:
				debug("WARNING: Did not get IP Address of eth0.")
			else:
				debug("INFO : Got IP Address for eth0: "+ret)

		return ret
	except:
		debug("ERROR : Exception getting IP addresses assigned to this ILIO. Exception was: "+sys.exc_info()[0])
	return ret
		

'''
Gets the current hostname assigned to this ILIO, and checks whether the
/etc/hosts file contains this hostname in the "127.0.0.1" line.
If we successfully got the hostname for this ILIO and if the hostname
does not exist in the /etc/hosts file, then this function will attempt to
add the hostname into the /etc/hosts file.

Parameters:
	None

Returns:
	True if hostname already exists in /etc/hosts file
	True if hostname did not exist in /etc/hosts, and this function added it
	False on errors
'''
def verify_hostname_in_hosts_file():
	try:
		# Get hostname and make sure that it exists in /etc/hosts
		ret, res = runcmd("hostname")
		if (ret != 0) or (res is None) or (not res) or (len(res) <= 0):
			return False

		hostname = res.strip()
		if hostname is None or not hostname:
			return False
		debug("Hostname set for this ILIO: "+hostname)
		debug("verifying whether the hostname exists in /etc/hosts...")
		cmd = "cat /etc/hosts | grep "+hostname+" | grep -v grep"
		ret,_ = runcmd(cmd)
		if ret == 0:
			debug("Hostname "+hostname+" exists in /etc/hosts, all good!")
			return True

		# If we got here, we need to add the hostname to /etc/hosts, on the
		# 127.0.0.1 line.
		debug("Hostname "+hostname+" does not exist in /etc/hosts, adding it.")
		cmd = 'perl -p -i -e "s/^(\s*?)127\.0\.0\.1.*$/127.0.0.1 localhost '+ hostname+'/g" /etc/hosts'
		ret,_ = runcmd(cmd)
		if ret != 0:
			debug("ERROR : Hostname "+hostname+" does not exist in /etc/hosts and we failed to add it to /etc/hosts")
			return False

		return True
	except:
		debug("ERROR : Exception verifying/setting hostname in hosts file.")
	
	return False

			
'''
Configures the file StorageEngine.properties which needs to be properly
configured with the host IP address to allow littleS3 to function 
correctly.

It also ensures that the StorageEngine.properties file contains the
correct path to the littleS3 metadata directory, which on a production
ILIO lives at /opt/milio/atlas/objectstore/StorageEngine (see the definition
of the global variables OBJECTSTORE_MAIN_DIR and OBJECTSTORE_METADATA_DIR)

Parameters:
	None

Returns:
	0  : No configuration changes needed
	1  : Successfully configured file
	<0 : Error
'''
def configure_objstore_server():
	global MAIN_IP
	try:
		# Verify that we have the hostname in /etc/hosts.
		# We can attempt to continue with the config even if we didn't verify this.
		debug("INFO : Configure Object Store Server : Verifying whether hostname is set correctly in /etc/hosts...")
		if not verify_hostname_in_hosts_file():
			debug("WARNING : Configure Object Store Server : Failed to verify whether hostname is set correctly in file. Continuing with other steps.")

		debug("INFO : Configure Object Store Server : Getting first valid IPv4 IP Address...")
		debug("INFO : Configure Object Store Server : Using IP Address: "+MAIN_IP)
		# Configure the LittleS3 StorageEngine.properties file
		debug("INFO : Configure Object Store Server : Configuring Object Store server config file if required...")
		prop_filepath = OBJECTSTORE_MAIN_DIR+"StorageEngine.properties"
		hostline = "host="+ipaddr+":"+OBJECTSTORE_HTTP_PORT
		storagelocation = OBJECTSTORE_MAIN_DIR+OBJECTSTORE_METADATA_DIR
		storagelocation_line = "storageLocation="+storagelocation

		# Check if the file is properly configured with the hostline
		cmd = 'cat '+prop_filepath+' | grep "'+hostline+'" | grep -v grep'
		ret,_ = runcmd(cmd)
		if ret == 0:
			# We've verified that the host line in the prop file is correct. We can exit now.
			debug("INFO : Configure Object Store Server : config file verified successfully, it needs no modification :-)")
			return 0

		# If we got here, we need to configure the prop file with the host line
		debug("INFO : Configure Object Store Server : Configuring config file with correct parameters...")
		cmd = 'perl -p -i -e "s/^(\s*?)host=.*$/'+hostline+'/g" '+prop_filepath
		ret,_ = runcmd(cmd)
		if ret != 0:
			debug("ERROR : Configure Object Store Server : Failed to properly configure Object Store config file. Object Store functionality might be affected!")
			return -1

		debug("INFO : Configure Object Store Server : Successfully configured Object Store Server config file")
		return 1

	except:
		debug("ERROR : Exception configuring Object Store Server.") 
		return -2


'''
This function checks whether the littleS3 war file is running using
jetty-runner as the web server process.

Parameters:
	None

Returns:
	True : littleS3/jetty-runner is running
	False : littleS3/jetty-runner is NOT running
'''
def check_object_server_running():
	global SERVER_RUNNING_CHECK_CMD
	ret,_ = runcmd(SERVER_RUNNING_CHECK_CMD)
	if ret == 0:
		return True
	return False

'''
Verifies whether the USX bucket exists in the littleS3 DB.

Parameters:
	None

Returns:
	True : USX Bucket exists
	False: USX Bucket does not exist, or error encountered while attempting
			to verify USX bucket existence.
'''
def verify_usx_bucket_exists():
	global MAIN_IP
	global OBJECTSTORE_HTTP_PORT
	global DEDUP_BUCKET_NAME
	if MAIN_IP is None or not MAIN_IP:
		debug("ERROR : Verify USX Bucket : Could not get a valid IP address for this ILIO.")
		return False
	bucket_path_http = '"http://'+MAIN_IP+':'+OBJECTSTORE_HTTP_PORT+'/'+DEDUP_BUCKET_NAME+'"'

	# Do a curl GET call on the bucket just to see if it has been created.
	# We call curl with the -f command to cause curl to return an error code
	# if we got a server error (anything other than HTTP code 200)
	debug("INFO : Verify USX Bucket : Verifying whether USX bucket exists...")
	cmd = 'curl -f --request GET '+bucket_path_http
	ret, res = runcmd(cmd)
	if ret != 0:
		debug("ERROR : Verify USX Bucket : Verify USX bucket returned non-zero error code "+str(ret)+" with message: "+res)
		return False
	# If we got here, we successfully verified the existence of the bucket
	debug("INFO : Verify USX Bucket : Successfully verified the existence of the USX bucket :-)")
	return True


'''
Kills jetty-runner/littleS3 if it is running.

Parameters:
	None

Returns:
	True : Killed running server successfully, or no need to kill server
	False : Needed to kill running server, but couldn't, due to errors

'''
def kill_object_server():
	getpidcmd = SERVER_RUNNING_CHECK_CMD+" | tr [:space:] ' ' | tr -s ' ' | cut -d' ' -f2"
	ret = check_object_server_running()
	if ret is False: # It's not running, nothing to kill
		debug("INFO : Kill Object Store Server : Object Store server not running, nothing to kill")
		return True
	# If we got here, we need to kill the server
	ret,kpid = runcmd(getpidcmd)
	if ret != 0:
		debug("ERROR : Kill Object Store DB/Server : server/DB process is running, but Unable to get PID of running server process, cannot kill running server!")
		return False
	kpid = kpid.strip()
	debug("INFO : Kill Object Store DB/Server : PID of running server process is "+kpid)
	killcmd = "kill -9 "+kpid
	ret,_ = runcmd(killcmd)
	if ret != 0:
		debug("ERROR : Kill Object Store DB/Server : Trying to kill process with PID "+pid+" returned non-zero, process may not have been killed!")
		return False

	# Check that it's really dead
	time.sleep(2)
	ret = check_object_server_running()
	if ret is False: # No server process is running
		debug("INFO : Kill Object Store DB/Server : Successfully stopped running server with PID "+kpid)
		return True

	# If we got here, we failed to kill the running server :-(
	debug("ERROR : Kill Object Store DB/Server : Failed Trying to kill process with PID "+pid+", old process may not have been killed!")
	return False


'''
Start the object store HTTP service/littleS3.

This function first checks if the required config files are configured
correctly, and configures them if required. 

If no configuration changes were made, and the HTTP server/littleS3 is
already running, then nothing needs to be done.

If the configuration files were changed, and the HTTP server is running, 
then we need to kill and restart the HTTP server/littleS3.

Parameters:
	None

Returns:
	False :	- if we could not verify config file details as per latest ILIO
			hostname and currently assigned IP address
			- If we needed to start/restart the HTTP server/littleS3, but failed.
			- On all other errors
	True  :	- No config files were changed, and server already running
			- Config files were changed, and we needed a server start/restart
			  which succeeded.
'''
def start_objectstore_server():
	global MAIN_IP
	# Configure the littleS3 config files to set the correct params
	debug("INFO : Start Object Store Server : Verifying configuration")
	configret = configure_objstore_server()
	if configret < 0: # Error verifying/setting config file details
		debug("ERROR : Failed to verify/set configuration for USX Object Store HTTP server. You will not have Object Store capabilities on this ILIO!")
		return False

	# Check if the littles3 war is already running
	server_running = check_object_server_running()
	if server_running:
		if configret == 0: # Config successfully verified with no changes
			debug("INFO : Object store http service is already running and seems to have been properly initialized earlier, no need to start anything.")
			return True
		else: # Config was changed
			# We need to kill the existing server so that we can
			# restart it with the changed config
			debug("INFO : Object store http service is already running, but since the configuration has changed we need to stop the currently running server.")
			ret = kill_object_server()
			if ret:
				debug("INFO : Kill running server due to config change : Successfully stopped running server process.")
			else:
				debug("ERROR : Kill running server due config change : Failed to kill running server process!")
				return False

	#### If we got here, we need to run the Object Store HTTP server

	# Run littleS3 as the 'nobody' user. We need to ensure that OBJECTSTORE_METADATA_DIR has write permissions for this user

	# Set permissions on OBJECTSTORE_METADATA_DIR to allow writes by the 'nobody' user
	objstor_metadata_dir_fullpath = OBJECTSTORE_MAIN_DIR+OBJECTSTORE_METADATA_DIR
	cmd = "chown -R nobody "+objstor_metadata_dir_fullpath
	ret,_ = runcmd(cmd)
	if ret != 0:
		debug("WARNING : Could not set ownership on Object Store metadata directory")
	else:
		debug("INFO : Successfully set ownership on Object Store metadata directory")

	# Delete all metadata in objstor_metadata_dir_fullpath; we will re-add 
	#this data when we start watching the dedupFS. This ensures that a stale,
	# out-of-sync (due to previous errors) object store returns to a sane
	# starting state.
	cmd = "rm -rf "+objstor_metadata_dir_fullpath+"/*"
	ret,_ = runcmd(cmd)
	if ret != 0:
		debug("WARNING : Could not clear contents of Object Store metadata directory prior to start. If your Object Store previously contained stale/incorrect information, this stale/incorrect information still exists. To clear stale/incorrect information, you will need to stop this program, manually clear the Object Store metadata directory, and re-start this program.")
	else:
		debug("INFO : Successfully cleared Object Store metadata directory, it will be repopulated with correct info when the Object Store service is started shortly.")

	# Start littleS3
	debug("Starting Object Store Server service...")
	jetty_runner_path = OBJECTSTORE_MAIN_DIR+JETTY_RUNNER_JARFILE
	littles3_warpath = OBJECTSTORE_MAIN_DIR+LITTLES3_WARFILE
	# Example command line to start littleS3:
	# su -c 'java -jar /opt/milio/atlas/objectstore/jetty-runner-8.1.9.v20130131.jar --port 9095 --classes /opt/milio/atlas/objectstore/ /opt/milio/atlas/objectstore/littleS3-2.3.0.war' nobody &
	cmd = "su -c 'java -jar "+jetty_runner_path+" --port "+OBJECTSTORE_HTTP_PORT+" --classes "+OBJECTSTORE_MAIN_DIR+" "+littles3_warpath+" &' nobody "
	ret,_ = runcmd(cmd)
	# Check if the server process is running
	waittime_in_seconds = 8
	debug("INFO : Waiting "+str(waittime_in_seconds)+" seconds for server startup...")
	time.sleep(waittime_in_seconds)
	ret = check_object_server_running()
	if ret is False:
		debug("ERROR : Object Store Server does not seem to be running! You will not have Object Store Capabilities on this ILIO!")
		return False

	# Create the USX bucket if it does not already exist.
	# Even if it exists, the PUT call to create it again
	# does no harm
	debug("INFO : USX bucket creation being done if required... ")
	usx_bucket = 'http://'+MAIN_IP+':'+OBJECTSTORE_HTTP_PORT+'/'+DEDUP_BUCKET_NAME
	debug("INFO : Creating USX Bucket: "+usx_bucket)
	bucket_create_cmd = 'curl -f --request PUT "'+usx_bucket+'"'
	ret,msg = runcmd(bucket_create_cmd) 
	debug("INFO : USX bucket creation command returned: "+str(ret))
	if ret != 0 and ret != 7:
		# Run it again!
		ret,msg = runcmd(bucket_create_cmd) 
		debug("INFO : USX bucket creation command try 2 returned: "+str(ret))
	# Verify whether the USX bucket exists - it should, because we just
	# created it.
	bret = verify_usx_bucket_exists()
	if bret is False:
		# Try bucket creation again
		ret,msg = runcmd(bucket_create_cmd) 
		debug("INFO : USX bucket creation command try 2 returned: "+str(ret))
		bret2 = verify_usx_bucket_exists()
		if bret2 is False:
			debug("ERROR : USX bucket DOES NOT seem to exist. This is a problem! ")
			return False

	# if we got here, we've verified that the USX bucket exists.
	debug("INFO : USX bucket exists, and we can now use it for the Object Store.")
	debug("SUCCESSFULLY Started Object Store Server :-)")
	return True


'''
Converts a file path from an absolute path on the DedupFS, to a path which
can be used to perform curl PUT/DELETE calls into the object store DB.

This essentially involves stripping the DedupFS mount point from the 
file's absolute path on the DedupFS, and stripping any leading "/" from
it.

This function is the reverse of the "convert_objstore_path_to_fs_path()"
function.

Parameters:
	fspath : The absolute path to the file on the DedupFS

Returns:
	Sring containing stripped path on success
	Python 'None' object on errors. Caller must check for 'None'
'''
def convert_fs_path_to_objstore_path(fspath):
	try:
		if fspath is None or not fspath:
			debug("ERROR : Convert FS Path to Object Store path : Null or empty FS path received!")
			return None

		#debug("INFO : Convert FS Path to Object Store path : Converting '"+fspath+"'")
		# To build up the Object Store file path, we need to strip the dedup mount point name from fpath
		objstore_file_path = fspath.replace(DEDUPFS_MOUNTPOINT, "", 1)
		if objstore_file_path is None or not objstore_file_path:
			debug("ERROR : Convert FS Path to Object Store path : Failed to convert '"+fspath+"' to Object Store path, conversion returned null or empty string!")
			return None

		# The Object Store paths in the bucket do not have a leading "/".
		# If our path has it, we need to strip it.
		if objstore_file_path[0] == '/':
			objstore_file_path = objstore_file_path[1:]

		return objstore_file_path
	except:
		debug("ERROR : Convert FS Path to Object Store Path : Exception converting.")
		return None


'''
Converts a file path from a path reference in the Object Store, to a path
which is the full path to the file on the DedupFS.

This essentially involves stripping any leading "/" from the Object Store
path, and then prepending the DedupFS mount point to it.

This function is the reverse of the "convert_fs_path_to_objstore_path()"
function.

Parameters:
	objpath : the object store file path which needs to be converted into
			 the full path to the file on the DedupFS

Returns:
	Sring containing full DedupFS path to file, on success
	Python 'None' object on errors. Caller must check for 'None'
'''
def convert_objstore_path_to_fs_path(objpath):
	if objpath is None or not objpath:
		debug("ERROR : Convert Object Store Path to FS Path : Received null or empty Object Store path.")
		return None

	# If objpath has a leading '/', remove it.
	if objpath[0] == '/':
		objpath = objpath[1:]

	# To convert an Object Store file path, we need to prepend the Dedup Mount point to it.
	fspath = DEDUPFS_MOUNTPOINT+"/"+objpath
	return fspath


'''
This function does the actual cURL call to add or remove a file present in the
DedupFS to/from the Object Store DB.

Parameters:
	fpath : The full path to the file on the DedupFS which we want to add to OR
			remove from the Object Store
	storeop : The Object Store operation to perform on the above file. Valid
			operations are defined in the StoreOps class defined above.

Returns:
	True : cURL call to add/delete file to/from Object Store DB succeeded
	False : Error in adding/deleting file to/from Object Store DB
'''
def do_curl_call(fpath, storeop):
	global MAIN_IP
	global OBJECTSTORE_HTTP_PORT
	global DEDUP_BUCKET_NAME
	if fpath is None or not fpath or storeop is None or (storeop != StoreOps.ADD and storeop != StoreOps.DELETE):
		debug("ERROR : Object Store operation : One or more required parameters was null or empty or otherwise invalid. Cannot perform Object Store operation")
		return False
	if MAIN_IP is None or not MAIN_IP:
		debug("ERROR : Object Store operation : Could not determine IP address, cannot perform Object Store operation")
		return False

	curl_cmd = ""
	http_req_data_substring = '--data-binary "@'+fpath+'" '
	# To build up the HTTP file path, we need to strip the dedup mount point name from fpath
	file_path_for_http = convert_fs_path_to_objstore_path(fpath)
	if file_path_for_http is None or not file_path_for_http:
		debug("ERROR : Object Store operation : Could not convert FS Path '"+fpath+"' to Object Store path; got null/empty string back!")
		return False

	file_path_http = '"http://'+MAIN_IP+':'+OBJECTSTORE_HTTP_PORT+'/'+DEDUP_BUCKET_NAME+'/'+file_path_for_http+'"'

	if storeop == StoreOps.DELETE:
		curl_cmd = 'curl -f --request DELETE '+file_path_http
	elif storeop == StoreOps.ADD:
		curl_cmd = 'curl '+http_req_data_substring+' -f --request PUT --header "Content-Type: application/octet-stream" '+file_path_http
	else:
		debug("ERROR : Object Store operation : Invalid operation '"+str_op+"' requested for file '"+fpath+"'. Cannot perform Object Store operation.")
		return False

	# If we got here, we should have built up the appropriate curl command
	if curl_cmd is None or not curl_cmd:
		debug("ERROR : Object Store operation : Failed to build command for operation '"+str_op+"' requested for file '"+fpath+"'. Cannot perform Object Store operation.")
		return False

	# If we got here, we have a valid curl command. Execute it.
	status,ret = runcmd(curl_cmd) 
	if status != 0:
		debug("ERROR : Object Store operation : Failed to perform command for operation '"+str(storeop)+"' requested for file '"+fpath+"'. Cannot perform Object Store operation. Output was: "+ret)
		return False

	# Successfully performed operation.
	return True


'''
Handles the addition of files already existing in a particular directory
on the ILIO file system to the Object Store DB.

Parameters:
	fsdir : The ILIO Filesystem directory whose contents we want to add to
			the object store

Returns:
	True : Succeeded in adding contents of given directory to the object store
	False : Failed to add contents of given directory to object store
'''
def add_existing_fs_dir_contents_to_objstore(fsdir):
	finalret = True
	if fsdir is None or not fsdir:
		debug("ERROR : Handle Existing Directory : Received null or empty FS directory name, cannot continue this operation.")
		return False
	# Add existing data in DedupFS directory to Object Store
	# Don't need to do anything with directories, since littleS3 has no
	# concept of directories. We only operate on files
	debug("INFO : Handle Existing Directory : Now handling existing files in '"+fsdir+"'")
	ret,output = runcmd("find "+fsdir+" -type f")
	if ret == 0:
		for line in output.split('\n'):
			if line is None or not line: # Skip empty lines
				continue
			ret = do_curl_call(line, StoreOps.ADD)
			if ret is False:
				debug("WARNING : Handle Existing Directory : Failed to add existing file '"+line+"' in '"+fsdir+"!")
				finalret = False

	else:
		debug("WARNING : Handle Existing Directory : Failed to find any existing files in '"+fsdir+"! This is NOT an error if the directory is actually empty.")

	return finalret


'''
When a directory is moved i.e renamed, this function handles keeping the 
Object Store DB in sync with the move operation for this directory.

When a directory is moved/renamed, inotify sends the MOVED_FROM event
for the old/previous name of the directory, and a MOVED_TO event for the
new name of the directory i.e the destination.

E.g. if DIRA is moved to DIRB using the command "mv DIRA DIRB", the 
following inotify events are sent:
	For DIRA : MOVED_FROM event
	For DIRB : MOVED_TO event

This function is called when an inotify MOVED_FROM event is triggered for a
particular directory on the ILIO file system.

This involves removing the object store objects which reference files which
were contained in the directory before it was moved.

Handling the addition of files in the destination directory to which the 
directory was moved is handled by the "add_existing_fs_dir_contents_to_objstore()"
function(), which is called on a MOVED_TO event.

Parameters:
	dirname : The old (source) path of the directory before it was moved.

Returns:
	True : successfully handled the MOVED_FROM event for the old directory
			name by deleting the objects in the object store which were 
			referenced in the directory before it was moved.
	False : failed to remove objects in the old directory. If this happens
			on a non-empty directory, the object store will not be perfectly
			in sync with the changes on the DedupFS.

'''
def handle_dir_movedfrom(dirname):
	tfname = ""
	finalret = True
	if dirname is None or not dirname:
		debug("ERROR : Handle dir moved : received null or empty dir name, cannot continue this operation.")
		return False

	try:
		# Convert the FS dir name into an object store path
		objdirname = convert_fs_path_to_objstore_path(dirname)
		if objdirname is None or not objdirname:
			debug("ERROR : Handle dir moved : Convert FS dir path to Object Store Path FAILED - received null or empty converted path, cannot continue this operation.")
			return False

		# Create a tempfile
		tempfile = TF.NamedTemporaryFile(delete=False)
		tempfile.close()
		tfname = tempfile.name

		# Do HTML get on bucket root (USX) and put resulting XML in tempfile
		bucket_path_http = '"http://'+MAIN_IP+':'+OBJECTSTORE_HTTP_PORT+'/'+DEDUP_BUCKET_NAME+'"'
		curl_cmd = "curl -o "+tfname+" -f --request GET "+bucket_path_http
		ret,_ = runcmd(curl_cmd) 
		if ret != 0:
			debug("ERROR : Handle directory move : Failed to get data in directory before it was moved.")
			return finalret

		# parse tempfile in ET tree
		if not os.path.isfile(tfname):
			debug("ERROR : Handle directory move : Failed to READ data in directory before it was moved.")
			return finalret
		tree = ET.parse(tfname)
		if tree is None:
			debug("ERROR : Handle directory move : Failed to PARSE data in directory before it was moved.")
			os.remove(tfname)
			return finalret

		# get root of tree
		root = tree.getroot()
		if root is None:
			debug("ERROR : Handle directory move : Failed to get ROOT of data in directory before it was moved.")
			os.remove(tfname)
			return finalret

		# process elements in root of tree to find all entries with dirname, and issue curl DELETE on them
		for child in root:
			if "Contents" in child.tag:
				for gch in child:
					if 'Key' in gch.tag:
						# gch.text will contain the fpath
						file_in_dir = gch.text
						if objdirname in file_in_dir:
							# Convert the Object Store Path into an FS path
							fsfile = convert_objstore_path_to_fs_path(file_in_dir)
							if fsfile is None or not fsfile:
								debug("ERROR : Handle dir moved : Found Object Store item referenced in dir move op, but could not convert it to an FS path. Ignoring this item! Your Object Store may be stale until this program is restarted!")
								continue
							# Remove the dang thing 
							ret = do_curl_call(fsfile, StoreOps.DELETE)
							if not ret:
								debug("ERROR : Handle directory move : Failed to handle item '"+file_in_dir+"' from directory move event; you may have stale data in your Object Store.")
								finalret = False



	except:
		debug("ERROR : Handle directory move : Exception handling directory move; you may have stale data in your Object Store.")
		finalret = False

	finally:
		if tfname and os.path.isfile(tfname):
			try:
				os.remove(tfname)
			except OSError:
				debug("ERROR : Handle directory move : Failed to remove temporary data used to parse moved directory information")
		return finalret


'''
Handles the different inotify events received for the entities in the DedupFS
and takes care of performing the appropriate action (Add/Delete) on the Object
Store DB to keep the Object Store DB entities in sync with the entities on the
DedupFS.

Since the Object Store has no concept of directories, this function mostly
concerns itself with handling inotify events received for individual files.

The only directory events which we really care about are MOVED_FROM and
MOVED_TO. Other directory events are received either for empty directories
which we do not care about, or other events on non-empty directories generate
individual events on the files contained in those directories.

FS entities and the corresponding inotify event received for that FS entity 
are passed to this function in the following format:
	<FULL_FS_PATH_OF_FILE_OR_DIRECTORY>:<EVENTNAME>

A mapping between the inotify event name and the Object Store operation to
be performed for this particular inotify event is maintained in the 
"storeops_dict" global dictionary.

Parameters:
	line : A line containing the full path to the FS entity and the inotify 
			event received for that FS entity in the format mentioned above.

Returns:
	True : Received event was successfully handled
	False : There was an error handling the event.
'''
def objstore_process_watch(line):
	if line is None or not line:
		debug("ERROR : Object Store Process Watch : data received to process is null or empty, nothing to process!")
		return False

	# Don't need to do anything with directories, since littleS3 has no
	# concept of directories. We only operate on files.
	# The exception to this are the directory move operations when a
	# directory is renamed. 
	# For directory create events, if the directory is empty then
	# there is no need to do anything.
	# For directory delete events, if the deleted directory contains
	# files, then we get individual delete notifications for those files
	# too, so we handle it as a file op. 
	line = line.strip()
	fname,watchop = line.split(':')
	if fname is None or watchop is None or not fname or not watchop:
		debug("ERROR : Object Store Process Watch : data received to process is not in the correct format, cannot process! Data received was: '"+line+"'")
		return False

	watchop = watchop.upper() # For sanity, though it should be caps to begin with
	# The only directory operation we need to track is MOVED_FROM
	if "ISDIR" in watchop:
		if "MOVED_FROM" in watchop:
			ret = handle_dir_movedfrom(fname)
			return ret
		elif "MOVED_TO" in watchop:
			ret = add_existing_fs_dir_contents_to_objstore(fname)
			return ret
		else: # Don't care about other directory operations
			return True

	# If we got here, it is non-directory operation
	# For safety, split watchop on ',' and take first part
	if ',' in watchop:
		wop,ign = watchop.split(',')
	else:
		wop = watchop

	if wop in storeops_dict:
		sop = storeops_dict[wop]
		return do_curl_call(fname, sop)
	else: # NOT A VALID STORE OP
		return True

	# Should not get here!
	return False



	
'''
This function is the primary point from which we start monitoring changes
to the DedupFS. This function is what is called by the main entry point to
this program.

It first handles all existing files in the DedupFS, by adding them to the
Object Store - thus getting the Object Store in sync with the existing 
contents of the DedupFS.

It then spawns the "inotifywait" system command in daemon and recursive 
mode on the main DedupFS mount point, watches the inotify events received
for the DedupFS, and handles each event as it is received.

This function basically listens for inotify events indefinitely, until this
whole program receives one of the termination signals defined in the 
"signal_handler()" handler above. So this functions effectively listens
forever until the program is killed.

This function performs a system exit if it encounters errors - if such a
condition is encountered, this program may not shut down cleanly.

Parameters:
	dedup_dir : The mount point of the DedupFS

Returns: 
	Nothing, but exits with non-zero return code on errors

'''
def watch_dedup_dir(dedup_dir):
	global INOTIFYWAIT_PROCESS
	if dedup_dir is None or not dedup_dir:
		debug("ERROR : No Dedup Directory was specified to watch, exiting! You will NOT get USX Object Store updates.")
		sys.exit(2)

	try:
		# Add existing data in DedupFS to Object Store
		# Don't need to do anything with directories, since littleS3 has no
		# concept of directories. We only operate on files
		debug("INFO : Object Server handler : Now handling existing files!")
		add_existing_fs_dir_contents_to_objstore(dedup_dir)

		# Establish watches for new data
		debug("INFO : Object Server handler : Starting handling changes to DedupFS. Object Server service will continue until this program is terminated ")

		cmd = "inotifywait -q -r -m --format %w%f:%e -e create -e modify -e move -e delete "+dedup_dir

		'''
		Use Popen WITHOUT communicate(), since we want realtime data
		Approach taken from:
		http://stackoverflow.com/questions/2715847/python-read-streaming-input-from-subprocess-communicate/17698359#17698359
		'''
		INOTIFYWAIT_PROCESS = Popen([cmd], shell=True, stdout=PIPE, bufsize=1)
		if INOTIFYWAIT_PROCESS is None:
			debug("ERROR : Unable to start directory watch process on Dedup Directory '"+dedup_dir+"', exiting! You will NOT get USX Object Store updates.")
			sys.exit(3)

		for line in iter(INOTIFYWAIT_PROCESS.stdout.readline, b''):
			if line is None or not line: # Skip empty input lines
				continue
			objstore_process_watch(line)
		INOTIFYWAIT_PROCESS.communicate() # close p.stdout, wait for the subprocess to exit
	except (OSError, ValueError, CalledProcessError) as e:
		debug("ERROR : Exception raised! Unable to start directory watch process on Dedup Directory '"+dedup_dir+"', exiting! You will NOT get USX Object Store updates.")
		sys.exit(4)


'''
Gets the mount of the DedupFS, if it is mounted.

Parameters:
	None

Returns:
	String containing mount point of DedupFS if it is mounted
	Empty string if DedupFS is not mounted, or on errors
'''
def get_dedup_dir():
	cmd = "mount | grep dedup | grep -v grep | cut -d' ' -f3"
	ret, msg = runcmd(cmd)
	if ret != 0:
		debug("ERROR : Could not find Dedup directory mountpoint")
		return ""

	if msg is None or not msg:
		debug("ERROR : Searching for Dedup directory mountpoint returned nothing")
		return ""

	# Remove newline from msg
	msg = msg.strip()
	return msg



########## MAIN ###############
'''
Here is the entry point to this program
'''

### Set Signal Handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)

### Get the dedup Directory
ddir = get_dedup_dir()
debug("Dedup DIR found is: '"+ddir+"'")
if ddir is None or not ddir:
	debug("ERROR : Could not determine dedupFS mountpoint on this ILIO, cannot continue! You WILL NOT HAVE Object Store capabilities on this ILIO! Exiting!")
	sys.exit(1)
# Strip trailing '/' from mountpoint, if it exists
if ddir[-1] == '/':
	ddir = ddir[:-1]
# Set global
DEDUPFS_MOUNTPOINT = ddir
debug("INFO: Setting global DedupFS mount point to: "+DEDUPFS_MOUNTPOINT)

### Get the IP Address for this ILIO
ipaddr = get_first_valid_ipaddress()
if ipaddr is None or not ipaddr:
	debug("ERROR : Failed to get valid IP Address!  You WILL NOT HAVE Object Store capabilities on this ILIO! Exiting!")
	sys.exit(5)
# Set global
MAIN_IP = ipaddr
debug("INFO : Using IP Address "+MAIN_IP)


### Start the Object Store Service by starting littleS3
if not start_objectstore_server():
	debug("ERROR : Could not start Object Store Service. You WILL NOT HAVE Object Store capabilities on this ILIO! EXITING!")
	sys.exit(6)

### Watch the dedup directory. This will never exit until the inotifywait
### process is killed, or until there is some sort of exception.
watch_dedup_dir(ddir)

### Should never get here in the normal course of events
debug("WARNING : Reached Exit point for USX Object Store watcher/updater. This program will now exit.")
sys.exit(100)


