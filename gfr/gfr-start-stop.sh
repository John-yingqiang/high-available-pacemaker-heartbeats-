#! /bin/bash
##############################################################################
#
#                  Global File Replication - GFR
# 
# This script contains the implementation of the USX Global File Replication
# (GFR) feature. This is basically a near-realtime sync of the DedupFS on
# on the source ILIO (the local machine) onto the DedupFS of another ILIO
# (the destination ILIO).
#
# To prevent a circular GFR sync between two ILIOs, the program will not
# start the GFR service if it detects that the destination ILIO has a GFR
# configuration which points to this ILIO.
#
# This program logs to: /var/log/gfr.log (defined in the LOGFILE global).
# 
# See "usage()" below, or run this script without any command line
# parameters, to know how to use/invoke this script.
#
# Author: Kartikeya Iyer (kartik@atlantiscomputing.com)
#
# Copyright (c) 2014 Atlantis Computing Inc. All rights reserved.
#
##############################################################################

############# GLOBAL VARIABLES ################
#### Globals that are static
CONFFILE="/etc/ilio/gfr.py"
LOGFILE="/var/log/gfr.log"
SSH_PREFIX="ssh poweruser@"
SCRIPT_VERSION="1.1"
GFR_VER="0.2.1"
GFR_BIN="inosync"

#### Globals that will be populated at runtime
REMOTE_IP=""
REMOTE_DEDUP_MNT=""
LOCAL_DEDUP_MNT=""
MYDIR=""
MYHOSTNAME=""
MY_SYNC_DIR_ON_REMOTE=""
############# END : GLOBAL VARIABLES ################


#
# Log messages to the log file specified in the LOGFILE global variable
#
logtofile()
{
	KDATE=`date`
	MSG="${KDATE} : GFR : $1"
	echo "${MSG}"
	echo "${MSG}" >> ${LOGFILE}
}


#
# Print the error message passed as a parameter, and exit this script
# with a return code of 1.
#
die()
{
	logtofile "***** ERROR in start/stop GFR Service *****"
	logtofile "$1"
	logtofile "ABORT: Could not start/stop GFR Service"
	logtofile "========== END : GFR Service : ERRORS Encountered  =========="
	exit 1
}


#
# Get the full path to the directory where this script lives
# Also get the hostname for this ILIO
#
get_my_dir()
{
	SOURCE="${BASH_SOURCE[0]}"
	while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
		DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
		SOURCE="$(readlink "$SOURCE")"
		[[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
	done
	MYDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
	if [[ -z ${MYDIR} ]];then
		die "Failed to get full directory path of this script!"
	fi
	logtofile "INFO : This program lives at: ${MYDIR}"
	
	# Now get the hostname of this ILIO
	MYHOSTNAME=`hostname`
	if [[ -z ${HOSTNAME} ]];then
		# Failed to get hostname. Does this ILIO have a valid IPv4 address assigned to eth0?
		logtofile "WARNING : Failed to get hostname assigned to this ILIO, attempting to get IP address of eth0 if assigned..."
		ifconfig eth0 | grep "inet addr" | cut -d':' -f2 | tr -s ' ' | cut -d' ' -f1 | grep -G "[[:digit:]]*\.[[:digit:]]*\.[[:digit:]]*\.[[:digit:]]*"
		RET=$?
		if [[ ${RET} -ne 0 ]];then
			# Failed to get an IP address for eth0
			logtofile "WARNING : Failed to get hostname AND eth0 IPv4 IP Address assigned to this ILIO, hostname qualifier will be blank..."
			return
		fi

		# If we got here, the above command succeeded and we have a valid IPv4
		# IP Address for eth0. Let's the the same command, but this time get
		# the actual output of the command rather than its return value.
		MYHOSTNAME=`ifconfig eth0 | grep "inet addr" | cut -d':' -f2 | tr -s ' ' | cut -d' ' -f1 | grep -G "[[:digit:]]*\.[[:digit:]]*\.[[:digit:]]*\.[[:digit:]]*"`
		if [[ -z ${MYHOSTNAME} ]];then
			logtofile "WARNING : Failed to get hostname AND eth0 IPv4 IP Address assigned to this ILIO, hostname qualifier will be blank..."
			return
		fi

		logtofile "INFO : Using IP Address for hostname. IP Address used is: ${MYHOSTNAME}"

	fi

	if [[ -z ${MYHOSTNAME} ]];then
		logtofile "WARNING : Failed to get hostname assigned to this ILIO, and also failed to get IP address of eth0 for this ILIO."
		return
	fi

	logtofile "INFO : Determined local hostname (or IPv4 IP Address of eth0): ${MYHOSTNAME}"
	MY_SYNC_DIR_ON_REMOTE="GFR_SRC_${HOSTNAME}"
	logtofile "INFO : Files in the DedupFS of this ILIO will be synced to the following directory on the remote ILIO: ${MY_SYNC_DIR_ON_REMOTE}"

}


#
#
# Check that the Remote ILIO is pingable and available to perform commands.
# Also check that the IP address given is not our own.
# Also check if we could potentially end up in a circular sync situation,
# and bail if we find that the remote ILIO is set up to use this ILIO as
# the GFR sync destination.
#
check_remote_host()
{
	if [[ -z ${REMOTE_IP} ]];then
		die "ERROR : Check remote host alive : Remote IP to check is empty!"
	fi

	logtofile "INFO : Check remote host alive : Checking whether ${REMOTE_IP} is the local machine..."
	ifconfig | grep ${REMOTE_IP} | grep -v grep > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -eq 0 ]];then
		die "ERROR : Check remote host alive : ${REMOTE_IP} is this (local) machine. Cannot GFR-sync onto the same ILIO."
	else
		logtofile "INFO : Check remote host alive : ${REMOTE_IP} is not the local machine. OK."
	fi

	logtofile "INFO : Check remote host alive : Checking whether ${REMOTE_IP} is reachable..."
	# ping it with 4 packets
	ping -c 4 ${REMOTE_IP} > /dev/null 2>&1 || die "ERROR : Check remote host alive : Failed to verify that remote host ${REMOTE_IP} is alive and reachable. It may be down, or on an unreachable subnet."

	logtofile "INFO : Check remote host alive : Checking whether ${REMOTE_IP} is available..."
	# Run a test command on it via ssh
	${SSH_PREFIX}${REMOTE_IP} mount > /dev/null 2>&1 || die "ERROR : Check remote host alive : Failed to verify that ${REMOTE_IP} is available!"
	# If we got here, it was pingable, and hence is reachable.
	logtofile "INFO : Check remote host alive : Successfully verified that ${REMOTE_IP} is reachable and available. Checking that it is not running GFR pointing to this machine."
	
	# Do the circular sync check on the remote ILIO. We enumerate all the 
	# local IP addresses, and check whether the remote ILIO has a GFR
	# config which contains one of these local IP addresses. If yes, then
	# we have a circular sync situation.
	for IPADDR in `ifconfig | grep "inet addr" | cut -d':' -f2 | tr -s ' ' | cut -d' ' -f1`;do
		# Ignore local loopback address
		if [[ ${IPADDR} == "127.0.0.1" ]];then
			continue
		fi
		RET=""
		logtofile "      Checking that the GFR service configuration (if any) on remote ${REMOTE_IP} does NOT point to ${IPADDR} on this ILIO..."
		${SSH_PREFIX}${REMOTE_IP} cat ${CONFFILE} | grep rpath | grep -v grep | grep ${IPADDR} 
		RET=$?
		if [[ ${RET} -eq 0 ]];then
			die "ERROR : Check Remote Host : Remote Host ${REMOTE_IP} has a GFR configuration which points to address ${IPADDR} on this ILIO. Cannot start GFR as this would set up a circular reference! Please run '`basename $0` -x' on ${REMOTE_IP} if you want this ILIO to use ${REMOTE_IP} as the GFR sync destination."
		fi
	done

}


#
# Get the local or remote DedupFS mount point.
#
# If a parameter is passed to this function, this parameter is assumed
# to be the IP address of the remote ILIO whose dedup mountpoint we want
# to get. 
#
# If no parameter is passed to this function, we assume that we want the
# local dedup mountpoint
#
get_dedupfs_mountpoint()
{
	CMD=""
	ISREMOTE=0
	BASECMD="mount | grep dedup | grep -v grep | head -n 1 | cut -d' ' -f3"
	if [[ ! -z $1 ]];then
		if [[ -z ${REMOTE_IP} ]];then
			die "ERROR : Get DedupFS mountpoint : Wanted to get Remote Dedup mountpoint but Remote IP is empty!"
		fi
		ISREMOTE=1
		CMD="${SSH_PREFIX}${REMOTE_IP} \"${BASECMD}\""
	else
		CMD="${BASECMD}"
	fi
	if [[ ${ISREMOTE} -eq 1 ]];then
		logtofile "INFO : Get DedupFS mountpoint : Getting remote Dedup mountpoint on ${REMOTE_IP}"
	else
		logtofile "INFO : Get DedupFS mountpoint : Getting local Dedup mountpoint"
	fi

	MNTPT=`/bin/bash -c "${CMD}"`
	logtofile "DEBUG : Get DedupFS mountpoint : First Dedup Mount Point returned from system = '${MNTPT}'"
	if [[ -z ${MNTPT} ]];then
		die "ERROR : Get DedupFS mountpoint : Failed to find a valid DedupFS mount point. Cannot continue!"
	fi
	if [[ ${ISREMOTE} -eq 1 ]];then
		REMOTE_DEDUP_MNT="${MNTPT}"
		logtofile "INFO : Get DedupFS mountpoint : Remote Dedup mountpoint on ${REMOTE_IP}: ${REMOTE_DEDUP_MNT}"
	else
		LOCAL_DEDUP_MNT="${MNTPT}"
		logtofile "INFO : Get DedupFS mountpoint : Local Dedup mountpoint: ${LOCAL_DEDUP_MNT}"
	fi

	# Create the remote sync destination directory if it does not exist
	logtofile "INFO : Get DedupFS mountpoint : Creating (if required) remote sync destination directory on ${REMOTE_IP} for syncing the contents of the Local Dedup mountpoint. The remote sync directory is: ${REMOTE_DEDUP_MNT}/${MY_SYNC_DIR_ON_REMOTE}"
	${SSH_PREFIX}${REMOTE_IP} mkdir -p "${REMOTE_DEDUP_MNT}/${MY_SYNC_DIR_ON_REMOTE}"
}


#
# Write the config file to be used for the GFR feature.
#
write_conf()
{
	# Remove any existing conf file
	if [[ -f "${CONFFILE}" ]];then
		logtofile "WARNING : Write config : Previous config file exists, attempting to remove it before writing new configuration..."
		rm -f "${CONFFILE}"
	fi
	
	# Check whether file exists again, just to make sure
	if [[ -f "${CONFFILE}" ]];then
		logtofile "WARNING : Write Config : Previous config file STILL exists after attempting to remove it before writing new configuration! Will attempt to overwrite existing config with data from this run."
	fi
	
	logtofile "INFO : Write config : Writing configuration file..."

	# We write it as a bash here-document
	cat << EOM > "${CONFFILE}"
wpath = "${LOCAL_DEDUP_MNT}"


# exclude list for sync daemon
rexcludes = [
      "${LOCAL_DEDUP_MNT}/lost+found",
]

# rpaths has one-to-one correspondence with wpaths for syncing multiple
# directories
rpath = "${REMOTE_DEDUP_MNT}"

# remote locations to which we sync
rnodes = [
      "poweruser@${REMOTE_IP}:"+rpath+"/GFR_SRC_${MYHOSTNAME}",
]

# extra, raw parameters to sync daemon
extra = "--rsh=ssh -a"

# limit remote sync speed (in KB/s, 0 = no limit)
rspeed = 0

# event mask (only sync on these events)
emask = [
     "IN_CLOSE_WRITE",
     "IN_CREATE",
     "IN_DELETE",
     "IN_MOVED_FROM",
     "IN_MOVED_TO",
]

# event delay in seconds (this prevents huge
# amounts of syncs, but dicreases the
# realtime side of things)
edelay = 2

EOM
##### END : Configuration file here-document

	# Check whether file exists, just to make sure
	if [[ ! -f "${CONFFILE}" ]];then
		die "ERROR : Write config : Failed to write new configuration file!"
	else
		logtofile "INFO : Write config : Successfully wrote configuration file."
	fi
	
}


#
# Check whether the GFR service daemon is running.
#
# Returns:
#	0 : Daemon is running
# non-zero : Daemon not running
#
gfr_running()
{
	ps aux | grep ${GFR_BIN} | grep -v grep > /dev/null 2>&1
	RET=$?
	return ${RET}
}


#
# Start the GFR daemon process and verify that it is running.
#
start_gfr()
{
	logtofile "INFO : Start : Starting GFR Services..."
	
	# Start it
	${GFR_BIN} -c ${CONFFILE} -d > /dev/null 2>&1 || die "ERROR : Start : Failed to start GFR daemon program!"
	sleep 3
	# Verify that it's running
	gfr_running
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		die "ERROR : Start : Failed to verify that GFR daemon was successfully started!"
	else
		logtofile "INFO : Start : GFR Daemon program has been started successfully."
	fi

}


#
# Check whether we have the prerequisites installed and that they're the
# correct version. 
# If the prerequisites are not installed, or are the wrong version, install
# the version that we require.
#
check_prerequisites()
{
	logtofile "INFO : Check prerequisites : Checking prerequisites required to run GFR..."
	which ${GFR_BIN} > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -eq 0 ]];then
		# Check that it actually runs
		logtofile "INFO : Check prerequisites : Prerequisites exist, checking required version to run GFR..."
		INVER=`${GFR_BIN} --version | cut -d' ' -f2`
		if [[ ${INVER} == ${GFR_VER} ]];then
			logtofile "INFO : Check prerequisites : Prerequisite version is correct, nothing further to check."
			return
		fi
	fi

	# If we got here, then either we have no prerequisites, or the version is wrong
	logtofile "INFO : Check prerequisites : Prerequisites are either not installed or the wrong version, attempting to install prerequisites... "
	get_my_dir
	# Install the prerequisite debs
	dpkg -i ${MYDIR}/*.deb > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		die "ERROR : Check prerequisites : Failed to install prerequisites!"
	fi
	sleep 3
	# Check version again, just to be safe
	INVER2=`${GFR_BIN} --version | cut -d' ' -f2`
	echo "INVER2: ${INVER2}" #TODO : Remove from production
	if [[ ${INVER2} != ${GFR_VER} ]];then
		die "ERROR : Check prerequisites : Failed Prerequisite version check after install, cannot continue!"
	fi

	# if we got here, everything checks out OK.
	logtofile "INFO : Check prerequisites : Prerequisites installed successfully."

}


#
# Start the GFR service daemon and make sure that it is running.
#
#
do_start()
{
	# Check whether we're already running. If we are, we need to stop ourselves
	gfr_running
	GRET=$?
	if [[ ${RET} -eq 0 ]];then
		logtofile "INFO : Start GFR : GFR daemon is already running, attempting to stop it..."
		do_stop
	fi

	# Now check prerequisites
	logtofile "INFO : Start GFR : Checking prerequisites..."
	check_prerequisites

	# Now get the remote DedupFS mountpoint
	logtofile "INFO : Start GFR : Verifying that ILIO at ${REMOTE_IP} is reachable..."
	check_remote_host
	logtofile "INFO : Start GFR : Getting remote DedupFS mountpoint on remote ILIO at: ${REMOTE_IP}"
	get_dedupfs_mountpoint "${REMOTE_IP}"

	# Get the local DedupFS mountpoint
	logtofile "INFO : Start GFR : Getting local DedupFS mountpoint..."
	get_dedupfs_mountpoint

	# Write the config file
	logtofile "INFO : Start GFR : Writing GFR configuration..."
	write_conf

	# Start the daemon
	logtofile "INFO : Start GFR : Starting GFR daemon service..."
	start_gfr

	# If we got here, GFR is successfully running (the start check is in
	# start_gfr() )
	logtofile "INFO : Start GFR : Local Dedup mountpoint...: ${LOCAL_DEDUP_MNT}"
	logtofile "INFO : Start GFR : Local ILIO Identifier....: ${MYHOSTNAME}"
	logtofile "INFO : Start GFR : Remote ILIO..............: ${REMOTE_IP}"
	logtofile "INFO : Start GFR : Remote sync directory....: ${REMOTE_DEDUP_MNT}/${MY_SYNC_DIR_ON_REMOTE}"
	logtofile "INFO : Start GFR : GFR service daemon has been started successfully."
}


#
# Stop the GFR daemon if it is running.
# Also remove the GFR config file.
#
do_stop()
{

	# Remove the GFR config file.
	if [[ -f ${CONFFILE} ]];then
		logtofile "INFO : Stop GFR : Removing GFR config file..."
		rm -f ${CONFFILE} > /dev/null 2>&1
		if [[ -f ${CONFFILE} ]];then
			die "ERROR : Stop GFR : Failed to remove configuration file; it still exists. Aborting Stop!"
		fi
	fi

	# Check if GFR daemon is running.
	gfr_running
	GRET=$?
	if [[ ${GRET} -ne 0 ]];then
		logtofile "INFO : Stop GFR : GFR service daemon is not running, nothing to stop."
		return
	fi

	# If we got here, we need to stop the GFR daemon
	logtofile "INFO : Stop GFR : GFR service daemon is running, attempting to stop it..."
	killall -9 ${GFR_BIN}
	sleep 3
	
	# Check that it's not running.
	logtofile "INFO : Stop GFR : Verifying that the GFR service daemon was stopped successfully..."
	gfr_running
	GRET=$?
	if [[ ${GRET} -eq 0 ]];then
		die "ERROR : Stop GFR : GFR service daemon is still running despite trying to stop it. Stop was unsuccessful."
	fi

	# If we got here, it was stopped correctly.
	logtofile "INFO : Stop GFR : The GFR service daemon was stopped successfully."

}


do_version()
{
	echo "${SCRIPT_VERSION}"
}



#
# Helpful banner for the clueless
#
usage()
{
	MYNAME=`basename $0`
	# We use a here-doc for the help text
	cat <<EOM
${MYNAME} - Start or stop the Global File Replicator (GFR) service daemon.
The GFR service allows near realtime synchronization of the DedupFS contents
of this ILIO (the source ILIO) to the DedupFS on a remote ILIO (the 
destination ILIO).

You cannot have a circular reference in GFR syncs, i.e a situation where
ILIO1 has ILIO2 as the GFR destination, and ILIO2 simultaneously has ILIO1
as the GFR destination. This program checks for such an occurrence, and 
will NOT start the GFR service on this ILIO if it detects that the remote
destination ILIO has its GFR configured with this ILIO as its destination.

This script logs to: ${LOGFILE}

Usage: ${MYNAME} [ -s <REMOTE_IP> | -x | -h | -v ]

Parameters:
-s <REMOTE_IP>  :   Start the GFR daemon using <REMOTE_IP> as the IPv4 
                    IP Address of the destination ILIO to which we want to
                    sync the DedupFS contents of this ILIO.

-x              :   Stop any running GFR daemon on this ILIO, and remove the
                    GFR configuration file.

-h              :   This help screen.

-v              :   Print version number and exit.

GFR Version: ${SCRIPT_VERSION}
EOM
}

######## MAIN ########

# Check that we got passed at least one command line parameter
if [[ -z $1 ]];then
	usage
	exit 1
fi

# start banner
logtofile "========== START : GFR Start/Stop Invoked ==========="
logtofile "GFR Version: ${SCRIPT_VERSION}"

# Get the directory where this script lives
get_my_dir

# Check prerequisites and install if required.
check_prerequisites

# Parse the command line parameters
while getopts ":s:xhv" OPTS; do
	case "${OPTS}" in
		s)
			REMOTE_IP=${OPTARG}
			if [[ -z ${REMOTE_IP} ]];then
				usage
				die "<REMOTE_IP> is a mandatory argument for the -s option."
			fi
			do_start
			logtofile "========== END : GFR START Invoked successfully ==========="
			exit 0
			;;
		x)
			do_stop
			logtofile "========== END : GFR STOP Invoked successfully ==========="
			exit 0
			;;
		v)
			logtofile "========== END : GFR Version check Invoked successfully ==========="
			do_version
			exit 0
			;;
		h)
			logtofile;logtofile "Usage information follows.";logtofile
			usage
			logtofile "========== END : GFR Help Invoked successfully ==========="
			exit 0
			;;
		*)
			usage
			die "Invalid option(s) on command line"
			;;
	esac
done
shift $((OPTIND-1))

# If we got here, we were maybe called with invalid options
usage
logtofile  "**** ERROR **** : Invalid option(s) on command line"
exit 2
