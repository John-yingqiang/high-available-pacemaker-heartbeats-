#! /bin/bash
##############################################################################
#
# start-stop-usxobjstore.sh - Start, stop/kill, or restart the USX Object
# store services.
#
# Copyright (c) 2014 Atlantis Computing Inc.
# Author: Kartikeya Iyer (kartik@atlantiscomputing.com)
#
##############################################################################

##### Some globals that we use #####

OBJSTORE_SERVICE_PFN="/opt/milio/atlas/objectstore/usxobjstore"
SRV_PYC_EXT=".pyc"
SRV_PY_EXT=".py"
LOGFILE="/var/log/usx-milio-atlas.log"
PIDFILE="/var/run/usxobjstore.pid"
SERVICE_PYC="${OBJSTORE_SERVICE_PFN}${SRV_PYC_EXT}"
SERVICE_PY="${OBJSTORE_SERVICE_PFN}${SRV_PY_EXT}"
OBJSTORE_SERVICE=""
SERVICE_PID=""

##### END : GLOBALS #####


#
# Log messages to the log file specified in the LOGFILE global variable
#
logtofile()
{
	KDATE=`date`
	MSG="${KDATE} : $1"
	echo "${MSG}"
	echo "${MSG}" >> ${LOGFILE}
}

#
# Print the error message passed as a parameter, and exit this script
# with a return code of 1.
#
die()
{
	logtofile "==== ERROR Starting USX Object Store Service ==="
	logtofile "$1"
	logtofile "ABORT: Could not start USX Object Store Program"
	exit 1
}


#
# Determines the full filename (including extension) of the Object Store
# Service program.
#
# It first checks to see if there is a compiled python script (.pyc)
# available, and if it is then it sets the OBJSTORE_SERVICE global variable
# to the path to the .pyc.
#
# If a .pyc does not exist, it then looks for a .py. If found, it sets
# OBJSTORE_SERVICE to the full path to the .py file
#
# If neither a .pyc nor a .py script was found, it exits this script
# with a return code of 1 by calling the die() function with an appropropriate
# error message.
#
find_service()
{
	OBJSTORE_SERVICE=""
	if [[ ! -f "${SERVICE_PYC}" ]];then
		if [[ ! -f "${SERVICE_PY}" ]];then
			die "Failed to find a valid Object Store service, exiting with error. You will not have Object Store capabilities on this ILIO."
		else
			OBJSTORE_SERVICE="${SERVICE_PY}"
		fi
	else
		OBJSTORE_SERVICE="${SERVICE_PYC}"
	fi
	if [[ -z "${OBJSTORE_SERVICE}" ]];then
		die "Failed to find a valid Object Store service, exiting with error. You will not have Object Store capabilities on this ILIO."
	fi
}


#
# Checks if the Object Store Service program is running, and gets its PID
# (Process ID) if it is. It then sets the global variable SERVICE_PID to
# the value of the PID that it determined.
# 
# If it failed to find a PID for a running Object Store Service process,
# or if there is an error, it sets the SERVICE_PID global variable to an
# empty string. Caller must check for empty string.
#
# *** WARNING: ***
# Note that the behavior of this function is undefined if there is MORE THAN
# ONE Object Store Service process running. This is a condition that should
# never occur during normal running of the Object Store Service
#
get_service_pid_if_running()
{

	# Check that we have a valid service binary
	find_service

	### If we got here, we have the name of a service binary
	SERVICE_PID=""

	# Check if service is running
	ps aux | grep ${OBJSTORE_SERVICE} | grep -v grep
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		SERVICE_PID=""
		return
	fi

	# If we got here, we determined that the service is running,
	# so get the PID of the running Object Store Service
	SERVICE_PID=`ps aux | grep ${OBJSTORE_SERVICE} | grep -v grep | tr [:space:] ' ' | tr -s ' ' | cut -d' ' -f2`
}


#
# Starts the Object Store Service program as a background process.
#
start_service()
{
	# Find the service to run - whether it is a .py or a .pyc
	find_service

	#### If we got here, we have a valid service binary name

	# Check if the service is already running
	# return without doing anything if service is already running
	get_service_pid_if_running
	if [[ ! -z ${SERVICE_PID} ]];then
		logtofile "INFO : Start Object Store Service : Exiting without starting service since we determined that the Object Store service is already running with PID ${SERVICE_PID}"
		return
	fi


	# If we got here, we need to Start the service
	logtofile "INFO : Start Object Store Service : Starting USX Object Store service..."
	python "${OBJSTORE_SERVICE}" &
	SPID_RET=$!
	sleep 2

	# Verify that the service is running
	get_service_pid_if_running
	if [[ ! -z ${SERVICE_PID} ]];then
		logtofile "INFO : Start Object Store Service : Object Store Service successfully started with PID ${SERVICE_PID}"
		# Verify running PID same as PID returned by start call. This is
		# not needed, strictly speaking, but is an icing-on-the-cake check
		if [[ "${SERVICE_PID}" != "${SPID_RET}" ]];then
			logtofile "WARNING : Start Object Store Service : Determined that PID of running process (${SERVICE_PID} is different from PID returned by start call (${SPID_RET})"
		fi
	else
		logtofile "ERROR : Start Object Store Service : Unable to verify that the Object Store Service was started."
	fi

}


# 
# Stops the Object Store Service program if it is running.
#
kill_service()
{
	# Find the service to run - whether it is a .py or a .pyc
	find_service

	#### If we got here, we have a valid service binary name

	# Check if the service is running
	# Return without doing anything if the service is not running.
	get_service_pid_if_running
	if [[ -z ${SERVICE_PID} ]];then
		logtofile "INFO : Kill Object Store Service : Object Store service is not running, nothing to do :-)"
		return	
	fi


	# If we got here, we need to kill the service
	OLD_PID="${SERVICE_PID}"
	logtofile "INFO : Kill Object Store Service : Stopping USX Object Store service with PID ${SERVICE_PID}"
	
	# Kill the service
	kill -SIGTERM "${OLD_PID}"
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		logtofile "WARNING : Kill Object Store Service : Kill command returned non-zero return code ${RET}"
	fi

	# Verify that service is not running
	# Sleep a little to give it time to be stopped
	sleep 6 
	get_service_pid_if_running
	if [[ -z ${SERVICE_PID} ]];then
		logtofile "INFO : Kill Object Store Service : Object Store Service with PID ${OLD_PID} was successfully stopped."
	else
		logtofile "WARNING : Kill Object Store Service : Unable to verify that the Object Store Service was stopped; there still seems to be a running Object Store Service process with PID ${SERVICE_PID}"
	fi

}


#
# Helpful message
#
usage()
{
	progname=`basename $0`
	cat <<EOF
${progname} - Start, stop, or restart the Object Store service

Usage: $progname [-s | -k | -r | -h]

Options:
-h	: 	This help screen.

-k 	: 	Stop/Kill a running Object Store service.
		If no running Object Store service process was found, it will exit
		with a success return code.

-r 	: 	Restart the Object Store service.
		If the Object Store service is already running, it will attempt to 
		stop the running Object Store service before starting a new instance.

-s 	: 	Start the Object Store service.
	 	If an existing Object Store service is already running, this program
		will exit with a success return code without starting another instance 
		of the Object Store service.

Log File:
		This program logs its activity to ${LOGFILE}

Return Codes:
0	: 	Successfully performed the requested action, or no action was
		required.
non-0	:	An error was encountered performing the requested action.
		user should check the log file for details.
EOF
}


########### MAIN #############
progname=`basename $0`

# Check if we were not passed any command line parameters
if [[ $# -eq 0 ]];then
	usage
	exit 0
fi

# Get the command line options passed to this script.
while getopts ":skrh" opt;do
	case $opt in
		(h) 
			### Help screen requested
			usage
			exit 0
			;;

		(s) 
			### Object Store Service start requested
			start_service
			get_service_pid_if_running
			if [[ -z ${SERVICE_PID} ]];then
				die "${progname} : ERROR : Main : Start : Service start was requested, but unable to verify that the Object Store Service is running."
			else
				logtofile "${progname} : INFO : Main : Start : Object Store Service is running with PID ${SERVICE_PID}."
				exit 0
			fi
			;;

		(k)
			### Object Store service stop/kill requested
			kill_service
			get_service_pid_if_running
			if [[ ! -z ${SERVICE_PID} ]];then
				die "${progname} : ERROR : Main : Stop : Service stop/kill was requested, but found a running Object Store Service process with PID ${SERVICE_PID}"
			else
				logtofile "${progname} : INFO : Main : Stop : Object Store Service was stopped successfully."
				exit 0
			fi
			;;

		(r)
			### Object Store Service restart requested

			# Kill any running Object Store Service process
			kill_service
			get_service_pid_if_running
			if [[ ! -z ${SERVICE_PID} ]];then
				# We have a running Object Store Service process. This means
				# that either the kill failed or there was more than 1 process
				# running. Exit this script with an error.
				die "${progname} : ERROR : Main : Restart : Tried to kill running service with PID ${SERVICE_PID}, but unable to stop it."
			fi

			# Now start the Object Store Service
			start_service
			get_service_pid_if_running
			if [[ -z ${SERVICE_PID} ]];then
				# We didn't get a PID for the Object Store service
				die "${progname} : ERROR : Main : Restart : Service start was requested, but unable to verify that the Object Store Service is running."
			else
				logtofile "${progname} : INFO : Main : Restart : Object Store Service is running with PID ${SERVICE_PID}."
				exit 0
			fi
			;;

		(*|?)
			# Invalid option passed in. Show usage screen and exit with
			# return code 127.
			usage
			logtofile "${progname} : ERROR : Main : Invalid option specified. Option must be one of the following: -h, -s, -r, -k"
			exit 127
			;;
	esac
done
shift $(($OPTIND - 1))

# Handle case where no getopts-style options were passed in
usage
logtofile "${progname} : ERROR : Main : Invalid option specified. Option must be one of the following: -h, -s, -r, -k"
exit 127

	
