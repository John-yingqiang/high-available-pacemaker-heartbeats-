#! /bin/bash

######## GLOBALS #######
HOSTNAME=""
DEDUP_MNT=""
LOGFILE="/var/log/milio-atlas.log"
SMB_CONF_LOC="/etc/samba/smb.conf"
SMB_USER_GROUP="usxshare"
SMB_SHARE_NAME=""
######## END : GLOBALS #######


#
# Log messages to the log file specified in the LOGFILE global variable
#
logtofile()
{
        KDATE=`date`
        MSG="${KDATE} : CIFS Export : $1"
        echo "${MSG}"
        echo "${MSG}" >> ${LOGFILE}
}

#
# Print the error message passed as a parameter, and exit this script
# with a return code of 1.
#
die()
{
        logtofile "***** ERROR Starting CIFS Export Service *****"
        logtofile "$1"
        logtofile "ABORT: Could not start CIFS Export Program"
		logtofile "========== END : CIFS EXPORT : ERRORS Encountered  =========="
        exit 1
}




# Get hostname
get_hostname()
{
	HN=`hostname`
	logtofile "Hostname returned by system ='${HN}'"
	if [[ -z ${HN} ]];then
		die "Failed to determine a valid hostname. Cannot continue!"
	fi
	HOSTNAME="${HN}"
}


# Get DedupFS mount point
get_dedupfs_mountpoint()
{
	MNTPT=`mount | grep dedup | grep -v grep | head -n 1 | cut -d' ' -f3`
	logtofile "First Dedup Mount Point returned from system = '${MNTPT}'"
	if [[ -z ${MNTPT} ]];then
		die "Failed to find a valid DedupFS mount point. Cannot continue!"
	fi
	DEDUP_MNT="${MNTPT}"
}

# Set up user group that will have write permissions on the DedupFS SMB Share
setup_permissions()
{
	# Make sure that we have a valid DedupFS mountpoint
	if [[ -z "${DEDUP_MNT}" ]];then
		die "Setup Permissions : Could not find a valid DedupFS mount point. Cannot continue!"
	fi
	# Get existing groups and check for our group
	cat /etc/group | grep "${SMB_USER_GROUP}" | grep -v grep > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		logtofile "Setup Permissions : Group '${SMB_USER_GROUP}' does not exist, creating it..."
		groupadd ${SMB_USER_GROUP}
		RET2=$?
		if [[ ${RET2} -ne 0 ]];then
			die "Setup Permissions : Group '${SMB_USER_GROUP}' does not exist, and we FAILED to create it. Cannot continue!"
		fi
	fi
	## Check that group exists again, in case we had to create it
	cat /etc/group | grep "${SMB_USER_GROUP}" | grep -v grep > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		die "Setup Permissions : Group '${SMB_USER_GROUP}' does not exist, and it looks like we failed creating it. Cannot continue!"
	fi

	#### If we got here, then we have the correct group.

	# Change the group of the DedupFS to SMB_USER_GROUP
	# Even if it already belongs to this group, running
	# chgrp again does no harm, and returns 0.
	chgrp -h -R "${SMB_USER_GROUP}" "${DEDUP_MNT}"
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		logtofile "WARNING : Setup Permissions : Failed to set group permission for Share group, you may not have WRITE access to this share!"
	fi
}


# Create smb.conf
create_smb_conf()
{
	### Make sure that we have all the required data.
	if [[ -z "${HOSTNAME}" ]];then
		die "Create conf : Could not find a valid hostname. Cannot continue!"
	fi
	if [[ -z "${DEDUP_MNT}" ]];then
		die "Create conf : Could not find a valid DedupFS mount point. Cannot continue!"
	fi
	cat /etc/group | grep "${SMB_USER_GROUP}" | grep -v grep > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		die "Create conf : Could not find a valid USX Share group. Cannot continue!"
	fi

	### Backup any existing conf file. If a previous backup existed, it
	### will be overwritten
	if [[ -f "${SMB_CONF_LOC}" ]];then
		logtofile "Backing up existing conf file..."
		mv "${SMB_CONF_LOC}" "${SMB_CONF_LOC}.BACKUP" > /dev/null 2>&1
		RET=$?
		if [[ ${RET} -ne 0 ]];then
			logtofile "WARNING : Failed to back up existing conf file, existing file will be overwritten!"
		fi
	fi

	SMB_SHARE_NAME="${SMB_USER_GROUP}-`echo $DEDUP_MNT | sed 's/.*\///g'`"
	### Write the conf file. We write this as a here-document
	cat << EOF > "${SMB_CONF_LOC}"
[global]
netbios name = ${HOSTNAME}
#workgroup = WORKGROUP
security = user
map to guest = Bad User
#encrypt passwords = no
#smb passwd file = /etc/samba/smbpasswd
interfaces = *
acl group control = yes
inherit owner = yes
inherit permissions = yes
inherit acls = yes
create mask = 0777
force create mode = 0777
force directory mode = 0777
force unknown acl user = yes
guest account = root

### Below lines needed to stop the logs being flooded with spurious CUPS
### printer warnings. We don't want to connect to the local CUPS framework.
load printers = no
printing = bsd
printcap name = /dev/null
disable spoolss = yes

[${SMB_SHARE_NAME}]
comment = DedupFS CIFS Export on ${HOSTNAME}
path = ${DEDUP_MNT}
writeable = yes
public = yes
guest only = yes
guest ok = yes
#admin users = nobody
browsable = yes
locking = yes
force group = ${SMB_USER_GROUP}

EOF

	## Do a sanity check on the conf file
	cat "${SMB_CONF_LOC}" | grep "${HOSTNAME}" | grep -v grep > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		die "Failed to verify sanity of config file, cannot continue!"
	fi
	
}



# Start smb
start_smb()
{
	# Start/restart smbd
	service smbd restart > /dev/null 2>&1
	sleep 2

	# Check that it's running
	service smbd status | grep -i "start/running" | grep -v grep > /dev/null 2>&1
	RET=$?
	if [[ ${RET} -ne 0 ]];then
		die "WARNING : Could not verify that CIFS Export service is running properly! You may not have CIFS capabilities on this ILIO!"
	fi

}


############### MAIN ###################
logtofile "========== STARTING CIFS EXPORT =========="
logtofile "Getting hostname..."
get_hostname
logtofile "Using hostname: ${HOSTNAME}"

logtofile "Getting DedupFS mount point..."
get_dedupfs_mountpoint
logtofile "Using DedupFS Mountpoint: ${DEDUP_MNT}"

logtofile "Setting up required permissions prior to sharing..."
setup_permissions

logtofile "Creating config..."
create_smb_conf

logtofile "Starting CIFS/SMB service..."
start_smb

# If we got here, everything went OK.
logtofile "DedupFS is successfully shared via CIFS on this ILIO!"
logtofile "Windows share name: ${SMB_SHARE_NAME}"
logtofile "Exiting with success code :-)"
logtofile "========== END : CIFS EXPORT =========="
exit 0
