#!/usr/bin/python
#
# Copyright (c) 2014 Atlantis Computing Inc.
# All rights reserved.
#
# No part of this program can be used, copied, redistributed, reproduced,
# or disclosed in any form or by any means without prior express written
# permission of the copyright holder.
#
# This file implements the default configuration parameters of Teleport
# file copy service.
#

#
# Teleport Versions and the current version.
#
ATP_VERSION1 = "ATP_VER1"
ATP_VERSION = ATP_VERSION1

#
# Default path of job directories.
#
ATP_JODS_DIR = ".dedup_private/teleport"

#
# Minimum size(in bytes) a file must have to be teleported.
# Currently set to 1024*1024 bytes
#
ATP_MIN_FSIZE_BYTES = 1048576

#
# Extra options to pass to Teleport run sub-command.
#
ATP_RUN_OPTS = ""

#
# The program that is used to calculate checksums of files being send back
# and forth between the srouce and destination.
#
ATP_CS_PROG = "/usr/bin/sha1sum"

#
# The remote copy program.
#
ATP_RCP = "rsync"

#
# The remote copy program options.
#
# -z: Compress the data that is being sent over the wire.
#
ATP_RCP_OPTS = ""

#
# The remote user name.
#
ATP_RUSER = "poweruser"

#
# Truncate program to set the file size
#
ATP_TRUNCATE_CMD = "/usr/bin/truncate"

#
# Failure and success error codes passed to atp_helper at the end of Teleport.
#
ATP_SUCCESS = 0
ATP_ERROR = 1

#
# Extra options to be passed to Teleport programs.
#
ATP_MAPPER_OPTS = ""
ATP_REDIR_OPTS = ""
ATP_DATABROKER_OPTS = ""

#
# Log Level for the application. Valid values are:
#    o 'notset'
#    o 'debug'
#    o 'info'
#    o 'warning'
#    o 'error'
#    o 'critical'
# Messages with severity less than ATP_LOG_LEVEL will be ignored.
#
ATP_LOG_LEVEL = "debug"

#
# Set the following flag to a non-zero value if you want to enable verbose
# mode system-wide.
#
# When enabling this flag, you will need to set the following in
# /etc/rsyslog.conf file to disable rate-limit:
#
#   '$SystemLogRateLimitInterval 0'
#
ATP_VERBOSE_MODE = 0

#
# In which directory the corpse files of failed Teleport jobs must be kept.
# NOTE: An empty string, means they should not be kept.
# Default is "/var/log".
#
#CORPSE_DIR = "/var/log"

#
# Inactivity period in seconds after which a Teleport job is a candidate to
# be killed.
# Default is 24 hours.
#
#ATP_INACTIVE_PERIOD = 24 * 3600

#
# The allowed number of efforts to finish Teleport jobs.
# Must be greater than 8.  Maximum is 21.  Default is 11.
#
#ATP_EFFORT = 11
