#!/usr/bin/python
#
# Copyright (c) 2014 Atlantis Computing Inc.
# All rights reserved.
#
# No part of this program can be used, copied, redistributed, reproduced,
# or disclosed in any form or by any means without prior express written
# permission of the copyright holder.
#
# This file implements some library routines for Atlas Teleport
#
# ATTENTION: The vi/vim settings used for editing this file are:
#
#    set expandtab ts=4 shiftwidth=4
#

import logging
import syslog

import atp_config

#
# Error exit code
#
EC_ERR = 1

#
# The Teleport copy commands.
#
ATP_CMD = "/usr/bin/python /opt/milio/atlas/teleport/atp.pyc"
ATP_CMD_CANCEL = ATP_CMD + " cancel"
ATP_CMD_ISACTIVE = ATP_CMD + " isactive"
ATP_CMD_RUN = ATP_CMD + " run"

#
# Set if '-v' option is used for verbose mode
#
vflag = False

#
# Name for logger
#
ATP_LOGGER_NAME = "atp"

#
# Logging levels dict. These are the valid logging levels
# defined in logging module of python.  The severity of
# levels increase in the following order:
#    CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
#
loglvl = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
    'notset': logging.NOTSET
}

#
# This class just acts as an wrapper to the syslog class
# The idea is to reduce some handler initialization code
# in the main programs.
#
class atp_syslog_handler(logging.Handler):
    #
    # __init__ method for the handler
    #
    def __init__(self):
        try:
            syslog.openlog(logoption=syslog.LOG_PID)
        except:
            #
            # XXX: Need to handle somehow showing that we won't be able
            # to log.  I am not sure where the message from the 'print'
            # below ends up.
            #
            print "Exception opening syslog"
        logging.Handler.__init__(self)
    #
    # writes a record to syslog
    #
    def emit(self, record):
        syslog.syslog(self.format(record))

# Print an debug/trace message.
# A good place for this is a utility library.
#
# Example:
#     dbg("Invalid command specified: {:s}", subcmd)
#
def dbg(fmt, *args):
    global vflag

    if not vflag:
        return
    logger.debug("DBG: " + fmt.format(*args))

#
# Print an error message.
#
# Example:
#     err("Invalid command specified: {:s}", subcmd)
#
def err(fmt, *args):
    logger.error("ERROR: " + fmt.format(*args))

#
# Print a warning message.
#
# Example:
#     warn("no file to be teleported")
#
def warn(fmt, *args):
    logger.warning("WARN: " + fmt.format(*args))

#
# Print an info message.
#
# Example:
#     info("You need to be polite to Teleport if you need your files.")
#
def info(fmt, *args):
    logger.warning("INFO: " + fmt.format(*args))

#
# Send the content of the specified file to the Teleport logfile.
#
def logfile(fname):
    err("== BEGIN: Contents of log file: '{:s}'==", fname)
    try:
        f = open(fname, 'r')
        try:
            for line in f:
                err(line)
        finally:
            f.close()
    except:
        err("Couldn't open/read {:s}: {:s} ", fname, sys.exc_info())
    err("== END: Contents of log file: '{:s}'==", fname)

#
# Print an error message and exit with error.
#
# Example:
#     bail("Invalid command specified: {:s}", subcmd)
#
def bail(fmt, *args):
    logger.error("ERROR: " + fmt.format(*args))
    exit(EC_ERR)

#
# Initialize the logger.
#
logger = logging.getLogger(ATP_LOGGER_NAME)
logger.setLevel(loglvl[atp_config.ATP_LOG_LEVEL])
h = atp_syslog_handler()
h.setLevel(loglvl[atp_config.ATP_LOG_LEVEL])
logger.addHandler(h)
