#!/usr/bin/python
#
# Copyright (c) 2014 Atlantis Computing Inc.
# All rights reserved.
#
# No part of this program can be used, copied, redistributed, reproduced,
# or disclosed in any form or by any means without prior express written
# permission of the copyright holder.
#
# This library implements a class which provides all the interfaces needed
# to teleport one path (a directory hierarchy or a file ) from one ILIO
# system to another.
#
# ATTENTION: The vi/vim settings used for editing this file are:
#
#    set expandtab ts=4 shiftwidth=4
#
# XXX:
# 1) Need to add check for newline char in file names of teleport files
#

import commands
import sys
import re
import os
import stat

sys.path.insert(0, "/opt/milio/atlas/teleport")
sys.path.insert(0, "/etc/ilio")
import atp_file
import atp_lib
import atp_jobstate
import atp_config

#
# Role of the node running the script.
#
# ATJ: Atlas Teleport Job
#
ATJ_ROLE_NONE = 0
ATJ_ROLE_SRC = 1
ATJ_ROLE_DST = 2

role_str = ['none', 'src', 'dst']

#
# Atlas Teleport Job (ATJ) states at source ILIO
#
# The following diagram shows how the state changes on the source ilio.
#
# Notes:
# 1) From any state, the job can enter ERROR state from any state.  Those
#    transitions are not shown here.
#
#     +------+
#     | NONE |
#     +--+---+
#        |
#        V
#    +--------+
#    | INITED |
#    +---+----+
#        |
#        V
#    +--------+
#    | MAPPED |
#    +---+----+
#        |
#        V
#   +----------+
#   | MAP_SENT |
#   +----+-----+
#        |
#        V
#   +-----------+
#   | DATA_PREP +-----------+
#   +-----+-----+           |
#         |                 |
#         V                 V
#   +-----------+     +-----------+
#   | FINISHING |     | DATA_SENT |
#   +-----+-----+     +-----+-----+
#         |                 |
#         |                 |
#         |   +---------+   |
#         +-->| CLEANUP |<--+
#             +----+----+
#                  |
#                  V
#               +------+      +-------+
#               | DONE |<-----| ERROR |
#               +------+      +-------+
#
ATJ_SRC_STATE_NONE = 0
ATJ_SRC_STATE_INITED = 1
ATJ_SRC_STATE_MAPPED = 2
ATJ_SRC_STATE_MAP_SENT = 3
ATJ_SRC_STATE_DATA_PREP = 4
ATJ_SRC_STATE_DATA_SENT = 5
ATJ_SRC_STATE_FINISHING = 6
ATJ_SRC_STATE_CLEANUP = 7
ATJ_SRC_STATE_DONE = 8
ATJ_SRC_STATE_LAST = ATJ_SRC_STATE_DONE
ATJ_SRC_STATE_ERR = ATJ_SRC_STATE_DONE + 1

#
# Atlas Teleport Job (ATJ) states at destination ILIO
#
# The following diagram shows how the state changes on the destination ilio.
#
# Notes:
# 1) From any state, the job can enter ERROR state from any state.  Those
#    transitions are not shown here.
# 2) Redir -> Finishing: This happens when there is no file to be copied by
#    Teleport.  All files are already moved to the destination in the base
#    tar file.
#
#      +------+
#      | NONE |
#      +---+--+
#          |
#          V
#      +-------+
#      | REDIR |--------+
#      +-------+        |
#        |  ^           |
#        |  |           |
#        V  |           |
#   +--------------+    |
#   | WAITING_DATA |    |
#   +------+-------+    |
#          |            |
#          V            |
#    +-----------+      |
#    | FINISHING |<-----+
#    +----+------+
#          |
#          V
#      +------+      +-------+
#      | DONE |<-----| ERROR |
#      +------+      +-------+
#
ATJ_DST_STATE_NONE = 0
ATJ_DST_STATE_REDIR = 1
ATJ_DST_STATE_WAITING_DATA = 2
ATJ_DST_STATE_FINISHING = 3
ATJ_DST_STATE_DONE = 4
ATJ_DST_STATE_LAST = ATJ_DST_STATE_DONE
ATJ_DST_STATE_ERR = ATJ_DST_STATE_LAST + 1

#
# The following shows the control flow between the state machines of source
# and destination.
#
#     Source ILIO                             Destination ILIO
# ===================                     ========================
#     +------+
#     | NONE |
#     +--+---+
#        |
#        V
#    +--------+
#    | INITED |
#    +--------+
#        |
#        V
#    +--------+
#    | MAPPED |
#    +--------+
#        |
#        |
#        V                                    +-------+
#   +----------+                              | REDIR |---------+
#   | MAP_SENT |----------------------------->|       |<-----+  |
#   +----------+                              +-------+      |  |
#                                                 |          |  |
#                                                 V          |  |
#                                          +--------------+  |  |
#        +---------------------------------| WAITING_DATA |  |  |
#        |                                 +--------------+  |  |
#        V                                                   |  |
#   +-----------+                                            |  |
#   | PREP_DATA |-----------+                                |  |
#   +-----------+           |                                |  |
#        |                  |                                |  |
#        V                  V                                |  |
#   +-----------+     +-----------+                          |  |
#   | FINISHING |     | DATA_SENT |--------------------------+  |
#   +-----------+     +-----------+                             |
#        |                                    +-----------+     |
#        +----------------------------------->| FINISHING |<----+
#                                             +-----------+
#                                                  |
#                                                  V
#             +---------+                       +------+
#             | CLEANUP |<----------------------| DONE |
#             +----+----+                       +------+
#                  |
#                  V
#               +------+
#               | DONE |
#               +------+
#

src_state_str = ['none', 'inited', 'mapped', 'map_sent', 'data_prep',
    'data_sent', 'finishing', 'cleanup', 'done', 'error']
dst_state_str = ['none', 'redir', 'waiting_data', 'finishing', 'done', 'error']

#
# A file to capture the output of the tar commands.
#
_TAR_LOG_FNAME = "tar.log"

#
# A file to capture the output of remote copy command (rsync(1) as of now).
#
_RCP_LOG_FNAME = "rcp.log"

#
# Log file name where the output of partner will be saved.
#
_KICKOFF_LOG_FILE = "atp.log"

#
# A file that shows what role this node is playing in Teleport.
#
_ROLE_FILE = "role"

#
# The tar file name that contains the shadow directory created on the source.
#
_BASE_TAR_GZ_FILE = "base.tgz"

#
# The tar file name that contains missing bitmap files created on the
# destination.
#
_MISSING_MBMAP_TAR_GZ_FILE = "mbmap.tgz"

#
# Default directory where Teleport corpse files will be saved.
#
_CORPSE_DIR_DEF = "/var/log"

#
# The miminum, default, and maximum of allowed number of times (effort) for
# finishing a Teleport job.
#
_ATP_EFFORT_MIN = 9
_ATP_EFFORT_DEF = 11
_ATP_EFFORT_MAX = 21

#
# Postfix for file attributes.
#
_FA_POSTFIX_SIZE = ".size"
_FA_POSTFIX_STATE = ".state"
_FA_POSTFIX_DATA_CS = ".dcs"
_FA_POSTFIX_BMAP_FNAME = ".bmap"
_FA_POSTFIX_CS_FNAME = ".cs"
_FA_POSTFIX_MBMAP_FNAME = ".mbmap"
_FA_POSTFIX_BD_FNAME = ".bd"
_FA_POSTFIX_BMAP_CS_FNAME = _FA_POSTFIX_BMAP_FNAME + ".cs"
_FA_POSTFIX_CS_CS_FNAME = _FA_POSTFIX_CS_FNAME + ".cs"
_FA_POSTFIX_MBMAP_CS_FNAME = _FA_POSTFIX_MBMAP_FNAME + ".cs"
_FA_POSTFIX_BD_CS_FNAME = _FA_POSTFIX_BD_FNAME + ".cs"

#
# Get the role from the role file.
#
def role_getf(path):
    if not os.path.exists(path):
        atp_lib.err("no such job directory'{:s}'", path)
        return ATJ_ROLE_NONE

    rolefname = os.path.join(path, _ROLE_FILE)
    try:
        rolefile = open(rolefname, 'r')
        role = rolefile.read().rstrip('\n')
        rolefile.close()
        if role == role_str[ATJ_ROLE_SRC]:
            return ATJ_ROLE_SRC
        if role == role_str[ATJ_ROLE_DST]:
            return ATJ_ROLE_DST
        atp_lib.err("invalid content in the role file ({:s}): '{:s}'",
            rolefname, role)
    except:
        atp_lib.err("reading the role file '{:s}'", rolefname)
    return ATJ_ROLE_NONE

class atp_path:
    """
    This class is used for copying files using Dedup Teleport engine from
    one ILIO system to another.  This class copies a path.  If the path can
    be either a regular file, or a directory.  If it is a directory all the
    regular files and directories underneath it will be copied.  The files
    go through a state machine until they are all copied.  At the end a
    notification is sent to AMC if a job ID is specified.

    Properties of job object:
      - Job UUID [optional]
      - Source ILIO host name or IP address
      - Destination ILIO host name or IP address
      - Source path
      - Destination path
    """

    #
    # Input:
    #   - jobid: Job id
    #   - mntpt: mount point of this node
    #
    def __init__(self, jobid, role, mntpt):
        self.jobid = jobid
        self.role_set(role)

        # Set up the current node's job path
        self.cjpath = self.__jpath_make(mntpt)

        # Init partner's node job path.
        self.pjpath = ""

        # Create an instance of job state for this run.
        self.jst = atp_jobstate.jobstate()

    #
    # Get the number of times this job has been executed.
    #
    # Returns:
    #   - The number as integer
    #
    def __effort_get(self):
        try:
            n = self.jst.val_get(atp_jobstate.JA_EFFORT)
        except:
            atp_lib.warn("No effort No. found for job {:s}", self.jobid)
            n = "0"
        return int(n)

    #
    # Set the number of times this job has been executed.
    #
    # Returns:
    #   None.
    #
    def __effort_set(self, newval):
        self.jst.val_set(atp_jobstate.JA_EFFORT, str(newval))

    #
    # Get the maximum number of times a job can be executed.
    #
    # Returns:
    #   Returns the maximum if specified in the config file, otherwise the
    #   default maximum.
    #
    def __effort_getmax(self):
        try:
            effort = atp_config.ATP_EFFORT
            try:
                n = int(effort)
            except:
                atp_lib.warn("invalid maximum effort: '{:s}'; setting to {:d}",
                    str(atp_config.ATP_EFFORT), _ATP_EFFORT_DEF)
                n = _ATP_EFFORT_DE
        except:
            n = _ATP_EFFORT_DEF
        if n < _ATP_EFFORT_MIN or n > _ATP_EFFORT_MAX:
            n = _ATP_EFFORT_DEF
            atp_lib.warn("out of range No. of effort {:d}, setting to {:d}",
                n, _ATP_EFFORT_DEF)
        return n

    #
    # Bump up the number of times this job has been executed.
    # Returns:
    #   None.
    #
    def __effort_bump(self):
        atp_lib.dbg("BEGIN: __effort_bump")
        cureffort = self.__effort_get()
        atp_lib.dbg("cureffort: {:d}", cureffort)
        self.__effort_set(cureffort + 1)
        self.save()
        atp_lib.dbg("END: __effort_bump")

    #
    # Check if the number of times that this job has been executed exceeds the
    # limit.  If it does, report it.
    #
    # Returns:
    #   - True if we have not passed the limit.
    #   - False: Otherwise.
    #
    def __effort_check(self):
        atp_lib.dbg("BEGIN: __effort_check")
        cureffort = self.__effort_get()
        maxeffort = self.__effort_getmax()
        if cureffort > maxeffort:
            atp_lib.err("job {:s}: No. of efforts ({:d}) > maximum efforts ({:d})",
                self.jobid, cureffort, maxeffort)
            return False
        atp_lib.dbg("END: __effort_check")
        return True

    #
    # Gigen a mount path, return the job directory path of the current job.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __jpath_make(self, mntpt):
        return os.path.join(mntpt, atp_config.ATP_JODS_DIR, self.jobid)

    #
    # Create a remote directory on the specified host.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __rmkdir(self, rhost, path):
        cmd = "ssh {:s}@{:s} mkdir -p '{:s}'".format(atp_config.ATP_RUSER,
            rhost, path)
        atp_lib.dbg("__rmkdir: '{:s}'", path)

        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("__rmkdir: mkdir failed '{:s}'", path)
            return False

        return True

    #
    # Execute the command on a remote host.
    #
    def __rexec(self, host, cmd):
        cmd = "ssh {:s}@{:s} \"{:s}\"".format(atp_config.ATP_RUSER, host, cmd)
        atp_lib.dbg("__rexec: '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("__rexec: failed '{:s}'", cmd)
            rc = 1
        return rc

    #
    # Change directory to the specified [job] directory.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __jpath_cd(self, path):
        atp_lib.dbg("__jpath_cd: '{:s}'", path)
        try:
            os.chdir(path)
        except:
            atp_lib.err("couldn't chdir to '{:s}'", path)
            return False

        return True

    #
    # Remove the job directory if it is not required to be kept.
    # 'opt' is the name of the option that tells if the corresponding job
    # directory should be kept or not.
    #
    def __jpath_rm(self, opt, path):
        rc = True
        if self.option_get(opt):
            atp_lib.dbg("keeping job dir '{:s}'", path)
        else:
            atp_lib.dbg("removing job dir '{:s}'", path)
            rc  = self.__rmtree(path)
            if rc != 0:
                atp_lib.err("error {:d} removing job dir '{:s}'", rc, path)
                rc = False
            else:
                rc = True
        return rc

    #
    # Unconditionally remove job directories of this and partner nodes.
    #
    def __jpaths_destroy(self):
        if self.__role_amisrc():
            chost = self.jst.val_get(atp_jobstate.JA_SRC_HOST)
            phost = self.jst.val_get(atp_jobstate.JA_DST_HOST)
        elif self.__role_amidst():
            chost = self.jst.val_get(atp_jobstate.JA_DST_HOST)
            phost = self.jst.val_get(atp_jobstate.JA_SRC_HOST)
        else:
            atp_lib.err("unknown role '{:d}'", self.role)
            return False

        #
        # Exit the current directory that will be deleted below.
        #
        self.__jpath_cd("/")

        #
        # Remove any job directory that can be on this node.
        #
        cmd = "rm -rf '{:s}'".format(self.cjpath)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        crc = self.__rexec(chost, cmd)

        cmd = "rm -rf '{:s}'".format(self.pjpath)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        prc = self.__rexec(phost, cmd)
        return crc == 0 and prc == 0

    #
    # Remove a directory tree.
    #
    # Returns:
    #   0: Upon success
    #   != 0: Otherwise
    #
    def __rmtree(self, path):
        cmd = "rm -rf '{:s}'".format(path)
        atp_lib.dbg("job_remove '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            return rc

        return 0

    #
    # Save the corpse of the job as a compressed tar file in the corpse
    # directory.  The files in the shadow directory and all the block data
    # files are removed before creating the tar file in order to save disk
    # space.
    #
    # Returns:
    #   Nothing.
    #
    def __corpse_save(self):
        atp_lib.dbg("BEGIN: __corpse_save")
        try:
            corpsedir = atp_config.CORPSE_DIR
            if not corpsedir:
                atp_lib.err("saving corpse of Teleport jobs '{:s}' is disabled",
                    self.jobid)
                self.__jpaths_destroy()
                atp_lib.dbg("END: __corpse_save")
                return
        except:
            corpsedir = _CORPSE_DIR_DEF

        #
        # Make sure the corpse path exists and it is a directory.
        #
        atp_lib.dbg("corpsedir: '{:s}'", corpsedir)
        try:
            mode = os.stat(corpsedir).st_mode
        except:
            atp_lib.err("could not access corpsedir '{:s}'", corpsedir)
            return
        if not stat.S_ISDIR(mode):
            atp_lib.err("corpse path is not a directory '{:s}'", corpsedir)
            return

        #
        # Remove the shadow directory and all the block data files.
        #
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        if self._isfile:
            sdir = os.path.dirname(spath)
        else:
            sdir = spath
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("sdir '{:s}'", sdir)
        rmlist = [sdir]
        atp_lib.dbg("rmlist '{:s}'", rmlist)
        cmd = "rm -r " + "*" + _FA_POSTFIX_BD_FNAME + " " + ' '.join("'" + item + "'" for item in rmlist)
        atp_lib.dbg("cmd '{:s}'", cmd)
        tarthem = True
        try:
            rc = os.system(cmd)
            atp_lib.dbg("rm rc '{:d}'", rc)
	    if rc != 0:
	        tarthem = False
        except:
            tarthem = False
            atp_lib.err("removing .bd and teleport files '{:s}'", rmlist)

        #
        # If we fail to remove the shadow directory and block data files, we
        # do not create the tar file, as it can hold a lot of disk space in
        # the corpse directory/filesystem.
        #
        if tarthem:
            self.__jpath_cd("..")
            tarfname = corpsedir + "/" + self.jobid + ".tgz"
            if  not self.__tar_make(tarfname, [self.jobid], "/dev/null"):
                atp_lib.err("couldn't save corpse of jobs '{:s}' in '{:s}",
                    self.jobid, tarfname)
            else:
                atp_lib.err("saved corpse of jobs '{:s}' in '{:s}", self.jobid,
                    tarfname)

        #
        # Remove the job directories on both the current and partner nodes.
        #
        self.__jpaths_destroy()
        atp_lib.dbg("END: __corpse_save")

    #
    # Sync the specified file system.
    #
    # Returns:
    #   0: Upon success
    #   != 0: Otherwise
    #
    def __sync(self):
        atp_lib.dbg("__sync")
        try:
            rc = os.system("sync")
        except:
            return rc

        return 0

    #
    # Remove '/' from the specified path and replace it with '_'.  This is
    # mainly used to prevent Teleport files to appear in the directory where
    # the actual files reside.
    #
    def __path2norm(self, path):
        return path.replace('/', '_')

    #
    # Report the status of the job to AMC.
    #
    def __job_report_state(self, status):
        #
        # Currently only VM migration uses single-file and in that case AMC
        # is not involved in the Teleport job.  No need to report the job
        # status back to AMC.
        #
        try:
            prog = self.jst.val_get(atp_jobstate.JA_STATE_REPORT_PROG)
        except:
            prog = ""
        if not prog:
            atp_lib.info("No status report program registered for '{:s}' job",
                self.jobid)
            return True

        try:
            tag = self.jst.val_get(atp_jobstate.JA_TAG)
        except:
            tag = ""
        cmd = "{:s} {:s} {:d} {:s} '{:s}' {:d}".format(prog, self.jobid, status, tag,
	    os.path.basename(self.jst.val_get(atp_jobstate.JA_SRC_PATH)), self.jst.val_get('ignore'))
        atp_lib.dbg("cmd '{:s}'", cmd)
        try:
            rc = os.system(cmd)
            if rc != 0:
                atp_lib.err("reporting job status {:s}: {:d} failed: {:d}",
                    self.jobid, status, rc)
                return False
        except:
            atp_lib.err("reporting job status {:s}: {:d} failed", self.jobid,
                status)
            return False

        atp_lib.info("Reported status of job {:s} status {:d}", self.jobid,
            status)
        return True

    #
    # Common routine to set file name (key, value) pair in the job state
    # file.  This normalizes the value to prevent Teleport files to appear
    # in the directory where the actual files reside.
    #
    # Returns:
    #   Nothing.
    #
    def __common_fname_set(self, fname, pfx, val):
        key = fname + pfx
        val = self.__path2norm(val)
        atp_lib.dbg("key_set: '{:s}' --> '{:s}'", key, val)
        self.jst.val_set(key, val)

    #
    # Common routine to get file name for the specified file name and its
    # postfix from the job state file.
    #
    # Returns:
    #   The value of the key
    #
    def __common_fname_get(self, fname, pfx):
        key = fname + pfx
        atp_lib.dbg("key_get: key '{:s}'", key)
        val = self.jst.val_get(key)
        atp_lib.dbg("key_get: val '{:s}'", val)
        return val

    #
    # Set and get methods of the size attr of a given file name.
    #
    def __fo_size_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_SIZE, val)

    def __fo_size_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_SIZE)

    #
    # Set and get methods of the state attr of a given file name.
    #
    def __fo_state_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_STATE, val)

    def __fo_state_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_STATE)

    #
    # Set and get methods of the SHA1 checksum attr of a given file name.
    #
    def __fo_file_cs_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_DATA_CS, val)

    def __fo_file_cs_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_DATA_CS)

    #
    # Set and get methods of the bitmap file name of a given file name.
    #
    def __fo_bmap_fname_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_BMAP_FNAME, val)

    def __fo_bmap_fname_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_BMAP_FNAME)

    #
    # Set and get methods of the checksum file name of a given file name.
    #
    def __fo_cs_fname_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_CS_FNAME, val)

    def __fo_cs_fname_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_CS_FNAME)

    #
    # Set and get methods of the missing bitmap file name of a given file name.
    #
    def __fo_mbmap_fname_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_MBMAP_FNAME, val)

    def __fo_mbmap_fname_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_MBMAP_FNAME)

    #
    # Set and get methods of the block data file name of a given file name.
    #
    def __fo_bd_fname_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_BD_FNAME, val)

    def __fo_bd_fname_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_BD_FNAME)

    #
    # Set and get methods of the checksum of the bitmap file name of a given
    # file name.
    #
    def __fo_bmap_cs_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_BMAP_CS_FNAME, val)

    def __fo_bmap_cs_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_BMAP_CS_FNAME)

    #
    # Set and get methods of the checksum of the checksum file name of a given
    # file name.
    #
    def __fo_cs_cs_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_CS_CS_FNAME, val)

    def __fo_cs_cs_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_CS_CS_FNAME)

    #
    # Set and get methods of the checksum of the missing bitmap file name of
    # a given file name.
    #
    def __fo_mbmap_cs_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_MBMAP_CS_FNAME, val)

    def __fo_mbmap_cs_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_MBMAP_CS_FNAME)

    #
    # Set and get methods of the checksum of the block data file name of
    # a given file name.
    #
    def __fo_bd_cs_set(self, fname, val):
        self.__common_fname_set(fname, _FA_POSTFIX_BD_CS_FNAME, val)

    def __fo_bd_cs_get(self, fname):
        return self.__common_fname_get(fname, _FA_POSTFIX_BD_CS_FNAME)

    #
    # An error has happened and the node needs to go to the 'error' state.
    # The state needs to be saved in the file.  We will try to save it.
    # The error is ignored, because in some cases we might have failed to
    # save it and that's why we need to go to 'error' state.
    #
    # Returns:
    #   None
    #
    def __state_goto_err(self):
        atp_lib.dbg("BEGIN: __state_goto_err")
        if self.__role_amisrc():
            self.__state_src_set(src_state_str[ATJ_SRC_STATE_ERR])
        else:
            self.__state_dst_set(dst_state_str[ATJ_DST_STATE_ERR])
        self.jst.save()
        self.__job_report_state(atp_config.ATP_ERROR)
        self.__corpse_save()
        atp_lib.dbg("END: __state_goto_err")

    #
    # Extract the tar file of this node. The role of this node can
    # be extracted from the job state.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __tar_extract(self):
        atp_lib.dbg("BEGIN: __tar_extract")
        if self.__role_amisrc():
            tar_fname = self.jst.val_get(atp_jobstate.JA_SRC_TAR_FNAME)
        else:
            tar_fname = self.jst.val_get(atp_jobstate.JA_DST_TAR_FNAME)

        cmd = "tar zxvf {:s} >> {:s} 2>&1".format(tar_fname, _TAR_LOG_FNAME)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("extracting '{:s}' tar file", tar_fname)
            return False

        atp_lib.dbg("END: __tar_extract")
        return True if rc == 0 else False

    #
    # Create a tar file for the partner node.
    #
    # Input:
    #   Space separated list of file name.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __tar_make(self, tar_fname, tarlist, tarlog=None):
        atp_lib.dbg("BEGIN: __tar_make")

        if tarlog is None:
            tarlog = _TAR_LOG_FNAME
        flist = ' '.join("'" + item + "'" for item in tarlist)
        cmd = "tar zcvf '{:s}' {:s} >> {:s} 2>&1".format(tar_fname, flist,
            tarlog)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("creating '{:s}' tar file", tar_fname)
            return False

        atp_lib.dbg("END: __tar_make")
        return True if rc == 0 else False

    #
    # Xfer the set of files, whose list is specified, to the partner's node in
    # a tar file.  Partner's role is extracted from the job state and the file
    # name is set accordingly in the job state file.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __tar_send(self, tarlist):
        atp_lib.dbg("BEGIN: __tar_send")

        #
        # The tar file is going to be created for the partner, not for this
        # node.
        #
        if self.__role_amisrc():
            tar_fname = self.jst.val_get(atp_jobstate.JA_DST_TAR_FNAME)
        else:
            tar_fname = self.jst.val_get(atp_jobstate.JA_SRC_TAR_FNAME)

        if not self.__tar_make(tar_fname, tarlist):
            atp_lib.err("creating '{:s}' tar file", tar_fname)
            return False

        if not self.__file_xfer(tar_fname):
            atp_lib.err("xfering '{:s}' tar file", tar_fname)
            return False

        atp_lib.dbg("END: __tar_send")
        return True

    #
    # Copy the specified files to the partner node in its job directory.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __flist_xfer(self, flist):
        atp_lib.dbg("BEGIN: __flist_xfer")
        if self.__role_amisrc():
            rhost = self.jst.val_get(atp_jobstate.JA_DST_HOST)
        else:
            rhost = self.jst.val_get(atp_jobstate.JA_SRC_HOST)

        files = ' '.join("'" + item + "'" for item in flist)
        atp_lib.dbg("rhost: '{:s}'", rhost)
        atp_lib.dbg("files: '{:s}'", files)

        #
        # The command should be like:
        #     rsync <files> <remote_usr>@<remote_ilio>:<remote_path>/
        #
        rc = 0
        cmd = "{:s} {:s} {:s} {:s}@{:s}:{:s} >> {:s} 2>&1".format(
            atp_config.ATP_RCP, atp_config.ATP_RCP_OPTS, files,
            atp_config.ATP_RUSER, rhost, self.pjpath, _RCP_LOG_FNAME)
        atp_lib.info("cmd: '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("xfering file '{:s}' to '{:s}''", files, rhost)
            return False

        atp_lib.info("END: __flist_xfer")
        return True if rc == 0 else False

    #
    # Copy the specified file to the partner node in its job directory.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def __file_xfer(self, fname):
        atp_lib.dbg("BEGIN: __file_xfer")
        rv = self.__flist_xfer([fname])
        atp_lib.dbg("END: __file_xfer")
        return rv

    #
    # Create the checksum of the specified file.  In this version, SHA1 is
    # used for verification of the content of the files.
    #
    # Returns:
    #   Non-empty string: The checksum of the file.
    #   Empty string: error
    #
    def __cs_make(self, fname):
        atp_lib.dbg("BEGIN: __cs_make")

        #
        # If the file name contains a character that needs to be escaped,
        # e.g.a new line, the SHA1 checksum will start with '\' character.
        # This seems to be a documented feature.  To avoid checking for
        # this strange output format, instead of passing the file name on
        # the command line, we redirect the stdin of checksum program to
        # be out file.
        #
        cmd = "{:s} < '{:s}' | cut -d' ' -f1".format(atp_config.ATP_CS_PROG,
            fname)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        try:
            (rc, output) = commands.getstatusoutput(cmd)
        except:
            atp_lib.err("failed to calculate the checksum of '{:s}'", fname)
            return ""

        if rc != 0:
            atp_lib.err("failed to calculate the checksum of '{:s}', err {:d}",
                fname, rc)
            return ""


        atp_lib.dbg("END: __cs_make")
        return output

    #
    # Verify the content of the specified file with the specified
    # checksum.  In this version, SHA1 is used for verification of
    # the content of the files.
    #
    # Returns:
    #   True: if the content of the file is OK
    #   False: Otherwise
    #
    def __cs_check(self, fname, cs):
        atp_lib.dbg("BEGIN: __cs_check")

        if not cs:
            atp_lib.dbg("END: __cs_check cs is empty")
            return False

        computed_cs = ""
        computed_cs = self.__cs_make(fname)
        atp_lib.dbg("computed_cs: '{:s}' ", computed_cs)

        if not computed_cs:
            atp_lib.dbg("END: __cs_check computed_cs is empty")
            return False

        atp_lib.dbg("END: __cs_check")
        return True if computed_cs == cs else False

    #
    # Show a message to let use know that the state machine must be
    # moved manually.
    #
    def __msg_cont_on_node(self, node):
        atp_lib.info("************************** INFO ******************")
        atp_lib.info("Run the 'run' sub-command on the {:s} system", node)
        atp_lib.info("to continue.  The state machine stops here.")

    #
    # Invoke the Teleport run script.  If the target node is specified, the
    # script is run on that node.  Otherwise, it will be executed on the
    # partner node.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def kickoff(self, target = None):
        atp_lib.dbg("BEGIN: kickoff")

        #
        # Determine the target if not specified, and the log file to where the
        # stdout and stderr of the program must be redirected.
        #
        if target is None:
            logpath = self.pjpath
            if self.__role_amisrc():
                target = ATJ_ROLE_DST
            elif self.__role_amidst():
                target = ATJ_ROLE_SRC
            else:
                atp_lib.err("internal error: unknown role {:d}", self.role)
                self.__state_goto_err()
                return False
        else:
            logpath = self.cjpath

        if target == ATJ_ROLE_DST:
            partner = "destination"
            rhost = self.jst.val_get(atp_jobstate.JA_DST_HOST)
            rmntpt = self.jst.val_get(atp_jobstate.JA_DST_MNTPATH)
            opt = "-d " + atp_config.ATP_RUN_OPTS
        else:
            partner = "source"
            rhost = self.jst.val_get(atp_jobstate.JA_SRC_HOST)
            rmntpt = self.jst.val_get(atp_jobstate.JA_SRC_MNTPATH)
            opt = "-s " + atp_config.ATP_RUN_OPTS

        #
        # Check if we have to stop the state machine here, per user's request.
        #
        if self.option_get(atp_jobstate.JA_RUN_ONESTEP):
            self.__msg_cont_on_node(partner)
            atp_lib.dbg("END: kickoff")
            return True

        if not self.jobid:
            atp_lib.err("internal error: empty job id")
            self.__state_goto_err()
            return False
        if not rhost:
            atp_lib.err("couldn't find partner's IP/hostname")
            self.__state_goto_err()
            return False
        if not rmntpt:
            atp_lib.err("couldn't find partner's mountpoint")
            self.__state_goto_err()
            return False

        rc = 0

        #
        # N.B.  The cancel method looks for a pattern to kill the atp run
        # process of a particular job.  The pattern is:
        #   atp_lib.ATP_CMD_RUN -j <jobid>
        # If the following command is going to change, make sure cancel()
        # method is also in sync with the new command syntax.
        #
        cmd = "ssh {:s}@{:s} \"nohup {:s} -j {:s} -m {:s} {:s} >> {:s} 2>&1 &\"".format(
            atp_config.ATP_RUSER, rhost, atp_lib.ATP_CMD_RUN, self.jobid,
            rmntpt, opt, os.path.join(logpath, _KICKOFF_LOG_FILE))
        atp_lib.dbg("cmd: '{:s}'", cmd)

        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("handling over to the partner")
            self.__state_goto_err()
            return False

        atp_lib.dbg("END: kickoff")
        return True if rc == 0 else False

    #
    # Take a Teleport job one host through its state machine based on its role.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def run(self):
        if self.__role_amisrc():
            atp_lib.info("Running job {:s} on source", self.jobid)
            rc = self.run_src()
        elif self.__role_amidst():
            atp_lib.info("Running job {:s} on destination", self.jobid)
            rc = self.run_dst()
        else:
            atp_lib.err("neither source no destination role specified")
            rc = False
        return rc

    #
    # Create a corresponding 'shadow' file for each file to the
    # teleported. Files smaller than the min size are hard linked
    # and sent over in the tar file.
    #
    def __file_create_shadow(self, fname, parent, shadow_dir, data_verify):
        fname_src = os.path.join(parent, fname)
        abs_fname_shadow = os.path.join(shadow_dir, fname_src)
        filesize = os.lstat(fname_src).st_size
        atp_lib.dbg("fname_src '{:s}'", fname_src)
        atp_lib.dbg("abs_fname_shadow '{:s}'", abs_fname_shadow)
        atp_lib.dbg("filesize {:d}", filesize)

        if filesize < atp_config.ATP_MIN_FSIZE_BYTES:
            atp_lib.dbg("Creating a hard link for {:s} [{:d} bytes]",
                fname_src, filesize)
            try:
                os.link(fname_src, abs_fname_shadow)
            except:
                error = sys.exc_info()
                atp_lib.err("creating hard link {:s} to {:s}, err {:s}",
                    abs_fname_shadow, fname_src, error)
                return (False, None)
            return (True, None)

        atp_lib.dbg("Creating a attributes only file for {:s}", fname_src)
        cmd = "cp --attributes-only -p '{:s}' '{:s}'".format(fname_src,
            abs_fname_shadow)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        try:
            ret = os.system(cmd)
            if ret != 0:
                atp_lib.err("failed to create shell file {:s}",
                    abs_fname_shadow)
                return (False, None)
        except:
            error = sys.exc_info()
            atp_lib.err("failed to create shell file {:s}, err {:s}",
                abs_fname_shadow, error)
            return (False, None)

        # Stip the leading "./" from file name
        tp_fname = fname_src.replace("./", "", 1)
        atp_lib.dbg("tp_fname '{:s}'", tp_fname)

        # Store the file size in the atp_file object
        self.__fo_size_set(tp_fname, str(filesize))

        # Set atp_file.state of the file to INITED
        self.__fo_state_set(tp_fname, "inited")

        #
        # Calculate SHA1 checksum of the file to verify its content
        # if requested.
        #
        if data_verify:
            cs = self.__cs_make(fname_src)
            atp_lib.dbg("fname '{:s}' cs '{:s}'", fname_src, cs)
            self.__fo_file_cs_set(tp_fname, cs)

        return (True, tp_fname)

    #
    # XXX:
    # 1) Directory names must not have trailing '/'. Strip it.
    #    Especially, JA_SRC_PATH, but take care of self.cjpath also.
    # 2) We do not handle files which have '\n' character in their names.
    # 3) Need to test special files such as block and char device files
    #    in the source directory hierarchy.
    #
    def __src_none_handler(self):
        # NONE:
        #     # init the job and the shadow directory
        #     # We do not support recovery from crash so we can
        #     # start from scratch if we crash in the middle of
        #     # this state.
        #     - Create the shadow directory
        #     - Create hard-link in the shadow directory for all the small files
        #     - For each to-be-teleported file
        #       * Create a atp_file object
        #       * Add the file to the list of files in the job state file list
        #       * Create a shell file for the it in the shadow dir
        #       * Store the file size in the atp_file object
        #       * Set atp_file.state of the file to INITED
        #     - Set src.state to INITED and continue
        #     - Store the job state to the job.state file (atomically)
        #
        atp_lib.dbg("BEGIN: __src_none_handler")

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        atp_lib.dbg("Min file size for teleport {:d}",
            atp_config.ATP_MIN_FSIZE_BYTES)

        rc = True
        smntpt = self.jst.val_get(atp_jobstate.JA_SRC_MNTPATH)
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        if self._isfile:
            sdir = os.path.dirname(spath)
        else:
            sdir = spath
        sbasedir = os.path.basename(sdir)
        shadow_dir = os.path.join(self.cjpath, sbasedir)
        data_verify = self.option_get(atp_jobstate.JA_DATA_VERIFY)

        atp_lib.dbg("smntpt '{:s}'", smntpt)
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("_isfile {:d}", self._isfile)
        atp_lib.dbg("sbasedir '{:s}'", sbasedir)
        atp_lib.dbg("shadow_dir '{:s}'", shadow_dir)
        atp_lib.dbg("data_verify '{:d}'", data_verify)

        # XXX BUG We need to preserve the directory attributes
        try:
            os.makedirs(shadow_dir)
        except:
            error = sys.exc_info()
            atp_lib.err("exception creating shadow dir {:s}, err {:s}",
                shadow_dir, error)
            self.__state_goto_err()
            return False

        cwd = os.getcwd()
        atp_lib.dbg("cwd '{:s}'", cwd)
        self.__jpath_cd(os.path.join(smntpt, sdir))
        atp_lib.dbg("cwd '{:s}'", os.getcwd())

        tp_filelist = []
        if self._isfile:
            fname = os.path.basename(spath)
            atp_lib.dbg("fname '{:s}'", fname)
            rc, tp_fname = self.__file_create_shadow(fname, ".", shadow_dir,
                data_verify)
            if rc and tp_fname:
                tp_filelist.append(tp_fname)
        else:
            #
            # Walk the directory to be teleported.
            #
            for parent, dirs, files in os.walk("."):
                atp_lib.dbg("(parent, dirs, files): ('{:s}', '{:s}', '{:s}')",
                    parent, dirs, files)

                #
                # Create a corresponding 'shadow' dir for each dir in the
                # source path.
                #
                for dname in dirs:
                    shadow_path = os.path.join(shadow_dir, parent, dname)
                    atp_lib.dbg("shadow_path: '{:s}'", shadow_path)
                    # XXX BUG We need to preserve the directory attributes
                    try:
                        os.makedirs(shadow_path)
                        atp_lib.dbg("Created shadow dir {:s}", shadow_path)
                    except:
                        error = sys.exc_info()
                        atp_lib.err("creating shadow dir {:s}, err {:s}",
                            shadow_path, error)
                        rc = False
                        break

                if not rc:
                    break

                #
                # Create the hard-link or shell files for each file in the
                # source directory.
                #
                for fname in files:
                    rc, tp_fname = self.__file_create_shadow(fname, parent,
                        shadow_dir, data_verify)
                    if not rc:
                        break
                    if tp_fname:
                        tp_filelist.append(tp_fname)

                if not rc:
                    break

        #
        # Go back to the job directory. No check is needed. This must succeed.
        #
        self.__jpath_cd(cwd)

        #
        # Save the file list even if there was an error.  it can be useful to
        # know which files were successfully processed without a need to dig
        # into the details of each file.
        #
        self.jst.val_set(atp_jobstate.JA_FILELIST, tp_filelist)
        atp_lib.dbg("Files to be teleported: '{:s}'",
            ", ".join(self.jst.val_get(atp_jobstate.JA_FILELIST)))

        if not rc:
            self.__state_goto_err()
            return False

        self.__state_src_set(src_state_str[ATJ_SRC_STATE_INITED])

        # Store the job state to the job.state file.
        if self.jst.save():
            atp_lib.err("storing job state")
            self.__state_goto_err()
            return False

        atp_lib.dbg("END: __src_none_handler")

        #
        # Go to the next step if we are not running in one-step mode.
        #
        if not self.option_get(atp_jobstate.JA_RUN_ONESTEP):
            rc = self.__src_inited_handler()
        else:
            self.__msg_cont_on_node("source")

        return rc

    def __src_inited_handler(self):
        # INITED:
        #     # Create bitmap and checksum files
        #     - For each to-be-teleported file
        #       * Create the bmap and cs file names
        #       * Run the mapper program for it
        #       * Set the file state to MAPPED
        #     - Set the src.state to MAPPED
        #     - Store the job state to the job.state file (atomically)
        atp_lib.dbg("BEGIN: __src_inited_handler")

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        rc = True
        smntpt = self.jst.val_get(atp_jobstate.JA_SRC_MNTPATH)
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        if self._isfile:
            sdir = os.path.dirname(spath)
        else:
            sdir = spath
        sbasedir = os.path.basename(sdir)
        atp_lib.dbg("smntpt '{:s}'", smntpt)
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("sbasedir '{:s}'", sbasedir)

        tp_filelist = self.jst.val_get(atp_jobstate.JA_FILELIST)
        atp_lib.dbg("Files to be teleported: '{:s}'", ", ".join(tp_filelist))

        for fname in tp_filelist:
            fpath = os.path.join(smntpt, sdir, fname)
            fsize = self.__fo_size_get(fname)
            atp_lib.dbg("Processing: '{:s}' fpath '{:s}' fsize {:s}",
                fname, fpath, fsize)

            #
            # Store the check sum and bitmap file names in job state.  This will
            # normalize the path of the bitmap and the checksum file so that
            # they will appear in the job directory instead of the shadow
            # directory.
            #
            self.__fo_bmap_fname_set(fname, fname + _FA_POSTFIX_BMAP_FNAME)
            self.__fo_cs_fname_set(fname, fname + _FA_POSTFIX_CS_FNAME)
            bmap_fname = self.__fo_bmap_fname_get(fname)
            cs_fname = self.__fo_cs_fname_get(fname)
            atp_lib.dbg("bmap_fname: '{:s}'", bmap_fname)
            atp_lib.dbg("cs_fname: '{:s}'", cs_fname)

            #
            # Create and populate the file object for the current file.
            #
            f = atp_file.atp_file(fpath, self.cjpath)
            f.bmap_fname_set(bmap_fname)
            f.cs_fname_set(cs_fname)

            #
            # Create the corresponding bitmap and the check sum files.
            #
            if not f.map():
               rc = False
               break

            #
            # Calculate the checksum of the bitmap and the checksum files.
            #
            bmap_cs = self.__cs_make(bmap_fname)
            if not bmap_cs:
                rc = False
                atp_lib.err("error calculating checksum for '{:s}' ",
                    bmap_fname)
                break

            cs_cs = self.__cs_make(cs_fname)
            if not cs_cs:
                rc = False
                atp_lib.err("error calculating checksum for '{:s}' ", cs_fname)
                break

            atp_lib.dbg("SHA1({:s}): '{:s}'", bmap_fname, bmap_cs)
            atp_lib.dbg("SHA1({:s}): '{:s}'", cs_fname, cs_cs)

            self.__fo_bmap_cs_set(fname, bmap_cs)
            self.__fo_cs_cs_set(fname, cs_cs)
            self.__fo_state_set(fname, f.state_get())

        if not rc:
            self.__state_goto_err()
            return rc

        self.__state_src_set(src_state_str[ATJ_SRC_STATE_MAPPED])

        if self.jst.save():
            atp_lib.err("storing job state")
            self.__state_goto_err()
            return False

        atp_lib.dbg("END: __src_inited_handler")

        #
        # Go to the next step if we are not running in one-step mode.
        #
        if not self.option_get(atp_jobstate.JA_RUN_ONESTEP):
            rc = self.__src_mapped_handler()
        else:
            self.__msg_cont_on_node("source")

        return rc

    def __src_mapped_handler(self):
        # MAPPED:
        #     # Copy the files to the dst
        #     - Create the compressed tar file of the shadow directory
        #     # Xfering the file to the dst ilio must create the directory
        #     # hierarchy automatically.
        #     - Xfer the tar file to the dst ilio
        #     - Set dst.state = REDIR
        #     - Set src.state = MAP_SENT
        #     - Store the job state to the job.state file (atomically)
        #     - Xfer the job.state file to dst ilio
        #     - Xfer control to dst
        atp_lib.dbg("BEGIN: __src_mapped_handler")

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        rc = True
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        if self._isfile:
            sdir = os.path.dirname(spath)
        else:
            sdir = spath
        sbasedir = os.path.basename(sdir)
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("sbasedir '{:s}'", sbasedir)

        #
        # Names of the check sum and bit map files for the files to be
        # teleported.
        #
        tarlist = []
        tp_filelist = self.jst.val_get(atp_jobstate.JA_FILELIST)
        atp_lib.dbg("Files to be teleported: '{:s}'", tp_filelist)
        for fname in tp_filelist:
            bmap_fname = self.__fo_bmap_fname_get(fname)
            cs_fname = self.__fo_cs_fname_get(fname)
            tarlist.append(bmap_fname)
            tarlist.append(cs_fname)

        atp_lib.dbg("tarlist: '{:s}'", tarlist)

        #
        # Name of the shadow directory needs to be added to the list of files
        # in the tar file.  All small files (hard links) and shadow files
        # for the files to be teleported are here.  This method assumes that
        # the code is at the parent of the shadow directory.
        #
        tarlist.append(sbasedir)

        #
        # Create and xfer the tar file to destination
        #
        self.jst.val_set(atp_jobstate.JA_DST_TAR_FNAME, _BASE_TAR_GZ_FILE)
        if not self.__tar_send(tarlist):
            atp_lib.err("error copying tar file from source to destination")
            self.__state_goto_err()
            return False

        #
        # Set the destination and source states in job.state file
        # And transfer the file to destination
        #
        self.__state_src_set(src_state_str[ATJ_SRC_STATE_DATA_SENT])
        self.__state_dst_set(dst_state_str[ATJ_DST_STATE_REDIR])

        if self.jst.save():
            atp_lib.err("storing job state")
            self.__state_goto_err()
            return False
        if not self.__file_xfer(self.jst.fname):
            atp_lib.err("error copying job.state file to destination")
            self.__state_goto_err()
            return False

        #
        # Xfer control to the destination node.
        #
        rc = self.kickoff()
        if not rc:
            atp_lib.err("kicking off partner")
            self.__state_goto_err()
            return False

        atp_lib.dbg("END: __src_mapped_handler")
        return rc

    def __src_map_sent_handler(self):
        atp_lib.err("the src node in 'map_sent' state, job '{:s}'", self.jobid)
        return False

    def __src_data_prep_handler(self):
        # DATA_PREP:
        #     # Run data-broker program to create Block Data File
        #     # If there is no data to be xfered start finishing phase
        #     # else transfer data and ask dst to try redirecting
        #     - For each to-be-teleported file
        #       * Run the Data Broker program for the file
        #     - If there is uniq data to be teleported
        #         - Xfer the Block Data files to the dst ilio
        #         - Set dst.state = REDIR
        #         - Set src.state = DATA_SENT
        #       else
        #         - Set dst.state = FINISHING
        #         - Set src.state = FINISHING
        #     - Store the job state to the job.state file (atomically)
        #     - Xfer control to dst
        #
        atp_lib.dbg("BEGIN: __src_data_prep_handler")

        #
        # Put a cap on the number of attempts to xfer data to the destination.
        #
        if not self.__effort_check():
            self.__state_goto_err()
            return False

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        rc = True
        smntpt = self.jst.val_get(atp_jobstate.JA_SRC_MNTPATH)
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        if self._isfile:
            sdir = os.path.dirname(spath)
        else:
            sdir = spath
        sbasedir = os.path.basename(sdir)
        atp_lib.dbg("smntpt '{:s}'", smntpt)
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("sbasedir '{:s}'", sbasedir)

        #
        # Extract the tar file containing missing bmap and other files
        #
        atp_lib.dbg("Extracting the tar file containing mbmap files")
        if not self.__tar_extract():
            atp_lib.err("extracting the tar file")
            self.__state_goto_err()
            return False
        #
        # Run data broker program for each file to be teleported.
        #
        tarlist = []
        flist = self.jst.val_get(atp_jobstate.JA_FILELIST)
        atp_lib.dbg("flist: '{:s}'", flist)
        for fname in flist:
            #
            # Verify checksum for missing bmap file. Return False
            # in case of any error.
            #
            mbmap_fname = self.__fo_mbmap_fname_get(fname)
            cs = self.__fo_mbmap_cs_get(fname)
            atp_lib.dbg("SHA1({:s}): cs recv: '{:s}'", mbmap_fname, cs)
            if not self.__cs_check(mbmap_fname, cs):
                rc = False
                atp_lib.err("checksum mismatch for {:s}", mbmap_fname)
                break

            #
            # Prep the file object and run data broker
            #
            fpath = os.path.join(smntpt, sdir, fname)
            atp_lib.dbg("fpath: '{:s}'", fpath)
            f = atp_file.atp_file(fpath, self.cjpath)

            #
            # Get the attributes of the file from job state and assign it to
            # the file object.
            #
            try:
                state = self.__fo_state_get(fname)
                mbmap_fname= self.__fo_mbmap_fname_get(fname)
            except:
                rc = False
                atp_lib.err("getting attributes of '{:s}'", fname)
                break

            atp_lib.dbg("file state '{:s}'", state)
            atp_lib.dbg("mbmap_fname '{:s}'", mbmap_fname)

            try:
                bd_fname = self.__fo_bd_fname_get(fname)
            except:
                bd_fname = self.__fo_bd_fname_set(fname,
                    fname + _FA_POSTFIX_BD_FNAME)
                bd_fname = self.__fo_bd_fname_get(fname)

            atp_lib.dbg("block data file: {:s}", bd_fname)

            f.state_set(state)
            f.mbmap_fname_set(mbmap_fname)
            f.bd_fname_set(bd_fname)

            if not f.dbroker():
               rc = False
               break

            #
            # Data Broker program has returned success.
            #
            self.__fo_state_set(fname, f.state_get())

            #
            # Check the state of each file being processed. We will
            # send over the block data files, if any file's state is
            # set to redirecting by the dbroker method.
            #
            if not f.isdone():
                tarlist.append(self.__fo_bd_fname_get(fname))
                atp_lib.dbg("db_fname size {:d}", os.path.getsize(bd_fname))

        #
        # If any error happened during the above loop, stop.
        #
        if not rc:
            self.__state_goto_err()
            return rc

        #
        # Compress and xfer the block data files to destination, if there
        # was any file set to 'redirecting' state by dbroker method.
        # Clear any previous tar file name to prevent untaring again on
        # the destination.
        #
        atp_lib.dbg("Block Data Files to be xferred to source: {:s}", tarlist)
        if tarlist:
            self.jst.val_set(atp_jobstate.JA_DST_TAR_FNAME, "")
            if not self.__flist_xfer(tarlist):
                atp_lib.err("error copying bdfile to destination '{:s}'",
                    tarlist)
                self.__state_goto_err()
                return False

            #
            # Let the destination try redirecting again.
            #
            self.__state_src_set(src_state_str[ATJ_SRC_STATE_DATA_SENT])
            self.__state_dst_set(dst_state_str[ATJ_DST_STATE_REDIR])
        else:
            #
            # We're done.  Let the destination start cleaning up process.
            #
            self.__state_src_set(src_state_str[ATJ_SRC_STATE_FINISHING])
            self.__state_dst_set(dst_state_str[ATJ_DST_STATE_FINISHING])

        atp_lib.dbg("src.state: {:s}", self.__state_src_get())
        atp_lib.dbg("dst.state: {:s}", self.__state_dst_get())

        #
        # Save the job state
        #
        if self.jst.save():
            atp_lib.err("storing job state")
            self.__state_goto_err()
            return False
        if not self.__file_xfer(self.jst.fname):
            atp_lib.err("error copying job.state file to destination")
            self.__state_goto_err()
            return False

        #
        # Xfer control to the destination node.
        #
        rc = self.kickoff()
        if not rc:
            atp_lib.err("kicking off partner")
            self.__state_goto_err()
            return False

        atp_lib.dbg("END: __src_data_prep_handler")
        return rc

    def __src_data_sent_handler(self):
        atp_lib.err("the src node in 'data_sent' state, job '{:s}'", self.jobid)
        return False

    def __src_finishing_handler(self):
        #
        # XXX: Is it OK to ignore that we are called when in this state?
        #
        atp_lib.dbg("BEGIN: __src_finishing_handler")
        atp_lib.dbg("END: __src_finishing_handler")
        return True

    def __src_cleanup_handler(self):
        atp_lib.dbg("BEGIN: __src_cleanup_handler")

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        #
        # The source needs to clean up.
        #
        self.__state_src_set(src_state_str[ATJ_SRC_STATE_DONE])

        #
        # We can ignore the error for saving this state, because in the worst
        # case we will try to remove the job dir again, which is benign,.
        #
        self.jst.save()

        #
        # Get out of the job directory before removing it.  We are going to
        # the top level directory, which we can assume exists.
        #
        self.__jpath_cd("..")

        #
        # Remove the job directory
        #
        rc = self.__jpath_rm(atp_jobstate.JA_SRC_KEEP_DIR, self.cjpath)
        if rc:
            self.__job_report_state(atp_config.ATP_SUCCESS)
        else:
            self.__job_report_state(atp_config.ATP_ERROR)

        atp_lib.dbg("END: __src_cleanup_handler")
        return rc

    def __src_done_handler(self):
        atp_lib.dbg("BEGIN: __src_done_handler")
        atp_lib.dbg("END: __src_done_handler")
        return True

    def __src_err_handler(self):
        atp_lib.err("the src node in 'error' state, job: '{:s}'", self.jobid)
        return False

    #
    # The following dictionary maps the source states to the corresponding
    # handler method.  Handler methods must return 'True' on success and
    # 'False' otherwise.
    #
    _src_methods = {
        src_state_str[ATJ_SRC_STATE_NONE]: __src_none_handler,
        src_state_str[ATJ_SRC_STATE_INITED]: __src_inited_handler,
        src_state_str[ATJ_SRC_STATE_MAPPED]: __src_mapped_handler,
        src_state_str[ATJ_SRC_STATE_MAP_SENT]: __src_map_sent_handler,
        src_state_str[ATJ_SRC_STATE_DATA_PREP]: __src_data_prep_handler,
        src_state_str[ATJ_SRC_STATE_DATA_SENT]: __src_data_sent_handler,
        src_state_str[ATJ_SRC_STATE_FINISHING]: __src_finishing_handler,
        src_state_str[ATJ_SRC_STATE_CLEANUP]: __src_cleanup_handler,
        src_state_str[ATJ_SRC_STATE_DONE]: __src_done_handler,
        src_state_str[ATJ_SRC_STATE_ERR]: __src_err_handler
    }

    #
    # Run the Teleport job through its state machine on the source node.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def run_src(self):
        # Get the state from the job state.
        try:
            sstate = self.__state_src_get()
        except:
            atp_lib.err("source state not found")
            return False

        # Execute the handler associated to the state.
        if not sstate in src_state_str:
            atp_lib.err("invalid source state: '{:s}'", sstate)
            rc = False
        else:
            rc = self._src_methods[sstate](self)

        atp_lib.dbg("END: run_src")
        return rc

    def __dst_verify_tplist(self, sbasedir):
        atp_lib.dbg("BEGIN: __dst_verify_tplist")
        if not self.option_get(atp_jobstate.JA_DATA_VERIFY):
            atp_lib.dbg("END: __dst_verify_tplist true")
            return True
        rv = True
        flist = self.jst.val_get(atp_jobstate.JA_FILELIST)
        atp_lib.dbg("flist: '{:s}'", flist)
        for fname in flist:
            cs = self.__fo_file_cs_get(fname)
            atp_lib.dbg("SHA1({:s}): cs recv: '{:s}'", fname, cs)
            fpath = os.path.join(sbasedir, fname)
            if not self.__cs_check(fpath, cs):
                atp_lib.warn("Checksum mismatch for '{:s}'", fname)
                rv = False

        atp_lib.dbg("END: __dst_verify_tplist {:d}", rv)
        return rv

    #
    # Handler for destination 'none' state.  The destination must not be in
    # this state.
    #
    def __dst_none_handler(self):
        atp_lib.err("the dest node in 'none' state, job: '{:s}'", self.jobid)
        return False

    #
    # Handler for destination 'redir' state
    #
    def __dst_redir_handler(self):
        atp_lib.dbg("BEGIN: __dst_redir_handler")

        #
        # Put a cap on the number of times we ask for data from the source.
        #
        if not self.__effort_check():
            self.__state_goto_err()
            return False

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        dmntpt = self.jst.val_get(atp_jobstate.JA_DST_MNTPATH)
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        if self._isfile:
            sbasedir = os.path.dirname(spath)
        else:
            sbasedir = os.path.basename(spath)
        atp_lib.dbg("cwd '{:s}'", os.getcwd())
        atp_lib.dbg("dmntpt '{:s}'", dmntpt)
        atp_lib.dbg("_isfile '{:d}'", self._isfile)
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("sbasedir '{:s}'", sbasedir)

        #
        # Get the files from the tar file if any.
        #
        tar_fname = self.jst.val_get(atp_jobstate.JA_DST_TAR_FNAME)
        atp_lib.dbg("tar_fname '{:s}'", tar_fname)
        if tar_fname:
            if not self.__tar_extract():
                atp_lib.err("extracting the tar file")
                self.__state_goto_err()
                return False
        else:
            atp_lib.dbg("no tar file specified")

        #
        # The following will make ensure that the data blocks from
        # the tar file (if any) will be on disk before we redirect
        # blocks to them.
        #
        rc = self.__sync()
        if rc != 0:
            atp_lib.err("syncing '{:s}' rc: {:d}", dmntpt, rc)
            self.__state_goto_err()
            return False

        flist = self.jst.val_get(atp_jobstate.JA_FILELIST)
        atp_lib.dbg("flist: '{:s}'", flist)
        if not flist:
            atp_lib.info("no file to be teleported")
            return self.__dst_finishing_handler()

        rc = True
        newflist = []
        tarlist = []
        for fname in flist:
            atp_lib.dbg("preparing file '{:s}' for redirecting", fname)
            #
            # We already changed directory to the job directory, so we can pass
            # "." for now.
            #
            fpath = os.path.join(sbasedir, fname)
            atp_lib.dbg("fpath '{:s}'", fpath)
            f = atp_file.atp_file(fpath, ".")

            #
            # Get file attribute(s) and map file names for the current file
            #
            fsize = ""
            fstate = ""
            bmap_fname = ""
            cs_fname = ""

            #
            # If the missing FBN bitmap file name is not specified assign it.
            # We do a set followed by get in order to avoid duplicating the
            # normalization of the file name here.  Let it be handled in the
            # set method for this field.
            #
            try:
                mbmap_fname = self.__fo_mbmap_fname_get(fname)
            except:
                mbmap_fname = self.__fo_mbmap_fname_set(fname,
                    fname + _FA_POSTFIX_MBMAP_FNAME)
                mbmap_fname = self.__fo_mbmap_fname_get(fname)

            try:
                fsize = self.__fo_size_get(fname)
                fstate = self.__fo_state_get(fname)
                bmap_fname = self.__fo_bmap_fname_get(fname)
                cs_fname = self.__fo_cs_fname_get(fname)
            except:
                atp_lib.err("getting file names for '{:s}' file", fname)
                self.__state_goto_err()
                return False

            atp_lib.dbg("(size, state, bm_fn, cs_fn, mbm_fn): "
                "('{:s}', '{:s}', '{:s}', '{:s}', '{:s}')",
                fsize, fstate, bmap_fname, cs_fname, mbmap_fname)
            atp_lib.dbg("'{:s}'.state: '{:s}'", fname, f.state_get())

            #
            # Verify the checksum of the bitmap and the checksum files.
            #
            cs = self.__fo_bmap_cs_get(fname)
            atp_lib.dbg("SHA1({:s}): cs recv: '{:s}'", bmap_fname, cs)
            if not self.__cs_check(bmap_fname, cs):
                rc = False
                atp_lib.err("checksum mismatch for '{:s}'", bmap_fname)
                self.__state_goto_err()
                break
            cs = self.__fo_cs_cs_get(fname)
            atp_lib.dbg("SHA1({:s}): cs recv: '{:s}'", cs_fname, cs)
            if not self.__cs_check(cs_fname, cs):
                rc = False
                atp_lib.err("checksum mismatch for '{:s}'", cs_fname)
                self.__state_goto_err()
                break

            #
            # Set attributes of the file ATP file object
            #
            f.state_set(fstate)
            f.bmap_fname_set(bmap_fname)
            f.cs_fname_set(cs_fname)
            f.mbmap_fname_set(mbmap_fname)
            f.dbg_dump()

            #
            # Truncate the file to the right size before redirecting.
            # Redirecting will catch and throw error redirecting blocks
            # that are beyond EOF, which is initially zero for shell-files.
            #
            cmd = "{:s} -s {:s} '{:s}'".format(atp_config.ATP_TRUNCATE_CMD,
                fsize, fpath)
            atp_lib.dbg("cmd: '{:s}'", cmd)
            try:
                os.system(cmd)
            except:
                rc = False
                atp_lib.err("truncating '{:s}'", fpath)
                self.__state_goto_err()
                break

            #
            # Redirect the blocks of the file.
            #
            if not f.redirect():
                rc = False
                atp_lib.err("error redirecting '{:s}' ", fname)
                self.__state_goto_err()
                break

            if not f.isdone():
                #
                # Calculate the checksum of the missing FBN bitmap file.
                #
                cs = self.__cs_make(mbmap_fname)
                if not cs:
                    rc = False
                    atp_lib.err("error calculating checksum for '{:s}' ",
                        mbmap_fname)
                    self.__state_goto_err()
                    break

                atp_lib.dbg("SHA1({:s}): '{:s}'", mbmap_fname, cs)
                self.__fo_mbmap_cs_set(fname, cs)
                tarlist.append(mbmap_fname)
                newflist.append(fname)
                atp_lib.dbg("redirecting '{:s}' successful", fname)

        #
        # Get out of here if an error happend.
        #
        if not rc:
            return rc

        atp_lib.dbg("tarlist '{:s}'", " ".join(tarlist))
        atp_lib.dbg("newflist: '{:s}'", newflist)

        #
        # We're done if none of the files ended up in the new list.
        #
        if not newflist:
            atp_lib.info("Redirecting all files are done")
            return self.__dst_finishing_handler()

        #
        # Update the file list, as some files might be already done.
        #
        self.jst.val_set(atp_jobstate.JA_FILELIST, newflist)

        #
        # Xfer the MFBN file(s) to the source
        #
        self.jst.val_set(atp_jobstate.JA_SRC_TAR_FNAME,
            _MISSING_MBMAP_TAR_GZ_FILE)
        if not self.__tar_send(tarlist):
            atp_lib.err("creating/send the tar file")
            self.__state_goto_err()
            return False

        #
        # The source needs to prepare the Block Data files that we need.
        #
        self.__state_src_set(src_state_str[ATJ_SRC_STATE_DATA_PREP])

        #
        # This node will be waiting for data to arrive.
        #
        self.__state_dst_set(dst_state_str[ATJ_DST_STATE_WAITING_DATA])

        #
        # Store the job state to the job.state file and copy it to the partner.
        #
        # Try to store error state and ignore the error if it happens again.
        # We will not kick off processing on the source, so Teleport will stop
        # anyway.
        #
        if self.jst.save():
            atp_lib.err("storing job state")
            self.__state_goto_err()
            return False
        if not self.__file_xfer(self.jst.fname_get()):
            atp_lib.err("copying job state")
            self.__state_goto_err()
            return False

        #
        # Xfer control to the source node.
        #
        rc = self.kickoff()
        if not rc:
            atp_lib.err("kicking off partner")
            self.__state_goto_err()
            return False

        atp_lib.dbg("END: __dst_redir_handler")
        return rc

    #
    # Handler for destination 'waiting_data' state
    #
    def __dst_waiting_data_handler(self):
        atp_lib.err("the dest node in 'waiting_data' state, job: '{:s}'",
            self.jobid)
        return False

    #
    # Handler for destination 'finishing' state
    #
    # It is time to move the files to their final destination [directory].
    # as the source to cleanup, and remove the job directory on the
    # destination.  The latter can be controlled by a flag saying to keep
    # the directories.  At the end, transfer the control to the source to
    # do its final cleaning.
    #
    def __dst_finishing_handler(self):
        atp_lib.dbg("BEGIN: __dst_finishing_handler")

        # Keep track of the number of times this job has been executed.
        self.__effort_bump()

        rc = True
        dmntpt = self.jst.val_get(atp_jobstate.JA_DST_MNTPATH)
        spath = self.jst.val_get(atp_jobstate.JA_SRC_PATH)
        atp_lib.dbg("_isfile '{:d}'", self._isfile)
        if self._isfile:
            sfname = os.path.basename(spath)
            sdir = os.path.basename(os.path.dirname(spath))
            sname = os.path.join(sdir, sfname)
            atp_lib.dbg("sdir '{:s}'", sdir)
            atp_lib.dbg("sfname '{:s}'", sfname)
            atp_lib.dbg("sname '{:s}'", sname)
        else:
            sdir = spath
            sname = os.path.basename(sdir)
        sbasedir = os.path.basename(sdir)
        atp_lib.dbg("dmntpt '{:s}'", dmntpt)
        atp_lib.dbg("spath '{:s}'", spath)
        atp_lib.dbg("sbasedir '{:s}'", sbasedir)
        atp_lib.dbg("sname '{:s}'", sname)

        #
        # Verify that the Teleported files are the same as their source.
        #
        if not self.__dst_verify_tplist(sbasedir):
            self.__state_goto_err()
            return False

        #
        # Remove the target if it exists.
        #
        dpath = os.path.join(dmntpt, self.jst.val_get(atp_jobstate.JA_DST_PATH))
        atp_lib.dbg("dpath: '{:s}'", dpath)
        if os.path.exists(dpath):
            atp_lib.dbg("removing existing target dir: '{:s}'", dpath)
            rc  = self.__rmtree(dpath)
            if rc != 0:
                atp_lib.err("error {:d} removing target '{:s}'", rc, dpath)
                self.__state_goto_err()
                return False

        #
        # Move the files/directory from the job directory to its final
        # target directory.
        #
        try:
            os.rename(sname, dpath)
        except os.error as e:
            atp_lib.err("os.err {:d} moving '{:s}' to target '{:s}'",
                e.errno, sname, dpath)
            self.__state_goto_err()
            return False

        #
        # Let the user know that the job moved the files to their final dest.
        #
        atp_lib.info("Teleport job {:s} created '{:s}'", self.jobid, dpath)

        #
        # The source needs to clean up.
        #
        self.__state_src_set(src_state_str[ATJ_SRC_STATE_CLEANUP])

        #
        # This node is done.
        #
        self.__state_dst_set(dst_state_str[ATJ_DST_STATE_DONE])

        #
        # Store the job state to the job.state file and copy it to the partner.
        #
        # Try to store error state and ignore the error if it happens again.
        # We will not kick off processing on the source, so Teleport will stop
        # anyway.
        #
        if self.jst.save():
            atp_lib.err("storing job state")
            self.__state_goto_err()
            return False
        if not self.__file_xfer(self.jst.fname_get()):
            atp_lib.err("copying job state")
            self.__state_goto_err()
            return False

        #
        # Xfer control to the source node.
        #
        rc = self.kickoff()
        if not rc:
            atp_lib.err("kicking off partner")
            self.__state_goto_err()
            return False

        #
        # Get out of the job directory before removing it.  We are going to
        # the top level directory, which we can assume exists.
        #
        self.__jpath_cd("..")

        #
        # Remove the job directory.
        #
        rc = self.__jpath_rm(atp_jobstate.JA_DST_KEEP_DIR, self.cjpath)

        atp_lib.dbg("END: __dst_finishing_handler")
        return rc

    #
    # Handler for destination 'done' state
    #
    def __dst_done_handler(self):
        atp_lib.err("the dest node in 'done' state, job: '{:s}'", self.jobid)
        return False

    #
    # Handler for destination 'error' state
    #
    def __dst_err_handler(self):
        atp_lib.err("the dest node in 'error' state, job: '{:s}'", self.jobid)
        return False

    #
    # The following dictionary maps the destination states to the corresponding
    # handler method.  Handler methods must return 'True' on success and
    # 'False' otherwise.
    #
    _dst_methods = {
        dst_state_str[ATJ_DST_STATE_NONE]: __dst_none_handler,
        dst_state_str[ATJ_DST_STATE_REDIR]: __dst_redir_handler,
        dst_state_str[ATJ_DST_STATE_WAITING_DATA]: __dst_waiting_data_handler,
        dst_state_str[ATJ_DST_STATE_FINISHING]: __dst_finishing_handler,
        dst_state_str[ATJ_DST_STATE_DONE]: __dst_done_handler,
        dst_state_str[ATJ_DST_STATE_ERR]: __dst_err_handler
    }

    #
    # Run the Teleport job through its state machine on the destination node.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def run_dst(self):
        atp_lib.dbg("BEGIN: run_dst")

        # Get the state from the job state.
        try:
            dstate = self.__state_dst_get()
        except:
            atp_lib.err("destination state not found")
            return False

        # Execute the handler associated to the state.
        if not dstate in dst_state_str:
            atp_lib.err("invalid destination state: '{:s}'", dstate)
            rc = False
        else:
            rc = self._dst_methods[dstate](self)

        atp_lib.dbg("END: run_dst")
        return rc

    #
    # Am I the source of Teleport?
    #
    # Returns:
    #   - True: if running as source node
    #   - False: Otherwise
    #
    def __role_amisrc(self):
        return self.role == ATJ_ROLE_SRC

    #
    # Am I the dst of Teleport?
    #
    # Returns:
    #   - True: if running as destination node
    #   - False: Otherwise
    #
    def __role_amidst(self):
        return self.role == ATJ_ROLE_DST

    def err_get(self):
        pass

    def err_set(self, errcode):
        pass

    # Get the source state
    def __state_src_get(self):
        return self.jst.val_get(atp_jobstate.JA_SRC_STATE)

    # Get the destination state
    def __state_dst_get(self):
        return self.jst.val_get(atp_jobstate.JA_DST_STATE)

    # Set the source state
    def __state_src_set(self, newst):
        if not newst in src_state_str:
            return False

        self.jst.val_set(atp_jobstate.JA_SRC_STATE, newst)
        return True

    # Set the destination state
    def __state_dst_set(self, newst):
        if not newst in dst_state_str:
            return False

        self.jst.val_set(atp_jobstate.JA_DST_STATE, newst)
        return True

    #
    # Get an option of the job.
    #
    # Returns:
    #   - True: If the option exists and it starts either with 't' or 'y'
    #   - False: otherwise.
    #
    def option_get(self, opt):
        try:
            val = self.jst.val_get(opt)
        except:
            return False

        return True if re.match("[tTyY]", val) else False

    #
    # Set an option of the job.
    #
    # Returns:
    #   Nothing
    #
    def option_set(self, opt, val):
        self.jst.val_set(opt, val)

    #
    # Set the role for this job instance
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def role_set(self, newrole):
        if newrole != ATJ_ROLE_SRC and newrole != ATJ_ROLE_DST:
            return False

        self.role = newrole
        return True

    #
    # Create a job directory with the specified jobid.
    #
    # Returns:
    # - True: On success
    # - False: Otherwise
    #
    def create(self, shost, dhost, smntpt, dmntpt, spath, dpath, cb_prog, tag, isfile, ignore):
        atp_lib.dbg("creating job (jobid, shost, dhost, smntpt, dmntpt, spath, dpath, cb_prog, tag, isfile): "
            "('{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', {:d}, {:d})",
            self.jobid, shost, dhost, smntpt, dmntpt, spath, dpath, cb_prog, tag, isfile, ignore)

        #
        # Make the job directory of the source node.
        #
        try:
            os.makedirs(self.cjpath)
        except OSError as e:
            atp_lib.err("creating job dir {:s}, err {:d}", self.cjpath, e.errno)
            return False

        #
        # All the fun happens in the job directory.  So change dir to it
        # to reduce the headache of creating full path names for each file.
        #
        if not self.__jpath_cd(self.cjpath):
            return False

        #
        # Populate the job state object to bootstrap Teleport on the source.
        #
        self.jst.val_set(atp_jobstate.JA_VER, atp_config.ATP_VERSION)
        self.jst.val_set(atp_jobstate.JA_ERROR, "")
        self.jst.val_set(atp_jobstate.JA_SRC_STATE,
            src_state_str[ATJ_SRC_STATE_NONE])
        self.jst.val_set(atp_jobstate.JA_DST_STATE,
            dst_state_str[ATJ_DST_STATE_NONE])
        self.jst.val_set(atp_jobstate.JA_SRC_HOST, shost)
        self.jst.val_set(atp_jobstate.JA_DST_HOST, dhost)
        self.jst.val_set(atp_jobstate.JA_SRC_PATH, spath)
        self.jst.val_set(atp_jobstate.JA_DST_PATH, dpath)
        self.jst.val_set(atp_jobstate.JA_SRC_MNTPATH, smntpt)
        self.jst.val_set(atp_jobstate.JA_DST_MNTPATH, dmntpt)
        self.jst.val_set(atp_jobstate.JA_FILELIST, "")
        self.jst.val_set(atp_jobstate.JA_EFFORT, "0")
        self.jst.val_set('ignore', ignore)
        if tag is not None:
            self.jst.val_set(atp_jobstate.JA_TAG, tag)
        if cb_prog is not None:
            self.jst.val_set(atp_jobstate.JA_STATE_REPORT_PROG, cb_prog)

        self._isfile = isfile
        if isfile:
            # Teleport requested for single file
            self.option_set(atp_jobstate.JA_ISFILE, "y")

        #
        # Souce and destination job paths need to be handcrafted every time
        # they are set.  A better way would be having a method that sets the
        # mount point of a node and updates the job directory of the node at
        # the same time.
        #
        self.cjpath = self.__jpath_make(smntpt)
        self.pjpath = self.__jpath_make(dmntpt)
        atp_lib.dbg("self.cjpath: '{:s}'", self.cjpath)
        atp_lib.dbg("self.pjpath: '{:s}'", self.pjpath)

        #
        # Create the job directory of the destination node.
        #
        if not self.__rmkdir(dhost, self.pjpath):
            atp_lib.err("creating destintion job dir '{:s}'", self.pjpath)
            self.__jpaths_destroy()
            return False

        #
        # Create the log file for remote invokations on both sides.
        #
        logf = os.path.join(self.cjpath, _KICKOFF_LOG_FILE)
        try:
            open(logf, 'w').close()
        except:
            atp_lib.err("creating source log file '{:s}'", logf)
            self.__jpaths_destroy()
            return False
        if not self.__file_xfer(logf):
            atp_lib.err("creating destination log file '{:s}'", logf)
            self.__jpaths_destroy()
            return False

        #
        # Store the role in the local and remote role files.
        #
        rolefname = os.path.join(self.cjpath, _ROLE_FILE)
        try:
            rolefile = open(rolefname, 'w')
            rolefile.write(role_str[ATJ_ROLE_SRC] + "\n")
            rolefile.close()
        except:
            atp_lib.err("creating local role file '{:s}'", rolefname)
            self.__jpaths_destroy()
            return False

        rolefname = os.path.join(self.pjpath, _ROLE_FILE)
        cmd = "echo {:s} > '{:s}'".format(role_str[ATJ_ROLE_DST], rolefname)
        if self.__rexec(dhost, cmd):
            atp_lib.err("creating remote role file '{:s}'", rolefname)
            self.__jpaths_destroy()
            return False

        #
        # Save the state to the file as a checkpoint of this phase.
        #
        error = self.jst.save()
        if error != 0:
            atp_lib.err("couldn't save job state file '{:s}' err {:d}",
                self.cjpath, error)
            self.__jpaths_destroy()
            return False
        if not self.__file_xfer(self.jst.fname_get()):
            atp_lib.err("copying job state")
            self.__jpaths_destroy()
            return False

        atp_lib.info("created job (jobid, shost, dhost, smntpt, dmntpt, spath, dpath, isfile, ignore): "
            "('{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', {:d}, {:d})",
            self.jobid, shost, dhost, smntpt, dmntpt, spath, dpath, isfile, ignore)

        return True

    #
    # Check if the job directory exists
    #
    # Returns:
    #   - True: if the job directory exists
    #   - False: Otherwise
    #
    def exist(self):
        return os.path.exists(self.cjpath)

    #
    # Remove the job directory hierarchy, i.e. the directory and everything
    # underneath it.
    #
    # Returns:
    #   - 0: Upon success
    #   - != 0: Otherwise
    #
    def remove(self):
        return self.__rmtree(self.cjpath)

    #
    # Load the job state from the file.
    #
    # Returns:
    #   - True: Upon success
    #   - False: Otherwise
    #
    def load(self):
        #
        # All the fun happens in the job directory.  So change dir to it
        # to reduce the headache of creating full path names for each file.
        #
        if not self.__jpath_cd(self.cjpath):
            return False

        rc = self.jst.load()
        if rc != 0:
            atp_lib.err("error loading job state '{:s}', err {:d}",
                self.jobid, rc)
            return False

        #
        # Set up job paths based on the this node and partner's node mount
        # point.
        #
        if self.__role_amisrc():
            cmntpt = self.jst.val_get(atp_jobstate.JA_SRC_MNTPATH)
            pmntpt = self.jst.val_get(atp_jobstate.JA_DST_MNTPATH)
        elif self.__role_amidst():
            cmntpt = self.jst.val_get(atp_jobstate.JA_DST_MNTPATH)
            pmntpt = self.jst.val_get(atp_jobstate.JA_SRC_MNTPATH)
        else:
            atp_lib.err("internal error: I'm neither source no destination")
            return False

        self.cjpath = self.__jpath_make(cmntpt)
        self.pjpath = self.__jpath_make(pmntpt)
        atp_lib.dbg("self.cjpath: '{:s}'", self.cjpath)
        atp_lib.dbg("self.pjpath: '{:s}'", self.pjpath)

        self._isfile = self.option_get(atp_jobstate.JA_ISFILE)
        self.jst.dbg_print()

        return True

    #
    # Store the state of the job in its job.state file.
    #
    def save(self):
        return self.jst.save()

    #
    # Cancel a job.
    #
    # Limitation: There can be cases where a job cannot be completely cleaned
    # up, because there is no trace of it on the node that is executing the
    # cancel command, while there are files on the partner's file system.
    # For example, the job directory of the destination is removed at the
    # end of its finishing state-handler, but the job directory on the source
    # are still there.
    #
    def cancel(self):
        atp_lib.dbg("BEGIN: cancel job {:s}", self.jobid)

        if self.__role_amisrc():
            chost = self.jst.val_get(atp_jobstate.JA_SRC_HOST)
            phost = self.jst.val_get(atp_jobstate.JA_DST_HOST)
        elif self.__role_amidst():
            chost = self.jst.val_get(atp_jobstate.JA_DST_HOST)
            phost = self.jst.val_get(atp_jobstate.JA_SRC_HOST)
        else:
            atp_lib.err("unknown role '{:d}'", self.role)
            return False

        #
        # Kill any 'atp run' command that might be running for this job.
        #
        cmd = "pkill -9 -f '{:s} -j {:s}'".format(atp_lib.ATP_CMD_RUN,
            self.jobid)
        atp_lib.dbg("cmd: '{:s}'", cmd)
        self.__rexec(chost, cmd)
        self.__rexec(phost, cmd)

        self.__jpaths_destroy()

        #
        # Report failure status to agent.
        #
        self.__job_report_state(atp_config.ATP_ERROR)

        atp_lib.dbg("END: cancel")
        return True

#
# Some unit test cases.
#
if __name__ == "__main__":
    pass
