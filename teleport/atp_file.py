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
# to teleport a single file from one system to another.
#
# ATTENTION: The vi/vim settings used for editing this file are:
#
#    set expandtab ts=4 shiftwidth=4
#

import os
import sys
import subprocess

sys.path.insert(0, "/opt/milio/atlas/teleport")
sys.path.insert(0, "/etc/ilio")
import atp_lib
import atp_config

state_str = ["none", "inited", "mapped", "redirecting", "blkxfer", "done",
    "error"]

INSTDIR = "/opt/milio"

MAPPER_PROG = INSTDIR + "/bin/mapper"
REDIRECTOR_PROG = INSTDIR + "/bin/redirector"
DBROKER_PROG = INSTDIR + "/bin/databroker"

# A temporary file for logging the output of the mapping program
MAP_OUTF = "atp_file.map.out"

# A temporary file for logging the output of the redirector program
REDIRECT_OUTF = "atp_file.redir.out"

# A temporary file for logging the output of the data broker program
DBROKER_OUTF = "atp_file.dbroker.out"

class atp_file:
    """
    This class is used for teleporting a single file from one system to
    another.  This class moves the file through the state machine.

    Properties of file object:
      - file name
      - file size
      - job default directory, where all the files are
      - FBN bitmap file name
      - checksum file name
      - missing FBN bitmap file name
      - block data file name
      - SHA1 checksum of FBN bitmap file
      - SHA1 checksum of checksum file
      - SHA1 checksum of missing FBN bitmap file
      - state
    """

    #
    # Teleport File States.
    #
    TF_STATE_NONE = 0
    TF_STATE_INITED = 1 # After the shell is created
    TF_STATE_MAPPED = 2 # After the bitmap and checksum files are created
    TF_STATE_REDIRECTING = 3 # While redirecting needed
    TF_STATE_BLKXFER = 4 # Block xfer needed
    TF_STATE_DONE = 5 # Teleport is done for the file
    TF_STATE_LAST = TF_STATE_DONE
    TF_STATE_ERR = 6

    def __init__(self, fname, jobdir):
        self._fname = fname
        self._jobdir = jobdir
        self._fsize = ""

        # Init the file names.
        self._bmap_fname = ""
        self._cs_fname = ""
        self._mbmap_fname = ""
        self._bd_fname = ""

        # Init the File CheckSums.
        self._bmap_fcs = ""
        self._cs_fcs = ""
        self._mbmap_fcs = ""

        self._state = self.TF_STATE_NONE
        self._errmsg = 0
        # Treat file size as string because we do not do any math on it.

    def isdone(self):
        return True if self._state == self.TF_STATE_DONE else False

    def __err(self, fmt, *args):
        atp_lib.err(fmt, *args)
        self._state = self.TF_STATE_ERR

    def __fname_isempty(self):
        if self._fname:
            return False

        self.__err("No file specified for Teleport")
        return True

    def __jobdir_isempty(self):
        if self._jobdir:
            return False

        self.__err("No job directory specified for '{:s}' file", self._fname)
        return True

    def __bmap_fname_isempty(self):
        if self._bmap_fname:
            return False

        self.__err("No bitmap file specified for '{:s}' file", self._fname)
        return True

    def __mbmap_fname_isempty(self):
        if self._mbmap_fname:
            return False

        self.__err("No missing bitmap file specified for '{:s}' file",
            self._fname)
        return True

    def __cs_fname_isempty(self):
        if self._cs_fname:
            return False

        self.__err("No checksum file specified for '{:s}' file", self._fname)
        return True

    def __bd_fname_isempty(self):
        if self._bd_fname:
            return False

        self.__err("No block data file specified for '{:s}' file",
            self._fname)
        return True

    #
    # Handle mapping of the file.
    #
    def map(self):
        #
        # Ensure all the pre-req values are provided.
        #
        # XXX: Do we need to limit the initial state, e.g. NONE, before allow
        # mapping?
        #
        if self._state == self.TF_STATE_ERR:
            self.__err("Mapping is not allowed in error state")
            return False
        if self.__fname_isempty():
            return False
        if self.__jobdir_isempty():
            return False
        if self.__bmap_fname_isempty():
            return False
        if self.__cs_fname_isempty():
            return False

        #
        # Change directory to the job jobdir
        #
        go_back = False
        saved_path = os.getcwd()
        atp_lib.dbg("cwd: '{:s}'", saved_path)
        if saved_path != self._jobdir:
            try:
                atp_lib.dbg("chdir '{:s}'", self._jobdir)
                go_back = True
                os.chdir(self._jobdir)
            except:
                self.__err("Couldn't chdir to '{:s}'", self._jobdir)
                return False

        #
        # Map it!
        #
        cmd =  "{:s} {:s} -f '{:s}' -c '{:s}' -b '{:s}' > {:s} 2>&1".format(
            MAPPER_PROG, atp_config.ATP_MAPPER_OPTS,
            self._fname, self._cs_fname, self._bmap_fname, MAP_OUTF)
        atp_lib.info("cmd '{:s}'", cmd)

        try:
            rc = subprocess.call(cmd, shell = True)
            if rc == 0:
                self._state = self.TF_STATE_MAPPED
            else:
                self.__err("mapping '{:s}' file failed", self._fname)
                atp_lib.logfile(MAP_OUTF)
        except OSError as e:
            self.__err("mapping '{:s}' file failed: {:d}", self._fname, e)
            atp_lib.logfile(MAP_OUTF)

        #
        # Change jobdir to original directory if needed.
        #
        if go_back:
            atp_lib.dbg("chdir back '{:s}'", saved_path)
            os.chdir(saved_path)

        return True if (self._state != self.TF_STATE_ERR) else False

    #
    # Handle redirecting the blocks of the file
    #
    def redirect(self):
        #
        # Ensure all the pre-req values are provided.
        #
        # XXX: Do we need to limit the initial state, e.g. MAPPED or
        # REDIRECTING, before allow redirecting?
        #
        if self._state == self.TF_STATE_ERR:
            self.__err("Redirecting is not allowed in error state")
            return False
        if self._state == self.TF_STATE_DONE:
            return True
        if self._state != self.TF_STATE_MAPPED and self._state != self.TF_STATE_REDIRECTING:
            self.__err("Can't map when in error state")
            return False
        if self.__fname_isempty():
            return False
        if self.__jobdir_isempty():
            return False
        if self.__bmap_fname_isempty():
            return False
        if self.__cs_fname_isempty():
            return False
        if self.__mbmap_fname_isempty():
            return False

        #
        # Change directory to the job jobdir
        #
        go_back = False
        saved_path = os.getcwd()
        atp_lib.dbg("cwd: '{:s}'", saved_path)
        if saved_path != self._jobdir:
            try:
                atp_lib.dbg("chdir '{:s}'", self._jobdir)
                go_back = True
                os.chdir(self._jobdir)
            except:
                self.__err("Couldn't chdir to '{:s}'", self._jobdir)
                return False

        cs_sz = os.path.getsize(self._cs_fname)
        atp_lib.dbg("cs_sz: {:d}", cs_sz)
        if cs_sz > 0:
            #
            # Redirect them!
            #
            cmd = "{:s} {:s} -f '{:s}' -c '{:s}' -b '{:s}' -m '{:s}' > {:s} 2>&1".format(
                REDIRECTOR_PROG, atp_config.ATP_REDIR_OPTS, self._fname,
                self._cs_fname, self._bmap_fname, self._mbmap_fname,
                REDIRECT_OUTF)
            atp_lib.info("cmd '{:s}'", cmd)

            try:
                rc = subprocess.call(cmd, shell = True)
                if rc == 0:
                    self._state = self.TF_STATE_BLKXFER
                else:
                    self.__err("redirecting '{:s}' file failed, ec {:d}",
                        self._fname, rc)
                    atp_lib.logfile(REDIRECT_OUTF)
            except OSError as e:
                self.__err("redirecting '{:s}' file failed: {:d}", self._fname, e)
                atp_lib.logfile(REDIRECT_OUTF)
        elif cs_sz == 0:
            # The checksum file is empty so this file is done.
            atp_lib.dbg("Done after redirect '{:s}'", self._fname)
            self._state = self.TF_STATE_DONE
        else:
            self.__err("Could not get checksum file size '{:s}'",
                self._cs_fname)

        #
        # Change jobdir to original directory if needed.
        #
        if go_back:
            atp_lib.dbg("chdir back '{:s}'", saved_path)
            os.chdir(saved_path)

        return True if (self._state != self.TF_STATE_ERR) else False

    #
    # Handle creating the 'block data file'
    #
    def dbroker(self):
        #
        # Ensure all the pre-req values are provided.
        #
        # XXX: Do we need to limit the initial state before allow block xfer?
        #
        if self._state == self.TF_STATE_ERR:
            self.__err("Creating block data file is not allowed in error state")
            return False
        if self.__fname_isempty():
            return False
        if self.__jobdir_isempty():
            return False
        if self.__mbmap_fname_isempty():
            return False
        if self.__bd_fname_isempty():
            return False

        #
        # Change directory to the job jobdir
        #
        go_back = False
        saved_path = os.getcwd()
        atp_lib.dbg("cwd: '{:s}'", saved_path)
        if saved_path != self._jobdir:
            try:
                atp_lib.dbg("chdir '{:s}'", self._jobdir)
                go_back = True
                os.chdir(self._jobdir)
            except:
                self.__err("Couldn't chdir to '{:s}'", self._jobdir)
                return False

        #
        # Create the Block Data File.
        #
        cmd = "{:s} {:s} -f '{:s}' -m '{:s}' -o '{:s}' > {:s} 2>&1".format(
            DBROKER_PROG, atp_config.ATP_DATABROKER_OPTS, self._fname,
            self._mbmap_fname, self._bd_fname, DBROKER_OUTF)
        atp_lib.info("cmd '{:s}'", cmd)

        rc = 0
        try:
            rc = subprocess.call(cmd, shell = True)
            if rc != 0:
                self.__err("creating block data '{:s}' file failed, rc {:d}",
                    self._fname, rc)
                atp_lib.logfile(DBROKER_OUTF)
        except OSError as e:
            self.__err("creating block data '{:s}' file failed: {:d}",
                self._fname, e)
            atp_lib.logfile(DBROKER_OUTF)

        #
        # If the block data block size is zero when data broker program was
        # successful, it is an indication that we are done.  So set the state
        # of the file correctly.
        #
        if self._state != self.TF_STATE_ERR:
            try:
                sz = 0
                if os.path.isfile(self._bd_fname):
                    atp_lib.info("calling getsize")
                    sz = os.path.getsize(self._bd_fname)
                if sz == 0:
                    self._state = self.TF_STATE_DONE
                else:
                    self._state = self.TF_STATE_REDIRECTING
                atp_lib.info("dbroker '{:s}' size '{:d}'", self._bd_fname, sz)
            except:
                self.__err("Could not get file size '{:s}'", self._bd_fname)

        #
        # Change jobdir to original directory if needed.
        #
        if go_back:
            atp_lib.dbg("chdir back '{:s}'", saved_path)
            os.chdir(saved_path)

        return True if self._state != self.TF_STATE_ERR else False

    #
    # Get and Set interfaces for different attributes
    #

    # Job directory path get
    def jobdir_get(self):
        return self._jobdir

    # Job directory path set
    def jobdir_set(self, dname):
        self._jobdir = dname

    # Blocks bitmap file name get
    def bmap_fname_get(self):
        return self._bmap_fname

    # Blocks bitmap file name set
    def bmap_fname_set(self, fname):
        self._bmap_fname = fname

    # Checksum file name get
    def cs_fname_get(self):
        return self._cs_fname

    # Checksum file name set
    def cs_fname_set(self, fname):
        self._cs_fname = fname

    # Missing blocks bitmap file name get
    def mbmap_fname_get(self):
        return self._mbmap_fname

    # Missing blocks bitmap file name set
    def mbmap_fname_set(self, fname):
        self._mbmap_fname = fname

    # Block Data file name get
    def bd_fname_get(self):
        return self._bd_fname

    # Block Data file name set
    def bd_fname_set(self, fname):
        self._bd_fname = fname

    # Get the error message
    def err_get(self):
        return self._errmsg

    # Set the error message
    def err_set(self, errmsg):
        self._errmsg = errmsg

    # Get the current state.  It returns the pre-defined strings.
    def state_get(self):
        return state_str[self._state]

    # Return the string representation of the internal state
    def __state_to_str(self, s):
        if s in range(self.TF_STATE_NONE, self.TF_STATE_ERR):
            return state_str[s]
        return ""

    # Check validity of the specified state string
    def state_isvalid(self, statestr):
        return statestr in state_str

    # convert a state in user-visible format to internal format
    def state_set(self, statestr):
        if statestr in state_str:
            self._state = state_str.index(statestr)
            return 0
        return -1

    def dbg_dump(self):
        atp_lib.dbg("fname: \"{:s}\"", self._fname)
        atp_lib.dbg("jobdir: \"{:s}\"", self._jobdir)
        atp_lib.dbg("bmap_fname: \"{:s}\"", self._bmap_fname)
        atp_lib.dbg("cs_fname: \"{:s}\"", self._cs_fname)
        atp_lib.dbg("mbmap_fname: \"{:s}\"", self._mbmap_fname)
        atp_lib.dbg("bd_fname: \"{:s}\"", self._bd_fname)
        atp_lib.dbg("state: \"{:d}\"", self._state)
        atp_lib.dbg("errmsg: {:d}", self._errmsg)

#
# Some unit test cases
#
if __name__ == "__main__":

    f1 = atp_file("main.vmdk", "/export/ILIO/.dedup/teleport/job_uuid")
    f1.dbg_dump()

    #
    # Test map() method
    #
    f1.bmap_fname_set("main.vmdk.bmap")
    f1.cs_fname_set("main.vmdk.cs")
    if f1.map():
        print "map returned true"
    else:
        print "map returned false"

    print

    #
    # Test redirect() method
    #
    f1.mbmap_fname_set("main.vmdk.bmap")
    if f1.redirect():
        print "redirect returned true"
    else:
        print "redirect returned false"

    print

    #
    # Test dbroker() method
    #
    f1.mbmap_fname_set("main.vmdk.bmap")
    if f1.dbroker():
        print "dbroker returned true"
    else:
        print "dbroker returned false"

    print
    print f1.state_get()
    print
