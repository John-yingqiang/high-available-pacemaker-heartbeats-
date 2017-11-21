#!/usr/bin/python
#
# Copyright (c) 2014 Atlantis Computing Inc.
# All rights reserved.
#
# No part of this program can be used, copied, redistributed, reproduced,
# or disclosed in any form or by any means without prior express written
# permission of the copyright holder.
#
# This library implements a class which provides a set of (key, value) pair
# items.  The items can be stored/loaded to/from a file in JSON format.
#
# ATTENTION: The vi/vim settings used for editing this file are:
#
#    set expandtab ts=4 shiftwidth=4
#

import os
import json
import sys

sys.path.insert(0, "/opt/milio/atlas/teleport")
import atp_lib

#
# Default job state file name
#
JOB_STATE_FNAME = "job.state"

#
# NOTE: Do NOT use this outside of this module.
#
# The postfix appended to the default file name for storing
# the job file temporary.
#
_TMP_POSTFIX = ".tmp"

#
# Constant job attribute names
#
JA_DATA_VERIFY = "job.data.verify"
JA_DST_HOST = "dst.ilio"
JA_DST_KEEP_DIR = "dst.keep.jobdir"
JA_DST_MNTPATH = "dst.mnt.path"
JA_DST_PATH = "dst.path"
JA_DST_STATE = "dst.state"
JA_DST_TAR_FLIST = "dst.tar.flist"
JA_DST_TAR_FNAME = "dst.tar.fname"
JA_EFFORT = "job.effort"
JA_ERROR = "job.error"
JA_FILELIST = "job.filelist"
JA_ISFILE = "job.isfile"
JA_RUN_ONESTEP = "job.one.step"
JA_SRC_HOST = "src.ilio"
JA_SRC_KEEP_DIR = "src.keep.jobdir"
JA_SRC_MNTPATH = "src.mnt.path"
JA_SRC_PATH = "src.path"
JA_SRC_STATE = "src.state"
JA_SRC_TAR_FLIST = "src.tar.flist"
JA_SRC_TAR_FNAME = "src.tar.fname"
JA_TAG = "job.tag"
JA_VER = "job.version"
JA_STATE_REPORT_PROG = "job.state.report.prog"

class jobstate:
    """
    This class is used to manage, save and load the status of a Teleport job
    in a JSON file.
    """

    def __init__(self):
        self.fname = JOB_STATE_FNAME
        self.fnametmp = JOB_STATE_FNAME + _TMP_POSTFIX
        self.state = {}
        self.val_set(JA_VER, "")
        self.val_set(JA_ERROR, "")
        self.val_set(JA_SRC_STATE, "")
        self.val_set(JA_DST_STATE, "")
        self.val_set(JA_SRC_HOST, "")
        self.val_set(JA_DST_HOST, "")
        self.val_set(JA_SRC_PATH, "")
        self.val_set(JA_DST_PATH, "")
        self.val_set(JA_FILELIST, "")
        self.val_set(JA_SRC_TAR_FNAME, "")
        self.val_set(JA_DST_TAR_FNAME, "")
        self.val_set(JA_STATE_REPORT_PROG, "")

    #
    # Save the state in the temporary file
    #
    def __save_tmp(self):
        with open(self.fnametmp, "w") as outfile:
            json.dump(self.state, outfile, indent = 4)
        outfile.close()

    #
    # Print the content of the job state.
    #
    def dbg_print(self):
        for x in sorted(self.state):
            atp_lib.dbg("{:s}: {:s}", x, str(self.state[x]))

    #
    # Return the value of a give key.
    #
    def val_get(self, key):
        return self.state[key]

    #
    # Add a new (key, value) pair.  If the pair exists, the value will be
    # overwritten.
    #
    def val_set(self, key, value):
        self.state[key] = value

    #
    # Return the default file name
    #
    def fname_get_def(self):
        return JOB_STATE_FNAME

    #
    # Return the current file name.
    #
    def fname_get(self):
        return self.fname

    #
    # Set the current file name to a new file.
    #
    def fname_set(self, newname):
        self.fname = newname
        self.fnametmp = newname + _TMP_POSTFIX

    #
    # Save the state atomically, i.e. write it to a temporary file
    # and then move the temporary file to the default file.
    #
    # Returns:
    #   - 0: Upon success
    #   - != 0: Otherwise
    #
    def save(self):
        try:
            self.__save_tmp()
            os.rename(self.fnametmp, self.fname)
        except OSError as e:
            return e.errno
        return 0

    #
    # Load the state from the current file
    #
    # Returns:
    #   - 0: Upon success
    #   - != 0: Otherwise
    #
    def load(self):
        try:
            with open(self.fname) as infile:
                self.state = json.load(infile)
            infile.close();
        except OSError as e:
            return e.errno
        return 0

#
# Unit test driver of the class.
#
# XXX: This should be out of this code in a separate file.
#
def jobstate_test():
    #
    # Create an instance of the job state.
    #
    st = jobstate()

    #
    # Check the default file name.
    #
    print "Default file name:", st.fname_get_def()

    #
    # Store the default setting in the file.
    #
    print "Saving the job state in", st.fname_get(), "file."
    st.save()

    #
    # Add a new test pair of (key, value).
    #
    print "Adding new pair (hello, world)"
    st.val_set("hello", "world")

    #
    # Change the file name.
    #
    print "Setting the new file name to", st.fname_get() + ".new"
    st.fname_set(st.fname_get() + ".new")
    print "New file name:", st.fname_get()

    #
    # Store everything in the new file.  The new file must have an extra pair
    # of key, value) compared to the default one.
    #
    print "Saving the job state in", st.fname_get(), "file."
    st.save()

if __name__ == "__main__":
    jobstate_test()
