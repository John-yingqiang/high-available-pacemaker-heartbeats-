#!/usr/bin/python
#
# Copyright (c) 2014 Atlantis Computing Inc.
# All rights reserved.
#
# No part of this program can be used, copied, redistributed, reproduced,
# or disclosed in any form or by any means without prior express written
# permission of the copyright holder.
#
# This file implements the CLI for Teleport File Copy service.
#
# ATTENTION: The vi/vim settings used for editing this file are:
#
#    set expandtab ts=4 shiftwidth=4
#

import argparse
import subprocess
import os
import re
import stat
import sys
import time
import uuid

sys.path.insert(0, "/opt/milio/atlas/teleport")
sys.path.insert(0, "/etc/ilio")
import atp_lib
import atp_job
import atp_config
import atp_jobstate

#
# Default inactivity period in seconds after which a Teleport job is
# considered stalled, or dead and is eligible to be killed.
#
_ATP_INACTIVE_PERIOD_DEF = 24 * 3600

def cleanup(mntpt, force):
    #
    # Check if the specified mount path exists and is a directory.
    #
    if not os.path.exists(mntpt):
        atp_lib.err("cleanup: no such mount point '{:s}'", mntpt)
        return False
    if not os.path.isdir(mntpt):
        atp_lib.err("cleanup: not a directory '{:s}'", mntpt)
        return False
    jobsdir = os.path.join(mntpt, atp_config.ATP_JODS_DIR)
    if not os.path.exists(jobsdir):
        atp_lib.dbg("cleanup: no such directory '{:s}'", jobsdir)
        return False
    if not os.path.isdir(jobsdir):
        atp_lib.err("cleanup: not a directory '{:s}'", jobsdir)
        return False

    atp_lib.dbg("cleanup: force {:d}", force)

    #
    # Change directory to the Teleport directory.
    #
    try:
        os.chdir(jobsdir)
    except:
        atp_lib.err("cleanup: couldn't chdir to '{:s}'", jobsdir)
        return False

    for job in os.listdir("."):
        #
        # Jobs are in directories, ignore non-directory items.
        #
        if not os.path.isdir(job):
            continue

        #
        # Check if the job is active or not.
        #
        cmd = "{:s} -m {:s} -j {:s}".format(atp_lib.ATP_CMD_ISACTIVE,
            mntpt, job)
        atp_lib.dbg("cmd '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("cleanup: checking activity of job: {:s}", job)
            continue

        #
        # The job is active, ignore it.
        #
        if not force and rc == 0:
            atp_lib.info("cleanup: job {:s} is active", job)
            continue

        cmd = "{:s} -m {:s} -j {:s}".format(atp_lib.ATP_CMD_CANCEL,
            mntpt, job)
        atp_lib.dbg("cmd '{:s}'", cmd)
        try:
            rc = os.system(cmd)
        except:
            atp_lib.err("cleanup: canceling job {:s}", job)
            continue

        if rc != 0:
            atp_lib.err("cleanup: couldn't cancel job {:s}", job)
        else:
            atp_lib.info("cleanup: Canceled inactive job {:s}", job)
    return True

#
# Create a unique job ID
#
# Return a UUID for the job
#
def __make_jobid():
    return str(uuid.uuid4())

#
# 'start' sub-command handler.
#
def cmd_start(args):
    #
    # This command is called from a system that is assumed to be the source
    # system of the teleport job.
    #
    # - Create a job ID if one not specified
    # - Remove the job directory if exists
    # - Create the job directory
    # - Create and initialize instance of the job state class
    # - Store the job state to the job.state file (atomically)

    atp_lib.dbg("BEGIN: cmd_start")
    rc = True
    if not args.src_host:
        atp_lib.bail("start: No source system specified")
    if hasattr(args,"dst_host") and not args.dst_host:
        atp_lib.bail("start: No destination system specified")
    else:
        #Establish trust relationship
        cmd = '/usr/bin/python /opt/milio/atlas/system/sshw.pyc -i %s' % args.dst_host
        rc = subprocess.call(cmd, shell = True)
        if rc != 0:
            atp_lib.bail('Failed to establish trust relationship bewteen volume')
    if not args.src_path:
        atp_lib.bail("start: No source path specified")
    if not args.dst_path:
        atp_lib.bail("start: No destination path specified")
    if not args.src_mntpt:
        atp_lib.bail("start: No source mount point specified")
    if not args.dst_mntpt:
        atp_lib.bail("start: No destination mount point specified")
    if not args.jobid:
        args.jobid = __make_jobid()
        if args.callback_prog:
            atp_lib.info("start: -c option specified without -j option; "
                "ignoring the -c option")
            args.callback_prog = None

    atp_lib.dbg("start.args: {:s}", args)

    #
    # Checks for restrictions of the source and destination paths and
    # mount points.
    #
    if re.match("/", args.src_path):
        atp_lib.bail("start: The source path must be relative not absolute path")
    if re.match("/", args.dst_path):
        atp_lib.bail("start: The destination path must be relative not absolute path")
    if not re.match("/", args.src_mntpt):
        atp_lib.bail("start: The source mount point must be absolute path")
    if not re.match("/", args.dst_mntpt):
        atp_lib.bail("start: The destination mount point must be absolute path")

    #
    # Currently, only directories are accepted as source path.
    #
    spath = "{:s}/{:s}".format(args.src_mntpt, args.src_path)
    try:
        mode = os.stat(spath).st_mode
    except:
        atp_lib.bail("start: Couldn't access source path '{:s}'", spath)
    if stat.S_ISDIR(mode):
        isfile = False
    else:
        if not stat.S_ISREG(mode):
            atp_lib.bail("start: Source path is neither a directory nor a file '{:s}'",
                spath)
        isfile = True
    job = atp_job.atp_path(args.jobid, atp_job.ATJ_ROLE_SRC, args.src_mntpt)

    #
    # Remove the job if it already exists.
    #
    if job.exist():
        if job.remove():
            return False

    #
    # Crete the job, and populate the job directory.
    #
    rc = job.create(args.src_host, args.dst_host,
        args.src_mntpt, args.dst_mntpt,
        args.src_path, args.dst_path,
        args.callback_prog, args.tag, isfile, args.ignore)

    #
    # The control flags on the job.
    #
    save = False
    if args.k:
        # Set the keep flags of source and destination.
        job.option_set(atp_jobstate.JA_DST_KEEP_DIR, "y")
        job.option_set(atp_jobstate.JA_SRC_KEEP_DIR, "y")
        save = True
    if args.o:
        # Do only one step at a time.
        job.option_set(atp_jobstate.JA_RUN_ONESTEP, "y")
        save = True
    if args.V:
        # Verify data at the end of Teleport.
        job.option_set(atp_jobstate.JA_DATA_VERIFY, "y")
        save = True
    if save:
        job.save()

    atp_lib.dbg("keep dst: '{:d}'", job.option_get(atp_jobstate.JA_DST_KEEP_DIR))
    atp_lib.dbg("keep src: '{:d}'", job.option_get(atp_jobstate.JA_SRC_KEEP_DIR))
    atp_lib.dbg("one-step '{:d}'", job.option_get(atp_jobstate.JA_RUN_ONESTEP))

    #
    # Start running the job.
    #
    if rc:
        if not job.option_get(atp_jobstate.JA_RUN_ONESTEP):
            atp_lib.dbg("Running the job")
            rc = job.kickoff(atp_job.ATJ_ROLE_SRC)
        else:
            atp_lib.info("************************** INFO **************************")
            atp_lib.info("You need to run the 'run' sub-command on the source system")
            atp_lib.info("to continue.  The source state machine stops here.")

    atp_lib.dbg("END: cmd_start")
    return rc

#
# 'run' sub-command handler.
#
def cmd_run(args):
    atp_lib.dbg("BEGIN: cmd_run")

    if not args.jobid:
        atp_lib.bail("run: no job ID specified")
    if args.s and args.d:
        atp_lib.bail("run: either source or destination can be specified")
    if not args.s and not args.d:
        atp_lib.bail("run: either source or destination must be specified")

    #
    # We already checked that args.s and args.d are exclusive.  So we do not
    # need to check if args.d is true when args.s is false.
    #
    if args.s:
        role = atp_job.ATJ_ROLE_SRC
    else:
        role = atp_job.ATJ_ROLE_DST

    job = atp_job.atp_path(args.jobid, role, args.mntpt)

    # The job dir must exist by now.
    if not job.exist():
        atp_lib.err("run: no such job exits '{:s}'", args.jobid)
        return False

    # Load the job state from the file.
    rc = job.load()
    if not rc:
        atp_lib.err("run: loading job state '{:s}', err {:d}", args.jobid, rc)
        return False

    atp_lib.dbg("job state loaded {:s}", args.jobid)

    # Run the job.
    rc = job.run()
    if not rc:
        atp_lib.err("run: running job '{:s}'", args.jobid)
        return False

    atp_lib.dbg("END: cmd_run")
    return rc

#
# 'cancel' sub-command handler.
#
def cmd_cancel(args):
    atp_lib.dbg("BEGIN: cmd_cancel")

    if not args.mntpt:
        atp_lib.bail("cancel: no mount point specified")
    if not args.jobid:
        atp_lib.bail("cancel: no job ID specified")

    #
    # TODO: If we fail to get the role from the role file, we can figure it
    # our from the IP address of the host.
    #
    jpath = os.path.join(args.mntpt, atp_config.ATP_JODS_DIR, args.jobid)
    role = atp_job.role_getf(jpath)
    if role == atp_job.ATJ_ROLE_NONE:
        atp_lib.bail("cancel: Cannot determine the role of this node")
    atp_lib.dbg("role_getf '{:s}'", atp_job.role_str[role])

    job = atp_job.atp_path(args.jobid, role, args.mntpt)

    # Load the job state from the file.
    rc = job.load()
    if not rc:
        atp_lib.err("cancel: loading job state '{:s}', err {:d}",
            args.jobid, rc)
        return False

    rc = job.cancel()
    if not rc:
        atp_lib.err("cancel: canceling job '{:s}'", args.jobid)
        return False

    atp_lib.dbg("END: cmd_cancel")
    return rc

#
# 'isactive' sub-command handler.
#
def cmd_isactive(args):
    atp_lib.dbg("BEGIN: cmd_isactive")

    if not args.mntpt:
        atp_lib.bail("isactive: no local mount point specified")
    if not args.jobid:
        atp_lib.bail("isactive: no job ID specified")

    jpath = os.path.join(args.mntpt, atp_config.ATP_JODS_DIR, args.jobid)

    #
    # Find out what's the age limitation for an inactive job.
    #
    if args.age is None:
        try:
            age = int(atp_config.ATP_INACTIVE_PERIOD)
        except:
            age = _ATP_INACTIVE_PERIOD_DEF
    else:
        age = int(args.age)
    atp_lib.dbg("age '{:d}'", age)
    if age <= 0:
        atp_lib.err("isactive: invalid inactivity period specified ({:s}), setting to {:d} seconds",
            str(atp_config.ATP_INACTIVE_PERIOD), _ATP_INACTIVE_PERIOD_DEF)
        age = _ATP_INACTIVE_PERIOD_DEF

    #
    # Get the list of files that are younger than the specified age (in seconds)
    # The list of files is not created by default, unless make_flist flag is
    # set to True.
    #
    nfiles = 0
    make_flist = False
    flist = []
    youngest = ""
    minage = cur_tm = int(time.time())
    atp_lib.dbg("cur_tm {:d}", cur_tm)

    #
    # See if the job directory age matches the age criteria.
    #
    try:
        st = os.stat(jpath)
    except:
        atp_lib.err("isactive: couldn't stat on '{:s}'", jpath)
        return False
    if not stat.S_ISDIR(st.st_mode):
        atp_lib.err("isactive: not a directory '{:s}'", jpath)
        return False
    fileage = cur_tm - st.st_mtime
    if fileage < age:
        atp_lib.dbg("{:s}: {:d}", jpath, int(st.st_mtime))
        nfiles += 1
        if make_flist:
            flist.append(jpath)
        if fileage < minage:
            minage = fileage
            youngest = jpath

    #
    # Scan the files in the job directory for the age criteria.
    #
    for parent, dirs, files in os.walk(jpath):
        for fname in files:
            fpath = parent + "/" + fname
            try:
                st = os.stat(fpath)
            except:
                continue
            if not stat.S_ISREG(st.st_mode):
                continue
            fileage = cur_tm - st.st_mtime
            atp_lib.dbg("{:s}: {:d} minage {:d}", fpath, int(fileage),
                int(minage))
            if fileage < age:
                nfiles += 1
                if make_flist:
                    flist.append(fpath)
            if fileage < minage:
                minage = fileage
                youngest = fpath

    atp_lib.dbg("fileage {:d}: '{:s}'", int(minage), youngest)
    atp_lib.dbg("nfiles {:d}: '{:s}'", nfiles, flist)

    #
    # if there is no file younger than the specified age, the Teleport job
    # is inactive.  Returning 'False' will cause the command to exit with
    # a non-zero error code.
    #
    if nfiles == 0:
        atp_lib.info("isactive: job '{:s}' is active", args.jobid)
        return False

    atp_lib.dbg("END: cmd_isactive")
    return True

#
# 'cleanup' sub-command handler.
#
def cmd_cleanup(args):
    atp_lib.dbg("BEGIN: cmd_cleanup")

    #
    # Cleanup the jobs of the specified mountpoint
    #
    if args.mntpt is not None and args.mntpt:
        return cleanup(args.mntpt, args.f)

    #
    # No mountpoint specified, get the list of mounted dedup FS from mount
    # command, iterate over them and cleanup any inactive Teleport job.
    #
    rv = True
    ps = subprocess.Popen(('mount', '-l', '-t', 'dedup'),
        stdout = subprocess.PIPE)
    (output, errmsg) = ps.communicate()
    if errmsg is not None:
        atp_lib.err("cleanup: errmsg: '{:s}'", errmsg)
    for fs in output.rstrip('\n').split('\n'):
        atp_lib.dbg("mounted dedup FS found: '{:s}'", fs)
        if not fs:
            break
        fields = fs.split()
        mntpt = fields[2]
        atp_lib.dbg("mntpt: '{:s}'", mntpt)
        if not cleanup(mntpt, args.f):
            rv = False

    atp_lib.dbg("END: cmd_cleanup")
    return rv

#
# The main() routine starts here.
#
# Set up argument parser objects.
#
parser = argparse.ArgumentParser()
# -v flag is for verbose
parser.add_argument("-v", action = "store_true", help = argparse.SUPPRESS)
subparsers = parser.add_subparsers(help = "commands")

#
# Define "start" sub-command.
#
start_parser = subparsers.add_parser("start", help="start Teleport")
#
# -k flag is for keeping job directories.
#
# WARNING, WARNING, WARNING: Using -k flag can result in file system blocks
# to be locked down by the Block Data File and hence in FS full situation.
# DO NOT USE THIS FLAG IF YOU ARE _NOT_ GOING TO CLEANUP JOB DIRECTORIES
# MANUALLY YOURSELF AT THE END.
#
# -o: Run only one step at a time and do not go to the next step automatically.
#
start_parser.add_argument("-k", action = "store_true", help = argparse.SUPPRESS)
start_parser.add_argument("-o", action = "store_true", help = argparse.SUPPRESS)
start_parser.add_argument("-V", action = "store_true", help = argparse.SUPPRESS)
start_parser.add_argument("-j", "--jobid", action = "store", help = "Job ID")
start_parser.add_argument("-f", "--src-host", action = "store",
    help = "Source host")
start_parser.add_argument("-t", "--dst-host", action = "store",
    help = "Destination host")
start_parser.add_argument("-s", "--src-path", action = "store",
    help = "Source path")
start_parser.add_argument("-d", "--dst-path", action = "store",
    help = "Destination path")
start_parser.add_argument("-m", "--src-mntpt", action = "store",
    help = "Source file system mount point")
start_parser.add_argument("-M", "--dst-mntpt", action = "store",
    help = "Destination file system mount point")
start_parser.add_argument("-c", "--callback-prog", action = "store",
    help = "Status callback-program")
start_parser.add_argument("-T", "--tag", action = "store",
    help = "Job tag/type")
start_parser.add_argument("-i", "--ignore", type=int, action = "store", default = 0,
    help = "Ignore send job status to USX server")
start_parser.set_defaults(func = cmd_start)

#
# Define "run" sub-command.
#
run_parser = subparsers.add_parser("run", help="Run a Teleport job")
run_parser.add_argument("-j", "--jobid", action = "store", help = "Job ID")
run_parser.add_argument("-m", "--mntpt", action = "store",
    help = "Mount point of the job")
run_parser.add_argument("-d", action = "store_true",
    help = "Run as destination")
run_parser.add_argument("-s", action = "store_true", help = "Run as source")
run_parser.set_defaults(func = cmd_run)

#
# Define "cancel" sub-command.
#
cancel_parser = subparsers.add_parser("cancel",
    help="Cancel an interrupted Teleport job")
cancel_parser.add_argument("-j", "--jobid", action = "store", help = "Job ID")
cancel_parser.add_argument("-m", "--mntpt", action = "store",
    help = "Mount point of the job")
cancel_parser.set_defaults(func = cmd_cancel)

#
# Define "isactive" sub-command.
#
isactive_parser = subparsers.add_parser("isactive",
    help="Verify that a Teleport job is active locally")
isactive_parser.add_argument("-a", "--age", action = "store", help = "Job age")
isactive_parser.add_argument("-j", "--jobid", action = "store", help = "Job ID")
isactive_parser.add_argument("-m", "--mntpt", action = "store",
    help = "Mount point of the job")
isactive_parser.set_defaults(func = cmd_isactive)

#
# Define "cleanup" sub-command.
#
cleanup_parser = subparsers.add_parser("cleanup",
    help="Monitor and kill inactive Teleport jobs")
cleanup_parser.add_argument("-f", action = "store_true",
    help = "Force to remove active jobs")
cleanup_parser.add_argument("-m", "--mntpt", action = "store",
    help = "Mount point of Teleport jobs")
cleanup_parser.set_defaults(func = cmd_cleanup)

#
# Parse the command line
#
args = parser.parse_args()

#
# Enable verbose mode if '-v' is provided on the command line, or if it is
# set in the config file.
#
atp_lib.vflag = args.v
if atp_config.ATP_VERBOSE_MODE != 0:
    atp_lib.vflag = 1

atp_lib.dbg("CLI args: {:s}", str(args))

# Run the specified sub-command
#
rc = args.func(args)

exit(0 if rc else atp_lib.EC_ERR)
