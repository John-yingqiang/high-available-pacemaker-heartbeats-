#!/usr/bin/python
from daemon import Daemon
from subprocess import *
import tempfile
import sys
import time
import os
import signal
#import logging

sys.path.insert(0, "/opt/milio/libs/atlas")
from log import *

PIDFILE = '/var/run/ha_daemon.pid'
LOG_FILENAME = '/var/log/usx-ha_daemon.log'
IO_WAIT_TIMEOUT = 20      # IO wait timeout 

ioping_cmd = '/usr/bin/ioping'
echo_cmd = '/bin/echo'

set_log_file(LOG_FILENAME)

'''
# Configure logging
#logging.basicConfig(filename=LOGFILE,level=logging.DEBUG)
logging.basicConfig(filename=LOGFILE,
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

def _debug(*args):
    logging.debug(''.join([str(x) for x in args]))
    print ''.join([str(x) for x in args])
'''

def runcmd(
    cmd,
    print_ret=False,
    lines=False,
    input_string=None,
    block=True,
    ):
    if print_ret:
        debug('Running: %s' % cmd)
    try:
        if not block:
            p = Popen([cmd], shell=True, close_fds=True)
            return (0, "")

        tmpfile = tempfile.TemporaryFile()
        p = Popen(
            [cmd],
            stdin=PIPE,
            stdout=tmpfile,
            stderr=STDOUT,
            shell=True,
            close_fds=True,
            )
        (out, err) = p.communicate(input_string)
        status = p.returncode
        tmpfile.flush()
        tmpfile.seek(0)
        out = tmpfile.read()
        tmpfile.close()

        if lines and out:
            out = [line for line in out.split('\n') if line != '']

        if print_ret:
            debug(' -> %s: %s: %s' % (status, err, out))
        return (status, out)
    except OSError:
        return (127, 'OSError')


def runcmd_nonblock(cmd, print_ret=False):
    if print_ret:
        debug('Running: %s' % cmd)
    try:
        tmpfile = tempfile.TemporaryFile()
        p = Popen(
            [cmd],
            stdin=PIPE,
            stdout=tmpfile,
            stderr=STDOUT,
            shell=True,
            close_fds=True,
            )
        time.sleep(5)
        if p.poll() is None:
            return 1
        return 0
    except OSError:
        debug('Exception with Popen')
        return -1

def timeout_handler(signum, frame):
    #reset_vm('local_storage_reset')
    raise IOError("Couldn't open local storage!")


class HaDaemon(Daemon):

    def run(self):
        global ioping_cmd
        global echo_cmd
        
        if os.path.exists('/run/shm/ioping'):
            ioping_cmd = '/run/shm/ioping'
        if os.path.exists('/run/shm/echo'):
            echo_cmd = '/run/shm/echo'

        # Define HA Daemon tasks here
        local_storage = None
        storage_status = "healthy"
        cmd = 'df -P / | tail -n 1 | awk \'/.*/ { print $1 }\''
        (ret,msg) = runcmd(cmd,print_ret=True,lines=True)
        for dev in msg:
            local_storage = dev
            break

        while True:
            # Set the signal handler for timeout alarm
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(IO_WAIT_TIMEOUT)

            cmd = ioping_cmd + ' -A -D -c 1 -s 512 ' + dev
            #(ret,msg) = runcmd(cmd, print_ret=False)
            ret = runcmd_nonblock(cmd, print_ret=False)
            if ret != 0:
                storage_status = "fatal"
                print dev + " is " + storage_status
            else:
                storage_status = "healthy"
            #print dev + " is " + storage_status
            signal.alarm(0)
            time.sleep(20)


if __name__ == "__main__":

    daemon = HaDaemon(PIDFILE)

    if len(sys.argv) == 2:

        if 'start' == sys.argv[1]:
            try:
                daemon.start()
            except:
                pass

        elif 'stop' == sys.argv[1]:
            print "Stopping ..."
            daemon.stop()

        elif 'restart' == sys.argv[1]:
            print "Restaring ..."
            daemon.restart()

        elif 'status' == sys.argv[1]:
            try:
                pf = file(PIDFILE,'r')
                pid = int(pf.read().strip())
                pf.close()
            except IOError:
                pid = None
            except SystemExit:
                pid = None

            if pid:
                print 'HaDaemon is running as pid %s' % pid
            else:
                print 'HaDaemon is not running.'

        else:
            print "Unknown command"
            sys.exit(2)
            sys.exit(0)
    else:
        print "usage: %s start|stop|restart|status" % sys.argv[0]
        sys.exit(2)


