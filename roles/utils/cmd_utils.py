#!/usr/bin/env python
import sys, tempfile, re, time, os, stat
from subprocess import *
import math

sys.path.append("/opt/milio/libs/atlas")
from log import debug, errormsg

NODE_INFO_JSON_FILE = '/etc/ilio/usx-node-local-info.json'


def runcmd(
        cmd,
        print_ret=False,
        lines=False,
        input_string=None,
        block=True,
        timeout=None):
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

        if timeout:
            time.sleep(timeout)
            if p.poll() is None:
                return (-1, "")
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
        errormsg('Exception caught in running %s' % cmd)
        return (127, 'OSError')


# sometimes we don't care about the status
def lines(cmd, print_ret=False):
    _, lines = runcmd(cmd, lines=True, print_ret=print_ret)
    if print_ret:
        debug("%s \n %s" % (cmd, lines))
    return lines


def read(cmd, print_ret=False):
    _, ret = runcmd(cmd, lines=False, print_ret=print_ret)
    return ret


def spawn(cmd, *args):
    return Popen([cmd] + list(args)).pid


def psgrep(cmd):
    ps = lines('ps ax')
    ps = [x.strip() for x in ps if re.search(cmd, x)]
    return [re.split(" *", x)[0] for x in ps]


def grep(cmd, s):
    out = lines(cmd)
    return [x for x in out if re.search(s, x)]


def is_new_simple_hybrid():
    is_new = False
    cmd_pvs = 'pvs --noheadings -o pv_name'
    out_put = lines(cmd_pvs, True)
    for l in out_put:
        m = re.search('sdb|xvdb', l)
        if m is not None:
            is_new = True
            break
    return is_new


def docker_make_device(docker_uuid):
    docker_dir = {}
    devname = '/dev/ibd' + str(docker_uuid)
    docker_dir['devname'] = devname
    if not os.path.exists(devname):
        idx = 1
        is_working = True
        while is_working:
            dev_name = '/dev/ibd%s' % idx
            dev = os.makedev(44, int(idx) * 16)
            mode = stat.S_IFBLK | 01660
            oldmask = os.umask(0)
            try:
                os.mknod(dev_name, mode, dev)
            except:
                idx = int(idx) + 1
                continue

            os.umask(oldmask)
            is_working = False
        cmd_link = 'ln -s %s %s' % (dev_name, devname)
        (ret, msg) = runcmd(cmd_link, print_ret=True)
        # docker_dir['minor'] = minor_nu
    try:
        ibdagent_dev_name = os.readlink(devname)
    except:
        return (1, {})
    docker_dir['ibdagent_dev'] = ibdagent_dev_name
    ret, minor_nu = get_block_device_minor_number(ibdagent_dev_name)
    if ret != 0:
        return (ret, docker_dir)
    docker_dir['minornum'] = minor_nu
    return (ret, docker_dir)


def get_block_device_minor_number(device_name):
    minor_number = ''
    try:
        os_mode = os.stat(device_name).st_mode
        if stat.S_ISBLK(os_mode):
            dev_r = os.stat(device_name).st_rdev
            minor_number = os.minor(dev_r)
            return 0, minor_number
    except Exception as e:
        errormsg('get device minor number failed with %s' % e)
    return 1, minor_number


def retry_cmd(cmd, retry_num, timeout, ip=None):
    debug('Enter retry_cmd: %s retry_num: %d timeout: %d' % (cmd, retry_num, timeout))
    cnt = retry_num
    ret = 0
    msg = []
    while cnt > 0:
        debug('ip address %s' % ip)
        if ip is not None:
            ret, msg = Remote_ssh().exec_cmd(ip, cmd)
        else:
            (ret, msg) = runcmd(cmd, print_ret=True, lines=True)
        if ret == 0:
            break
        cnt -= 1
        if cnt > 0:
            time.sleep(timeout)
    return (ret, msg)


def get_blockdev_size_byte(dev_name):
    cmd = 'blockdev --getsz %s' % dev_name
    (ret, msg) = runcmd(cmd)
    if ret == 0:
        dev_size = long(msg.split('\n')[0]) * 512
        debug('get  %s block size %s bytes' % (dev_name, dev_size))
        return dev_size
    debug('ERROR: get block device [%s] size failed' % dev_name)
    return 0


def stop_ibdserver():
    debug('stop ibdserver!')
    cmd_stop = '/bin/ibdmanager -r s -S'
    runcmd(cmd_stop, print_ret=True)
    # Check the process again.
    try:
        waiting_for_ibdserver_stop(30)
    except Exception as e:
        cmd_run = 'ps -ef | grep ibdserver | grep -v upgrade_ibdserver | grep -v grep'
        cmd_kill = 'pkill -9 ibdserver'
        (ret, msg) = runcmd(cmd_run, print_ret=True)
        if ret == 0:
            runcmd(cmd_kill, print_ret=True)
        waiting_for_ibdserver_stop(60)
    else:
        debug('successfully stop ibdserver')


def waiting_for_ibdserver_stop(time_out):
    debug('wait for ibdserver normal closed.')
    deadtime = time.time() + int(time_out)
    while time.time() < deadtime:
        cmd_run = 'ps -ef | grep ibdserver | grep -v upgrade_ibdserver | grep -v grep'
        (ret, msg) = runcmd(cmd_run, print_ret=True)
        if ret != 0:
            debug('ibdserver was closed clearly')
            break
        time.sleep(0.5)
    else:
        raise OSError('waiting for ibdserver closed more than 60 seconds.')


class BlockDevice(object):
    def __init__(self, dev_name):
        self._name = dev_name
        self._get_size()

    @property
    def name(self):
        return self._name

    @property
    def size_byte(self):
        return self._size_byte

    @property
    def size_kb(self):
        return long(math.floor(self._size_kb))

    @property
    def size_mb(self):
        return long(math.floor(self._size_mb))

    @property
    def size_gb(self):
        return long(math.floor(self._size_gb))

    def _get_size(self):
        self._size_byte = get_blockdev_size_byte(self._name)
        self._size_kb = self._size_byte / 1024.0
        self._size_mb = self._size_byte / 1024.0 / 1024.0
        self._size_gb = self._size_byte / 1024.0 / 1024.0 / 1024.0
