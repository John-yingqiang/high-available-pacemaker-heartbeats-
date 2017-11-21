#!/usr/bin/python
'''
'''

from comm_utils import singleton
import sys
sys.path.append("/opt/usx-cli/usx-cli/libs/atlas")
from log import debug, info, errormsg
import fcntl
import time

DEFAULT_FILE_PATH = '/tmp/default-proc.lock'


def synclock(filepath=DEFAULT_FILE_PATH):
    def _synclock(func):
        def _filelock(*args, **kwargs):
            while True:
                rc = FileLock().lock(filepath)
                if rc == 0:
                    break
                time.sleep(1)
            debug('lock success, hold the lock %s' % filepath)
            rc = func(*args, **kwargs)
            FileLock().unlock(filepath)
            debug('unlock the %s' % filepath)
            return rc
        return _filelock
    return _synclock


def asynclock(filepath=DEFAULT_FILE_PATH):
    def _asynclock(func):
        def _filelock(*args, **kwargs):
            rc = FileLock().lock(filepath)
            if rc == 0:
                debug('try to lock success, hold the lock %s' % filepath)
                rc = func(*args, **kwargs)
                FileLock().unlock(filepath)
            else:
                debug('try to lock failed, can not hold the lock %s' % filepath)
            return rc
        return _filelock
    return _asynclock


def asynclockCustomized(func):
    def _filelock(*args, **kwargs):
        filepath = "/tmp/{func}_{parameters}".format(func=func.__name__, parameters=''.join(args).replace('/', '_'))
        rc = FileLock().lock(filepath)
        if rc == 0:
            debug('try to lock success, hold the lock %s' % filepath)
            rc = func(*args, **kwargs)
            FileLock().unlock(filepath)
        else:
            debug('try to lock failed, can not hold the lock %s' % filepath)
        return rc
    return _filelock


class SimpleLock(object):

    """
    If need to do simple sync job, can use this lock.
    The following is the example about usage:

    class TestLock(object):
        def __init__(self):
            # self._lock = SimpleLock(lock_file_path)
            self._lock = SimpleLock()

        def run_1(self):
            with self._lock:
                print 'test 1'

        def run_2(self):
            with self._lock:
                print 'test 2'
    """

    def __init__(self, lock_file_path=DEFAULT_FILE_PATH):
        self.lock_file = lock_file_path

    def __enter__(self):
        while True:
            ret = FileLock().lock(self.lock_file)
            if ret == 0:
                break
            time.sleep(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        FileLock().unlock(self.lock_file)


@singleton
class FileLock:
    def __init__(self):
        self.lockfds = {}

    def __del__(self):
        for (filename, fd) in self.lockfds.items():
            fd.close()

    def lock(self, filepath):
        if filepath in self.lockfds:
            fd = self.lockfds[filepath]
        else:
            fd = open(filepath, 'w')
            self.lockfds[filepath] = fd
        try:
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            rc = 0
        except:
            rc = 10001
        return rc

    def unlock(self, filepath):
        if filepath in self.lockfds:
            fd = self.lockfds[filepath]
            fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
        else:
            errormsg('unlock error, does not match the lock [lock file: %s]' % filepath)
