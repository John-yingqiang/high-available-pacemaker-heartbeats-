#!/usr/bin/python

'''
'''
import os
import signal
import sys
import time
import copy
from abc import ABCMeta, abstractmethod

sys.path.append("/opt/milio/libs/atlas")
from log import debug, info, errormsg


def printcall(func):
    def wrapper_func(*args, **kwargs):
        debug("before %s called." % func.__name__)
        ret = func(*args, **kwargs)
        if isinstance(ret, int) and ret != 0:
            errormsg("after %s called. result: %s" % (func.__name__, ret))
        else:
            debug("after %s called. result: %s" % (func.__name__, ret))
        return ret
    return wrapper_func


def tryfunc(times=3):
    def _try_func_times(func):
        def _try_func(*args, **kwargs):
            for i in range(0, times):
                ret = func(*args, **kwargs)
                #check state
                if ret == 0:
                    break
                time.sleep(5)
                debug('Failed with result %d, start to retry the function [%s]' %(ret, func.__name__))
            return ret
        return _try_func
    return _try_func_times


def singleton(cls):
    instances = {}

    def _singleton(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton


class BaseParam(object):
    """
    This is Template of getting paramters from configuration.
    must implement the <parse> method to wrap the configuration like:

    def test_base_param():
        class TestParam(BaseParam):
            def __init__(self, setup_info):
                BaseParam.__init__(self, setup_info)
                print 'docker __init__'

            def parse(self, vs_setup_info):
                self.name = vs_setup_info['name']
                self.uuid = vs_setup_info['uuid']
                self.arg = 'default'
        setup_info = {'name': 'test', 'uuid': '1234'}
        param = TestParam(setup_info)
        print param.name, param['uuid']
        param['new_arg'] = 'value'
        return 0
    """
    __metaclass__ = ABCMeta

    def __init__(self, setup_info):
        self.parse(setup_info)

    @abstractmethod
    def parse(self, setup_info):
        pass

    def __getitem__(self, key):
        # if key is of invalid type or value, the list values will raise the error
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def __getattr__(self, key):
        return self.__dict__[key]

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def __str__(self):
        return str(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)

    def __contains__(self, value):
        return value in self.__dict__

    def head(self):
        # get the first element
        return self.__dict__[0]

    def tail(self):
        # get all elements after the first
        return self.__dict__[1:]

    def init(self):
        # get elements up to the last
        return self.__dict__[:-1]

    def last(self):
        # get last element
        return self.__dict__[-1]

    def drop(self, n):
        # get all elements except first n
        return self.__dict__[n:]

    def take(self, n):
        # get first n elements
        return self.__dict__[:n]

    @property
    def dict_param(self):
        return copy.deepcopy(self.__dict__)


class BaseFoo(object):
    """
    This base class can support auto print log of calling function like:
    def func(args...):
        ret = 0
        ...
        do something
        ...
        return ret

    Can use it simplely by inheriting the class like:

    class TestClass(BaseFoo):
        def test():
            print 'Test BaseFoo'
            return 0
    """
    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if hasattr(attr, '__call__'):
            def wrapper_func(*args, **kwargs):
                if attr.__name__.startswith('_'):
                    return attr(*args, **kwargs)
                debug('before %s called' % attr.__name__)
                ret = attr(*args, **kwargs)
                if isinstance(ret, int) and ret != 0:
                    errormsg("after %s called. result: %s" % (attr.__name__, ret))
                else:
                    debug("call %s successfully." % attr.__name__)
                return ret
            return wrapper_func
        else:
            return attr


def asyncmethod(func):
    """Summary:fork a child process to deal with the time-consuming method
            and you do not care the result of it.
    >>>
        Sample 1:
        @asyncmethod
        def foo(*args, **kwagrs):
            # do something
            pass

        foo(*args, **kwagrs)

        Sample 2:
        def foo(*args, **kwagrs):
            # do something
            pass

        asyncmethod(foo)(*args, **kwagrs)

    Note:
        the method will redirect standard IO to devnull.

    Args:
        func (function): the aync method .

    Returns:
        int: non-zero return value means failed.
    """
    def wrapper_func(*args, **kwargs):
        try:
            pid = os.fork()
        except:
            debug('ERROR: fork child process to exce the {} method failed.'.format(func.__name__))
            return 1
        if pid > 0:
            # grandparent process return immediately.
            return 0
        elif pid == 0:
            # decouple from parent environment
            os.setsid()
            # ensure future opens won't allocate controlling TTYs.
            try:
                pid = os.fork()
            except:
                os._exit(1)
            if pid > 0:
                # parent process exit immediately.
                os._exit(0)
            else:
                # child process deal this method and ignore the final result of it.
                os.chdir('.')
                os.umask(0)

                # ignore the buffers of standard IO and redirect them to /dev/null
                # sys.stdout.flush()
                # sys.stderr.flush()
                si = file(os.devnull, 'r')
                so = file(os.devnull, 'a+')
                se = so
                os.dup2(si.fileno(), sys.stdin.fileno())
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())

                # handle the os signal
                signal.signal(signal.SIGTERM, lambda signum, frame: os._exit(0))
                signal.signal(signal.SIGINT, lambda signum, frame: os._exit(0))

                # exec the method and exit child process.
                func(*args, **kwargs)
                os._exit(0)
    return wrapper_func
