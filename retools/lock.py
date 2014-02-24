"""A Redis backed distributed global lock

This code uses the formula here:
https://github.com/jeffomatic/redis-exp-lock-js

It provides several improvements over the original version based on:
http://chris-lamb.co.uk/2010/06/07/distributing-locking-python-and-redis/

It provides a few improvements over the one present in the Python redis
library, for example since it utilizes the Lua functionality, it no longer
requires every client to have synchronized time.

"""
# Copyright 2010,2011 Chris Lamb <lamby@debian.org>

import time
import uuid

from retools import global_connection


acquire_lua = """
local result = redis.call('SETNX', KEYS[1], ARGV[1])
if result == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return result"""


release_lua = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class Lock(object):
    def __init__(self, key, expires=60, timeout=10, redis=None):
        """Distributed locking using Redis Lua scripting for CAS operations.

        Usage::

            with Lock('my_lock'):
                print "Critical section"

        :param  expires:    We consider any existing lock older than
                            ``expires`` seconds to be invalid in order to
                            detect crashed clients. This value must be higher
                            than it takes the critical section to execute.
        :param  timeout:    If another client has already obtained the lock,
                            sleep for a maximum of ``timeout`` seconds before
                            giving up. A value of 0 means we never wait.
        :param  redis:      The redis instance to use if the default global
                            redis connection is not desired.

        """
        self.key = key
        self.timeout = timeout
        self.expires = expires
        if not redis:
            redis = global_connection.redis
        self.redis = redis
        self._acquire_lua = redis.register_script(acquire_lua)
        self._release_lua = redis.register_script(release_lua)
        self.lock_key = None

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self):
        """Acquire the lock

        :returns: Whether the lock was acquired or not
        :rtype: bool

        """
        self.lock_key = uuid.uuid4().hex
        timeout = self.timeout
        retry_sleep = 0.005
        while timeout >= 0:
            if self._acquire_lua(keys=[self.key],
                                 args=[self.lock_key, self.expires]):
                return
            timeout -= 1
            if timeout >= 0:
                time.sleep(retry_sleep)
                retry_sleep = min(retry_sleep*2, 1)
        raise LockTimeout("Timeout while waiting for lock")

    def release(self):
        """Release the lock

        This only releases the lock if it matches the UUID we think it
        should have, to prevent deleting someone else's lock if we
        lagged.

        """
        if self.lock_key:
            self._release_lua(keys=[self.key], args=[self.lock_key])
        self.lock_key = None


class LockTimeout(BaseException):
    """Raised in the event a timeout occurs while waiting for a lock"""
