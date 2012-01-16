"""A Redis backed distributed global lock

This lock based mostly on this excellent example:
http://chris-lamb.co.uk/2010/06/07/distributing-locking-python-and-redis/

This code add's one change as suggested by the Redis documentation regarding
using locks in Redis, which is to only delete the Redis lock if we actually
completed within the timeout period. If we took too long to execute, then the
lock stored here is actually from a *different* client holding a lock and
we shouldn't be deleting their lock.

"""
# Copyright 2010,2011 Chris Lamb <lamby@debian.org>

import time

from retools import global_connection


class Lock(object):
    def __init__(self, key, expires=60, timeout=10, redis=None):
        """
        Distributed locking using Redis SETNX and GETSET.

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
        self.start_time = time.time()

    def __enter__(self):
        redis = self.redis
        timeout = self.timeout
        while timeout >= 0:
            expires = time.time() + self.expires + 1

            if redis.setnx(self.key, expires):
                # We gained the lock; enter critical section
                self.start_time = time.time()
                redis.expire(self.key, int(expires))
                return

            current_value = redis.get(self.key)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
               redis.getset(self.key, expires) == current_value:
                self.start_time = time.time()
                redis.expire(self.key, int(expires))
                return

            timeout -= 1
            if timeout >= 0:
                time.sleep(1)
        raise LockTimeout("Timeout while waiting for lock")

    def __exit__(self, exc_type, exc_value, traceback):
        # Only delete the key if we completed within the lock expiration,
        # otherwise, another lock might've been established
        if time.time() - self.start_time < self.expires:
            self.redis.delete(self.key)


class LockTimeout(BaseException):
    """Raised in the event a timeout occurs while waiting for a lock"""
