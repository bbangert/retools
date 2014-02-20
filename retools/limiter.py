"""Generic Limiter to ensure N parallel operations

.. note::

    The limiter functionality is new.
    Please report any issues found on `the retools Github issue
    tracker <https://github.com/bbangert/retools/issues>`_.

The limiter is useful when you want to make sure that only N operations for a given process happen at the same time,
i.e.: concurrent requests to the same domain.

The limiter works by acquiring and releasing limits.

Creating a limiter::

    from retools.limiter import Limiter

    def do_something():
        limiter = Limiter(limit=10, prefix='my-operation')  # using default redis connection

        for i in range(100):
            if limiter.acquire_limit('operation-%d' % i):
                execute_my_operation()
                limiter.release_limit('operation-%d' % i)  # since we are releasing it synchronously
                                                           # all the 100 operations will be performed with
                                                           # one of them locked at a time

Specifying a default expiration in seconds::

    def do_something():
        limiter = Limiter(limit=10, expiration_in_seconds=45)  # using default redis connection

Specifying a redis connection::

    def do_something():
        limiter = Limiter(limit=10, redis=my_redis_connection)

Every time you try to acquire a limit, the expired limits you previously acquired get removed from the set.

This way if your process dies in the mid of its operation, the keys will eventually expire.
"""

import time

import redis

from retools import global_connection
from retools.util import flip_pairs


class Limiter(object):
    '''Configures and limits operations'''
    def __init__(self, limit, redis=None, prefix='retools_limiter', expiration_in_seconds=10):
        """Initializes a Limiter.

        :param limit: An integer that describes the limit on the number of items
        :param redis: A Redis instance. Defaults to the redis instance
                      on the global_connection.
        :param prefix: The default limit set name. Defaults to 'retools_limiter'.
        :param expiration_in_seconds: The number in seconds that keys should be locked if not
                            explicitly released.
        """

        self.limit = limit
        self.redis = redis or global_connection.redis
        self.prefix = prefix
        self.expiration_in_seconds = expiration_in_seconds

    def acquire_limit(self, key, expiration_in_seconds=None, retry=True):
        """Tries to acquire a limit for a given key. Returns True if the limit can be acquired.

        :param key: A string with the key to acquire the limit for.
                    This key should be used when releasing.
        :param expiration_in_seconds: The number in seconds that this key should be locked if not
                            explicitly released. If this is not passed, the default is used.
        :param key: Internal parameter that specifies if the operation should be retried.
                        Defaults to True.
        """

        limit_available = self.redis.zcard(self.prefix) < self.limit

        if limit_available:
            self.__lock_limit(key, expiration_in_seconds)
            return True

        if retry:
            self.redis.zremrangebyscore(self.prefix, '-inf', time.time())
            return self.acquire_limit(key, expiration_in_seconds, retry=False)

        return False

    def release_limit(self, key):
        """Releases a limit for a given key.

        :param key: A string with the key to release the limit on.
        """

        self.redis.zrem(self.prefix, key)

    def __lock_limit(self, key, expiration_in_seconds=None):
        expiration = expiration_in_seconds or self.expiration_in_seconds
        self.__zadd(self.prefix, key, time.time() + expiration)

    def __zadd(self, set_name, *args, **kwargs):
        """
        Custom ZADD interface that adapts to match the argument order of the currently
        used backend.  Using this method makes it transparent whether you use a Redis
        or a StrictRedis connection.

        Found this code at https://github.com/ui/rq-scheduler/pull/17.
        """
        conn = self.redis

        # If we're dealing with StrictRedis, flip each pair of imaginary
        # (name, score) tuples in the args list
        if conn.__class__ is redis.StrictRedis:  # StrictPipeline is a subclass of StrictRedis, too
            args = tuple(flip_pairs(args))

        return conn.zadd(set_name, *args)
