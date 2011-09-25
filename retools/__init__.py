"""retools

This module holds a default Redis instance, which can be configured
process-wide::

    from redis import Redis
    from retools import global_connection

    global_connection.redis = Redis(host='192.168.1.1', db=2)

Alternatively, many parts of retools accept Redis instances that may be passed
directly.

"""
from redis import Redis

__all__ = ['Connection']


class Connection(object):
    """The default Redis Connection

    A :obj:`retools.global_connection` object is created using this
    during import. The ``.redis`` property can be set on it to change
    the connection used globally by retools, or individual ``retools``
    functions can be called with a custom ``Redis`` object.

    """
    def __init__(self):
        self._redis = None

    def _get_redis(self):
        if not self._redis:
            self._redis = Redis()
        return self._redis

    def _set_redis(self, conn):
        self._redis = conn

    redis = property(_get_redis, _set_redis)

global_connection = Connection()
