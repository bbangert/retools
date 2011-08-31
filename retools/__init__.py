"""retools

This module holds a default Redis instance, which can be configured
process-wide::

    from retools import Connection

    Connection.set_default(host='127.0.0.1', db=0, **kwargs)

Alternatively, many parts of retools accept Redis instances that may be passed
directly.

"""
from redis import Redis

__all__ = ['Connection']


class Connection(object):
    # For testing
    Redis = Redis
    
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self._redis_params = dict(host=host, port=port, db=db,
                                  password=password)
        self._redis = None
    
    @property
    def redis(self):
        if not self._redis:
            self._redis = Connection.Redis(**self._redis_params)
        return self._redis


global_connection = Connection()
