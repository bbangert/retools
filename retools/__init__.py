"""retools

This module holds a default Redis instance, which can be
configured process-wide::
    
    from retools import Connection
    
    Connection.set_default(host='127.0.0.1', db=0, **kwargs)

"""
from redis import Redis

__all__ = ['Connection']


class Connection(object):
    redis = None
    
    @classmethod
    def set_default(cls, host='localhost', port=6379, db=0, password=None):
        Connection.redis = Redis(host=host, port=port, db=db,
                                 password=password)

    @classmethod
    def get_default(cls):
        if cls.redis:
            return cls.redis
        else:
            cls.redis = Redis()
            return cls.redis
