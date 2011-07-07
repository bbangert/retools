"""retools

This module holds a default Redis instance, which can be
configured process-wide::
    
    from retools import Connection
    
    Connection.set_default(host='127.0.0.1', db=0, **kwargs)

"""
from redis import Redis

__all__ = ['Connection']


class Connection(object):
    # For testing
    Redis = Redis
    
    redis = None
    
    @classmethod
    def set_default(cls, host='localhost', port=6379, db=0, password=None):
        cls.redis = cls.Redis(host=host, port=port, db=db,
                                 password=password)

    @classmethod
    def get_default(cls):
        if not cls.redis:
            cls.redis = cls.Redis()
        return cls.redis
