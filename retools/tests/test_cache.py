import unittest
import time

import redis
import redis.client

from nose.tools import raises
from nose.tools import eq_
from mock import Mock
from mock import patch

class TestCacheKey(unittest.TestCase):
    def _makeOne(self):
        from retools.cache import CacheKey
        return CacheKey
    
    def test_key_config(self):
        CK = self._makeOne()('home', 'my_func', '1 2 3', today='2004-02-02')
        eq_(CK.redis_hit_key, 'retools:hits:2004-02-02:home:my_func:1 2 3')

class TestCacheRegion(unittest.TestCase):
    def _makeOne(self):
        from retools.cache import CacheRegion
        return CacheRegion
    
    def _make_Mock(self):
        from retools import Connection
        mock = Mock(spec=Connection)
        return mock
    
    def test_add_region(self):
        CR = self._makeOne()
        CR.add_region('short_term', 60)
        eq_(CR.regions['short_term']['expires'], 60)
        
    
    def test_generate_value(self):
        CR = self._makeOne()
        CR.add_region('short_term', 60)
        mock_conn = self._make_Mock()
        mock_redis = Mock(spec=redis.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_conn.get_default.return_value = mock_redis
        mock_redis.pipeline.return_value = mock_pipeline
        
        mock_pipeline.execute.return_value = [None, '0']
        
        def a_func():
            return "This is a value: %s" % time.time()
        with patch('retools.Connection', mock_conn):
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
        assert 'This is a value' in value
