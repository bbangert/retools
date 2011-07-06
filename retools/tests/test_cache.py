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
    
    def test_add_region(self):
        CR = self._makeOne()
        CR.add_region('short_term', 60)
        eq_(CR.regions['short_term']['expires'], 60)
    
    def test_generate_value(self):
        CR = self._makeOne()
        CR.add_region('short_term', 60)
        mock_execute = Mock()
        redis.client.Pipeline.execute = mock_execute
        
        results = ['0', (None, '0')]
        def side_effect(*args, **kwargs):
            return results.pop()
        mock_execute.side_effect = side_effect
        
        def a_func():
            return "This is a value: %s" % time.time()
        value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
        assert 'This is a value' in value
        eq_(mock_execute.call_count, 2)
