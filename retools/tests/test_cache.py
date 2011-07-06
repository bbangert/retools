import unittest
import time
import cPickle

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
        mock_redis = Mock()
        mock_pipeline = Mock()
        results = ['0', (None, '0')]
        def side_effect(*args, **kwargs):
            return results.pop()
        
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.Connection.get_default') as mock:
            mock.return_value = mock_redis
            CR = self._makeOne()
            CR.add_region('short_term', 60)
            
            def a_func():
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls if x[0] == 'execute']
            eq_(len(exec_calls), 2)
    
    def test_existing_value(self):
        mock_redis = Mock()
        mock_pipeline = Mock()
        now = time.time()
        results = ['0', ({'created': now, 'value': cPickle.dumps("This is a value")}, '0')]
        def side_effect(*args, **kwargs):
            return results.pop()
        
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.Connection.get_default') as mock:
            mock.return_value = mock_redis
            CR = self._makeOne()
            CR.add_region('short_term', 60)
            
            called = []
            def a_func(): # pragma: nocover
                called.append(1) 
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            eq_(called, [])
            exec_calls = [x for x in mock_pipeline.method_calls if x[0] == 'execute']
            eq_(len(exec_calls), 1)

    def test_new_value_and_misses(self):
        mock_redis = Mock()
        mock_pipeline = Mock()
        now = time.time()
        results = [None, ['30'], (None, '0')]
        def side_effect(*args, **kwargs):
            return results.pop()
        
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.Connection.get_default') as mock:
            mock.return_value = mock_redis
            CR = self._makeOne()
            CR.add_region('short_term', 60)
            
            called = []
            def a_func(): #pragma: nocover
                called.append(1)
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls if x[0] == 'execute']
            eq_(len(exec_calls), 3)
            
            # Check that we increment the miss counter by 30
            last_incr_call = filter(lambda x: x[0] == 'incr', mock_pipeline.method_calls)[-1]
            eq_(last_incr_call[2], {'amount': 30})
