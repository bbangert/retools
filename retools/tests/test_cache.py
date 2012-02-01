# coding: utf-8
import unittest
import time
import cPickle
from contextlib import nested

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
        CacheRegion.enabled = True
        return CacheRegion

    def _marker(self):
        from retools.cache import NoneMarker
        return NoneMarker

    def test_add_region(self):
        CR = self._makeOne()
        CR.add_region('short_term', 60)
        eq_(CR.regions['short_term']['expires'], 60)

    def test_generate_value(self):
        mock_redis = Mock(spec=redis.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', (None, '0')]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            def a_func():
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(mock_pipeline.method_calls), 11)
            eq_(len(exec_calls), 2)

    def test_existing_value_no_regen(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', ({'created': '111',
                          'value': "S'This is a value: 1311702429.28'\n."},
                   '0')]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)
            value = CR.load('short_term', 'my_func', '1 2 3', regenerate=False)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(mock_pipeline.method_calls), 4)
            eq_(len(exec_calls), 1)

    def test_value_created_after_check_but_expired(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', (None, '0')]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {'created': '1',
              'value': "S'This is a value: 1311702429.28'\n."}
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            def a_func():
                return "This is a value: %s" % time.time()

            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(mock_pipeline.method_calls), 11)
            eq_(len(exec_calls), 2)

    def test_value_expired_and_no_lock(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', ({'created': '111',
                          'value': "S'This is a value: 1311702429.28'\n."},
                   '0')]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        mock_redis.exists.return_value = False
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            def a_func():
                return "This is a value: %s" % time.time()

            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(mock_pipeline.method_calls), 11)
            eq_(len(exec_calls), 2)

    def test_generate_value_no_stats(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', (None, '0')]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            now = time.time()

            def a_func():
                return "This is a value: %s" % now
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func,
                            statistics=False)
            assert 'This is a value' in value
            assert str(now) in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                if x[0] == 'execute']
            eq_(len(mock_pipeline.method_calls), 6)
            eq_(len(exec_calls), 1)

    def test_generate_value_other_creator(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        now = time.time()
        results = ['0', (None, None)]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {'created': now,
              'value': cPickle.dumps("This is a NEW value")}
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            def a_func():  # pragma: nocover
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a NEW value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                if x[0] == 'execute']
            eq_(len(exec_calls), 1)

    def test_existing_value(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        now = time.time()
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.return_value = ({'created': now,
              'value': cPickle.dumps("This is a value")}, '0')
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            called = []

            def a_func():  # pragma: nocover
                called.append(1)
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            eq_(called, [])
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(exec_calls), 1)

    def test_existing_expired_value_with_lock_timeout(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        now = time.time()
        results = [0, ({'created': now - 200,
                        'value': cPickle.dumps("This is a value")},
                   '0')]

        def side_effect(*args):
            return results.pop()

        # Mock up an existing lock
        mock_redis.setnx.return_value = False
        mock_redis.get.return_value = False

        # Other mocks
        mock_redis.exists.return_value = True
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            called = []

            def a_func():  # pragma: nocover
                called.append(1)
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            eq_(called, [])
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(exec_calls), 1)

    def test_no_value_with_lock_timeout(self):
        from retools.cache import NoneMarker
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = [0, ({}, '0')]

        def side_effect(*args):
            return results.pop()

        # Mock up an existing lock
        mock_redis.setnx.return_value = False
        mock_redis.get.return_value = False

        # Other mocks
        mock_redis.exists.return_value = True
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect

        mock_sleep = Mock(spec=time.sleep)

        with nested(
            patch('retools.global_connection._redis', mock_redis),
            patch('time.sleep', mock_sleep)
            ):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            called = []

            def a_func():  # pragma: nocover
                called.append(1)
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            eq_(value, NoneMarker)
            eq_(called, [])
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(exec_calls), 1)

    def test_new_value_and_misses(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = [None, ['30'], (None, '0')]

        def side_effect(*args, **kwargs):
            return results.pop()

        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            called = []

            def a_func():  # pragma: nocover
                called.append(1)
                return "This is a value: %s" % time.time()
            value = CR.load('short_term', 'my_func', '1 2 3', callable=a_func)
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(exec_calls), 3)

            # Check that we increment the miss counter by 30
            last_incr_call = filter(lambda x: x[0] == 'incr',
                                    mock_pipeline.method_calls)[-1]
            eq_(last_incr_call[2], {'amount': 30})

    def test_return_marker(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.return_value = (None, '0')
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            value = CR.load('short_term', 'my_func', '1 2 3', regenerate=False)
            eq_(value, self._marker())
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(exec_calls), 1)


class TestInvalidateRegion(unittest.TestCase):
    def _makeOne(self):
        from retools.cache import invalidate_region
        return invalidate_region

    def _makeCR(self):
        from retools.cache import CacheRegion
        CacheRegion.regions = {}
        return CacheRegion

    def test_invalidate_region_empty(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_redis.smembers.return_value = set([])

        invalidate_region = self._makeOne()
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeCR()
            CR.add_region('short_term', expires=600)

            invalidate_region('short_term')
            eq_(len(mock_redis.method_calls), 1)

    def test_invalidate_small_region(self):
        mock_redis = Mock(spec=redis.client.Redis)
        results = [set(['keyspace']), set(['a_func'])]

        def side_effect(*args):
            return results.pop()

        mock_redis.smembers.side_effect = side_effect

        invalidate_region = self._makeOne()
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeCR()
            CR.add_region('short_term', expires=600)

            invalidate_region('short_term')
            calls = mock_redis.method_calls
            eq_(calls[0][1], ('retools:short_term:namespaces',))
            eq_(len(calls), 6)

    def test_remove_nonexistent_key(self):
        mock_redis = Mock(spec=redis.client.Redis)
        results = [set(['keyspace']), set(['a_func'])]

        def side_effect(*args):
            return results.pop()

        mock_redis.smembers.side_effect = side_effect
        mock_redis.exists.return_value = False

        invalidate_region = self._makeOne()
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeCR()
            CR.add_region('short_term', expires=600)

            invalidate_region('short_term')
            calls = mock_redis.method_calls
            eq_(calls[0][1], ('retools:short_term:namespaces',))
            eq_(len(calls), 6)


class TestInvalidFunction(unittest.TestCase):
    def _makeOne(self):
        from retools.cache import invalidate_function
        return invalidate_function

    def _makeCR(self):
        from retools.cache import CacheRegion
        CacheRegion.regions = {}
        return CacheRegion

    def test_invalidate_function_without_args(self):

        def my_func():  # pragma: nocover
            return "Hello"
        my_func._region = 'short_term'
        my_func._namespace = 'retools:a_key'

        mock_redis = Mock(spec=redis.client.Redis)
        mock_redis.smembers.return_value = set(['1'])

        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline

        invalidate_function = self._makeOne()
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeCR()
            CR.add_region('short_term', expires=600)

            invalidate_function(my_func)
            calls = mock_redis.method_calls
            eq_(calls[0][1], ('retools:short_term:retools:a_key:keys',))
            eq_(len(calls), 2)

    def test_invalidate_function_with_args(self):

        def my_func(name):  # pragma: nocover
            return "Hello %s" % name
        my_func._region = 'short_term'
        my_func._namespace = 'retools:a_key decarg'

        mock_redis = Mock(spec=redis.client.Redis)
        mock_redis.smembers.return_value = set(['1'])

        invalidate_function = self._makeOne()
        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeCR()
            CR.add_region('short_term', expires=600)

            invalidate_function(my_func, 'fred')
            calls = mock_redis.method_calls
            eq_(calls[0][1][0], 'retools:short_term:retools:a_key decarg:fred')
            eq_(calls[0][0], 'hset')
            eq_(len(calls), 1)

            # And a unicode key
            mock_redis.reset_mock()
            invalidate_function(my_func,
                  u"\u03b5\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ac")
            calls = mock_redis.method_calls
            eq_(calls[0][1][0],
                  u'retools:short_term:retools:a_key' \
                  u' decarg:\u03b5\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ac')
            eq_(calls[0][0], 'hset')
            eq_(len(calls), 1)


class TestCacheDecorator(unittest.TestCase):
    def _makeOne(self):
        from retools.cache import CacheRegion
        CacheRegion.enabled = True
        CacheRegion.regions = {}
        return CacheRegion

    def _decorateFunc(self, func, *args):
        from retools.cache import cache_region
        return cache_region(*args)(func)

    def test_no_region(self):
        from retools.exc import CacheConfigurationError

        @raises(CacheConfigurationError)
        def test_it():
            CR = self._makeOne()
            CR.add_region('short_term', 60)

            def dummy_func():  # pragma: nocover
                return "This is a value: %s" % time.time()
            decorated = self._decorateFunc(dummy_func, 'long_term')
            decorated()
        test_it()

    def test_generate(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', (None, '0')]

        def side_effect(*args, **kwargs):
            return results.pop()
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}

        def dummy_func():
            return "This is a value: %s" % time.time()

        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)
            decorated = self._decorateFunc(dummy_func, 'short_term')
            value = decorated()
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                if x[0] == 'execute']
            eq_(len(exec_calls), 2)

    def test_cache_disabled(self):
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline

        def dummy_func():
            return "This is a value: %s" % time.time()

        with patch('retools.global_connection._redis', mock_redis):
            CR = self._makeOne()
            CR.add_region('short_term', 60)
            CR.enabled = False
            decorated = self._decorateFunc(dummy_func, 'short_term')
            value = decorated()
            assert 'This is a value' in value
            exec_calls = [x for x in mock_pipeline.method_calls \
                  if x[0] == 'execute']
            eq_(len(exec_calls), 0)

    def test_unicode_keys(self):
        keys = [
            # arabic (egyptian)
            u"\u0644\u064a\u0647\u0645\u0627\u0628\u062a\u0643\u0644\u0645" \
            u"\u0648\u0634\u0639\u0631\u0628\u064a\u061f",
            # Chinese (simplified)
            u"\u4ed6\u4eec\u4e3a\u4ec0\u4e48\u4e0d\u8bf4\u4e2d\u6587",
            # Chinese (traditional)
            u"\u4ed6\u5011\u7232\u4ec0\u9ebd\u4e0d\u8aaa\u4e2d\u6587",
            # czech
            u"\u0050\u0072\u006f\u010d\u0070\u0072\u006f\u0073\u0074\u011b" \
            u"\u006e\u0065\u006d\u006c\u0075\u0076\u00ed\u010d\u0065\u0073" \
            u"\u006b\u0079",
            # hebrew
            u"\u05dc\u05de\u05d4\u05d4\u05dd\u05e4\u05e9\u05d5\u05d8\u05dc" \
            u"\u05d0\u05de\u05d3\u05d1\u05e8\u05d9\u05dd\u05e2\u05d1\u05e8" \
            u"\u05d9\u05ea",
            # Hindi (Devanagari)
            u"\u092f\u0939\u0932\u094b\u0917\u0939\u093f\u0928\u094d\u0926" \
            u"\u0940\u0915\u094d\u092f\u094b\u0902\u0928\u0939\u0940\u0902" \
            u"\u092c\u094b\u0932\u0938\u0915\u0924\u0947\u0939\u0948\u0902",
            # Japanese (kanji and hiragana)
            u"\u306a\u305c\u307f\u3093\u306a\u65e5\u672c\u8a9e\u3092\u8a71" \
            u"\u3057\u3066\u304f\u308c\u306a\u3044\u306e\u304b",
            # Russian (Cyrillic)
            u"\u043f\u043e\u0447\u0435\u043c\u0443\u0436\u0435\u043e\u043d" \
            u"\u0438\u043d\u0435\u0433\u043e\u0432\u043e\u0440\u044f\u0442" \
            u"\u043f\u043e\u0440\u0443\u0441\u0441\u043a\u0438",
            # Spanish
            u"\u0050\u006f\u0072\u0071\u0075\u00e9\u006e\u006f\u0070\u0075" \
            u"\u0065\u0064\u0065\u006e\u0073\u0069\u006d\u0070\u006c\u0065" \
            u"\u006d\u0065\u006e\u0074\u0065\u0068\u0061\u0062\u006c\u0061" \
            u"\u0072\u0065\u006e\u0045\u0073\u0070\u0061\u00f1\u006f\u006c",
            # Vietnamese
            u"\u0054\u1ea1\u0069\u0073\u0061\u006f\u0068\u1ecd\u006b\u0068" \
            u"\u00f4\u006e\u0067\u0074\u0068\u1ec3\u0063\u0068\u1ec9\u006e" \
            u"\u00f3\u0069\u0074\u0069\u1ebf\u006e\u0067\u0056\u0069\u1ec7" \
            u"\u0074",
            # Japanese
            u"\u0033\u5e74\u0042\u7d44\u91d1\u516b\u5148\u751f",
            # Japanese
            u"\u5b89\u5ba4\u5948\u7f8e\u6075\u002d\u0077\u0069\u0074\u0068" \
            u"\u002d\u0053\u0055\u0050\u0045\u0052\u002d\u004d\u004f\u004e" \
            u"\u004b\u0045\u0059\u0053",
            # Japanese
            u"\u0048\u0065\u006c\u006c\u006f\u002d\u0041\u006e\u006f\u0074" \
            u"\u0068\u0065\u0072\u002d\u0057\u0061\u0079\u002d\u305d\u308c" \
            u"\u305e\u308c\u306e\u5834\u6240",
            # Japanese
            u"\u3072\u3068\u3064\u5c4b\u6839\u306e\u4e0b\u0032",
            # Japanese
            u"\u004d\u0061\u006a\u0069\u3067\u004b\u006f\u0069\u3059\u308b" \
            u"\u0035\u79d2\u524d",
            # Japanese
            u"\u30d1\u30d5\u30a3\u30fc\u0064\u0065\u30eb\u30f3\u30d0",
            # Japanese
            u"\u305d\u306e\u30b9\u30d4\u30fc\u30c9\u3067",
            # greek
            u"\u03b5\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ac",
            # Maltese (Malti)
            u"\u0062\u006f\u006e\u0121\u0075\u0073\u0061\u0127\u0127\u0061",
            # Russian (Cyrillic)
            u"\u043f\u043e\u0447\u0435\u043c\u0443\u0436\u0435\u043e\u043d" \
            u"\u0438\u043d\u0435\u0433\u043e\u0432\u043e\u0440\u044f\u0442" \
            u"\u043f\u043e\u0440\u0443\u0441\u0441\u043a\u0438"
        ]
        mock_redis = Mock(spec=redis.client.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        results = ['0', (None, '0')]

        def side_effect(*args, **kwargs):
            return results.pop()
        mock_redis.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.side_effect = side_effect
        mock_redis.hgetall.return_value = {}

        def dummy_func(arg):
            return "This is a value: %s" % time.time()

        for key in keys:
            with patch('retools.global_connection._redis', mock_redis):
                CR = self._makeOne()
                CR.add_region('short_term', 60)
                decorated = self._decorateFunc(dummy_func, 'short_term')
                value = decorated(key)
                assert 'This is a value' in value
                exec_calls = [x for x in mock_pipeline.method_calls \
                      if x[0] == 'execute']
                eq_(len(exec_calls), 2)
            mock_pipeline.reset_mock()
            results.extend(['0', (None, '0')])

        for key in keys:
            with patch('retools.global_connection._redis', mock_redis):
                CR = self._makeOne()
                CR.add_region('short_term', 60)

                class DummyClass(object):
                    def dummy_func(self, arg):
                        return "This is a value: %s" % time.time()
                    dummy_func = self._decorateFunc(dummy_func, 'short_term')
                cl_inst = DummyClass()
                value = cl_inst.dummy_func(key)
                assert 'This is a value' in value
                exec_calls = [x for x in mock_pipeline.method_calls \
                      if x[0] == 'execute']
                eq_(len(exec_calls), 2)
            mock_pipeline.reset_mock()
            results.extend(['0', (None, '0')])
