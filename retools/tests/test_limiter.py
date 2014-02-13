# coding: utf-8

import unittest
import time

import redis
from nose.tools import eq_
from mock import Mock
from mock import patch

from retools.limiter import Limiter
from retools import global_connection


class TestLimiterWithMockRedis(unittest.TestCase):
    def test_can_create_limiter_without_prefix_and_without_connection(self):
        limiter = Limiter(limit=10)

        eq_(limiter.redis, global_connection.redis)
        eq_(limiter.limit, 10)
        eq_(limiter.prefix, 'retools_limiter')

    def test_can_create_limiter_without_prefix(self):
        mock_redis = Mock(spec=redis.Redis)

        limiter = Limiter(limit=10, redis=mock_redis)

        eq_(limiter.redis, mock_redis)
        eq_(limiter.prefix, 'retools_limiter')

    def test_can_create_limiter_with_prefix(self):
        mock_redis = Mock(spec=redis.Redis)

        limiter = Limiter(limit=10, redis=mock_redis, prefix='something')

        eq_(limiter.redis, mock_redis)
        eq_(limiter.prefix, 'something')

    def test_can_create_limiter_with_expiration(self):
        mock_redis = Mock(spec=redis.Redis)

        limiter = Limiter(limit=10, redis=mock_redis, expiration_in_seconds=20)

        eq_(limiter.expiration_in_seconds, 20)

    def test_has_limit(self):
        mock_time = Mock()
        mock_time.return_value = 40.5

        mock_redis = Mock(spec=redis.Redis)
        mock_redis.zcard.return_value = 0

        limiter = Limiter(limit=10, redis=mock_redis, expiration_in_seconds=20)

        with patch('time.time', mock_time):
            has_limit = limiter.acquire_limit(key='test1')

        eq_(has_limit, True)

        mock_redis.zadd.assert_called_once_with('retools_limiter', 'test1', 60.5)

    def test_acquire_limit_after_removing_items(self):
        mock_time = Mock()
        mock_time.return_value = 40.5

        mock_redis = Mock(spec=redis.Redis)
        mock_redis.zcard.side_effect = [10, 8]

        limiter = Limiter(limit=10, redis=mock_redis, expiration_in_seconds=20)

        with patch('time.time', mock_time):
            has_limit = limiter.acquire_limit(key='test1')

        eq_(has_limit, True)

        mock_redis.zadd.assert_called_once_with('retools_limiter', 'test1', 60.5)
        mock_redis.zremrangebyscore.assert_called_once_with('retools_limiter', '-inf', 40.5)

    def test_acquire_limit_fails_even_after_removing_items(self):
        mock_time = Mock()
        mock_time.return_value = 40.5

        mock_redis = Mock(spec=redis.Redis)
        mock_redis.zcard.side_effect = [10, 10]

        limiter = Limiter(limit=10, redis=mock_redis, expiration_in_seconds=20)

        with patch('time.time', mock_time):
            has_limit = limiter.acquire_limit(key='test1')

        eq_(has_limit, False)

        eq_(mock_redis.zadd.called, False)
        mock_redis.zremrangebyscore.assert_called_once_with('retools_limiter', '-inf', 40.5)

    def test_release_limit(self):
        mock_redis = Mock(spec=redis.Redis)

        limiter = Limiter(limit=10, redis=mock_redis, expiration_in_seconds=20)

        limiter.release_limit(key='test1')

        mock_redis.zrem.assert_called_once_with('retools_limiter', 'test1')


class TestLimiterWithActualRedis(unittest.TestCase):
    def test_has_limit(self):
        limiter = Limiter(prefix='test-%.6f' % time.time(), limit=2, expiration_in_seconds=400)

        has_limit = limiter.acquire_limit(key='test1')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test2')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test3')
        eq_(has_limit, False)

    def test_has_limit_after_removing_items(self):
        limiter = Limiter(prefix='test-%.6f' % time.time(), limit=2, expiration_in_seconds=400)

        has_limit = limiter.acquire_limit(key='test1')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test2', expiration_in_seconds=-1)
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test3')
        eq_(has_limit, True)

    def test_has_limit_after_releasing_items(self):
        limiter = Limiter(prefix='test-%.6f' % time.time(), limit=2, expiration_in_seconds=400)

        has_limit = limiter.acquire_limit(key='test1')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test2')
        eq_(has_limit, True)

        limiter.release_limit(key='test2')

        has_limit = limiter.acquire_limit(key='test3')
        eq_(has_limit, True)

class TestLimiterWithStrictRedis(unittest.TestCase):
    def setUp(self):
        self.redis = redis.StrictRedis()

    def test_has_limit(self):
        limiter = Limiter(prefix='test-%.6f' % time.time(), limit=2, expiration_in_seconds=400, redis=self.redis)

        has_limit = limiter.acquire_limit(key='test1')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test2')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test3')
        eq_(has_limit, False)

    def test_has_limit_after_removing_items(self):
        limiter = Limiter(prefix='test-%.6f' % time.time(), limit=2, expiration_in_seconds=400, redis=self.redis)

        has_limit = limiter.acquire_limit(key='test1')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test2', expiration_in_seconds=-1)
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test3')
        eq_(has_limit, True)

    def test_has_limit_after_releasing_items(self):
        limiter = Limiter(prefix='test-%.6f' % time.time(), limit=2, expiration_in_seconds=400, redis=self.redis)

        has_limit = limiter.acquire_limit(key='test1')
        eq_(has_limit, True)

        has_limit = limiter.acquire_limit(key='test2')
        eq_(has_limit, True)

        limiter.release_limit(key='test2')

        has_limit = limiter.acquire_limit(key='test3')
        eq_(has_limit, True)
