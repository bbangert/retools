import unittest

import redis.client

from nose.tools import raises
from nose.tools import eq_
from mock import Mock
from mock import patch

class TestNamespaceFunc(unittest.TestCase):
    def _makeOne(self):
        from retools import Connection
        Connection.redis = None
        return Connection

    def test_set_default_and_get_it(self):
        with patch('retools.Connection.Redis') as mock:
            Conn = self._makeOne()
            new_conn = Mock(spec=redis.client.Redis)
            mock.return_value = new_conn
            Conn.set_default(host='127.0.0.1')
            eq_(mock.called, True)
            
            redis_conn = Conn.get_default()
            eq_(redis_conn, new_conn)

    def test_get_default(self):
        with patch('retools.Connection.Redis') as mock:
            Conn = self._makeOne()
            new_conn = Mock(spec=redis.client.Redis)
            mock.return_value = new_conn
            redis_conn = Conn.get_default()
            eq_(new_conn, redis_conn)
