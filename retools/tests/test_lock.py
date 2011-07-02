import unittest

from nose.tools import raises
from mock import Mock
from mock import patch

class TestLock(unittest.TestCase):
    def _makeOne(self):
        from retools.lock import Lock
        return Lock
    
    def _lockException(self):
        from retools.lock import LockTimeout
        return LockTimeout
    
    def test_nocontention(self):
        mock = Mock()
        dummy = DummyRedis(nonexistent=True)
        mock.return_value = dummy
        @patch('retools.Connection.get_default', mock)
        def test_it():
            lock = self._makeOne()
            with lock('somekey'):
                val = 2 + 4
        test_it()
        assert 'somekey' in dummy.keys_set
        assert 'somekey' in dummy.keys_deleted
    
    def test_contention(self):
        mock = Mock()
        dummy = DummyRedis(expired=True)
        mock.return_value = dummy
        @patch('retools.Connection.get_default', mock)
        def test_it():
            lock = self._makeOne()
            with lock('somekey'):
                val = 2 + 4
        test_it()
        assert 'somekey' in dummy.keys_getset
        assert 'somekey' in dummy.keys_deleted
    
    def test_timeout(self):
        mock = Mock()
        dummy = DummyRedis()
        mock.return_value = dummy
        timeout = self._lockException()
        mock_time = Mock()
        mock_time.return_value = 0
        
        array = []
        @raises(timeout)
        @patch('retools.Connection.get_default', mock)
        @patch('time.sleep', mock_time)
        def test_it():
            lock = self._makeOne()
            with lock('somekey', timeout=1, redis=dummy):
                array.append(4) # pragma: nocover
        test_it()
        assert len(array) == 0


class DummyRedis(object):
    keys_set = {}
    keys_getset = {}
    keys_deleted = []
    keys_fetched = []
    
    def __init__(self, nonexistent=False, expired=False):
        self.nonexistent = nonexistent
        self.expired = expired
    
    def setnx(self, key, expires):
        if self.nonexistent:
            self.keys_set[key] = expires
            return expires
        else:
            return False
    
    def get(self, key):
        return 150
    
    def getset(self, key, value):
        if self.expired:
            self.keys_getset[key] = value
            return 150
        else:
            return False
    
    def delete(self, key):
        self.keys_deleted.append(key)
