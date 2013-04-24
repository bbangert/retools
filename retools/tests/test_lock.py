import unittest
import time
import threading
import uuid

import redis
from nose.tools import raises
from nose.tools import eq_

from retools import global_connection


class TestLock(unittest.TestCase):
    def _makeOne(self):
        from retools.lock import Lock
        return Lock

    def _lockException(self):
        from retools.lock import LockTimeout
        return LockTimeout

    def setUp(self):
        self.key = uuid.uuid4()

    def tearDown(self):
        global_connection.redis.delete(self.key)

    def test_lock_runs(self):
        Lock = self._makeOne()
        x = 0
        with Lock(self.key):
            x += 1

    def test_lock_fail(self):
        Lock = self._makeOne()

        bv = threading.Event()
        ev = threading.Event()

        def get_lock():
            with Lock(self.key):
                bv.set()
                ev.wait()
        t = threading.Thread(target=get_lock)
        t.start()
        ac = []

        @raises(self._lockException())
        def test_it():
            with Lock(self.key, timeout=0):
                ac.append(10)  # pragma: nocover
        bv.wait()
        test_it()
        eq_(ac, [])
        ev.set()
        t.join()
        with Lock(self.key, timeout=0):
            ac.append(10)
        eq_(ac, [10])

    def test_lock_retry(self):
        Lock = self._makeOne()
        bv = threading.Event()
        ev = threading.Event()

        def get_lock():
            with Lock(self.key):
                bv.set()
                ev.wait()
        t = threading.Thread(target=get_lock)
        t.start()
        ac = []

        bv.wait()

        @raises(self._lockException())
        def test_it():
            with Lock(self.key, timeout=1):
                ac.append(10)  # pragma: nocover
        test_it()
        ev.set()
        t.join()
