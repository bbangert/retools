# coding: utf-8
import unittest
import time

import blinker
import redis
import redis.client
import json

from nose.tools import raises
from nose.tools import eq_
from mock import Mock
from mock import patch


class TestQueue(unittest.TestCase):
    def _makeQM(self, **kwargs):
        from retools.queue import QueueManager
        return QueueManager(**kwargs)

class TestJob(TestQueue):
    def test_job(self):
        qm = self._makeQM()
        qm.scan('retools.tests.jobs')
        eq_(len(qm.registered_jobs), 2)
        eq_(qm.registered_jobs['retools.tests.jobs.echo_default'](), 'hello')
        eq_(qm.registered_jobs['retools.tests.jobs.echo_back'](), 'howdy all')

    def test_enqueue_job(self):
        mock_redis = Mock(spec=redis.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline
        qm = self._makeQM(redis=mock_redis)
        qm.scan('retools.tests.jobs')
        job = qm.registered_jobs['retools.tests.jobs.echo_default']
        job_id = qm.enqueue(job, default='hi there')
        meth, args, kw = mock_pipeline.method_calls[0]
        eq_('rpush', meth)
        eq_(kw, {})
        queue_name, job_body = args
        job_data = json.loads(job_body)
        eq_(job_data['job_id'], job_id)
        eq_(job_data['kwargs'], {"default": "hi there"})

    def test_enqueue_job_by_name(self):
        mock_redis = Mock(spec=redis.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline
        qm = self._makeQM(redis=mock_redis)
        qm.scan('retools.tests.jobs')
        job_id = qm.enqueue('retools.tests.jobs.echo_default', default='hi there')
        meth, args, kw = mock_pipeline.method_calls[0]
        eq_('rpush', meth)
        eq_(kw, {})
        queue_name, job_body = args
        job_data = json.loads(job_body)
        eq_(job_data['job_id'], job_id)
        eq_(job_data['kwargs'], {"default": "hi there"})


class TestQueueManager(TestQueue):
    def test_configure_category(self):
        qm = self._makeQM()
        qm.configure_category('test_cat', queue_name='test_cat')
        eq_(qm.registered_categories['test_cat']['queue_name'], 'test_cat')

    def test_configure_category_with_signals(self):
        qm = self._makeQM()
        mock_sig = Mock(spec=blinker.Signal)
        sigs = [(mock_sig, lambda x, kw: x)]
        qm.scan('retools.tests.jobs')
        qm.configure_category('test_cat', queue_name='test_cat', signals=sigs)
        eq_(qm.registered_categories['test_cat']['queue_name'], 'test_cat')
        eq_(len(mock_sig.method_calls), 1)
    
    def test_no_job_registered(self):
        from retools.exc import UnregisteredJob
        qm = self._makeQM()
        @raises(UnregisteredJob)
        def testit():
            qm.enqueue('no_such_job')
        testit()
