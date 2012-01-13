# coding: utf-8
import unittest
import time

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
    def test_enqueue_job(self):
        mock_redis = Mock(spec=redis.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline
        qm = self._makeQM(redis=mock_redis)
        job_id = qm.enqueue('retools.tests.jobs:echo_default',
                            default='hi there')
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
        job_id = qm.enqueue('retools.tests.jobs:echo_default',
                            default='hi there')
        meth, args, kw = mock_pipeline.method_calls[0]
        eq_('rpush', meth)
        eq_(kw, {})
        queue_name, job_body = args
        job_data = json.loads(job_body)
        eq_(job_data['job_id'], job_id)
        eq_(job_data['kwargs'], {"default": "hi there"})
