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
        mock_redis.llen = Mock(return_value=1)

        created = time.time()

        # trying get_jobs/get_job
        job = json.dumps({'job_id': job_id,
                          'job': 'retools.tests.jobs:echo_default',
                          'kwargs': {},
                          'state': '',
                          'events': {},
                          'metadata': {'created': created}
                         })

        mock_redis.lindex = Mock(return_value=job)

        jobs = list(qm.get_jobs())
        self.assertEqual(len(jobs), 1)
        my_job = qm.get_job(job_id)
        self.assertEqual(my_job.job_name, 'retools.tests.jobs:echo_default')
        self.assertEqual(my_job.metadata['created'], created)

        # testing the Worker class methods
        from retools.queue import Worker
        mock_redis = Mock(spec=redis.Redis)
        mock_pipeline = Mock(spec=redis.client.Pipeline)
        mock_redis.pipeline.return_value = mock_pipeline
        mock_redis.smembers = Mock(return_value=[])

        workers = list(Worker.get_workers(redis=mock_redis))
        self.assertEqual(len(workers), 0)

        worker = Worker(queues=['main'])
        mock_redis.smembers = Mock(return_value=[worker.worker_id])
        worker.register_worker()
        try:
            workers = list(Worker.get_workers(redis=mock_redis))
            self.assertEqual(len(workers), 1, workers)
            ids = Worker.get_worker_ids(redis=mock_redis)
            self.assertEqual(ids, [worker.worker_id])
        finally:
            worker.unregister_worker()
