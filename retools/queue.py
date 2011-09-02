"""Queue worker and manager

Any job that takes keyword arguments can be a ``job`` that a worker runs. The
:class:`~retools.queue.QueueManager` handles configuration and enqueing jobs
to be run.

Declaring jobs::
    
    # jobs.py
    from retools.queue import job
    from retools.signals import job_failure

    @job
    def default_job():
        # do some basic thing

    @job(queue_name='critical')
    def important(somearg=None):
        # do an important thing
    
    # Hook up a job_failure handler to the default_job
    @job_failure.connect_via(default_job)
    def my_handler(sender, **kwargs):
        # do something
    
    # Hook up a handler for all job failures
    @job_failure.connect
    def save_error(sender, **kwargs):
        # record error


Running Jobs::

    # main.py
    from retools.queue import QueueManager
    
    qm = QueueManager()
    qm.scan('mypackage.jobs')
    qm.enqueue('mypackage.jobs.important', somearg='fred')

The :meth:`~retools.queue.QueueManager.scan` must be called before jobs can
be enqueued.

"""
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime
from optparse import OptionParser

try:
    import json
except ImportError: #pragma: nocover
    import simplejson as json

import venusian
from setproctitle import setproctitle

from retools import global_connection
from retools.exc import UnregisteredJob
from retools.exc import ConfigurationError
from retools.signals import worker_startup
from retools.signals import worker_shutdown
from retools.signals import worker_prefork
from retools.signals import worker_postfork
from retools.signals import job_prerun
from retools.signals import job_postrun
from retools.signals import job_wrapper
from retools.signals import job_failure
from retools.util import with_nested_contexts

# Global to indicate the current job being processed
current_job = None

registered_jobs = {}
registered_functions = {}


def _decorate_function(func, kwargs):
    """Register this function and its kwargs with the job/function
    registries"""
    queue_name = kwargs.pop('queue_name', None)
    job_name = kwargs.pop('name', func.__module__ + '.' + func.__name__)
    registered_jobs[job_name] = func
    registered_functions[func] = (job_name, kwargs, queue_name)
    return func


def job(*args, **kwargs):
    """Register a job for this function

    :param name: Set the job name manually. By default the job name is the
                 module + function name. I.e. ``retools.queue.job_func``.
    :param queue_name: Queue name this job should be added to. Defaults
                       to the
                       :attr:`~retools.queue.QueueManager.default_queue_name`
                       setting which has the default value of ``main``.

    These arguments must be passed as keyword arguments, additional
    keyword arguments are ignored but may be useful for extensibility.

    This decorator can be used without arguments, in which case the
    defaults are used. Decorated functions are not modified by the
    :func:`~retools.queue.job` decorator and can be used normally.

    All arguments for the decorated function **must** be keyword arguments.

    Example::

        from retools.queue import job

        @job
        def do_task(val=None):
            # do the task

        @job(category='cache_cleanup')
        def regenerate_values(cache_args=None):
            # regenerate cache values

    """
    if kwargs:
        def fold(func):
            return _decorate_function(func, kwargs)
        return fold
    else:
        return _decorate_function(args[0], {})


class QueueManager(object):
    """Configures and enqueues jobs"""
    def __init__(self, redis=None, default_queue_name='main'):
        """Initialize a QueueManager

        :param redis: A Redis instance. Defaults to the redis instance
                      on the global_connection.
        """
        self.default_queue_name = default_queue_name
        self.redis = redis or global_connection.redis

    def scan(self, package_name):
        """Scan a package/module for jobs to register with the
        :class:`~retools.queue.QueueManager`

        This must be called before jobs can be enqueued.

        :param package_name: Dotted notation for a package to scan. I.e.
                             'mypackage.subpackage'

        """
        package_obj = __import__(package_name)
        scanner = venusian.Scanner(queue_manager=self)
        scanner.scan(package_obj)

    def enqueue(self, job, **kwargs):
        """Enqueue a job

        :param job: Either the dotted name of the job to enqueue, or
                    a function that has been decorated as a job.
        :param kwargs: Keyword arguments the job should be called with.
                       These arguments must be serializeable by JSON.
        :returns: The job id that was queued.

        """
        if job in registered_functions:
            job_name, dec_kwargs, queue_name = registered_functions[job]
        elif job in registered_jobs:
            func = registered_jobs[job]
            job_name, dec_kwargs, queue_name = registered_functions[func]
        else:
            raise UnregisteredJob("No job registered for: %s" % job)
        
        if not queue_name:
            queue_name = self.default_queue_name
        
        full_queue_name = 'retools:queue:' + queue_name
        job_id = uuid.uuid4().hex
        job_dct = {
            'job_id': job_id,
            'job': job_name,
            'kwargs': kwargs,
            'dec_kwargs': dec_kwargs
        }
        pipeline = self.redis.pipeline()
        pipeline.rpush(full_queue_name, json.dumps(job_dct))
        pipeline.sadd('retools:queues', queue_name)
        pipeline.execute()
        return job_id

    def enqueue_job(self, job):
        """Enqueue a job given a ``Job`` instance

        All parameters from the existing job instance are used to enqueue
        this job with the current ``Job`` instance parameters.

        """
        full_queue_name = job.queue_name
        job_dct = {
            'job_id': job.job_id,
            'job': job.job_name,
            'kwargs': job.kwargs,
            'dec_kwargs': job.dec_kwargs
        }
        queue_name = full_queue_name.lstrip('retools:queue:')
        pipeline = self.redis.pipeline()
        pipeline.rpush(full_queue_name, json.dumps(job_dct))
        pipeline.sadd('retools:queues', queue_name)
        pipeline.execute()
        return job_id


global_queue_manager = QueueManager()


class Job(object):
    def __init__(self, queue_name, job_payload, redis):
        """Create a job instance given a JSON job payload

        :param job_payload: A JSON string representing a job.
        :param queue_name: The queue this job was pulled off of.
        :param redis: The redis instance used to pull this job.

        A ``Job`` instance is created when the Worker pulls a
        job payload off the queue. The ``current_job`` global is set
        upon creation to indicate the current job being processed.

        """
        global current_job
        current_job = self

        self.payload = payload = json.loads(job_payload)
        self.job_id = payload['job_id']
        self.job_name = payload['job']
        self.kwargs = payload['kwargs']
        self.dec_kwargs = payload['dec_kwargs']
        self.redis = redis
        self.func = registered_jobs[self.job_name]

    def perform(self):
        """Runs the job calling all the job signals as appropriate"""
        job_prerun.send(self.func, job=self)
        try:
            if job_wrapper.has_receivers_for(self.func):
                handlers = list(job_wrapper.recievers_for(self.func))
                result = with_nested_contexts(handlers, self.func, [self], self.kwargs)
            else:
                result = self.func(**self.kwargs)
            job_postrun.send(self.func, job=self, result=result)
            return True
        except Exception, exc:
            job_failure.send(self.func, exc=exc)
            return False


class Worker(object):
    """A Worker works on jobs"""
    def __init__(self, queues, redis=None):
        """Create a worker

        :param queues: List of queues to process
        :type queues: list
        :param redis: Redis instance to use, defaults to the global_connection.

        In the event that there is only a single queue in the list
        Redis list blocking will be used for lower latency job
        processing

        """
        self.redis = redis or global_connection.redis
        if not queues:
            raise ConfigurationError("No queues were configured for this worker")
        self.queues = ['retools:queue:%s' % x for x in queues]
        self.paused = self.shutdown = False
        self.job = None
        self.child_id = None

    @property
    def worker_id(self):
        """Returns this workers id based on hostname, pid, queues"""
        return '%s:%s:%s' % (socket.gethostname(), os.getpid(), self.queue_names)
    
    @property
    def queue_names(self):
        names = [x.lstrip('retools:queue:') for x in self.queues]
        return ','.join(names)

    def work(self, interval=5, blocking=False):
        """Work on jobs

        This is the main method of the Worker, and will register itself
        with Redis as a Worker, wait for jobs, then process them.

        :param interval: Time in seconds between polling.
        :type interval: int
        :param blocking: Whether or not blocking pop should be used. If the
                         blocking pop is used, then the worker will block for
                         ``interval`` seconds at a time waiting for a new
                         job. This affects how often the worke can respond to
                         OS signals.
        :type blocking: bool

        """
        self.set_proc_title('Starting')
        self.startup()

        try:
            while 1:
                if self.shutdown:
                    break
                if not self.paused and self.reserve(interval, blocking):
                    worker_prefork.send(self, job=self.job)
                    self.working_on()
                    self.child_id = os.fork()
                    if self.child_id:
                        self.set_proc_title("Forked %s at %s" % (
                            self.child_id, datetime.now()))
                        os.wait()
                    else:
                        self.set_proc_title("Processing %s since %s" % (
                            self.job.queue_name, datetime.now()))
                        self.perform()
                        sys.exit()
                    self.done_working()
                    self.child_id = None
                    self.job = None
                else:
                    if self.paused:
                        self.set_proc_title("Paused")
                    elif not blocking:
                        self.set_proc_title("Waiting for %s" % self.queue_names)
                        time.sleep(interval)
        finally:
            self.unregister_worker()
            worker_shutdown.send(self)

    def reserve(self, interval, blocking):
        """Attempts to pull a job off the queue(s)"""
        queue_name = None
        if blocking:
            result = self.redis.blpop(self.queues, timeout=interval)
            if result:
                queue_name, job_payload = result
        else:
            for queue in self.queues:
                job_payload = self.redis.lpop(queue)
                if job_payload:
                    queue_name = queue
                    break
        if not queue_name:
            return False

        self.job = Job(queue_name=queue_name, job_payload=job_payload,
                       redis=self.redis)
        return True

    def set_proc_title(self, title):
        """Sets the active process title, retains the retools prefic"""
        setproctitle('retools: ' + title)

    def register_worker(self):
        """Register this worker with Redis"""
        pipeline = self.redis.pipeline()
        pipeline.sadd("retools:workers", self.worker_id)
        pipeline.set("retools:worker:%s:started" % self.worker_id, time.time())
        pipeline.execute()

    def unregister_worker(self, worker_id=None):
        """Unregister this worker with Redis"""
        worker_id = worker_id or self.worker_id
        pipeline = self.redis.pipeline()
        pipeline.srem("retools:workers", worker_id)
        pipeline.delete("retools:worker:%s" % worker_id)
        pipeline.delete("retools:worker:%s:started" % worker_id)
        pipeline.execute()

    def startup(self):
        """Runs basic startup tasks"""
        self.register_signal_handlers()
        self.prune_dead_workers()
        self.register_worker()
        worker_startup.send(self)

    def trigger_shutdown(self):
        """Graceful shutdown of the worker"""
        self.shutdown = True

    def immediate_shutdown(self, *args):
        """Immediately shutdown the worker, kill child process if needed"""
        self.shutdown = True
        self.kill_child()

    def kill_child(self):
        """Kill the child process immediately"""
        if self.child_id:
            os.kill(self.child_id, signal.SIGTERM)

    def pause_processing(self):
        """Cease pulling jobs off the queue for processing"""
        self.paused = True

    def resume_processing(self):
        """Resume pulling jobs for processing off the queue"""
        self.paused = False

    def prune_dead_workers(self):
        """Prune dead workers from Redis"""
        all_workers = self.redis.smembers("retools:workers")
        known_workers = self.worker_pids()
        hostname = socket.gethostname()
        for worker in all_workers:
            print worker
            host, pid, queues = worker.split(':')
            if host != hostname or pid in known_workers:
                continue
            self.unregister_worker(worker)

    def register_signal_handlers(self):
        """Setup all the signal handlers"""
        signal.signal(signal.SIGTERM, self.immediate_shutdown)
        signal.signal(signal.SIGINT, self.immediate_shutdown)
        signal.signal(signal.SIGQUIT, self.trigger_shutdown)
        signal.signal(signal.SIGUSR1, self.kill_child)
        signal.signal(signal.SIGUSR2, self.pause_processing)
        signal.signal(signal.SIGCONT, self.resume_processing)

    def working_on(self):
        """Indicate with Redis what we're working on"""
        data = {
            'queue': self.job.queue_name,
            'run_at': time.time(),
            'payload': self.job.payload
        }
        self.redis.set("retools:worker:%s" % self.worker_id, json.dumps(data))

    def done_working(self):
        """Called when we're done working on a job"""
        self.redis.delete("retools:worker:%s" % self.worker_id)

    def worker_pids(self):
        """Returns a list of all the worker processes"""
        ps = subprocess.Popen("ps -U 0 -A | grep 'retools:'", shell=True,
                              stdout=subprocess.PIPE)
        data = ps.stdout.read()
        ps.stdout.close()
        ps.wait()
        return [x.split()[0] for x in data.split('\n') if x]

    def perform(self):
        """Run the job and call the appropriate signal handlers"""
        worker_postfork.send(self, job=self.job)
        self.job.perform()


def run_worker():
    usage = "usage: %prog queues packages_to_scan"
    parser = OptionParser(usage=usage)
    parser.add_option("--interval", dest="interval", type="int", default=5,
                      help="Polling interval")
    (options, args) = parser.parse_args()
    worker = Worker(queues=args[0].split(','))
    worker.work(interval=options.interval)
    sys.exit()
