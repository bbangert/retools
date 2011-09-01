"""Queue worker and manager

Any job that takes keyword arguments can be a ``job`` that a worker runs. The
:class:`~retools.queue.QueueManager` handles configuration and enqueing jobs
to be run.

Example job::
    
    from retools.queue import job
    from retools.queue import global_queue_manager
    
    @job
    def default_job():
        # do some basic thing
    
    @job(category='critical')
    def important(somearg=None):
        # do an important thing
    
    global_queue_manager.scan('.')
    global_queue_manager.configure_category('critical', queue_name='critical')
    
    global_queue_manager.enqueue(important, somearg='fred')

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

try:
    import json
except ImportError: #pragma: nocover
    import simplejson as json

import venusian
from blinker import Signal
from setproctitle import setproctitle

from retools import global_connection
from retools.exc import UnregisteredJob
from retools.exc import ConfigurationError
from retools.util import with_nested_contexts

# Global to indicate the current job being processed
current_job = None

# Signals
job_prerun = Signal(doc="""\
Runs in the child process immediately before the job is performed.

If a :obj:`job_prerun` function raises :exc:`~retools.exc.AbortJob`then the
job will be aborted gracefully and the :obj:`job_failure` will not be called.

Signal handler will recieve the job function as the sender with the keyword
argument ``job``, which is a :class:`~retools.queue.Job` instance.

""")

job_postrun = Signal(doc="""\
Runs in the child process after the job is performed.

These will be skipped if the job segfaults or raises an exception.

Signal handler will recieve the job function as the sender with the keyword 
arguments ``job`` and ``result``, which is the :class:`~retools.queue.Job`
instance and the result of the function.

""")

job_wrapper = Signal(doc="""\
Runs in the child process and wraps the job execution

Objects configured for this signal must be context managers, and can be
ensured they will have the opportunity to before and after the job. Commonly
used for locking and other events which require ensuring cleanup of a resource
after the job is called regardless of the outcome.

Signal handler will be called with the job function, the
:class:`~retools.queue.Job` instance, and the keyword arguments for the job.

""")

job_failure = Signal(doc="""\
Runs in the child process when a job throws an exception

Each object registered for this signal will be called with the exception
details. These should not raise an exception. After they are run, the original
exception is raised again.
""")

worker_startup = Signal(doc="""\
Runs when the worker starts up in the main worker process before any jobs have
been processed.

Signal handler will recieve the :class:`~retools.queue.Worker` instance as 
the sender.
""")

worker_shutdown = Signal(doc="""\
Runs when the worker shuts down
""")

worker_prefork = Signal(doc="""\
Runs at the beginning of every loop in the worker after a job has been 
reserved immediately before forking

Signal handler will recieve the :class:`~retools.queue.Worker` instance and
a keyword argument of ``job`` which is a :class:`~retools.queue.Job` instance.
""")

worker_postfork = Signal(doc="""\
Runs in the worker child process immediately after a job was reserved and
the worker child process was forked
""")


def _decorate_function(func, kwargs):
    """Return the original wrapped function, but register it with venusian"""
    def callback(scanner, name, ob):
        scanner.queue_manager.register_job(func, **kwargs)
    venusian.attach(func, callback, category='retools_jobs')
    return func


def job(*args, **kwargs):
    """Register a function with the :class:`~retools.queue.QueueManager`
    as job

    :param category: The category settings to use when enqueing and
                     running the job.
    :param queue_name: Queue name this job should be added to. Defaults
                       to the
                       :attr:`~retools.queue.QueueManager.default_queue_name`
                       setting which has the default value of ``main``.
    :param name: Set the job name manually. By default the job name is the
                 module + function name. I.e. ``retools.queue.job_func``.

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
        self.registered_jobs = {}
        self.registered_functions = {}
        self.registered_categories = {}
        
        if not redis:
            self.redis = global_connection.redis
        else:
            self.redis = redis

    def scan(self, package_name):
        """Scan a package/module for jobs to register with the
        :class:`~retools.queue.QueueManager`

        This must be called before jobs can be enqueued.

        :param package_name: Dotted notation for a package to scan. I.e.
                             'mypackage.subpackage'

        """
        package_obj = __import__(package_name)
        scanner = venusian.Scanner(queue_manager=self)
        scanner.scan(package_obj, categories=('retools_jobs',))

    def register_job(self, func, **kwargs):
        """Internal class method to register a job

        This method is not part of the public API and should only be
        called by the :func:`retools.queue.job` decorator.

        """
        category = kwargs.pop('category', 'default')
        queue_name = kwargs.pop('queue_name', self.default_queue_name)

        job_name = kwargs.pop('name', func.__module__ + '.' + func.__name__)
        if category and category not in self.registered_categories:
            self.registered_categories[category] = {
                'jobs': [],
                'queue_name': self.default_queue_name
            }
        self.registered_categories[category]['jobs'].append(func)
        self.registered_jobs[job_name] = func
        self.registered_functions[func] = (job_name, category, kwargs,
                                           queue_name)

    def configure_category(self, category, queue_name=None, signals=None):
        """Configure a categories queue

        :param queue_name: Set the queue name to use for enqueing jobs
                           in this category. Optional.
        :param signals: List of tuples indicating the signal and subscriber
                        to be hooked up to every job in the category. I.e.
                        [(job_postrun, my_postrun_func)]

        This method may be called multiple times to add additional
        signals to register.

        """
        if category not in self.registered_categories:
            self.registered_categories[category] = {
                'jobs': [],
                'queue_name': self.default_queue_name
            }
        if queue_name:
            self.registered_categories[category]['queue_name'] = queue_name
        if signals:
            for signal, handler in signals:
                for job in self.registered_categories[category]['jobs']:
                    signal.connect(handler, sender=job)

    def enqueue(self, job, **kwargs):
        """Enqueue a job

        :param job: Either the dotted name of the job to enqueue, or
                    a function that has been decorated as a job.
        :param kwargs: Keyword arguments the job should be called with.
                       These arguments must be serializeable by JSON.
        :returns: The job id that was queued.

        """
        if job in self.registered_functions:
            job_name, category, dec_kwargs, queue_name = self.registered_functions[job]
        elif job in self.registered_jobs:
            func = self.registered_jobs[job]
            job_name, category, dec_kwargs, queue_name = self.registered_functions[func]
        else:
            raise UnregisteredJob("No job registered for: %s" % job)

        if not queue_name:
            queue_name = self.registered_categories[category]
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
    def __init__(self, queue_name, job_payload, queue_manager):
        """Create a job instance given a JSON job payload
        
        :param job_payload: A JSON string representing a job.
        :param queue_name: The queue this job was pulled off of.
        :param queue_manager: A queue manager instance that this job was
                              registered with, and should be used in the event
                              the job should be enqueued again.
        
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
        self.queue_name = queue_name
        self.func = global_queue_manager.registered_jobs[payload['job']]
    
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
    def __init__(self, queues, redis=None, queue_manager=None):
        """Create a worker
        
        :param queues: List of queues to process
        :type queues: list
        :param redis: Redis instance to use, defaults to the global_connection.
        :param queue_manager: Queue manager instance to use. This should be
                              configured and have scan'd all the jobs that
                              this worker should be handling. An error will be
                              raised if this worker attempts to handle a job
                              the ``queue_manager`` instance isn't aware of.
        
        In the event that there is only a single queue in the list
        Redis list blocking will be used for lower latency job
        processing
        
        """
        self.redis = redis or global_connection.redis
        self.queue_manager = queue_manager or global_queue_manager
        if not queues:
            raise ConfigurationError("No queues were configured for this worker")
        self.queues = ['retools:queue:%s' % x for x in queues]
        self.paused = self.shutdown = False
        self.job = None
        self.child_id = None
    
    @property
    def worker_id(self):
        """Returns this workers id based on hostname, pid, queues"""
        return '%s:%s:%s' % (socket.gethostname(), os.getpid(), ','.join(self.queues))
    
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
                        self.set_proc_title("Waiting for %s" % ','.join(self.queues))
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
                       queue_manager=self.queue_manager)
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
    
    def immediate_shutdown(self):
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
        ps = subprocess.Popen("ps -U 0 -A | grep 'retools:'", shell=True, stdout=subprocess.PIPE)
        data = ps.stdout.read()
        ps.stdout.close()
        ps.wait()
        return [x.split()[0] for x in data.split('\n') if x]
    
    def perform(self):
        """Run the job and call the appropriate signal handlers"""
        worker_postfork.send(self, job=self.job)
        self.job.perform()
