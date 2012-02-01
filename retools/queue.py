"""Queue worker and manager

.. note::

    The queueing functionality is new, and has gone through some preliminary
    testing. Please report any issues found on `the retools Github issue
    tracker <https://github.com/bbangert/retools/issues>`_.

Any function that takes keyword arguments can be a ``job`` that a worker runs.
The :class:`~retools.queue.QueueManager` handles configuration and enqueing jobs
to be run.

Declaring jobs::

    def default_job():
        # do some basic thing

    def important(somearg=None):
        # do an important thing


    def my_event_handler(sender, **kwargs):
        # do something

    def save_error(sender, **kwargs):
        # record error


Running Jobs::

    from retools.queue import QueueManager

    qm = QueueManager()
    qm.subscriber('job_failure', handler='mypackage.jobs:save_error')
    qm.subscriber('job_postrun', 'mypackage.jobs:important',
                  handler='mypackage.jobs:my_event_handler')
    qm.enqueue('mypackage.jobs:important', somearg='fred')


.. note::

    The events for a job are registered with the :class:`QueueManager` and are
    encoded in the job's JSON blob. Updating events for a job will therefore
    only take effect for new jobs queued, and not existing ones on the queue.

.. _queue_events:

Events
======

The retools queue has events available for additional functionality without
having to subclass or directly extend retools. These functions will be run by
the worker when the job is handled.

Available events to register for:

* **job_prerun**: Runs immediately before the job is run.
* **job_wrapper**: Wraps the execution of the job, these should be context
  managers.
* **job_postrun**: Runs after the job completes *successfully*, this will not
  be run if the job throws an exception.
* **job_failure**: Runs when a job throws an exception.

Event Function Signatures
-------------------------

Event functions have different call semantics, the following is a list of how
the event functions will be called:

* **job_prerun**: (job=job_instance)
* **job_wrapper**: (job_function, job_instance, **job_keyword_arguments)
* **job_postrun**: (job=job_instance, result=job_function_result)
* **job_failure**: (job=job_instance, exc=job_exception)

Attributes of interest on the job instance are documented in the
:meth:`Job.__init__` method.

.. _queue_worker:

Running the Worker
==================

After installing ``retools``, a ``retools-worker`` command will be available
that can spawn a worker. Queues to watch can be listed in order for priority
queueing, in which case the worker will try each queue in order looking for jobs
to process.

Example invokation:

.. code-block:: bash

    $ retools-worker high,medium,main

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

import pkg_resources

try:
    import json
except ImportError:  # pragma: nocover
    import simplejson as json

from setproctitle import setproctitle

from retools import global_connection
from retools.exc import ConfigurationError
from retools.util import with_nested_contexts


class QueueManager(object):
    """Configures and enqueues jobs"""
    def __init__(self, redis=None, default_queue_name='main'):
        """Initialize a QueueManager

        :param redis: A Redis instance. Defaults to the redis instance
                      on the global_connection.
        """
        self.default_queue_name = default_queue_name
        self.redis = redis or global_connection.redis
        self.names = {}  # cache name lookups
        self.job_config = {}
        self.job_events = {}
        self.global_events = {}

    def set_queue_for_job(self, job_name, queue_name):
        """Set the queue that a given job name will go to

        :param job_name: The pkg_resource name of the job function. I.e.
                         retools.jobs:my_function
        :param queue_name: Name of the queue on Redis job payloads should go
                           to
        """
        self.job_config[job_name] = queue_name

    def subscriber(self, event, job=None, handler=None):
        """Set events for a specific job or for all jobs

        :param event: The name of the event to subscribe to.
        :param job: Optional, a specific job to bind to.
        :param handler: The location of the handler to call.

        """
        if job:
            job_events = self.job_events.setdefault(job, {})
            job_events.setdefault(event, []).append(handler)
        else:
            self.global_events.setdefault(event, []).append(handler)

    def enqueue(self, job, **kwargs):
        """Enqueue a job

        :param job: The pkg_resouce name of the function. I.e.
                    retools.jobs:my_function
        :param kwargs: Keyword arguments the job should be called with.
                       These arguments must be serializeable by JSON.
        :returns: The job id that was queued.

        """
        if job not in self.names:
            job_func = pkg_resources.EntryPoint.parse('x=%s' % job).load(False)
            self.names[job] = job_func

        queue_name = kwargs.pop('queue_name', None)
        if not queue_name:
            queue_name = self.job_config.get('job', self.default_queue_name)

        full_queue_name = 'retools:queue:' + queue_name
        job_id = uuid.uuid4().hex
        events = self.global_events.copy()
        if job in self.job_events:
            for k, v in self.job_events[job].items():
                events.setdefault(k, []).extend(v)

        job_dct = {
            'job_id': job_id,
            'job': job,
            'kwargs': kwargs,
            'events': events,
            'state': {}
        }
        pipeline = self.redis.pipeline()
        pipeline.rpush(full_queue_name, json.dumps(job_dct))
        pipeline.sadd('retools:queues', queue_name)
        pipeline.execute()
        return job_id


class Job(object):
    def __init__(self, queue_name, job_payload, redis):
        """Create a job instance given a JSON job payload

        :param job_payload: A JSON string representing a job.
        :param queue_name: The queue this job was pulled off of.
        :param redis: The redis instance used to pull this job.

        A :class:`Job` instance is created when the Worker pulls a
        job payload off the queue. The ``current_job`` global is set
        upon creation to indicate the current job being processed.

        Attributes of interest for event functions:

        * **job_id**: The Job's ID
        * **job_name**: The Job's name (it's package + function name)
        * **queue_name**: The queue this job came from
        * **kwargs**: The keyword arguments the job is called with
        * **state**: The state dict, this can be used by events to retain
          additional arguments. I.e. for a retry extension, retry information
          can be stored in the ``state`` dict.
        * **func**: A reference to the job function
        * **redis**: A :class:`redis.Redis` instance.

        """
        global current_job
        current_job = self

        self.payload = payload = json.loads(job_payload)
        self.job_id = payload['job_id']
        self.job_name = payload['job']
        self.queue_name = queue_name
        self.kwargs = payload['kwargs']
        self.state = payload['state']
        self.events = {}
        self.redis = redis
        self.func = None
        self.events = self.load_events(event_dict=payload['events'])

    def __repr__(self):
        """Display representation of self"""
        res = '<%s object at %s: ' % (self.__class__.__name__, hex(id(self)))
        res += 'Events: %s, ' % self.events
        res += 'State: %s, ' % self.state
        res += 'Job ID: %s, ' % self.job_id
        res += 'Job Name: %s, ' % self.job_name
        res += 'Queue: %s' % self.queue_name
        res += '>'
        return res

    @staticmethod
    def load_events(event_dict):
        """Load all the events given the references

        :param event_dict: A dictionary of events keyed by event name
                           to a list of handlers for the event.

        """
        events = {}
        for k, v in event_dict.items():
            funcs = []
            for name in v:
                mod_name, func_name = name.split(':')
                try:
                    mod = sys.modules[mod_name]
                except KeyError:
                    __import__(mod_name)
                    mod = sys.modules[mod_name]
                funcs.append(getattr(mod, func_name))
            events[k] = funcs
        return events

    def perform(self):
        """Runs the job calling all the job signals as appropriate"""
        self.run_event('job_prerun')
        try:
            if 'job_wrapper' in self.events:
                result = with_nested_contexts(self.events['job_wrapper'],
                                              self.func, [self], self.kwargs)
            else:
                result = self.func(**self.kwargs)
            self.run_event('job_postrun', result=result)
            return True
        except Exception, exc:
            self.run_event('job_failure', exc=exc)
            return False

    def enqueue(self):
        """Queue this job in Redis"""
        full_queue_name = self.queue_name
        job_dct = {
            'job_id': self.job_id,
            'job': self.job_name,
            'kwargs': self.kwargs,
            'events': self.payload['events'],
            'state': self.state
        }
        queue_name = full_queue_name.lstrip('retools:queue:')
        pipeline = self.redis.pipeline()
        pipeline.rpush(full_queue_name, json.dumps(job_dct))
        pipeline.sadd('retools:queues', queue_name)
        pipeline.execute()
        return self.job_id

    def run_event(self, event, **kwargs):
        """Run all registered events for this job"""
        for event_func in self.events.get(event, []):
            event_func(job=self, **kwargs)


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
            raise ConfigurationError(
                  "No queues were configured for this worker")
        self.queues = ['retools:queue:%s' % x for x in queues]
        self.paused = self.shutdown = False
        self.job = None
        self.child_id = None
        self.jobs = {}  # job function import cache

    @property
    def worker_id(self):
        """Returns this workers id based on hostname, pid, queues"""
        return '%s:%s:%s' % (socket.gethostname(), os.getpid(),
              self.queue_names)

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
                         job. This affects how often the worker can respond to
                         signals.
        :type blocking: bool

        """
        self.set_proc_title('Starting')
        self.startup()

        try:
            while 1:
                if self.shutdown:
                    break

                # Set this first since reserve may block for awhile
                self.set_proc_title("Waiting for %s" % self.queue_names)

                if not self.paused and self.reserve(interval, blocking):
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
                        self.set_proc_title(
                              "Waiting for %s" % self.queue_names)
                        time.sleep(interval)
        finally:
            self.unregister_worker()

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

        self.job = job = Job(queue_name=queue_name, job_payload=job_payload,
                             redis=self.redis)
        try:
            job.func = self.jobs[job.job_name]
        except KeyError:
            mod_name, func_name = job.job_name.split(':')
            __import__(mod_name)
            mod = sys.modules[mod_name]
            job.func = self.jobs[job.job_name] = getattr(mod, func_name)
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

    def trigger_shutdown(self, *args):
        """Graceful shutdown of the worker"""
        self.shutdown = True

    def immediate_shutdown(self, *args):
        """Immediately shutdown the worker, kill child process if needed"""
        self.shutdown = True
        self.kill_child()

    def kill_child(self, *args):
        """Kill the child process immediately"""
        if self.child_id:
            os.kill(self.child_id, signal.SIGTERM)

    def pause_processing(self, *args):
        """Cease pulling jobs off the queue for processing"""
        self.paused = True

    def resume_processing(self, *args):
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
        ps = subprocess.Popen("ps -U 0 -A | grep 'retools:'", shell=True,
                              stdout=subprocess.PIPE)
        data = ps.stdout.read()
        ps.stdout.close()
        ps.wait()
        return [x.split()[0] for x in data.split('\n') if x]

    def perform(self):
        """Run the job and call the appropriate signal handlers"""
        self.job.perform()


def run_worker():
    usage = "usage: %prog queues"
    parser = OptionParser(usage=usage)
    parser.add_option("--interval", dest="interval", type="int", default=5,
                      help="Polling interval")
    parser.add_option("-b", dest="blocking", action="store_true",
                      default=False,
                      help="Whether to use blocking queue semantics")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        sys.exit("Error: Failed to provide queues or packages_to_scan args")

    worker = Worker(queues=args[0].split(','))
    worker.work(interval=options.interval, blocking=options.blocking)
    sys.exit()
