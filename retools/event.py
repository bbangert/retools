"""Queue Events

Queue events allow for custimization of what occurs when a worker runs, and
adds extension points for job execution.

Job Events
==========

job_prerun
----------

Runs in the child process immediately before the job is performed.

If a :obj:`job_prerun` function raises :exc:`~retools.exc.AbortJob`then the
job will be aborted gracefully and the :obj:`job_failure` will not be called.

Signal handler will recieve the job function as the sender with the keyword
argument ``job``, which is a :class:`~retools.queue.Job` instance.


job_postrun
-----------

Runs in the child process after the job is performed.

These will be skipped if the job segfaults or raises an exception.

Signal handler will recieve the job function as the sender with the keyword
arguments ``job`` and ``result``, which is the :class:`~retools.queue.Job`
instance and the result of the function.


job_wrapper
-----------

Runs in the child process and wraps the job execution

Objects configured for this signal must be context managers, and can be
ensured they will have the opportunity to before and after the job. Commonly
used for locking and other events which require ensuring cleanup of a resource
after the job is called regardless of the outcome.

Signal handler will be called with the job function, the
:class:`~retools.queue.Job` instance, and the keyword arguments for the job.


job_failure
-----------

Runs in the child process when a job throws an exception

Signal handler will be called with the job function, the 
:class:`~retools.queue.Job` instance, and the exception object. The signal
handler **should not raise an exception**.


Worker Events
=============

worker_startup
--------------

Runs when the worker starts up in the main worker process before any jobs have
been processed.

Signal handler will recieve the :class:`~retools.queue.Worker` instance as
the sender.


worker_shutdown
---------------

Runs when the worker shuts down


worker_prefork
--------------

Runs at the beginning of every loop in the worker after a job has been
reserved immediately before forking

Signal handler will recieve the :class:`~retools.queue.Worker` instance and
a keyword argument of ``job`` which is a :class:`~retools.queue.Job` instance.


worker_postfork
---------------

Runs in the worker child process immediately after a job was reserved and
the worker child process was forked

"""
from blinker import Signal

class Event(object):
    """Event class that manages job events"""
    def __init__(self):
        events = {}
        for name in ['job_prerun', 'job_postrun', 'job_wrapper',
                     'job_failure', 'worker_startup', 'worker_shutdown',
                     'worker_prefork', 'worker_postfork']:
            events[name] = Signal()
        self.events = events
    
    def listen(event, handler, jobs=None):
        """Bind a handler to an event, optionally for specific senders
        
        :param event: The name of the event to bind for
        :param handler: A reference to a function that should handle this
                        event
        :param jobs: Job functions that should be bound to this event. When
                     not passed in, the handler will be bound globally
                     to this event for all jobs.
        :type jobs: list of function objects
        
        """
        if event not in self.events:
            raise Exception("Event '%s' is not a valid event name" % event)
        
        if jobs:
            for job in jobs:
                self.events[event].connect(handler, sender=job)
        else:
            self.events[event].connect(handler)
    
    def __getitem__(self, name):
        return self.events[name]
