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

"""
