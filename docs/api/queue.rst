.. _queue_module:

:mod:`retools.queue`
--------------------

.. automodule:: retools.queue


Public API Classes
~~~~~~~~~~~~~~~~~~

.. autoclass:: QueueManager
    :members: __init__, set_queue_for_job, subscriber, enqueue

Private API Classes
~~~~~~~~~~~~~~~~~~~

.. autoclass:: Job
    :members: __init__, load_events, perform, enqueue, run_event

.. autoclass:: Worker
    :members: __init__, worker_id, queue_names, work, reserve, set_proc_title, register_worker, unregister_worker, startup, trigger_shutdown, immediate_shutdown, kill_child, pause_processing, resume_processing, prune_dead_workers, register_signal_handlers, working_on, done_working, worker_pids, perform
