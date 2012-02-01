.. _index:

================================
retools - A Python Redis Toolset
================================

`retools` is a concise set of well-tested extensible Python Redis tools.

- :mod:`Caching <retools.cache>`
    - Hit/Miss Statistics
    - Regions for common expiration periods and invalidating batches of
      functions at once.
    - Write-lock to prevent the `Thundering Herd`_
- :mod:`Distributed Locking <retools.lock>`
    - Python context-manager with lock timeouts and retries
- :mod:`Queuing <retools.queue>`
    - Simple :ref:`forking worker <queue_worker>` based on `Resque`_
    - Jobs stored as JSON in Redis for easy introspection
    - `setproctitle`_ used by workers for easy worker introspection on
      the command line
    - :ref:`Rich event system <queue_events>` for extending job processing behavior
- Well Tested [1]_
    - 100% statement coverage
    - 100% condition coverage (via instrumental_)


Reference Material
==================

Reference material includes documentation for every `retools` API.

.. toctree::
   :maxdepth: 1

   api
   Changelog <changelog>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`

.. [1] queuing not up to 100% testing yet

.. _`Thundering Herd`: http://en.wikipedia.org/wiki/Thundering_herd_problem
.. _instrumental: http://pypi.python.org/pypi/instrumental
.. _Resque: https://github.com/defunkt/resque
.. _setproctitle: http://pypi.python.org/pypi/setproctitle