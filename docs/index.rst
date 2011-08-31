.. _index:

================================
retools - A Python Redis Toolset
================================

`retools` is a concise set of well-tested extensible Python Redis tools.

- Caching
    - Hit/Miss Statistics
    - Regions for common expiration periods and invalidating batches of
      functions at once.
    - Write-lock to prevent the `Thundering Herd`_
- Distributed Locking
    - Python context-manager with lock timeouts and retries
- Well Tested
    - 100% statement coverage
    - 100% condition coverage (via instrumental_)


Using Retools
=============

.. toctree::
   :maxdepth: 2
   
   caching


Reference Material
==================

Reference material includes documentation for every `retools` API.

.. toctree::
   :maxdepth: 1

   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _`Thundering Herd`: http://en.wikipedia.org/wiki/Thundering_herd_problem
.. _instrumental: http://pypi.python.org/pypi/instrumental
