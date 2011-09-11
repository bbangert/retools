.. _cache_module:

:mod:`retools.cache`
--------------------

.. automodule:: retools.cache

Constants
~~~~~~~~~

.. py:data:: NoneMarker

    A module global returned to indicate no value is present in Redis
    rather than a ``None`` object.

Functions
~~~~~~~~~

.. autofunction:: cache_region
.. autofunction:: invalidate_region
.. autofunction:: invalidate_function


Classes
~~~~~~~

.. autoclass:: CacheKey
    :members:
    :inherited-members:

.. autoclass:: CacheRegion
    :members:
    :inherited-members:
