=========
Changelog
=========


0.2 (02/01/2012)
================

Bug Fixes
---------

- Critical fix for caching that prevents old values from being displayed
  forever. Thanks to Daniel Holth for tracking down the problem-aware.
- Actually sets the Redis expiration for a value when setting the cached
  value in Redis. This defaults to 1 week.

Features
--------

- Statistics for the cache is now optional and can be disabled to slightly
  reduce the Redis queries used to store/retrieve cache data.
- Added first revision of worker/job Queue system, with event support.

Internals
---------

- Heavily refactored ``Connection`` to not be a class singleton, instead
  a global_connection instance is created and used by default.
- Increased conditional coverage to 100% (via instrumental_).

Backwards Incompatibilities
---------------------------

- Changing the default global Redis connection has changed semantics, instead
  of using ``Connection.set_default``, you should set the global_connection's
  redis property directly::

      import redis
      from retools import global_connection
      
      global_connection.redis = redis.Redis(host='myhost')


Incompatibilities
-----------------

- Removed clear argument from invalidate_region, as removing keys from the
  set but not removing the hit statistics can lead to data accumulating in
  Redis that has no easy removal other than .keys() which should not be run
  in production environments.

- Removed deco_args from invalidate_callable (invalidate_function) as its
  not actually needed since the namespace is already on the callable to
  invalidate.


0.1 (07/08/2011)
================

Features
--------

- Caching in a similar style to Beaker, with hit/miss statistics, backed by
  a Redis global write-lock with old values served to prevent the dogpile
  effect
- Redis global lock

.. _instrumental: http://pypi.python.org/pypi/instrumental
