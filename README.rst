=====================
Redis Tools (retools)
=====================

``retools`` is a package of Redis tools. It's aim is to provide a variety of
Redis backed Python tools that are always 100% unit tested, fast, efficient,
and utilize the capabilities of Redis.

Current tools in ``retools``:

* Caching
* Global Lock

On the horizon for future implementation:

* A worker/job processing system similar to Celery but based on how Ruby's
  Resque system works.


Caching
=======

A high performance caching system that can act as a drop-in replacement for
Beaker's caching. Unlike Beaker's caching, this utilizes Redis for distributed
write-locking dogpile prevention. It also collects hit/miss cache statistics
along with recording what regions are used by which functions and arguments.

Example::
    
    from retools.cache import CacheRegion, cache_region, invalidate_function
    
    CacheRegion.add_region('short_term', expires=3600)
    
    @cache_region('short_term')
    def slow_function(*search_terms):
        # Do a bunch of work
        return results
    
    my_results = slow_function('bunny')
    
    # Invalidate the cache for 'bunny'
    invalidate_function(slow_function, [], 'bunny')


Differences from Beaker
-----------------------

Unlike Beaker's caching system, this is built strictly for Redis. As such, it
adds several features that Beaker doesn't possess:

* A distributed write-lock so that only one writer updates the cache at a time
  across a cluster.
* Hit/Miss cache statistics to give you insight into what caches are less
  effectively utilized (and may need either higher expiration times, or just
  not very worthwhile to cache).
* Very small, compact code-base with 100% unit test coverage.


Locking
=======

A Redis based lock implemented as a Python context manager, based on `Chris
Lamb's example
<http://chris-lamb.co.uk/2010/06/07/distributing-locking-python-and-redis/>`_.

Example::
    
    from retools.lock import Lock
    
    with Lock('a_key', expires=60, timeout=10):
        # do something that should only be done one at a time


License
=======

``retools`` is offered under the MIT license.


Authors
=======

``retools`` is made available by `Ben Bangert`.
