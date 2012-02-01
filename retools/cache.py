"""Caching

Cache regions are used to simplify common expirations and group function
caches.

To indicate functions should use cache regions, apply the decorator::

    from retools.cache import cache_region

    @cache_region('short_term')
    def myfunction(arg1):
        return arg1

To configure the cache regions, setup the :class:`~retools.cache.CacheRegion`
object::

    from retools.cache import CacheRegion

    CacheRegion.add_region("short_term", expires=60)

"""
import cPickle
import time
from datetime import date

from retools import global_connection
from retools.exc import CacheConfigurationError
from retools.lock import Lock
from retools.lock import LockTimeout
from retools.util import func_namespace
from retools.util import has_self_arg


class _NoneMarker(object):
    pass
NoneMarker = _NoneMarker()


class CacheKey(object):
    """Cache Key object

    Generator of cache keys for a variety of purposes once
    provided with a region, namespace, and key (args).

    """
    def __init__(self, region, namespace, key, today=None):
        """Setup a CacheKey object

        The CacheKey object creates the key-names used to store and
        retrieve values from Redis.

        :param region: Name of the region
        :type region: string
        :param namespace: Namespace to use
        :type namespace: string
        :param key: Key of the cached data, to differentiate various
                    arguments to the same callable

        """
        if not today:
            today = str(date.today())
        self.lock_key = 'retools:lock:%s:%s:%s' % (region, namespace, key)
        self.redis_key = 'retools:%s:%s:%s' % (region, namespace, key)
        self.redis_hit_key = 'retools:hits:%s:%s:%s:%s' % (
            today, region, namespace, key)
        self.redis_miss_key = 'retools:misses:%s:%s:%s:%s' % (
            today, region, namespace, key)
        self.redis_keyset = 'retools:%s:%s:keys' % (region, namespace)


class CacheRegion(object):
    """CacheRegion manager and configuration object

    For organization sake, the CacheRegion object is used to configure
    the available cache regions, query regions for currently cached
    keys, and set batches of keys by region for immediate expiration.

    Caching can be turned off globally by setting enabled to False::

        CacheRegion.enabled = False

    Statistics should also be turned on or off globally::

        CacheRegion.statistics = False

    However, if only some namespaces should have statistics recorded,
    then this should be used directly.

    """
    regions = {}
    enabled = True
    statistics = True

    @classmethod
    def add_region(cls, name, expires, redis_expiration=60 * 60 * 24 * 7):
        """Add a cache region to the current configuration

        :param name: The name of the cache region
        :type name: string
        :param expires: The expiration in seconds.
        :type expires: integer
        :param redis_expiration: How long the Redis key expiration is
                                 set for. Defaults to 1 week.
        :type redis_expiration: integer

        """
        cls.regions[name] = dict(expires=expires,
                                 redis_expiration=redis_expiration)

    @classmethod
    def _add_tracking(cls, pipeline, region, namespace, key):
        """Add's basic set members for tracking

        This is added to a Redis pipeline for a single round-trip to
        Redis.

        """
        pipeline.sadd('retools:regions', region)
        pipeline.sadd('retools:%s:namespaces' % region, namespace)
        pipeline.sadd('retools:%s:%s:keys' % (region, namespace), key)

    @classmethod
    def invalidate(cls, region):
        """Invalidate an entire region

        .. note::

            This does not actually *clear* the region of data, but
            just sets the value to expire on next access.

        :param region: Region name
        :type region: string

        """
        redis = global_connection.redis
        namespaces = redis.smembers('retools:%s:namespaces' % region)
        if not namespaces:
            return None

        # Locate the longest expiration of a region, so we can set
        # the created value far enough back to force a refresh
        longest_expire = max(
              [x['expires'] for x in CacheRegion.regions.values()])
        new_created = time.time() - longest_expire - 3600

        for ns in namespaces:
            cache_keyset_key = 'retools:%s:%s:keys' % (region, ns)
            keys = set(['']) | redis.smembers(cache_keyset_key)
            for key in keys:
                cache_key = 'retools:%s:%s:%s' % (region, ns, key)
                if not redis.exists(cache_key):
                    redis.srem(cache_keyset_key, key)
                else:
                    redis.hset(cache_key, 'created', new_created)

    @classmethod
    def load(cls, region, namespace, key, regenerate=True, callable=None,
             statistics=None):
        """Load a value from Redis, and possibly recreate it

        This method is used to load a value from Redis, and usually
        regenerates the value using the callable when provided.

        If ``regenerate`` is ``False`` and a ``callable`` is not passed
        in, then :obj:`~retools.cache.NoneMarker` will be returned.

        :param region: Region name
        :type region: string
        :param namespace: Namespace for the value
        :type namespace: string
        :param key: Key for this value under the namespace
        :type key: string
        :param regenerate: If False, then existing keys will always be
                           returned regardless of cache expiration. In the
                           event that there is no existing key and no
                           callable was provided, then a NoneMarker will
                           be returned.
        :type regenerate: bool
        :param callable: A callable to use when the cached value needs to be
                         created
        :param statistics: Whether or not hit/miss statistics should be
                           updated
        :type statistics: bool

        """
        if statistics is None:
            statistics = cls.statistics
        redis = global_connection.redis
        now = time.time()
        region_settings = cls.regions[region]
        expires = region_settings['expires']
        redis_expiration = region_settings['redis_expiration']

        keys = CacheKey(region=region, namespace=namespace, key=key)

        # Create a transaction to update our hit counter for today and
        # retrieve the current value.
        if statistics:
            p = redis.pipeline(transaction=True)
            p.hgetall(keys.redis_key)
            p.get(keys.redis_hit_key)
            p.incr(keys.redis_hit_key)
            results = p.execute()
            result, existing_hits = results[0], results[1]
            if existing_hits is None:
                existing_hits = 0
            else:
                existing_hits = int(existing_hits)
        else:
            result = redis.hgetall(keys.redis_key)

        expired = True
        if result and now - float(result['created']) < expires:
            expired = False

        if (result and not regenerate) or not expired:
            # We have a result and were told not to regenerate so
            # we always return it immediately regardless of expiration,
            # or its not expired
            return cPickle.loads(result['value'])

        if not result and not regenerate:
            # No existing value, but we were told not to regenerate it and
            # there's no callable, so we return a NoneMarker
            return NoneMarker

        # Don't wait for the lock if we have an old value
        if result and 'value' in result:
            timeout = 0
        else:
            timeout = 60 * 60

        try:
            with Lock(keys.lock_key, expires=expires, timeout=timeout):
                # Did someone else already create it?
                result = redis.hgetall(keys.redis_key)
                now = time.time()
                if result and 'value' in result and \
                   now - float(result['created']) < expires:
                    return cPickle.loads(result['value'])

                value = callable()

                p = redis.pipeline(transaction=True)
                p.hmset(keys.redis_key, {'created': now,
                                    'value': cPickle.dumps(value)})
                p.expire(keys.redis_key, redis_expiration)
                cls._add_tracking(p, region, namespace, key)
                if statistics:
                    p.getset(keys.redis_hit_key, 0)
                    new_hits = int(p.execute()[0])
                else:
                    p.execute()
        except LockTimeout:
            if result:
                return cPickle.loads(result['value'])
            else:
                # log some sort of error?
                return NoneMarker

        # Nothing else to do if not recording stats
        if not statistics:
            return value

        misses = new_hits - existing_hits
        if misses:
            p = redis.pipeline(transaction=True)
            p.incr(keys.redis_hit_key, amount=existing_hits)
            p.incr(keys.redis_miss_key, amount=misses)
            p.execute()
        else:
            redis.incr(keys.redis_hit_key, amount=existing_hits)
        return value


def invalidate_region(region):
    """Invalidate all the namespace's in a given region

    .. note::

        This does not actually *clear* the region of data, but
        just sets the value to expire on next access.

    :param region: Region name
    :type region: string

    """
    CacheRegion.invalidate(region)


def invalidate_callable(callable, *args):
    """Invalidate the cache for a callable

    :param callable: The callable that was cached
    :type callable: callable object
    :param \*args: Arguments the function was called with that
                   should be invalidated. If the args is just the
                   differentiator for the function, or not present, then all
                   values for the function will be invalidated.

    Example::

        @cache_region('short_term', 'small_engine')
        def local_search(search_term):
            # do search and return it

        @cache_region('long_term')
        def lookup_folks():
            # look them up and return them

        # To clear local_search for search_term = 'fred'
        invalidate_function(local_search, 'fred')

        # To clear all cached variations of the local_search function
        invalidate_function(local_search)

        # To clear out lookup_folks
        invalidate_function(lookup_folks)

    """
    redis = global_connection.redis
    region = callable._region
    namespace = callable._namespace

    # Get the expiration for this region
    new_created = time.time() - CacheRegion.regions[region]['expires'] - 3600

    if args:
        try:
            cache_key = " ".join(map(str, args))
        except UnicodeEncodeError:
            cache_key = " ".join(map(unicode, args))
        redis.hset('retools:%s:%s:%s' % (region, namespace, cache_key),
                   'created', new_created)
    else:
        cache_keyset_key = 'retools:%s:%s:keys' % (region, namespace)
        keys = set(['']) | redis.smembers(cache_keyset_key)
        p = redis.pipeline(transaction=True)
        for key in keys:
            p.hset('retools:%s:%s:%s' % (region, namespace, key), 'created',
                   new_created)
        p.execute()
    return None
invalidate_function = invalidate_callable


def cache_region(region, *deco_args, **kwargs):
    """Decorate a function such that its return result is cached,
    using a "region" to indicate the cache arguments.

    :param region: Name of the region to cache to
    :type region: string
    :param \*deco_args: Optional ``str()``-compatible arguments which will
      uniquely identify the key used by this decorated function, in addition
      to the positional arguments passed to the function itself at call time.
      This is recommended as it is needed to distinguish between any two
      functions or methods that have the same name (regardless of parent
      class or not).
    :type deco_args: list

    .. note::

        The function being decorated must only be called with
        positional arguments, and the arguments must support
        being stringified with ``str()``.  The concatenation
        of the ``str()`` version of each argument, combined
        with that of the ``*args`` sent to the decorator,
        forms the unique cache key.

    Example::

        from retools.cache import cache_region

        @cache_region('short_term', 'load_things')
        def load(search_term, limit, offset):
            '''Load from a database given a search term, limit, offset.'''
            return database.query(search_term)[offset:offset + limit]

    The decorator can also be used with object methods.  The ``self``
    argument is not part of the cache key.  This is based on the
    actual string name ``self`` being in the first argument
    position::

        class MyThing(object):
            @cache_region('short_term', 'load_things')
            def load(self, search_term, limit, offset):
                '''Load from a database given a search term, limit, offset.'''
                return database.query(search_term)[offset:offset + limit]

    Classmethods work as well - use ``cls`` as the name of the class argument,
    and place the decorator around the function underneath ``@classmethod``::

        class MyThing(object):
            @classmethod
            @cache_region('short_term', 'load_things')
            def load(cls, search_term, limit, offset):
                '''Load from a database given a search term, limit, offset.'''
                return database.query(search_term)[offset:offset + limit]

    .. note::

        When a method on a class is decorated, the ``self`` or ``cls``
        argument in the first position is
        not included in the "key" used for caching.

    """
    def decorate(func):
        namespace = func_namespace(func, deco_args)
        skip_self = has_self_arg(func)
        regenerate = kwargs.get('regenerate', True)

        def cached(*args):
            if region not in CacheRegion.regions:
                raise CacheConfigurationError(
                    'Cache region not configured: %s' % region)
            if not CacheRegion.enabled:
                return func(*args)

            if skip_self:
                try:
                    cache_key = " ".join(map(str, args[1:]))
                except UnicodeEncodeError:
                    cache_key = " ".join(map(unicode, args[1:]))
            else:
                try:
                    cache_key = " ".join(map(str, args))
                except UnicodeEncodeError:
                    cache_key = " ".join(map(unicode, args))

            def go():
                return func(*args)
            return CacheRegion.load(region, namespace, cache_key,
                                    regenerate=regenerate, callable=go)
        cached._region = region
        cached._namespace = namespace
        return cached
    return decorate
