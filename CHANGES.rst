=======
retools
=======


0.2 (**tip**)
=============

Features
--------

- Statistics for the cache is now optional and can be disabled to slightly
  reduce the Redis queries used to store/retrieve cache data.

Internals
---------

- Increased conditional coverage to 100% (via instrumental).


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
