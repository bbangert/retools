=======
retools
=======

Next release
============

- Job/Worker system based on Ruby's Resque model.


0.2 (**tip**)
=============

Incompatibilities
-----------------

- Removed clear argument from invalidate_region, as removing keys from the
  set but not removing the hit statistics can lead to data accumulating in
  Redis that has no easy removal other than .keys() which should not be run
  in production environments.


0.1 (07/08/2011)
================

Features
--------

- Caching in a similar style to Beaker, with hit/miss statistics, backed by
  a Redis global write-lock with old values served to prevent the dogpile
  effect
- Redis global lock
