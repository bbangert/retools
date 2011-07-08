=======
retools
=======

Next release
============

- Job/Worker system based on Ruby's Resque model.

0.1 (07/08/2011)
================

Features
--------

- Caching in a similar style to Beaker, with hit/miss statistics, backed by
  a Redis global write-lock with old values served to prevent the dogpile
  effect
- Redis global lock
