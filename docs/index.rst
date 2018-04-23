.. sophy documentation master file, created by
   sphinx-quickstart on Sun Apr 22 20:55:15 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: http://media.charlesleifer.com/blog/photos/sophia-logo.png
   :target: http://sophia.systems
   :alt: sophia database

sophy
=====

Python binding for `sophia <http://sophia.systems>`_ embedded database, v2.2.

* Written in Cython for speed and low-overhead
* Clean, memorable APIs
* Comprehensive support for Sophia's features
* Supports Python 2 and 3.
* No 3rd-party dependencies besides Cython (for building).

About Sophia:

* Ordered key/value store
* Keys and values can be composed of multiple fieldsdata-types
* ACID transactions
* MVCC, optimistic, non-blocking concurrency with multiple readers and writers.
* Multiple databases per environment
* Multiple- and single-statement transactions across databases
* Prefix searches
* Automatic garbage collection and key expiration
* Hot backup
* Compression
* Multi-threaded compaction
* ``mmap`` support, direct I/O support
* APIs for variety of statistics on storage engine internals
* BSD licensed

Some ideas of where Sophia might be a good fit:

* Running on application servers, low-latency / high-throughput
* Time-series
* Analytics / Events / Logging
* Full-text search
* Secondary-index for external data-store

Limitations:

* Not tested on Windoze.

If you encounter any bugs in the library, please `open an issue <https://github.com/coleifer/sophy/issues/new>`_,
including a description of the bug and any related traceback.

.. image:: http://media.charlesleifer.com/blog/photos/sophy-logo.png
   :alt: Sophy logo

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   :glob:

   installation
   quickstart
   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
