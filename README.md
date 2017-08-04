<a href="http://sophia.systems/"><img src="http://media.charlesleifer.com/blog/photos/sophia-logo.png" width="215px" height="95px" /></a>

`sophy`, fast Python bindings for [Sophia embedded database](http://sophia.systems), v2.2.

About sophy:

* Written in Cython for speed and low-overhead
* Clean, memorable APIs
* Extensive support for Sophia's features
* Python 2 **and** Python 3 support
* No 3rd-party dependencies besides Cython

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
* `mmap` support, direct I/O support
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

If you encounter any bugs in the library, please [open an issue](https://github.com/coleifer/sophy/issues/new), including a description of the bug and any related traceback.

## Installation

The [sophia](http://sophia.systems) sources are bundled with the `sophy` source
code, so the only thing you need to install is [Cython](http://cython.org). You
can install from [GitHub](https://github.com/coleifer/sophy) or from
[PyPI](https://pypi.python.org/pypi/sophy/).

Pip instructions:

```console
$ pip install Cython
$ pip install sophy
```

Git instructions:

```console
$ pip install Cython
$ git clone https://github.com/coleifer/sophy
$ cd sophy
$ python setup.py build
$ python setup.py install
```

## Overview

Sophy is very simple to use. It acts like a Python `dict` object, but in
addition to normal dictionary operations, you can read slices of data that are
returned efficiently using cursors. Similarly, bulk writes using `update()` use
an efficient, atomic batch operation.

Despite the simple APIs, Sophia has quite a few advanced features. There is too
much to cover everything in this document, so be sure to check out the official
[Sophia storage engine documentation](http://sophia.systems/v2.2/).

The next section will show how to perform common actions with `sophy`.

## Using Sophy

Let's begin by import `sophy` and creating an environment. The environment
can host multiple databases, each of which may have a different schema. In this
example our database will store arbitrary binary data as the key and value.
Finally we'll open the environment so we can start storing and retrieving data.

```python
from sophy import Sophia, Schema, StringIndex

# Instantiate our environment by passing a directory path which will store the
# various data and metadata for our databases.
env = Sophia('/path/to/store/data')

# We'll define a very simple schema consisting of a single binary value for the
# key, and a single binary value for the associated value.
schema = Schema(key_parts=[StringIndex('key')],
                value_parts=[StringIndex('value')])

# Create a key/value database using the schema above.
db = env.add_database('example_db', schema)

if not env.open():
    raise Exception('Unable to open Sophia environment.')
```

### CRUD operations

Sophy databases use the familiar `dict` APIs for CRUD operations:

```python

db['name'] = 'Huey'
db['animal_type'] = 'cat'
print db['name'], 'is a', db['animal_type']

db['temp_val'] = 'foo'
del db['temp_val']
print db['temp_val']  # raises a KeyError.

'name' in db  # True
'color' in db  # False
```
