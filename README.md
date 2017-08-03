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

To begin, I've opened a Python terminal and instantiated a Sophia environment
with one database (an environment can have multiple databases, though). When I
called `create_database()` I specified my key/value store's name and the schema
for the keys and values. Keys and values can be composed of one or more fields,
where each field can be a string, 64-, 32-, 16- or 8-bit integer.

```pycon
>>> from sophy import Sophia
>>> env = Sophia('/path/to/data-dir')
>>> key_schema = [
...     U64Index('timestamp'),
...     StringIndex('event')]
>>> value_schema = [
...     StringIndex('source'),
...     StringIndex('data')]
>>> events = env.add_database('events, Schema(key_schema, value_schema))
>>> env.open()
True
```

The `db` object can now be used to store and retrieve data. Our keys will be
2-tuples consisting of an integer timestamp and an event identifier. The values
will also be 2-tuples consisting of a string identifying the event source and
arbitrary data associated with the event.

In the examples below we will look at how to store and retrieve data:

```pycon
>>> def timestamp():
...     # Store timestamp as integer in milliseconds.
...     return int(time.time() * 1000)
...
>>> db[time.time()
>>> db.update(k2='v2', k3='v3', k4='v4')  # Efficient, atomic.
```

You can also use transactions:

```pycon
>>> with db.transaction() as txn:  # Same as .update()
...     txn['k1'] = 'v1-e'
...     txn['k5'] = 'v5'
...
```

We can read values individually or in groups. When requesting a slice, the third parameter (`step`) is used to indicate the results should be returned in reverse. Alternatively, if the first key is higher than the second key in a slice, `sophy` will interpret that as ordered in reverse.

Here we're reading a single value, then iterating over a range of keys. When iterating over slices of the database, both the keys and the values are returned.

```pycon
>>> db['k1']
'v1-e'
>>> [item for item in db['k2': 'k444']]
[('k2', 'v2'), ('k3', 'v3'), ('k4', 'v4')]
```

Slices can also return results in reverse, starting at the beginning, end, or middle.

```pycon
>>> list(db['k3':'k1'])  # Results are returned in reverse.
[('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1-e')]

>>> list(db[:'k3'])  # All items from lowest key up to 'k3'.
[('k1', 'v1-e'), ('k2', 'v2'), ('k3', 'v3')]

>>> list(db[:'k3':True])  # Same as above, but ordered in reverse.
[('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1-e')]

>>> list(db['k3':])  # All items from k3 to highest key.
[('k3', 'v3'), ('k4', 'v4'), ('k5', 'v5')]
```

Values can also be deleted singly or in groups. To delete multiple items atomically use the `transaction()` method.

```pycon
>>> del db['k3']
>>> db['k3']  # 'k3' no longer exists.
Traceback (most recent call last):
  ...
KeyError: 'k3'

>>> with db.transaction() as txn:
...     del txn['k2']
...     del txn['k5']
...     del txn['kxx']  # No error raised.
...
>>> list(db)
[('k1', 'v1-e'), ('k4', 'v4')]
```

### Transactions

```pycon
>>> with db.transaction() as txn:
...     txn['k2'] = 'v2-e'
...     txn['k1'] = 'v1-e2'
...
>>> list(db[::True])
[('k4', 'v4'), ('k2', 'v2-e'), ('k1', 'v1-e2')]
```

You can call `commit()` or `rollback()` inside the transaction block itself:

```pycon
>>> with db.transaction() as txn:
...     txn['k1'] = 'whoops'
...     txn.rollback()
...     txn['k2'] = 'v2-e2'
...
>>> list(db.items())
[('k1', 'v1-e2'), ('k2', 'v2-e2'), ('k4', 'v4')]
```

If an exception occurs in the wrapped block, the transaction will automatically be rolled back.

### Cursors

Sophia is an ordered key/value store, so cursors will by default iterate through the keyspace in ascending order. To iterate in descending order, you can specify this using the slicing technique described above. For finer-grained control, however, you can use the `cursor()` method.

The `Database.cursor()` method supports a number of interesting parameters:

* `order`: either `'>='` (default), `'<='` (reverse), `'>'` (ascending not including endpoint), `'<'` (reverse, not including endpoint).
* `key`: seek to this key before beginning to iterate.
* `prefix`: perform a prefix search.
* `keys`: include the database key while iterating (default `True`).
* `values`: include the database value while iterating (default `True`).

To perform a prefix search, for example, you might do something like:

```pycon
>>> db.update(aa='foo', abc='bar', az='baze', baz='nugget')
>>> for item in db.cursor(prefix='a'):
...     print item
...
('aa', 'foo')
('abc', 'bar')
('az', 'baze')
```

### Configuration

Sophia supports a huge number of [configuration options](http://sophia.systems/v2.1/conf/sophia.html), most of which are exposed as simple properties on the `Sophia` or `Database` objects. For example, to configure `sophy` with a memory limit and to use mmap and compression:

```pycon
>>> env = Sophia('my/data-dir', auto_open=False)
>>> env.memory_limit = 1024 * 1024 * 1024  # 1GB
>>> db = env.create_database('test-db', index_type=('string', 'u64'))
>>> db.mmap = True
>>> db.compression = 'lz4'
>>> env.open()
```

You can also force checkpointing, garbage-collection, and other things using simple methods:

```pycon
>>> env.scheduler_checkpoint()
>>> env.scheduler_gc()
```

Some properties are read-only:

```pycon
>>> db.index_count
10
>>> len(db)
10
>>> db.status
'online'
>>> db.memory_used
69
```

Take a look at the [configuration docs](http://sophia.systems/v2.1/conf/database.html) for more details.

### Multi-part keys

In addition to string keys, Sophy supports uint32, uint64, and any combination of the above. So, if I had a database that had a natural index on a timestamp (stored as an unsigned 64-bit integer) and a string, I could have a fast, multi-part key that stored both.

To use multi-part keys, just use tuples where you would otherwise use strings or ints.

```pycon
>>> db = env.create_database('multi', ('string', 'u64'))
>>> db[('hello', 100)] = 'sophy'
>>> print db[('hello', 100)]
sophy
```

Multi-part keys support slicing, and the ordering is derived from left-to-right.

### Specifying databases on startup

Because Sophia does not store any schema information, every time your app starts up you will need to provide it with the databases to connect to.

`Sophy` supports a very simple API for telling a database environment what key/value stores are present and should be loaded up:

```python
db_list = [
    ('users', 'string'),
    ('clicks', ('string', 'u64')),
    ('tweets', ('string', 'u64')),
]

env = Sophia('/var/lib/sophia/my-app', databases=db_list)

# After creating the environment, we can now access our data-stores.
user_db = env['users']
click_db = env['clicks']
tweet_db = env['tweets']
```

### Views

Views provide a read-only, point-in-time snapshot of the database. Views cannot be written to nor deleted from, but they support all the familiar reading and iteration APIs. Here is an example:

```pycon
>>> db.update(k1='v1', k2='v2', k3='v3')
>>> view = db.view('view-1')
>>> print view['k1']
'v1'
```

Now we'll make modifications to the database, and observe the view is not affected:

```pycon
>>> db['k1'] = 'v1-e'
>>> db['k3'] = 'v3-e'
>>> del db['k2']
>>> print [item for item in view]  # Values in view are unmodified.
[('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]
```

When you are done with a view, you can call `view.close()`.
