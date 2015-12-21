<a href="http://sphia.org"><img src="http://media.charlesleifer.com/blog/photos/sophia-logo.png" width="215px" height="95px" /></a>

`sophy`, fast Python bindings for [Sophia Database](http://sphia.org), v2.1.

Features:

* Append-only MVCC database
* ACID transactions
* Consistent cursors
* Compression
* Ordered key/value store
* Range searches
* Prefix searches
* Python 2.x and 3.x

Limitations:

* Not tested on Windoze.

The source for `sophy` is [hosted on GitHub](https://github.com/coleifer/sophy)

If you encounter any bugs in the library, please [open an issue](https://github.com/coleifer/sophy/issues/new), including a description of the bug and any related traceback.

## Installation

The [sophia](http://sphia.org) sources are bundled with the `sophy` source code, so the only thing you need to install is [Cython](http://cython.org). You can install from [GitHub](https://github.com/coleifer/sophy) or from [PyPI](https://pypi.python.org/pypi/sophy/).

Pip instructions:

```console
$ pip install Cython sophy
```

Git instructions:

```console
$ pip install Cython
$ git clone https://github.com/coleifer/sophy
$ cd sophy
$ python setup.py build
$ python setup.py install
```

## Usage

Sophy is very simple to use. It acts primarly like a Python `dict` object, but in addition to normal dictionary operations, you can read slices of data that are returned efficiently using cursors. Similarly, bulk writes using `update()` use an efficient, atomic batch operation.

To begin, instantiate your Sophia database. Multiple databases can exist under the same path:

```pycon
>>> from sophy import Sophia
>>> db = Sophia('db-name', path='/tmp/sophia-env')
>>> db.open()
True
```

We can set values individually or in groups:

```pycon
>>> db['k1'] = 'v1'
>>> db.update(k2='v2', k3='v3', k4='v4')  # Efficient, atomic.
>>> with db.batch() as wb:  # Same as .update()
...     wb['k1'] = 'v1-e'
...     wb['k5'] = 'v5'
...
```

We can read values individually or in groups. When requesting a slice, the third parameter (`step`) is used to indicate the results should be returned in reverse. Alternatively, if the first key is higher than the second key in a slice, `sophy` will interpret that as ordered in reverse.

```pycon
>>> db['k1']
'v1-e'
>>> [item for item in db['k2': 'k444']]
[('k2', 'v2'), ('k3', 'v3'), ('k4', 'v4')]

>>> list(db['k3':'k1'])  # Results are returned in reverse.
[('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1-e')]

>>> list(db[:'k3'])  # All items from lowest key up to 'k3'.
[('k1', 'v1-e'), ('k2', 'v2'), ('k3', 'v3')]

>>> list(db[:'k3':True])  # Same as above, but ordered in reverse.
[('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1-e')]

>>> list(db['k3':])  # All items from k3 to highest key.
[('k3', 'v3'), ('k4', 'v4'), ('k5', 'v5')]
```

Values can also be deleted singly or in groups. To delete multiple items atomically use the `batch()` method.

```pycon
>>> del db['k3']
>>> db['k3']  # 'k3' no longer exists.
Traceback (most recent call last):
  ...
KeyError: 'k3'

>>> with db.batch() as wb:
...     del db['k2']
...     del db['k5']
...     del db['kxx']  # No error raised.
...
>>> list(db)
[('k1', 'v1-e'), ('k4', 'v4')]
```

### Transactions

In addition to atomic write batches, which support multiple writes and deletes, `sophy` supports transactions.

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

The same applies to `batch()` operations. `batch()` operations can also be committed or rolled back explicitly.

### Cursors

Sophia is an ordered key/value store, so cursors will by default iterate through the keyspace in ascending order. To iterate in descending order, you can specify this using the slicing technique described above. For finer-grained control, however, you can use the `cursor()` method.

The `Sophia.cursor()` method supports a number of interesting parameters:

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

Sophia supports a huge number of [configuration options](http://sphia.org/configuration.html), most of which are exposed as simple properties on the `Sophia` database object. For example, to configure `Sophia` to use mmap and compression:

```pycon
>>> db.close()  # We may need to close the db to set some options.
True
>>> db.mmap = True
>>> db.compression = 'zstd'
>>> db.open()  # Now using mmap and compression.
True
```

You can also force checkpointing, garbage-collection, and other things using simple methods:

```pycon
>>> db.checkpoint()
>>> db.gc()
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

Take a look at the [configuration docs](http://sphia.org/configuration.html) for more details.
