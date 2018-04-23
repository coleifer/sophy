<a href="http://sophia.systems/"><img src="http://media.charlesleifer.com/blog/photos/sophia-logo.png" width="215px" height="95px" /></a>

[sophy](http://sophy.readthedocs.io/en/latest/), fast Python bindings for
[Sophia embedded database](http://sophia.systems), v2.2.

<a href="https://travis-ci.org/coleifer/sophy"><img src="https://api.travis-ci.org/coleifer/sophy.svg?branch=master" /></a>

#### About sophy

* Written in Cython for speed and low-overhead
* Clean, memorable APIs
* Extensive support for Sophia's features
* Python 2 **and** Python 3 support
* No 3rd-party dependencies besides Cython
* [Documentation on readthedocs](http://sophy.readthedocs.io/en/latest/)

#### About Sophia

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

#### Some ideas of where Sophia might be a good fit

* Running on application servers, low-latency / high-throughput
* Time-series
* Analytics / Events / Logging
* Full-text search
* Secondary-index for external data-store

#### Limitations

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

Or to install the latest code from master:

```console
$ pip install -e git+https://github.com/coleifer/sophy#egg=sophy
```

Git instructions:

```console
$ pip install Cython
$ git clone https://github.com/coleifer/sophy
$ cd sophy
$ python setup.py build
$ python setup.py install
```

To run the tests:

```console
$ python tests.py
```

![](http://media.charlesleifer.com/blog/photos/sophy-logo.png)

---------------------------------------------

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

# We'll define a very simple schema consisting of a single utf-8 string for the
# key, and a single utf-8 string for the associated value.
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
print db['name'], 'is a', db['animal_type']  # Huey is a cat

'name' in db  # True
'color' in db  # False

db['temp_val'] = 'foo'
del db['temp_val']
print db['temp_val']  # raises a KeyError.
```

Use `update()` for bulk-insert, and `multi_get()` for bulk-fetch. Unlike
`__getitem__()`, calling `multi_get()` with a non-existant key will not raise
an exception and return `None` instead.

```python
db.update(k1='v1', k2='v2', k3='v3')

for value in db.multi_get('k1', 'k3', 'kx'):
    print value
# v1
# v3
# None

result_dict = db.multi_get_dict(['k1', 'k3', 'kx'])
# {'k1': 'v1', 'k3': 'v3'}
```

### Other dictionary methods

Sophy databases also provides efficient implementations for  `keys()`,
`values()` and `items()`. Unlike dictionaries, however, iterating directly over
a Sophy database will return the equivalent of the `items()` (as opposed to the
just the keys):

```python

db.update(k1='v1', k2='v2', k3='v3')

list(db)
# [('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]


db.items()
# same as above.


db.keys()
# ['k1', 'k2', 'k3']


db.values()
# ['v1', 'v2', 'v3']
```

There are two ways to get the count of items in a database. You can use the
`len()` function, which is not very efficient since it must allocate a cursor
and iterate through the full database. An alternative is the `index_count`
property, which may not be exact as it includes transactional duplicates and
not-yet-merged duplicates.

```python

print(len(db))
# 4

print(db.index_count)
# 4
```

### Fetching ranges

Because Sophia is an ordered data-store, performing ordered range scans is
efficient. To retrieve a range of key-value pairs with Sophy, use the ordinary
dictionary lookup with a `slice` instead.

```python

db.update(k1='v1', k2='v2', k3='v3', k4='v4')


# Slice key-ranges are inclusive:
db['k1':'k3']
# [('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]


# Inexact matches are fine, too:
db['k1.1':'k3.1']
# [('k2', 'v2'), ('k3', 'v3')]


# Leave the start or end empty to retrieve from the first/to the last key:
db[:'k2']
# [('k1', 'v1'), ('k2', 'v2')]

db['k3':]
# [('k3', 'v3'), ('k4', 'v4')]


# To retrieve a range in reverse order, use the higher key first:
db['k3':'k1']
# [('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1')]
```

To retrieve a range in reverse order where the start or end is unspecified, you
can pass in `True` as the `step` value of the slice to also indicate reverse:

```python

db[:'k2':True]
# [('k2', 'k1'), ('k1', 'v1')]

db['k3'::True]
# [('k4', 'v4'), ('k3', 'v3')]

db[::True]
# [('k4', 'v4'), ('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1')]
```

### Cursors

For finer-grained control over iteration, or to do prefix-matching, Sophy
provides a cursor interface.

The `cursor()` method accepts 5 parameters:

* `order` (default=`>=`) -- semantics for matching the start key and ordering
  results.
* `key` -- the start key
* `prefix` -- search for prefix matches
* `keys` -- (default=`True`) -- return keys while iterating
* `values` -- (default=`True`) -- return values while iterating

Suppose we were storing events in a database and were using an
ISO-8601-formatted date-time as the key. Since ISO-8601 sorts
lexicographically, we could retrieve events in correct order simply by
iterating. To retrieve a particular slice of time, a prefix could be specified:

```python

# Iterate over events for July, 2017:
for timestamp, event_data in db.cursor(key='2017-07-01T00:00:00',
                                       prefix='2017-07-'):
    do_something()
```

### Transactions

Sophia supports ACID transactions. Even better, a single transaction can cover
operations to multiple databases in a given environment.

Example usage:

```python

account_balance = env.add_database('balance', ...)
transaction_log = env.add_database('transaction_log', ...)

# ...

def transfer_funds(from_acct, to_acct, amount):
    with env.transaction() as txn:
        # To write to a database within a transaction, obtain a reference to
        # a wrapper object for the db:
        txn_acct_bal = txn[account_balance]
        txn_log = txn[transaction_log]

        # Transfer the asset by updating the respective balances. Note that we
        # are operating on the wrapper database, not the db instance.
        from_bal = txn_acct_bal[from_acct]
        txn_acct_bal[to_account] = from_bal + amount
        txn_acct_bal[from_account] = from_bal - amount

        # Log the transaction in the transaction_log database. Again, we use
        # the wrapper for the database:
        txn_log[from_account, to_account, get_timestamp()] = amount
```

Multiple transactions are allowed to be open at the same time, but if there are
conflicting changes, an exception will be thrown when attempting to commit the
offending transaction:

```python

# Create a basic k/v store. Schema.key_value() is a convenience/factory-method.
kv = env.add_database('main', Schema.key_value())

# ...

# Instead of using the context manager, we'll call begin() explicitly so we
# can show the interaction of 2 open transactions.
txn = env.transaction().begin()

t_kv = txn[kv]
t_kv['k1'] = 'v1'

txn2 = env.transaction().begin()
t2_kv = txn2[kv]

t2_kv['k1'] = 'v1-x'

txn2.commit()  # ERROR !!
# SophiaError('txn is not finished, waiting for concurrent txn to finish.')

txn.commit()  # OK

# Try again?
txn2.commit()  # ERROR !!
# SophiaError('transasction rolled back by another concurrent transaction.')
```

## Index types, multi-field keys and values

Sophia supports multi-field keys and values. Additionally, the individual
fields can have different data-types. Sophy provides the following field
types:

* `StringIndex` - stores UTF8-encoded strings, e.g. text.
* `BytesIndex` - stores bytestrings, e.g. binary data.
* `JsonIndex` - stores arbitrary objects as UTF8-encoded JSON data.
* `MsgPackIndex` - stores arbitrary objects using `msgpack` serialization.
* `PickleIndex` - stores arbitrary objects using Python `pickle` library.
* `UUIDIndex` - stores UUIDs.
* `U64Index` and reversed, `U64RevIndex`
* `U32Index` and reversed, `U32RevIndex`
* `U16Index` and reversed, `U16RevIndex`
* `U8Index` and reversed, `U8RevIndex`
* `SerializedIndex` - which is basically a `BytesIndex` that accepts two
  functions: one for serializing the value to the db, and another for
  deserializing.

To store arbitrary data encoded using msgpack, you could use `MsgPackIndex`:

```python

schema = Schema(StringIndex('key'), MsgPackIndex('value'))
db = sophia_env.add_database('main', schema)
```

To declare a database with a multi-field key or value, you will pass the
individual fields as arguments when constructing the `Schema` object. To
initialize a schema where the key is composed of two strings and a 64-bit
unsigned integer, and the value is composed of a string, you would write:

```python

key = [StringIndex('last_name'), StringIndex('first_name'), U64Index('area_code')]
value = [StringIndex('address_data')]
schema = Schema(key_parts=key, value_parts=value)

address_book = sophia_env.add_database('address_book', schema)
```

To store data, we use the same dictionary methods as usual, just passing tuples
instead of individual values:

```python
sophia_env.open()

address_book['kitty', 'huey', 66604] = '123 Meow St'
address_book['puppy', 'mickey', 66604] = '1337 Woof-woof Court'
```

To retrieve our data:

```python
huey_address = address_book['kitty', 'huey', 66604]
```

To delete a row:

```python
del address_book['puppy', 'mickey', 66604]
```

Indexing and slicing works as you would expect.

**Note:** when working with a multi-part value, a tuple containing the value
components will be returned. When working with a scalar value, instead of
returning a 1-item tuple, the value itself is returned.

## Configuring and Administering Sophia

Sophia can be configured using special properties on the `Sophia` and
`Database` objects. Refer to the [configuration
document](http://sophia.systems/v2.2/conf/sophia.html) for the details on the
available options, including whether they are read-only, and the expected
data-type.

For example, to query Sophia's status, you can use the `status` property, which
is a readonly setting returning a string:

```python
print(env.status)
"online"
```

Other properties can be changed by assigning a new value to the property. For
example, to read and then increase the number of threads used by the scheduler:

```python
nthreads = env.scheduler_threads
env.scheduler_threads = nthread + 2
```

Database-specific properties are available as well. For example to get the
number of GET and SET operations performed on a database, you would write:

```python
print(db.stat_get, 'get operations')
print(db.stat_set, 'set operations')
```

Refer to the [documentation](http://sophia.systems/v2.2/conf/sophia.html) for
complete lists of settings. Dotted-paths are translated into
underscore-separated attributes.
