.. _quickstart:

Quick-start
===========

Sophy is very simple to use. It acts like a Python ``dict`` object, but in
addition to normal dictionary operations, you can read slices of data that are
returned efficiently using cursors. Similarly, bulk writes using ``update()``
use an efficient, atomic batch operation.

Despite the simple APIs, Sophia has quite a few advanced features. There is too
much to cover everything in this document, so be sure to check out the official
`Sophia storage engine documentation <http://sophia.systems/v2.2/>`_.

The next section will show how to perform common actions with ``sophy``.

Using Sophy
-----------

Let's begin by importing ``sophy`` and creating an *environment*. The
environment can host multiple *databases*, each of which may have a different
*schema*. In this example our database will store UTF-8 strings as the key and
value (though other data-types are supported). Finally we'll open the
environment so we can start storing and retrieving data.

.. code-block:: python

    from sophy import Sophia, Schema, StringIndex

    # Instantiate our environment by passing a directory path which will store
    # the various data and metadata for our databases.
    env = Sophia('/tmp/sophia-example')

    # We'll define a very simple schema consisting of a single utf-8 string for
    # the key, and a single utf-8 string for the associated value. Note that
    # the key or value accepts multiple indexes, allowing for composite
    # data-types.
    schema = Schema([StringIndex('key')], [StringIndex('value')])

    # Create a key/value database using the schema above.
    db = env.add_database('example_db', schema)

    if not env.open():
        raise Exception('Unable to open Sophia environment.')

In the above example we used :py:class:`StringIndex` which stores UTF8-encoded
string data. The following index types are available:

* :py:class:`StringIndex` - UTF8-encoded string data (text, in other words).
* :py:class:`BytesIndex` - bytestrings (binary data).
* :py:class:`JsonIndex` - store value as UTF8-encoded JSON.
* :py:class:`MsgPackIndex` - store arbitrary data using msgpack encoding.
* :py:class:`PickleIndex` - store arbitrary data using python pickle module.
* :py:class:`UUIDIndex` - store UUIDs.
* :py:class:`SerializedIndex` - index that accepts serialize/deserialize
  functions and can be used for msgpack or pickled data, for example.
* :py:class:`U64Index` - store 64-bit unsigned integers.
* :py:class:`U32Index` - store 32-bit unsigned integers.
* :py:class:`U16Index` - store 16-bit unsigned integers.
* :py:class:`U8Index` - store 8-bit unsigned integers (or single bytes).
* There are also :py:class:`U64RevIndex`, :py:class:`U32RevIndex`,
  :py:class:`U16RevIndex` and :py:class:`U8RevIndex` for storing integers in
  reverse order.

CRUD operations
---------------

Sophy databases use the familiar ``dict`` APIs for CRUD operations:

.. code-block:: pycon

    >>> db['name'] = 'Huey'
    >>> db['animal_type'] = 'cat'
    >>> print(db['name'], 'is a', db['animal_type'])
    Huey is a cat

    >>> 'name' in db
    True
    >>> 'color' in db
    False

    >>> del db['name']
    >>> del db['animal_type']
    >>> print(db['name'])  # raises a KeyError.
    KeyError: ('name',)

To insert multiple items efficiently, use the :py:meth:`Database.update`
method. Multiple items can be retrieved or deleted efficiently using
:py:meth:`Database.multi_get`, :py:meth:`Database.multi_get_dict`, and
:py:meth:`Database.multi_delete`:

.. code-block:: pycon

    >>> db.update(k1='v1', k2='v2', k3='v3')
    >>> for value in db.multi_get('k1', 'k3', 'kx'):
    ...     print(value)

    v1
    v3
    None

    >>> db.multi_get_dict(['k1', 'k3', 'kx'])
    {'k1': 'v1', 'k3': 'v3'}

    >>> db.multi_delete('k1', 'k3', 'kx')
    >>> 'k1' in db
    False

Other dictionary methods
------------------------

Sophy databases also provide efficient implementations of
:py:meth:`~Database.keys`, :py:meth:`~Database.values` and
:py:meth:`~Database.items` for iterating over the data-set. Unlike
dictionaries, however, iterating directly over a Sophy :py:class:`Database`
will return the equivalent of the :py:meth:`~Database.items` method (as opposed
to just the keys).

.. note::
    Sophia is an ordered key/value store, so iteration will return items in the
    order defined by their index. So for strings and bytes, this is
    lexicographic ordering. For integers it can be ascending or descending.

.. code-block:: pycon

    >>> db.update(k1='v1', k2='v2', k3='v3')
    >>> list(db)
    [('k1', 'v1'),
     ('k2', 'v2'),
     ('k3', 'v3')]

    >>> db.items()  # Returns a Cursor, which can be iterated.
    <sophy.Cursor at 0x7f1dac231ee8>
    >>> [item for item in db.items()]
    [('k1', 'v1'),
     ('k2', 'v2'),
     ('k3', 'v3')]

    >>> list(db.keys())
    ['k1', 'k2', 'k3']

    >>> list(db.values())
    ['v1', 'v2', 'v3']

There are two ways to get the count of items in a database. You can use the
``len()`` function, which is not very efficient since it must allocate a cursor
and iterate through the full database. An alternative is the
:py:attr:`Database.index_count` property, which may not be exact as it includes
transaction duplicates and not-yet-merged duplicates:

.. code-block:: pycon

    >>> len(db)
    3
    >>> db.index_count
    3

Range queries
-------------

Because Sophia is an ordered data-store, performing ordered range scans is
efficient. To retrieve a range of key-value pairs with Sophy, use the ordinary
dictionary lookup with a ``slice`` as the index:

.. code-block:: python

    >>> db.update(k1='v1', k2='v2', k3='v3', k4='v4')
    >>> db['k1':'k3']
    <generator at 0x7f1db413bbf8>

    >>> list(db['k1':'k3'])  # NB: other examples omit list() for clarity.
    [('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]

    >>> db['k1.x':'k3.x']  # Inexact matches are OK, too.
    [('k2', 'v2'), ('k3', 'v3')]

    >>> db[:'k2']  # Omitting start or end retrieves from first/last key.
    [('k1', 'v1'), ('k2', 'v2')]

    >>> db['k3':]
    [('k3', 'v3'), ('k4', 'v4')]

    >>> db['k3':'k1']  # To retrieve a range in reverse, use the higher key first.
    [('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1')]

To retrieve a range in reverse order where the start or end is unspecified, you
can pass in ``True`` as the ``step`` value of the slice to also indicate
reverse:

.. code-block:: pycon

    >>> db[:'k2':True]  # Start-to-"k2" in reverse.
    [('k2', 'v2'), ('k1', 'v1')]

    >>> db['k3'::True]
    [('k4', 'v4'), ('k3', 'v3')]

    >>> db[::True]
    [('k4', 'v4'), ('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1')]

Cursors
-------

For finer-grained control over iteration, or to do prefix-matching, Sophy
provides a :py:class:`Cursor` interface.

The :py:meth:`~Database.cursor` method accepts five parameters:

* ``order`` (default=``>=``) - semantics for matching the start key and
  ordering results.
* ``key`` - the start key
* ``prefix`` - search for prefix matches
* ``keys`` - (default=``True``) -- return keys while iterating
* ``values`` - (default=``True``) -- return values while iterating

Suppose we were storing events in a database and were using an
ISO-8601-formatted date-time as the key. Since ISO-8601 sorts
lexicographically, we could retrieve events in correct order simply by
iterating. To retrieve a particular slice of time, a prefix could be specified:

.. code-block:: python

    # Iterate over events for July, 2017:
    cursor = db.cursor(key='2017-07-01T00:00:00', prefix='2017-07-')
    for timestamp, event_data in cursor:
        process_event(timestamp, event_data)

Transactions
------------

Sophia supports ACID transactions. Even better, a single transaction can cover
operations to multiple databases in a given environment.

Example of using :py:meth:`Sophia.transaction`:

.. code-block:: python

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

Multiple transactions are allowed to be open at the same time, but if there are
conflicting changes, an exception will be thrown when attempting to commit the
offending transaction:

.. code-block:: pycon

    # Create a basic k/v store. Schema.key_value() is a convenience method
    # for string key / string value.
    >>> kv = env.add_database('main', Schema.key_value())

    # Open the environment in order to access the new db.
    >>> env.open()

    # Instead of using the context manager, we'll call begin() explicitly so we
    # can show the interaction of 2 open transactions.
    >>> txn = env.transaction().begin()

    >>> t_kv = txn[kv]  # Obtain reference to kv database in transaction.
    >>> t_kv['k1'] = 'v1'  # Set k1=v1.

    >>> txn2 = env.transaction().begin()  # Start a 2nd transaction.
    >>> t2_kv = txn2[kv]  # Obtain a reference to the "kv" db in 2nd transaction.
    >>> t2_kv['k1'] = 'v1-x'  # Set k1=v1-x

    >>> txn2.commit()  # ERROR !!
    SophiaError
    ...
    SophiaError('transaction is not finished, waiting for concurrent transaction to finish.')

    >>> txn.commit()  # OK

    >>> txn2.commit()  # Retry committing 2nd transaction. ERROR !!
    SophiaError
    ...
    SophiaError('transasction rolled back by another concurrent transaction.')

Sophia detected a conflict and rolled-back the 2nd transaction.

Index types, multi-field keys and values
----------------------------------------

Sophia supports multi-field keys and values. Additionally, the individual
fields can have different data-types. Sophy provides the following field
types:

* :py:class:`StringIndex` - UTF8-encoded string data (text, in other words).
* :py:class:`BytesIndex` - bytestrings (binary data).
* :py:class:`JsonIndex` - store value as UTF8-encoded JSON.
* :py:class:`MsgPackIndex` - store arbitrary data using msgpack encoding.
* :py:class:`PickleIndex` - store arbitrary data using python pickle module.
* :py:class:`UUIDIndex` - store UUIDs.
* :py:class:`SerializedIndex` - index that accepts serialize/deserialize
  functions and can be used for custom serialization formats.
* :py:class:`U64Index` - store 64-bit unsigned integers.
* :py:class:`U32Index` - store 32-bit unsigned integers.
* :py:class:`U16Index` - store 16-bit unsigned integers.
* :py:class:`U8Index` - store 8-bit unsigned integers (or single bytes).
* There are also :py:class:`U64RevIndex`, :py:class:`U32RevIndex`,
  :py:class:`U16RevIndex` and :py:class:`U8RevIndex` for storing integers in
  reverse order.

To store arbitrary data encoded using msgpack, for example:

.. code-block:: python

    schema = Schema(StringIndex('key'), MsgPackIndex('value'))
    db = sophia_env.add_database('main', schema)

If you have a custom serialization library you would like to use, you can use
:py:class:`SerializedIndex`, passing the serialize/deserialize callables:

.. code-block:: python

    # Equivalent to previous msgpack example.
    import msgpack

    schema = Schema(StringIndex('key'),
                    SerializedIndex('value', msgpack.packb, msgpack.unpackb))
    db = sophia_env.add_database('main', schema)

To declare a database with a multi-field key or value, you will pass the
individual fields as arguments when constructing the :py:class:`Schema` object.
To initialize a schema where the key is composed of two strings and a 64-bit
unsigned integer, and the value is composed of a string, you would write:

.. code-block:: python

    # Declare a schema consisting of a multi-part key and a string value.
    key_parts = [StringIndex('last_name'),
                 StringIndex('first_name'),
                 U64Index('area_code')]
    value_parts = [StringIndex('address_data')]
    schema = Schema(key_parts, value_parts)

    # Create a database using the above schema.
    address_book = env.add_database('address_book', schema)
    env.open()

To store data, we use the same dictionary methods as usual, just passing tuples
instead of individual values:

.. code-block:: python

    address_book['kitty', 'huey', 66604] = '123 Meow St'
    address_book['puppy', 'mickey', 66604] = '1337 Woof-woof Court'

To retrieve our data:

.. code-block:: pycon

    >>> address_book['kitty', 'huey', 66604]
    '123 Meow St.'

To delete a row:

.. code-block:: pycon

    >>> del address_book['puppy', 'mickey', 66604]

Indexing and slicing works as you would expect, with tuples being returned
instead of scalar values where appropriate.

.. note::
    When working with a multi-part value, a tuple containing the value
    components will be returned. When working with a scalar value, instead of
    returning a 1-item tuple, the value itself is returned.

Configuring and Administering Sophia
------------------------------------

Sophia can be configured using special properties on the :py:class:`Sophia` and
:py:class:`Database` objects. Refer to the :ref:`settings configuration document <settings>`
for the details on the available options, including whether they are read-only,
and the expected data-type.

For example, to query Sophia's status, you can use the :py:attr:`Sophia.status`
property, which is a readonly setting returning a string:

.. code-block:: pycon

    >>> print(env.status)
    online

Other properties can be changed by assigning a new value to the property. For
example, to read and then increase the number of threads used by the scheduler:

.. code-block:: pycon

    >>> env.scheduler_threads
    6
    >>> env.scheduler_threads = 8

Database-specific properties are available as well. For example to get the
number of GET and SET operations performed on a database, you would write:

.. code-block:: pycon

    >>> print(db.stat_get, 'get operations')
    24 get operations
    >>> print(db.stat_set, 'set operations')
    33 set operations

Refer to the :ref:`settings configuration table <settings>` for a complete
list of available settings.

Backups
-------

Sophia can create a backup the database while it is running. To configure
backups, you will need to set the path for backups before opening the
environment:

.. code-block:: python

    env = Sophia('/path/to/data')
    env.backup_path = '/path/for/backup-data/'

    env.open()

At any time while the environment is open, you can call the ``backup_run()``
method, and a backup will be started in a background thread:

.. code-block:: python

    env.backup_run()

Backups will be placed in numbered folders inside the ``backup_path`` specified
during environment configuration. You can query the backup status and get the
ID of the last-completed backup:

.. code-block:: python

    env.backup_active  # Returns 1 if running, 0 if completed/idle
    env.backup_last  # Get ID of last-completed backup
    env.backup_last_complete  # Returns 1 if last backup succeeded
