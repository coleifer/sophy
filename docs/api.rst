.. _api:

Sophy API
=========

.. py:class:: SophiaError

    General exception class used to indicate error returned by Sophia database.

Environment
-----------

.. py:class:: Sophia(path)

    :param str path: Directory path to store environment and databases.

    Environment object providing access to databases and for controlling
    transactions.

    Example of creating environment, attaching a database and reading/writing
    data:

    .. code-block:: python

        from sophy import *


        # Environment for managing one or more databases.
        env = Sophia('/tmp/sophia-test')

        # Schema describes the indexes that comprise the key and value portions
        # of a database.
        kv_schema = Schema([StringIndex('key')], [StringIndex('value')])
        db = env.add_data('kv', kv_schema)

        # We need to open the env after configuring the database(s), in order
        # to read/write data.
        assert env.open(), 'Failed to open environment!'

        # We can use dict-style APIs to read/write key/value pairs.
        db['k1'] = 'v1'
        assert db['k1'] == 'v1'

        # Close the env when finished.
        assert env.close(), 'Failed to close environment!'

    .. py:method:: open()

        :return: Boolean indicating success.

        Open the environment. The environment must be opened in order to read
        and write data to the configured databases.

    .. py:method:: close()

        :return: Boolean indicating success.

        Close the environment.

    .. py:method:: add_database(name, schema)

        :param str name: database name
        :param Schema schema: schema for keys and values.
        :return: a database instance
        :rtype: :py:class:`Database`

        Add or declare a database. Environment must be closed to add databases.
        The :py:class:`Schema` will declare the data-types and structure of the
        key- and value-portion of the database.

        .. code-block:: python

            env = Sophia('/path/to/db-env')

            # Declare an events database with a multi-part key (ts, type) and
            # a msgpack-serialized data field.
            events_schema = Schema(
                key_parts=[U64Index('timestamp'), StringIndex('type')],
                value_parts=[SerializedIndex('data', msgpack.packb, msgpack.unpackb)])
            db = env.add_database('events', events_schema)

            # Open the environment for read/write access to the database.
            env.open()

            # We can now write to the database.
            db[current_time(), 'init'] = {'msg': 'event logging initialized'}

    .. py:method:: remove_database(name)

        :param str name: database name

        Remove a database from the environment. Environment must be closed to
        remove databases. This method does really not have any practical value
        but is provided for consistency.

    .. py:method:: get_database(name)

        :return: the database corresponding to the provided name
        :rtype: :py:class:`Database`

        Obtain a reference to the given database, provided the database has
        been added to the environment by a previous call to
        :py:meth:`~Sophia.add_database`.

    .. py:method:: __getitem__(name)

        Short-hand for :py:meth:`~Sophia.get_database`.

    .. py:method:: transaction()

        :return: a transaction handle.
        :rtype: :py:class:`Transaction`

        Create a transaction handle which can be used to execute a transaction
        on the databases in the environment. The returned transaction can be
        used as a context-manager.

        Example:

        .. code-block:: python

            env = Sophia('/tmp/sophia-test')
            db = env.add_database('test', Schema.key_value())
            env.open()

            with env.transaction() as txn:
                t_db = txn[db]
                t_db['k1'] = 'v1'
                t_db.update(k2='v2', k3='v3')

            # Transaction has been committed.
            print(db['k1'], db['k3'])  # prints "v1", "v3"

        See :py:class:`Transaction` for more information.


Database
--------

.. py:class:: Database()

    Database interface. This object is not created directly, but references can
    be obtained via :py:meth:`Sophia.add_database` or :py:meth:`Sophia.get_database`.

    For example:

    .. code-block:: python

        env = Sophia('/path/to/data')

        kv_schema = Schema(
            [StringIndex('key')],
            [SerializedIndex('value', msgpack.packb, msgpack.unpackb)])
        kv_db = env.add_database('kv', kv_schema)

        # Another reference to "kv_db":
        kv_db = env.get_database('kv')

        # Same as above:
        kv_db = env['kv']

    .. py:method:: set(key, value)

        :param key: key corresponding to schema (e.g. scalar or tuple).
        :param value: value corresponding to schema (e.g. scalar or tuple).
        :return: No return value.

        Store the value at the given key. For single-index keys or values, a
        scalar value may be provided as the key or value. If a composite or
        multi-index key or value is used, then a ``tuple`` must be provided.

        Examples:

        .. code-block:: python

            simple = Schema(StringIndex('key'), StringIndex('value'))
            simple_db = env.add_database('simple', simple)

            composite = Schema(
                [U64Index('timestamp'), StringIndex('type')],
                [SerializedIndex('data', msgpack.packb, msgpack.unpackb)])
            composite_db = env.add_database('composite', composite)

            env.open()  # Open env to access databases.

            # Set k1=v1 in the simple key/value database.
            simple_db.set('k1', 'v1')

            # Set new value in composite db. Note the key is a tuple and, since
            # the value is serialized using msgpack, we can transparently store
            # data-types like dicts.
            composite_db.set((current_time, 'evt_type'), {'msg': 'foo'})

    .. py:method:: get(key[, default=None])

        :param key: key corresponding to schema (e.g. scalar or tuple).
        :param default: default value if key does not exist.
        :return: value of given key or default value.

        Get the value at the given key. If the key does not exist, the default
        value is returned.

        If a multi-part key is defined for the given database, the key must be
        a tuple.

        Example:

        .. code-block:: python

            simple_db.set('k1', 'v1')
            simple_db.get('k1')  # Returns "v1".

            simple_db.get('not-here')  # Returns None.

    .. py:method:: delete(key)

        :param key: key corresponding to schema (e.g. scalar or tuple).
        :return: No return value

        Delete the given key, if it exists. If a multi-part key is defined for
        the given database, the key must be a tuple.

        Example:

        .. code-block:: python

            simple_db.set('k1', 'v1')
            simple_db.delete('k1')  # Deletes "k1" from database.

            simple_db.exists('k1')  # False.

    .. py:method:: exists(key)

        :param key: key corresponding to schema (e.g. scalar or tuple).
        :return: Boolean indicating if key exists.
        :rtype: bool

        Return whether the given key exists. If a multi-part key is defined for
        the given database, the key must be a tuple.

    .. py:method:: multi_set([__data=None[, **kwargs]])

        :param dict __data: Dictionary of key/value pairs to set.
        :param kwargs: Specify key/value pairs as keyword-arguments.
        :return: No return value

        Set multiple key/value pairs efficiently.

    .. py:method:: multi_get(*keys)

        :param keys: key(s) to retrieve
        :return: a list of values associated with the given keys. If a key does
            not exist a ``None`` will be indicated for the value.
        :rtype: list

        Get multiple values efficiently. Returned as a list of values
        corresponding to the ``keys`` argument, with missing values as
        ``None``.

        Example:

        .. code-block:: python

            db.update(k1='v1', k2='v2', k3='v3')
            db.multi_get('k1', 'k3', 'k-nothere')
            # ['v1', 'v3', None]

    .. py:method:: multi_get_dict(keys)

        :param list keys: list of keys to get
        :return: a list of values associated with the given keys. If a key does
            not exist a ``None`` will be indicated for the value.
        :rtype: list

        Get multiple values efficiently. Returned as a dict of key/value pairs.
        Missing values are not represented in the returned dict.

        Example:

        .. code-block:: python

            db.update(k1='v1', k2='v2', k3='v3')
            db.multi_get_dict(['k1', 'k3', 'k-nothere'])
            # {'k1': 'v1', 'k3': 'v3'}

    .. py:method:: multi_delete(*keys)

        :param keys: key(s) to delete
        :return: No return value

        Efficiently delete multiple keys.

    .. py:method:: get_range(start=None, stop=None, reverse=False)

        :param start: start key (omit to start at first record).
        :param stop: stop key (omit to stop at the last record).
        :param bool reverse: return range in reverse.
        :return: a generator that yields the requested key/value pairs.

        Fetch a range of key/value pairs from the given start-key, up-to and
        including the stop-key (if given).

    .. py:method:: keys()

        Return a cursor for iterating over the keys in the database.

    .. py:method:: values()

        Return a cursor for iterating over the values in the database.

    .. py:method:: items()

        Return a cursor for iterating over the key/value pairs in the database.

    .. py:method:: __getitem__(key_or_slice)

        :param key_or_slice: key or range of keys to retrieve.
        :return: value of given key, or an iterator over the range of keys.
        :raises: KeyError if single key requested and does not exist.

        Retrieve a single value or a range of values, depending on whether the
        key represents a single row or a slice of rows.

        Additionally, if a slice is given, the start and stop values can be
        omitted to indicate you wish to start from the first or last key,
        respectively.

    .. py:method:: __setitem__(key, value)

        Equivalent to :py:meth:`~Database.set`.

    .. py:method:: __delitem__(key)

        Equivalent to :py:meth:`~Database.delete`.

    .. py:method:: __contains__(key)

        Equivalent to :py:meth:`~Database.exists`.

    .. py:method:: __iter__()

        Equivalent to :py:meth:`~Database.items`.

    .. py:method:: __len__()

        Equivalent to iterating over all keys and returning count. This is the
        most accurate way to get the total number of keys, but is not very
        efficient. An alternative is to use the :py:attr:`Database.index_count`
        property, which returns an approximation of the number of keys in the
        database.

    .. py:method:: cursor(order='>=', key=None, prefix=None, keys=True, values=True)

        :param str order: ordering semantics (default is ">=")
        :param key: key to seek to before iterating.
        :param prefix: string prefix to match.
        :param bool keys: return keys when iterating.
        :param bool values: return values when iterating.

        Create a cursor with the given semantics. Typically you will want both
        ``keys=True`` and ``values=True`` (the defaults), which will cause the
        cursor to yield a 2-tuple consisting of ``(key, value)`` during
        iteration.


Transaction
-----------

.. py:class:: Transaction()

    Transaction handle, used for executing one or more operations atomically.
    This class is not created directly - use :py:meth:`Sophia.transaction`.

    The transaction can be used as a context-manager. To read or write during a
    transaction, you should obtain a transaction-specific handle to the
    database you are operating on.

    Example:

    .. code-block:: python

        env = Sophia('/tmp/my-env')
        db = env.add_database('kv', Schema.key_value())
        env.open()

        with env.transaction() as txn:
            tdb = txn[db]  # Obtain reference to "db" in the transaction.
            tdb['k1'] = 'v1'
            tdb.update(k2='v2', k3='v3')

        # At the end of the wrapped block, the transaction is committed.
        # The writes have been recorded:
        print(db['k1'], db['k3'])
        # ('v1', 'v3')

    .. py:method:: begin()

        Begin a transaction.

    .. py:method:: commit()

        :raises: SophiaError

        Commit all changes. An exception can occur if:

        1. The transaction was rolled back, either explicitly or implicitly due
           to conflicting changes having been committed by a different
           transaction. **Not recoverable**.
        2. A concurrent transaction is open and must be committed before this
           transaction can commit.  **Possibly recoverable**.

    .. py:method:: rollback()

        Roll-back any changes made in the transaction.

    .. py:method:: __getitem__(db)

        :param Database db: database to reference during transaction
        :return: special database-handle for use in transaction
        :rtype: :py:class:`DatabaseTransaction`

        Obtain a reference to the database for use within the transaction. This
        object supports the same APIs as :py:class:`Database`, but any reads or
        writes will be made within the context of the transaction.


Schema Definition
-----------------

.. py:class:: Schema(key_parts, value_parts)

    :param list key_parts: a list of ``Index`` objects (or a single index
        object) to use as the key of the database.
    :param list value_parts: a list of ``Index`` objects (or a single index
        object) to use for the values stored in the database.

    The schema defines the structure of the keys and values for a given
    :py:class:`Database`. They can be comprised of a single index-type or
    multiple indexes for composite keys or values.

    Example:

    .. code-block:: python

        # Simple schema defining text keys and values.
        simple = Schema(StringIndex('key'), StringIndex('value'))

        # Schema with composite key for storing timestamps and event-types,
        # along with msgpack-serialized data as the value.
        event_schema = Schema(
            [U64Index('timestamp'), StringIndex('type')],
            [SerializedIndex('value', msgpack.packb, msgpack.unpackb)])

    Schemas are used when adding databases using the
    :py:meth:`Sophia.add_database` method.

    .. py:method:: add_key(index)

        :param BaseIndex index: an index object to add to the key parts.

        Add an index to the key. Allows :py:class:`Schema` to be built-up
        programmatically.

    .. py:method:: add_value(index)

        :param BaseIndex index: an index object to add to the value parts.

        Add an index to the value. Allows :py:class:`Schema` to be built-up
        programmatically.

    .. py:classmethod:: key_value()

        Short-hand for creating a simple text schema consisting of a single
        :py:class:`StringIndex` for both the key and the value.


.. py:class:: BaseIndex(name)

    :param str name: Name for the key- or value-part the index represents.

    Indexes are used to define the key and value portions of a
    :py:class:`Schema`. Traditional key/value databases typically only
    supported a single-value, single-datatype key and value (usually bytes).
    Sophia is different in that keys or values can be comprised of multiple
    parts with differing data-types.

    For example, to emulate a typical key/value store:

    .. code-block:: python

        schema = Schema([BytesIndex('key')], [BytesIndex('value')])
        db = env.add_database('old_school', schema)

    Suppose we are storing time-series event logs. We could use a 64-bit
    integer for the timestamp (in micro-seconds) as well as a key to denote
    the event-type. The value could be arbitrary msgpack-encoded data:

    .. code-block:: python

        key = [U64Index('timestamp'), StringIndex('type')]
        value = [SerializedIndex('value', msgpack.packb, msgpack.unpackb)]
        events = env.add_database('events', Schema(key, value))

.. py:class:: SerializedIndex(name, serialize, deserialize)

    :param str name: Name for the key- or value-part the index represents.
    :param serialize: a callable that accepts data and returns bytes.
    :param deserialize: a callable that accepts bytes and deserializes the data.

    The :py:class:`SerializedIndex` can be used to transparently store data as
    bytestrings. For example, you could use a library like ``msgpack`` or
    ``pickle`` to transparently store and retrieve Python objects in the
    database:

    .. code-block:: python

        key = StringIndex('key')
        value = SerializedIndex('value', pickle.dumps, pickle.loads)
        pickled_db = env.add_database('data', Schema([key], [value]))

.. py:class:: BytesIndex(name)

    Store arbitrary binary data in the database.

.. py:class:: StringIndex(name)

    Store text data in the database as UTF8-encoded bytestrings. When reading
    from a :py:class:`StringIndex`, data is decoded and returned as unicode.

.. py:class:: U64Index(name)
.. py:class:: U32Index(name)
.. py:class:: U16Index(name)
.. py:class:: U8Index(name)

    Store unsigned integers of the given sizes.

.. py:class:: U64RevIndex(name)
.. py:class:: U32RevIndex(name)
.. py:class:: U16RevIndex(name)
.. py:class:: U8RevIndex(name)

    Store unsigned integers of the given sizes in reverse order.


Cursor
------

.. py:class:: Cursor()

    Cursor handle for a :py:class:`Database`. This object is not created
    directly but through the :py:meth:`Database.cursor` method or one of the
    database methods that returns a row iterator (e.g.
    :py:meth:`Database.items`).

    Cursors are iterable and, depending how they were configured, can return
    keys, values or key/value pairs.
