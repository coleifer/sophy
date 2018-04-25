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
                value_parts=[MsgPackIndex('data')])
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

        kv_schema = Schema(StringIndex('key'), MsgPackIndex('value'))
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
                [MsgPackIndex('data')])
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
            [MsgPackIndex('value')])

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
        value = [MsgPackIndex('value')]
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

    **Note**: ``sophy`` already provides indexes for :py:class:`JsonIndex`,
    :py:class:`MsgPackIndex` and :py:class:`PickleIndex`.

.. py:class:: BytesIndex(name)

    Store arbitrary binary data in the database.

.. py:class:: StringIndex(name)

    Store text data in the database as UTF8-encoded bytestrings. When reading
    from a :py:class:`StringIndex`, data is decoded and returned as unicode.

.. py:class:: JsonIndex(name)

    Store data as UTF8-encoded JSON. Python objects will be transparently
    serialized and deserialized when writing and reading, respectively.

.. py:class:: MsgPackIndex(name)

    Store data using the msgpack serialization format. Python objects will
    be transparently serialized and deserialized when writing and reading.

    **Note**: Requires the ``msgpack-python`` library.

.. py:class:: PickleIndex(name)

    Store data using Python's pickle serialization format. Python objects will
    be transparently serialized and deserialized when writing and reading.

.. py:class:: UUIDIndex(name)

    Store UUIDs. Python ``uuid.UUID()`` objects will be stored as raw bytes and
    decoded to ``uuid.UUID()`` instances upon retrieval.

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

.. _settings:

Settings
--------

Sophia supports a wide range of settings and configuration options. These
settings are also documented in the `Sophia documentation <http://sophia.systems/v2.2/conf/log.html>`_.

Environment settings
^^^^^^^^^^^^^^^^^^^^

The following settings are available as properties on :py:class:`Sophia`:

=============================== ============= ================================================
Setting                         Type          Description
=============================== ============= ================================================
version                         string, ro    Get current Sophia version
version_storage                 string, ro    Get current Sophia storage version
build                           string, ro    Get git commit hash of build
status                          string, ro    Get environment status (eg online)
errors                          int, ro       Get number of errors
**error**                       string, ro    Get last error description
path                            string, ro    Get current Sophia environment directory
------------------------------- ------------- ------------------------------------------------
**Backups**
------------------------------- ------------- ------------------------------------------------
**backup_path**                 string        Set backup path
**backup_run**                  method        Start backup in background (non-blocking)
backup_active                   int, ro       Show if backup is running
backup_last                     int, ro       Show ID of last-completed backup
backup_last_complete            int, ro       Show if last backup succeeded
------------------------------- ------------- ------------------------------------------------
**Scheduler**
------------------------------- ------------- ------------------------------------------------
scheduler_threads               int           Get or set number of worker threads
scheduler_trace(thread_id)      method        Get a worker trace for given thread
------------------------------- ------------- ------------------------------------------------
**Transaction Manager**
------------------------------- ------------- ------------------------------------------------
transaction_online_rw           int, ro       Number of active read/write transactions
transaction_online_ro           int, ro       Number of active read-only transactions
transaction_commit              int, ro       Total number of completed transactions
transaction_rollback            int, ro       Total number of transaction rollbacks
transaction_conflict            int, ro       Total number of transaction conflicts
transaction_lock                int, ro       Total number of transaction locks
transaction_latency             string, ro    Average transaction latency from start to end
transaction_log                 string, ro    Average transaction log length
transaction_vlsn                int, ro       Current VLSN
transaction_gc                  int, ro       SSI GC queue size
------------------------------- ------------- ------------------------------------------------
**Metrics**
------------------------------- ------------- ------------------------------------------------
metric_lsn                      int, ro       Current log sequential number
metric_tsn                      int, ro       Current transaction sequential number
metric_nsn                      int, ro       Current node sequential number
metric_dsn                      int, ro       Current database sequential number
metric_bsn                      int, ro       Current backup sequential number
metric_lfsn                     int, ro       Current log file sequential number
------------------------------- ------------- ------------------------------------------------
**Write-ahead Log**
------------------------------- ------------- ------------------------------------------------
log_enable                      int           Enable or disable transaction log
log_path                        string        Get or set folder for log directory
log_sync                        int           Sync transaction log on every commit
log_rotate_wm                   int           Create a new log after "rotate_wm" updates
log_rotate_sync                 int           Sync log file on every rotation
log_rotate                      method        Force Sophia to rotate log file
log_gc                          method        Force Sophia to garbage-collect log file pool
log_files                       int, ro       Number of log files in the pool
=============================== ============= ================================================

Database settings
^^^^^^^^^^^^^^^^^

The following settings are available as properties on :py:class:`Database`. By
default, Sophia uses ``pread(2)`` to read from disk. When ``mmap``-mode is on
(by default), Sophia handles all requests by directly accessing memory-mapped
node files.

=============================== ============= ===================================================
Setting                         Type          Description
=============================== ============= ===================================================
database_name                   string, ro    Get database name
database_id                     int, ro       Database sequential ID
database_path                   string, ro    Directory for storing data
**mmap**                        int           Enable or disable mmap-mode
direct_io                       int           Enable or disable ``O_DIRECT`` mode.
**sync**                        int           Sync node file on compaction completion
expire                          int           Enable or disable key expiration
**compression**                 string        Specify compression type: lz4, zstd, none (default)
limit_key                       int, ro       Scheme key size limit
limit_field                     int           Scheme field size limit
------------------------------- ------------- ---------------------------------------------------
**Index**
------------------------------- ------------- ---------------------------------------------------
index_memory_used               int, ro       Memory used by database for in-memory key indexes
index_size                      int, ro       Sum of nodes size in bytes (e.g. database size)
index_size_uncompressed         int, ro       Full database size before compression
**index_count**                 int, ro       Total number of keys in db, includes unmerged dupes
index_count_dup                 int, ro       Total number of transactional duplicates
index_read_disk                 int, ro       Number of disk reads since start
index_read_cache                int, ro       Number of cache reads since start
index_node_count                int, ro       Number of active nodes
index_page_count                int, ro       Total number of pages
------------------------------- ------------- ---------------------------------------------------
**Compaction**
------------------------------- ------------- ---------------------------------------------------
**compaction_cache**            int           Total write cache size used for compaction
compaction_checkpoint           int
compaction_node_size            int           Set a node file size in bytes.
compaction_page_size            int           Set size of page
compaction_page_checksum        int           Validate checksum during compaction
compaction_expire_period        int           Run expire check process every ``N`` seconds
compaction_gc_wm                int           GC starts when watermark value reaches ``N`` dupes
compaction_gc_period            int           Check for a gc every ``N`` seconds
------------------------------- ------------- ---------------------------------------------------
**Performance**
------------------------------- ------------- ---------------------------------------------------
stat_documents_used             int, ro       Memory used by allocated document
stat_documents                  int, ro       Number of currently allocated documents
stat_field                      string, ro    Average field size
stat_set                        int, ro       Total number of Set operations
stat_set_latency                string, ro    Average Set latency
stat_delete                     int, ro       Total number of Delete operations
stat_delete_latency             string, ro    Average Delete latency
stat_get                        int, ro       Total number of Get operations
stat_get_latency                string, ro    Average Get latency
stat_get_read_disk              string, ro    Average disk reads by Get operation
stat_get_read_cache             string, ro    Average cache reads by Get operation
stat_pread                      int, ro       Total number of pread operations
stat_pread_latency              string, ro    Average pread latency
stat_cursor                     int, ro       Total number of cursor operations
stat_cursor_latency             string, ro    Average cursor latency
stat_cursor_read_disk           string, ro    Average disk reads by Cursor operation
stat_cursor_read_cache          string, ro    Average cache reads by Cursor operation
stat_cursor_ops                 string, io    Average number of keys read by Cursor operation
------------------------------- ------------- ---------------------------------------------------
**Scheduler**
------------------------------- ------------- ---------------------------------------------------
scheduler_gc                    int, ro       Show if GC operation is in progress
scheduler_expire                int, ro       Show if expire operation is in progress
scheduler_backup                int, ro       Show if backup operation is in progress
scheduler_checkpoint            int, ro
=============================== ============= ===================================================
