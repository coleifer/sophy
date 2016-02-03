from cpython.bytes cimport PyBytes_AsStringAndSize
from libc.stdlib cimport malloc
from libc.stdint cimport int64_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
import os
import sys

from functools import wraps


cdef extern from "src/sophia.h":
    cdef void *sp_env()
    cdef void *sp_document(void *)

    cdef int sp_open(void *)
    cdef int sp_drop(void *)
    cdef int sp_destroy(void *)
    cdef int sp_error(void *)

    cdef void *sp_asynchronous(void *)
    cdef void *sp_poll(void*)

    cdef int sp_setstring(void*, const char*, const void*, int)
    cdef int sp_setint(void*, const char*, int64_t)

    cdef void *sp_getobject(void*, const char*)
    cdef void *sp_getstring(void*, const char*, int*)
    cdef int64_t sp_getint(void*, const char*)

    cdef int sp_set(void*, void*)
    cdef int sp_upsert(void*, void*)
    cdef int sp_delete(void*, void*)
    cdef void *sp_get(void*, void*)

    cdef void *sp_cursor(void*)
    cdef void *sp_begin(void *)
    cdef int sp_prepare(void *)
    cdef int sp_commit(void *)

cdef bint IS_PY3K = sys.version_info[0] == 3

cdef bytes encode(obj):
    if isinstance(obj, unicode):
        return obj.encode('utf-8')
    elif isinstance(obj, bytes):
        return obj
    elif obj is None:
        return obj
    elif IS_PY3K:
        return bytes(str(obj), 'utf-8')
    return bytes(obj)

cdef inline _getstring(void *obj, const char *key):
    cdef:
        char *buf
        int nlen

    buf = <char *>sp_getstring(obj, key, &nlen)
    if buf:
        value = buf[:nlen - 1]
        if IS_PY3K:
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                pass
        return value

cdef inline _check(void *env, int rc):
    if rc == -1:
        error = _getstring(env, 'sophia.error')
        if error:
            raise Exception(error)
        else:
            raise Exception('unknown error occurred.')


def _sp(propname, string=False, readonly=False):
    def _getter(self):
        return self.config.get_option(propname, string)
    if readonly:
        return property(_getter)
    else:
        def _setter(self, value):
            self.config.set_option(propname, value)
        return property(_getter, _setter)

def _sm(propname):
    def _method(self):
        self.config.set_option(propname, 0)
    return _method

def _dbp(prop, string=False, readonly=False):
    def _getter(self):
        return self.config.get_option('db.%s.%s' % (self.name, prop), string)
    if readonly:
        return property(_getter)
    else:
        def _setter(self, value):
            self.config.set_option('db.%s.%s' % (self.name, prop), value)
        return property(_getter, _setter)

def _dbm(propname):
    def _method(self):
        self.config.set_option('db.%s.%s' % (self.name, propname), 0)
    return _method


cdef class _ConfigManager(object):
    cdef:
        dict _config
        Sophia sophia
        void *handle

    def __cinit__(self, Sophia sophia):
        self._config = {}
        self.sophia = sophia

    cpdef set_option(self, key, value):
        self._config[key] = value
        if self.sophia.is_open:
            self.apply_config(key, value)

    cpdef get_option(self, key, string=True):
        if string:
            return _getstring(self.sophia.handle, key)
        else:
            bkey = encode(key)
            return sp_getint(self.sophia.handle, <const char *>bkey)

    cpdef clear_option(self, key):
        del self._config[key]

    cdef apply_config(self, key, value):
        cdef:
            bytes bkey = encode(key)
            int rc

        if isinstance(value, bool):
            value = value and 1 or 0
        if isinstance(value, int):
            rc = sp_setint(self.sophia.handle, <const char *>bkey, <int>value)
        elif isinstance(value, basestring):
            bvalue = encode(value)
            rc = sp_setstring(
                self.sophia.handle,
                <const char *>bkey,
                <const char *>bvalue,
                0)
        _check(self.sophia.handle, rc)

    cdef apply_all(self):
        for key, value in self._config.items():
            self.apply_config(key, value)


cdef class _SophiaObject(object):
    cdef:
        void *handle

    def __cinit__(self, *_, **__):
        self.handle = <void *>0

    cdef destroy(self):
        sp_destroy(self.handle)
        self.handle = <void *>0


cdef class Sophia(_SophiaObject):
    cdef:
        bint is_open
        bytes b_path
        readonly dict dbs
        list db_defs
        readonly bint auto_open
        readonly _ConfigManager config
        readonly object path

    def __cinit__(self):
        self.handle = sp_env()

    def __init__(self, path, databases=None, auto_open=True,
                 backup_path=None, memory_limit=None, threads=None,
                 log_path=None):
        self.path = path
        self.auto_open = auto_open

        self.config = _ConfigManager(self)
        self.b_path = encode(path)
        self.db_defs = databases or []
        self.dbs = {}

        if backup_path is not None:
            self.backup_path = backup_path
        if memory_limit is not None:
            self.memory_limit = memory_limit
        if threads is not None:
            self.scheduler_threads = threads
        if log_path is not None:
            self.log_path = log_path

        if self.auto_open:
            self.open()

    def __dealloc__(self):
        if self.handle:
            sp_destroy(self.handle)

    cpdef bint open(self):
        if self.is_open:
            return False

        cdef:
            Database db_obj
            tuple db_def

        # Configure the environment, databases and indexes.
        sp_setstring(self.handle, 'sophia.path', <const char *>self.b_path, 0)
        for db_def in self.db_defs:
            self._add_db(Database(self, *db_def))
        self.config.apply_all()

        # Open the environment.
        _check(self.handle, sp_open(self.handle))

        # Open all databases.
        for db_obj in self.dbs.values():
            db_obj.open(configure=False)

        self.is_open = True
        return True

    cdef _add_db(self, Database db_obj, open_database=False):
        self.dbs[db_obj.name] = db_obj
        db_obj.configure()
        if open_database:
            db_obj.open(configure=False)

    cpdef bint close(self):
        if not self.is_open:
            return False

        self.dbs = {}
        sp_destroy(self.handle)
        self.handle = sp_env()
        self.is_open = False
        return True

    def __getitem__(self, name):
        return self.dbs[name]

    def create_database(self, name, index_type='string'):
        self._add_db(Database(self, name, index_type), self.is_open)
        self.db_defs.append((name, index_type))
        return self.dbs[name]

    # Properties: key, is_string?, is_readonly?

    # Sophia Environment.
    version = _sp('sophia.version', True, True)
    version_storage = _sp('sophia.version_storage', True, True)
    build = _sp('sophia.build', True, True)
    error = _sp('sophia.error', True, True)
    sophia_path = _sp('sophia.path', True)
    sophia_path_create = _sp('sophia.path_create')
    recover = _sp('sophia.recover')

    # Memory Control.
    memory_limit = _sp('memory.limit')
    memory_used = _sp('memory.used', readonly=True)
    memory_anticache = _sp('memory.anticache')
    memory_pager_pool_size = _sp('memory.pager_pool_size', readonly=True)
    memory_pager_page_size = _sp('memory.pager_page_size', readonly=True)
    memory_pager_pools = _sp('memory.pager_pools', readonly=True)

    # Scheduler.
    scheduler_threads = _sp('scheduler.threads')
    scheduler_zone = _sp('scheduler.zone', readonly=True)
    scheduler_checkpoint_active = _sp('scheduler.checkpoint_active', readonly=True)
    scheduler_checkpoint_lsn = _sp('scheduler.checkpoint_lsn', readonly=True)
    scheduler_checkpoint_lsn_last = _sp('scheduler.checkpoint_lsn_last', readonly=True)
    scheduler_checkpoint = _sm('scheduler.checkpoint')
    scheduler_anticache_active = _sp('scheduler.anticache_active', readonly=True)
    scheduler_anticache_asn = _sp('scheduler.anticache_asn', readonly=True)
    scheduler_anticache_asn_last = _sp('scheduler.anticache_asn_last', readonly=True)
    scheduler_anticache = _sp('scheduler.anticache')
    scheduler_snapshot_active = _sp('scheduler.snapshot_active', readonly=True)
    scheduler_snapshot_ssn = _sp('scheduler.snapshot_ssn', readonly=True)
    scheduler_snapshot_ssn_last = _sp('scheduler.snapshot_ssn_last', readonly=True)
    scheduler_snapshot = _sm('scheduler.snapshot')
    scheduler_gc_active = _sm('scheduler.gc_active')
    scheduler_gc = _sm('scheduler.gc')
    scheduler_lru_active = _sm('scheduler.lru_active')
    scheduler_lru = _sm('scheduler.lru')
    scheduler_run = _sm('scheduler.run')

    # Compaction.
    compaction_redzone = _sp('compaction.redzone')
    compaction_redzone_mode = _sp('compaction.redzone.mode')
    compaction_redzone_compact_wm = _sp('compaction.redzone.compact_wm')
    compaction_redzone_compact_mode = _sp('compaction.redzone.compact_mode')
    compaction_redzone_branch_prio = _sp('compaction.redzone.branch_prio')
    compaction_redzone_branch_wm = _sp('compaction.redzone.branch_wm')
    compaction_redzone_branch_age = _sp('compaction.redzone.branch_age')
    compaction_redzone_branch_age_period = _sp('compaction.redzone.branch_age_period')
    compaction_redzone_branch_age_wm = _sp('compaction.redzone.branch_age_wm')
    compaction_redzone_anticache_period = _sp('compaction.redzone.anticache_period')
    compaction_redzone_backup_prio = _sp('compaction.redzone.backup_prio')
    compaction_redzone_gc_wm = _sp('compaction.redzone.gc_wm')
    compaction_redzone_gc_db_prio = _sp('compaction.redzone.gc_db_prio')
    compaction_redzone_gc_period = _sp('compaction.redzone.gc_period')
    compaction_redzone_lru_prio = _sp('compaction.redzone.lru_prio')
    compaction_redzone_lru_period = _sp('compaction.redzone.lru_period')
    compaction_redzone_async = _sp('compaction.redzone.async')

    # Performance.
    performance_documents = _sp('performance.documents', readonly=True)
    performance_documents_used = _sp('performance.documents_used', readonly=True)
    performance_key = _sp('performance.key', True, readonly=True)
    performance_value = _sp('performance.value', True, readonly=True)
    performance_set = _sp('performance.set', readonly=True)
    performance_set_latency = _sp('performance.set_latency', True, readonly=True)
    performance_delete = _sp('performance.delete', readonly=True)
    performance_delete_latency = _sp('performance.delete_latency', True, readonly=True)
    performance_get = _sp('performance.get', readonly=True)
    performance_get_latency = _sp('performance.get_latency', True, readonly=True)
    performance_get_read_disk = _sp('performance.get_read_disk', True, readonly=True)
    performance_get_read_cache = _sp('performance.get_read_cache', True, readonly=True)
    performance_tx_active_rw = _sp('performance.tx_active_rw', readonly=True)
    performance_tx_active_ro = _sp('performance.tx_active_ro', readonly=True)
    performance_tx = _sp('performance.tx', readonly=True)
    performance_tx_rollback = _sp('performance.tx_rollback', readonly=True)
    performance_tx_conflict = _sp('performance.tx_conflict', readonly=True)
    performance_tx_lock = _sp('performance.tx_lock', readonly=True)
    performance_tx_latency = _sp('performance.tx_latency', True, readonly=True)
    performance_tx_ops = _sp('performance.tx_ops', True, readonly=True)
    performance_tx_gc_queue = _sp('performance.tx_gc_queue', readonly=True)
    performance_cursor = _sp('performance.cursor', readonly=True)
    performance_cursor_latency = _sp('performance.cursor_latency', readonly=True)
    performance_cursor_read_disk = _sp('performance.cursor_read_disk', readonly=True)
    performance_cursor_read_cache = _sp('performance.cursor_read_cache', readonly=True)
    performance_cursor_ops = _sp('performance.cursor_ops', readonly=True)

    # Metric.
    metric_lsn = _sp('metric.lsn')
    metric_tsn = _sp('metric.tsn')
    metric_nsn = _sp('metric.nsn')
    metric_ssn = _sp('metric.ssn')
    metric_asn = _sp('metric.asn')
    metric_dsn = _sp('metric.dsn')
    metric_bsn = _sp('metric.bsn')
    metric_lfsn = _sp('metric.lfsn')

    # WAL.
    log_enable = _sp('log.enable')
    log_path = _sp('log.path', True)
    log_sync = _sp('log.sync')
    log_rotate_wm = _sp('log.rotate_wm')
    log_rotate_sync = _sp('log.rotate_sync')
    log_rotate = _sm('log.rotate')
    log_gc = _sm('log.gc')
    log_files = _sp('log.files', readonly=True)

    # Backup.
    backup_path = _sp('backup.path', True)
    backup_run = _sm('backup.run')
    backup_active = _sp('backup.active')
    backup_last = _sp('backup.last')
    backup_last_complete = _sp('backup.last_complete')


cdef class _BaseDBObject(_SophiaObject):
    cdef:
        readonly Sophia sophia
        _Index index

    def __cinit__(self, Sophia sophia, *_):
        self.sophia = sophia

    cdef configure(self):
        self.index = self._get_index()

    cpdef bint open(self, bint configure=True):
        if self.handle:
            return False

        if configure:
            self.configure()

        self.handle = self._create_handle()
        if not self.handle:
            raise MemoryError('Unable to allocate object: %s.' % self)
        return True

    cpdef bint close(self):
        if not self.handle:
            return False

        self.destroy()
        return True

    cdef void *_create_handle(self):
        raise NotImplementedError

    cdef _Index _get_index(self):
        raise NotImplementedError

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return wrapper

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.get_range(key.start, key.stop, key.step)
        return self.index.get(self, key)

    def __setitem__(self, key, value):
        self.index.set(self, key, value)

    def __delitem__(self, key):
        self.index.delete(self, key)

    def __contains__(self, key):
        return self.index.exists(self, key)

    cdef _update(self, Database db, dict _data=None, dict k=None):
        with db.transaction() as txn:
            if _data:
                for key in _data:
                    txn[key] = _data[key]
            if k:
                for key in k:
                    txn[key] = k[key]

    def get_range(self, start=None, stop=None, reverse=False):
        cdef:
            Cursor cursor

        first = start is None
        last = stop is None

        if reverse:
            if (first and not last) or (last and not first):
                start, stop = stop, start
            if (not first and not last) and (start < stop):
                start, stop = stop, start
        elif (not first and not last) and (start > stop):
            reverse = True

        order = '<=' if reverse else '>='
        cursor = self.cursor(order=order, key=start)

        for key, value in cursor:
            if stop:
                if reverse and key < stop:
                    raise StopIteration
                elif not reverse and key > stop:
                    raise StopIteration

            yield (key, value)

    def keys(self):
        cdef Cursor cursor = self.cursor(values=False)
        for item in cursor:
            yield item

    def values(self):
        cdef Cursor cursor = self.cursor(keys=False)
        for item in cursor:
            yield item

    def items(self):
        cdef Cursor cursor = self.cursor()
        for item in cursor:
            yield item

    def __iter__(self):
        return iter(self.cursor())

    def __len__(self):
        cdef:
            int i = 0
            curs = self.cursor(keys=False, values=False)

        for _ in curs:
            i += 1

        return i

    cpdef cursor(self, order='>=', key=None, prefix=None, keys=True,
                 values=True):
        raise NotImplemented


class CannotCloseException(Exception): pass


cdef class Database(_BaseDBObject):
    cdef:
        readonly bytes name
        readonly _ConfigManager config
        tuple index_type

    def __cinit__(self, Sophia sophia, name, index_type=None):
        self.name = encode(name)

        if not index_type:
            self.index_type = ('string',)
        elif isinstance(index_type, basestring):
            self.index_type = (index_type,)
        else:
            self.index_type = tuple(index_type)

        self.config = self.sophia.config

    def __dealloc__(self):
        if self.sophia.handle and self.handle:
            sp_destroy(self.handle)

    cpdef bint close(self):
        if self.sophia.is_open:
            raise CannotCloseException()
        return False

    cdef destroy(self):
        if self.sophia.handle and self.handle:
            sp_destroy(self.handle)
            self.handle = <void *>0

    cdef configure(self):
        sp_setstring(self.sophia.handle, 'db', <const char *>self.name, 0)
        self.index = self._get_index()
        self.index.configure()
        self.mmap = 1

    cdef void *_create_handle(self):
        cdef:
            void *handle
        handle = sp_getobject(self.sophia.handle, encode('db.%s' % self.name))
        if self.sophia.is_open:
            sp_open(handle)
        return handle

    cdef _Index _get_index(self):
        cdef:
            _Index index

        if len(self.index_type) == 1:
            try:
                IndexType = INDEX_TYPE_MAP[self.index_type[0]]
            except KeyError:
                raise ValueError('Unrecognized index type, must be one of: %s'
                                 % ', '.join(sorted(INDEX_TYPE_MAP)))
            else:
                index = IndexType(self.sophia, self)
        else:
            index = _MultiIndex(self.sophia, self, index_types=self.index_type)

        return index

    def update(self, dict _data=None, **k):
        self._update(self, _data, k)

    cpdef transaction(self):
        return Transaction(self.sophia, self)

    cpdef view(self, name):
        return View(self.sophia, self, name)

    cpdef cursor(self, order='>=', key=None, prefix=None, keys=True,
                 values=True):
        return Cursor(
            sophia=self.sophia,
            db=self,
            target=self.sophia,
            order=order,
            key=key,
            prefix=prefix,
            keys=keys,
            values=values)

    # key, is string?, is readonly?
    database_id = _dbp('id')
    status = _dbp('status', True, True)
    storage = _dbp('storage', True)
    format = _dbp('format', True)
    amqf = _dbp('amqf')
    database_path = _dbp('path', True)
    path_fail_on_exists = _dbp('path_fail_on_exists')
    path_fail_on_drop = _dbp('path_fail_on_drop')
    cache_mode = _dbp('cache_mode')
    cache = _dbp('cache', True)
    mmap = _dbp('mmap')
    sync = _dbp('sync')
    node_preload = _dbp('node_preload')
    node_size = _dbp('node_size')
    page_size = _dbp('page_size')
    page_checksum = _dbp('page_checksum')
    compression_key = _dbp('compression_key')
    compression = _dbp('compression', True)
    compression_branch = _dbp('compression_branch', True)
    lru = _dbp('lru')
    lru_step = _dbp('lru_step')
    branch = _dbm('branch')
    compact = _dbm('compact')
    compact_index = _dbm('compact_index')
    index_memory_used = _dbp('index.memory_used', readonly=True)
    index_size = _dbp('index.size', readonly=True)
    index_size_uncompressed = _dbp('index.size_uncompressed', readonly=True)
    index_size_snapshot = _dbp('index.size_snapshot', readonly=True)
    index_size_amqf = _dbp('index.size_amqf', readonly=True)
    index_count = _dbp('index.count', readonly=True)
    index_count_dup = _dbp('index.count_dup', readonly=True)
    index_read_disk = _dbp('index.read_disk', readonly=True)
    index_read_cache = _dbp('index.read_cache', readonly=True)
    index_node_count = _dbp('index.node_count', readonly=True)
    index_branch_count = _dbp('index.branch_count', readonly=True)
    index_branch_max = _dbp('index.branch_max', readonly=True)
    index_page_count = _dbp('index.page_count', readonly=True)


cdef class View(_BaseDBObject):
    cdef:
        Database db
        bytes name

    def __cinit__(self, Sophia sophia, Database db, name):
        self.db = db
        self.name = encode(name)

    def __init__(self, Sophia sophia, Database db, name):
        self.open()

    def __dealloc__(self):
        if self.sophia.handle and self.db.handle and self.handle:
            sp_destroy(self.handle)

    cdef destroy(self):
        if self.sophia.handle and self.db.handle and self.handle:
            sp_destroy(self.handle)
            self.handle = <void *>0

    cdef void *_create_handle(self):
        sp_setstring(self.sophia.handle, 'view', <char *>self.name, 0)
        return sp_getobject(self.sophia.handle, encode('view.%s' % self.name))

    cdef _Index _get_index(self):
        return self.db.index

    def __setitem__(self, key, value):
        raise ValueError('Views are read-only.')

    def __delitem__(self, key):
        raise ValueError('Views are read-only.')

    cpdef cursor(self, order='>=', key=None, prefix=None, keys=True,
                 values=True):
        return Cursor(
            sophia=self.sophia,
            db=self.db,
            target=self,
            order=order,
            key=key,
            prefix=prefix,
            keys=keys,
            values=values)


cdef class Transaction(_BaseDBObject):
    cdef:
        Database db

    def __cinit__(self, Sophia sophia, Database db):
        self.db = db

    def __dealloc__(self):
        if self.sophia.handle and self.db.handle and self.handle:
            sp_destroy(self.handle)

    cdef void *_create_handle(self):
        return sp_begin(self.sophia.handle)

    cdef _Index _get_index(self):
        return self.db.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback(False)
        else:
            try:
                self.commit(False)
            except:
                self.rollback(False)
                raise

    cpdef commit(self, begin=True):
        cdef int rc = sp_commit(self.handle)
        _check(self.sophia.handle, rc)
        if rc == 1:
            raise Exception('transaction was rolled back by another '
                            'concurrent transaction.')
        elif rc == 2:
            raise Exception('transaction is not finished, waiting for a '
                            'concurrent transaction to finish.')
        self.handle = <void *>0
        if begin:
            self.open()

    cpdef rollback(self, begin=True):
        self.destroy()
        if begin:
            self.open()


cdef class Cursor(_SophiaObject):
    cdef:
        Sophia sophia
        Database db
        _SophiaObject target
        readonly bytes order
        readonly bytes prefix
        readonly bint keys
        readonly bint values
        readonly object key
        tuple indexes
        void *current_item

    def __cinit__(self, Sophia sophia, Database db, _SophiaObject target,
                  order='>=', key=None, prefix=None, keys=True, values=True):
        self.sophia = sophia
        self.db = db
        self.target = target
        self.order = encode(order)
        self.keys = keys
        self.values = values
        if key:
            self.key = key
        if prefix:
            self.prefix = encode(prefix)

        self.indexes = self.db.index.keys
        self.current_item = <void *>0

    def __dealloc__(self):
        if self.current_item:
            sp_destroy(self.current_item)

        if self.sophia.handle and self.db.handle and self.handle:
            sp_destroy(self.handle)

    def __iter__(self):
        cdef:
            char *kbuf
            Py_ssize_t klen

        if self.handle:
            self.destroy()
        self.handle = sp_cursor(self.target.handle)
        self.current_item = sp_document(self.db.handle)
        if self.key:
            self.db.index.set_key(self.current_item, self.key)
        sp_setstring(self.current_item, 'order', <char *>self.order, 0)
        if self.prefix:
            sp_setstring(
                self.current_item,
                'prefix',
                <char *>self.prefix,
                (sizeof(char) * len(self.prefix)))
        return self

    def __next__(self):
        self.current_item = sp_get(self.handle, self.current_item)
        if not self.current_item:
            #self.destroy()
            raise StopIteration

        cdef bkey
        if self.keys and self.values:
            bkey = self.db.index.extract_key(self.current_item)
            return (bkey,
                    _getstring(self.current_item, 'value'))
        elif self.keys:
            bkey = self.db.index.extract_key(self.current_item)
            return bkey
        elif self.values:
            return _getstring(self.current_item, 'value')


cdef class _Index(object):
    cdef:
        Sophia sophia
        Database db
        bytes b_path, b_type
        bytes key
        object empty_value
        tuple keys

    index_type = 'string'

    def __cinit__(self, Sophia sophia, Database db, key='key', **_):
        self.sophia = sophia
        self.db = db
        self.key = encode(key)
        self.keys = (self.key,)
        self.b_path = encode('db.%s.index.%s' % (db.name, key))
        self.b_type = encode(self.index_type)

    def __init__(self, Sophia sophia, Database db, key='key', **_):
        self.empty_value = self._get_empty_value()

    cdef _get_empty_value(self):
        return ''

    cdef configure(self):
        sp_setstring(
            self.sophia.handle,
            <char *>self.b_path,
            <char *>self.b_type,
            0)

    cdef set(self, _BaseDBObject target, key, value):
        cdef:
            char *kbuf
            char *vbuf
            Py_ssize_t klen, vlen
            void *obj = sp_document(self.db.handle)

        self.set_key(obj, key)

        if IS_PY3K:
            value = encode(value)
        PyBytes_AsStringAndSize(value, &vbuf, &vlen)
        sp_setstring(obj, 'value', vbuf, vlen + 1)

        _check(self.sophia.handle, sp_set(target.handle, obj))

    cdef set_key(self, void *obj, key):
        cdef:
            char *kbuf
            Py_ssize_t klen

        if IS_PY3K:
            key = encode(key)
        PyBytes_AsStringAndSize(key, &kbuf, &klen)
        sp_setstring(obj, <char *>self.key, kbuf, klen + 1)

    cdef extract_key(self, void *obj):
        return _getstring(obj, self.key)

    cdef get(self, _BaseDBObject target, key):
        cdef:
            void *obj = sp_document(self.db.handle)

        self.set_key(obj, key)

        obj = sp_get(target.handle, obj)
        if not obj:
            raise KeyError(key)

        return _getstring(obj, 'value')

    cdef delete(self, _BaseDBObject target, key):
        cdef:
            void *obj = sp_document(self.db.handle)

        self.set_key(obj, key)
        _check(self.sophia.handle, sp_delete(target.handle, obj))

    cdef exists(self, _BaseDBObject target, key):
        cdef:
            void *obj = sp_document(self.db.handle)

        self.set_key(obj, key)

        obj = sp_get(target.handle, obj)
        if not obj:
            return False
        return True


cdef class _UInt32Index(_Index):
    index_type = 'u32'

    cdef _get_empty_value(self):
        return 0

    cdef set_key(self, void *obj, key):
        cdef:
            uint32_t* key_ptr = <uint32_t*>malloc(sizeof(uint32_t))
            Py_ssize_t klen = sizeof(uint32_t)

        key_ptr[0] = <uint32_t>key
        sp_setstring(obj, <char *>self.key, key_ptr, klen + 1)

    cdef extract_key(self, void *obj):
        cdef:
            int nlen
            void *ptr

        ptr = sp_getstring(obj, <char *>self.key, &nlen)
        if ptr:
            return <uint32_t>((<uint32_t *>ptr)[0])


cdef class _UInt32RevIndex(_UInt32Index):
    index_type = 'u32rev'


cdef class _UInt64Index(_Index):
    index_type = 'u64'

    cdef _get_empty_value(self):
        return 0

    cdef set_key(self, void *obj, key):
        cdef:
            uint64_t* key_ptr = <uint64_t*>malloc(sizeof(uint64_t))
            Py_ssize_t klen = sizeof(uint64_t)

        key_ptr[0] = <uint64_t>key
        sp_setstring(obj, <char *>self.key, key_ptr, klen + 1)

    cdef extract_key(self, void *obj):
        cdef:
            int nlen
            void *ptr

        ptr = sp_getstring(obj, <char *>self.key, &nlen)
        if ptr:
            return <uint64_t>((<uint64_t *>ptr)[0])


cdef class _UInt64RevIndex(_UInt64Index):
    index_type = 'u64rev'


cdef class _MultiIndex(_Index):
    cdef:
        tuple indexes

    def __cinit__(self, Sophia sophia, Database db, key='key', index_types=None,
                  **_):
        if not index_types:
            raise ValueError('index_types is a required parameter.')

        self.initialize_indexes(index_types)

    cdef initialize_indexes(self, tuple index_types):
        cdef:
            bytes bkey = encode('key')
            bytes suffixes = encode(' bcdefgh')
            _Index index
            int i
            list accum = []

        for i, subindex in enumerate(index_types):
            try:
                IndexType = INDEX_TYPE_MAP[subindex]
            except KeyError:
                raise ValueError('Unrecognized index type, must be one of: %s'
                                 % ', '.join(sorted(INDEX_TYPE_MAP)))
            if i > 0:
                bkey = encode('key_%s' % suffixes[i])

            accum.append(IndexType(self.sophia, self.db, key=bkey))

        self.indexes = tuple(accum)
        self.keys = tuple([index.key for index in accum])

    cdef _get_empty_value(self):
        cdef:
            _Index idx
            list accum = []

        for idx in self.indexes:
            accum.append(idx.empty_value)
        return tuple(accum)

    cdef configure(self):
        cdef:
            bytes db_idx_path = encode('db.%s.index' % self.db.name)
            _Index index
            bindex_type

        for index in self.indexes[1:]:
            sp_setstring(
                self.sophia.handle,
                <char *>db_idx_path,
                <char *>index.key,
                0)

        for index in self.indexes:
            sp_setstring(
                self.sophia.handle,
                <char *>index.b_path,
                <char *>index.b_type,
                0)

    cdef set_key(self, void *obj, key):
        cdef:
            _Index subindex

        for subindex, key_part in zip(self.indexes, key):
            subindex.set_key(obj, key_part)

    cdef extract_key(self, void *obj):
        cdef:
            bytes bkey
            _Index subindex
            list result = []

        for subindex in self.indexes:
            bkey = subindex.extract_key(obj)
            result.append(bkey)

        return tuple(result)


cdef class SimpleDatabase(Sophia):
    cdef:
        readonly db_name

    def __init__(self, filename, index_type=None, auto_open=True):
        path = os.path.dirname(filename)
        self.db_name = os.path.basename(filename).split('.')[0] or 'default'
        databases = [(self.db_name, index_type or 'string')]
        super(SimpleDatabase, self).__init__(
            path=path,
            databases=databases,
            auto_open=auto_open)

    def __getitem__(self, key):
        return self.dbs[self.db_name][key]

    def __setitem__(self, key, value):
        self.dbs[self.db_name][key] = value

    def __delitem__(self, key):
        del self.dbs[self.db_name][key]

    def __contains__(self, key):
        return key in self.dbs[self.db_name]

    def keys(self):
        return self.dbs[self.db_name].keys()

    def values(self):
        return self.dbs[self.db_name].values()

    def items(self):
        return self.dbs[self.db_name].items()

    def __iter__(self):
        return iter(self.cursor())

    def __len__(self):
        return len(self.dbs[self.db_name])

    def cursor(self, *args, **kwargs):
        return self.dbs[self.db_name].cursor(*args, **kwargs)

    def update(self, dict _data=None, **k):
        return self.dbs[self.db_name].update(_data, **k)

    cpdef transaction(self):
        return Transaction(self, self.dbs[self.db_name])

    cpdef view(self, name):
        return View(self, self.dbs[self.db_name], name)


def connect(data_dir, db_name, index_type=None):
    cdef Sophia sophia
    sophia = Sophia(data_dir, [(db_name, index_type)])
    sophia.open()
    return sophia[db_name]


cdef dict INDEX_TYPE_MAP = {
    'string': _Index,
    'u32': _UInt32Index,
    'u32rev': _UInt32RevIndex,
    'u64': _UInt64Index,
    'u64rev': _UInt64RevIndex,
}

"""ADD: # cython: profile=True to top of file to use with cProfile."""
