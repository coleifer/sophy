from cpython.string cimport PyString_AsStringAndSize
from cpython.string cimport PyString_FromStringAndSize
from libc.stdlib cimport malloc
from libc.stdint cimport int64_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

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


cdef bytes encode(obj):
    if isinstance(obj, unicode):
        return obj.encode('utf-8')
    elif not isinstance(obj, bytes):
        return bytes(obj)
    return obj

cdef inline _getstring(void *obj, const char *key):
    cdef:
        char *buf
        int nlen

    buf = <char *>sp_getstring(obj, key, &nlen)
    if buf:
        return PyString_FromStringAndSize(buf, nlen - 1)

cdef inline _check(void *env, int rc):
    if rc == -1:
        error = _getstring(env, 'sophia.error')
        if error:
            raise Exception(error)
        else:
            raise Exception('unknown error occurred.')


def _sophia_property(propname, string=True, readonly=False):
    def _getter(self):
        return self.config.get_option(propname, string)
    if readonly:
        return property(_getter)
    else:
        def _setter(self, value):
            self.config.set_option(propname, value)
        return property(_getter, _setter)

def _sophia_method(propname):
    def _method(self):
        self.config.set_option(propname, 0)
    return _method

def _db_property(prop, string=True, readonly=False):
    def _getter(self):
        return self.config.get_option('db.%s.%s' % (self.name, prop), string)
    if readonly:
        return property(_getter)
    else:
        def _setter(self, value):
            self.config.set_option('db.%s.%s' % (self.name, prop), value)
        return property(_getter, _setter)


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
        readonly bint auto_open
        readonly _ConfigManager config
        readonly object path

    def __cinit__(self, path='sophia', auto_open=True):
        self.path = path
        self.auto_open = auto_open

        self.b_path = encode(path)
        self.handle = sp_env()

    def __init__(self, path='sophia', auto_open=True):
        self.config = _ConfigManager(self)
        if self.auto_open:
            self.open()

    def __dealloc__(self):
        if self.handle:
            sp_destroy(self.handle)

    cpdef bint open(self):
        if self.is_open:
            return False

        sp_setstring(self.handle, 'sophia.path', <const char *>self.b_path, 0)
        self.config.apply_all()
        _check(self.handle, sp_open(self.handle))

        self.is_open = True
        return True

    cpdef bint close(self):
        if not self.is_open:
            return False

        sp_destroy(self.handle)
        self.handle = sp_env()
        self.is_open = False
        return True

    cpdef database(self, name, index_type=None):
        return Database(self, name, index_type, self.auto_open)

    # Memory control properties.
    memory_limit = _sophia_property('memory.limit', False)
    memory_pager_page_size = _sophia_property('memory.pager_page_size', False, True)
    memory_pager_pools = _sophia_property('memory.pager_pools', False, True)
    memory_pager_pool_size = _sophia_property('memory.pager_pool_size', False, True)
    memory_used = _sophia_property('memory.used', False, True)

    # Compaction properties.
    compaction_node_size = _sophia_property('compaction.node_size', False)
    compaction_page_size = _sophia_property('compaction.page_size', False)
    compaction_page_checksum = _sophia_property('compaction.page_checksum', False)
    compaction_redzone = _sophia_property('compaction.redzone', False)
    compaction_redzone_mode = _sophia_property('compaction.redzone.mode', False)
    compaction_redzone_async = _sophia_property('compaction.redzone.async', False)

    # Scheduler properties and methods.
    checkpoint = _sophia_method('scheduler.checkpoint')
    gc = _sophia_method('scheduler.gc')
    scheduler_threads = _sophia_property('scheduler.threads', False)
    scheduler_checkpoint_active = _sophia_property('scheduler.checkpoint_active', False, True)
    scheduler_gc_active = _sophia_property('scheduler.gc_active', False, True)
    scheduler_reqs = _sophia_property('scheduler.reqs', False)

    # WAL properties and methods.
    log_rotate = _sophia_method('log.rotate')
    log_gc = _sophia_method('log.gc')
    log_enable = _sophia_property('log.enable', False)
    log_path = _sophia_property('log.path')
    log_sync = _sophia_property('log.sync', False)
    log_files = _sophia_property('log.files', False, True)

    # Backup properties and methods.
    backup = _sophia_method('backup.run')
    backup_path = _sophia_property('backup.path')
    backup_active = _sophia_property('backup.active', False)
    backup_last = _sophia_property('backup.last', False)
    backup_last_complete = _sophia_property('backup.last_complete', False)

    # Sophia properites.
    build = _sophia_property('sophia.build', readonly=True)
    version = _sophia_property('sophia.version', readonly=True)


cdef class _BaseDBObject(_SophiaObject):
    cdef:
        readonly Sophia sophia
        _Index index

    def __cinit__(self, Sophia sophia, *_):
        self.sophia = sophia

    cpdef bint open(self):
        if self.handle:
            return False

        self.handle = self._create_handle()
        if not self.handle:
            raise MemoryError('Unable to allocate object: %s.' % self)
        self.index = self._get_index()
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


cdef class Database(_BaseDBObject):
    cdef:
        bint auto_open
        readonly bytes name
        readonly _ConfigManager config
        tuple index_type

    def __cinit__(self, Sophia sophia, name, index_type=None, auto_open=True):
        self.name = encode(name)

        if not index_type:
            self.index_type = ('string',)
        elif isinstance(index_type, basestring):
            self.index_type = (index_type,)
        else:
            self.index_type = tuple(index_type)

        self.auto_open = auto_open
        self.config = self.sophia.config

    def __init__(self, Sophia sophia, name, index_type=None, auto_open=True):
        if self.auto_open:
            self.open()

    def __dealloc__(self):
        if self.sophia.handle and self.handle:
            sp_destroy(self.handle)

    cdef destroy(self):
        pass

    cdef void *_create_handle(self):
        cdef:
            void *handle
        sp_setstring(self.sophia.handle, 'db', <const char *>self.name, 0)
        handle = sp_getobject(self.sophia.handle, 'db.%s' % self.name)
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

        index.configure()

        # NOTE: We need to configure the index before opening the db.
        _check(self.sophia.handle, sp_open(self.handle))
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

    # Database properties.
    compression = _db_property('compression')
    compression_key = _db_property('compression_key', False)
    database_id = _db_property('id', False)
    format = _db_property('format')
    mmap = _db_property('mmap', False)
    sync = _db_property('sync', False)
    status = _db_property('status', readonly=True)

    # Database index properties.
    index_branch_avg = _db_property('index.branch_avg', False, True)
    index_branch_count = _db_property('index.branch_count', False, True)
    index_branch_max = _db_property('index.branch_max', False, True)
    index_count = _db_property('index.count', False, True)
    index_count_dup = _db_property('index.count_dup', False, True)
    index_memory_used = _db_property('index.memory_used', False, True)
    index_node_count = _db_property('index.node_count', False, True)
    index_node_size = _db_property('index.node_size', False, True)
    index_node_origin_size = _db_property(
        'index.node_origin_size',
        False,
        True)
    index_page_count = _db_property('index.page_count', False, True)
    index_read_cache = _db_property('index.read_cache', False, True)
    index_read_disk = _db_property('index.read_disk', False, True)


cdef class View(_BaseDBObject):
    cdef:
        Database db
        bytes name

    def __cinit__(self, Sophia sophia, Database db, name):
        self.db = db
        self.name = encode(name)

    def __init__(self, Sophia sophia, Database db, name):
        if self.db.auto_open:
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

        PyString_AsStringAndSize(value, &vbuf, &vlen)
        sp_setstring(obj, 'value', vbuf, vlen + 1)

        _check(self.sophia.handle, sp_set(target.handle, obj))

    cdef set_key(self, void *obj, key):
        cdef:
            char *kbuf
            Py_ssize_t klen

        PyString_AsStringAndSize(key, &kbuf, &klen)
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


def connect(data_dir, db_name, index_type=None):
    sophia = Sophia(path=data_dir, auto_open=True)
    sophia.open()
    return sophia.database(db_name, index_type)


cdef dict INDEX_TYPE_MAP = {
    'string': _Index,
    'u32': _UInt32Index,
}

"""ADD: # cython: profile=True to top of file to use with cProfile."""
