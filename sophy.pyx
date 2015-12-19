from cpython.string cimport PyString_AsStringAndSize
from cpython.string cimport PyString_FromStringAndSize
from libc.stdlib cimport free, malloc
from libc.string cimport memset
from libc.stdint cimport int64_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t


cdef extern from "Python.h":
    cdef void Py_Initialize()
    cdef int Py_IsInitialized()


cdef extern from "src/sophia.h":
    cdef void *sp_env()
    cdef void *sp_object(void *)

    cdef int sp_open(void *)
    cdef int sp_drop(void *)
    cdef int sp_destroy(void *)
    cdef int sp_error(void *)

    cdef void *sp_asynchronous(void *)
    cdef void *sp_poll(void*)

    cdef int sp_setobject(void*, const char*, const void*)
    cdef int sp_setstring(void*, const char*, const void*, int)
    cdef int sp_setint(void*, const char*, int64_t)

    cdef void *sp_getobject(void*, const char*)
    cdef void *sp_getstring(void*, const char*, int*)
    cdef int64_t sp_getint(void*, const char*)

    cdef int sp_set(void*, void*)
    cdef int sp_update(void*, void*)
    cdef int sp_delete(void*, void*)
    cdef void *sp_get(void*, void*)

    cdef void *sp_cursor(void*)
    cdef void *sp_batch(void *)
    cdef void *sp_begin(void *)
    cdef int sp_prepare(void *)
    cdef int sp_commit(void *)


cdef bytes encode(obj):
    if isinstance(obj, unicode):
        return obj.encode('utf-8')
    return bytes(obj)

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
        return self.get_option(propname, string)
    if readonly:
        return property(_getter)
    else:
        def _setter(self, value):
            self.set_option(propname, value)
        return property(_getter, _setter)

def _db_property(propname, string=True, readonly=False):
    def _getter(self):
        return self.get_option('db.%s.%s' % (self.name, propname), string)
    if readonly:
        return property(_getter)
    else:
        def _setter(self, value):
            self.set_option('db.%s.%s' % (self.name, propname), value)
        return property(_getter, _setter)


cdef class Sophia(object):
    cdef:
        readonly object name
        readonly object path
        _Index idx
        bint is_open
        bytes b_name
        bytes b_path
        dict config
        void *env
        void *db

    def __cinit__(self, name, path='sophia', format=None, mmap=None, sync=None,
                  compression=None, compression_key=None):
        self.name = name
        self.path = path
        self.config = {}

        self.b_name = encode(name)
        self.b_path = encode(path)

        self.env = sp_env()

    def __init__(self, name, path='sophia', format=None, mmap=None, sync=None,
                 compression=None, compression_key=None):
        if format is not None:
            self.format = format
        if mmap is not None:
            self.mmap = mmap
        if sync is not None:
            self.sync = sync
        if compression is not None:
            self.compression = compression
        if compression_key is not None:
            self.compression_key = compression_key

    def __dealloc__(self):
        if self.db:
            sp_destroy(self.db)
        if self.env:
            sp_destroy(self.env)

    cpdef set_option(self, key, value):
        cdef:
            bytes bkey
            int rc

        self.config[key] = value
        if self.is_open:
            self._config(key, value)

    cpdef get_option(self, key, string=True):
        if string:
            return _getstring(self.env, key)
        else:
            bkey = encode(key)
            return sp_getint(self.env, <const char *>bkey)

    cpdef clear_option(self, key):
        del self.config[key]

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

    # Memory control properties.
    memory_limit = _sophia_property('memory.limit', False)
    memory_pager_page_size = _sophia_property(
        'memory.pager_page_size',
        False,
        True)
    memory_pager_pools = _sophia_property('memory.pager_pools', False, True)
    memory_pager_pool_size = _sophia_property(
        'memory.pager_pool_size',
        False,
        True)
    memory_used = _sophia_property('memory.used', False, True)

    # Compaction properties.
    compaction_node_size = _sophia_property('compaction.node_size', False)
    compaction_page_size = _sophia_property('compaction.page_size', False)
    compaction_page_checksum = _sophia_property(
        'compaction.page_checksum',
        False)
    compaction_redzone = _sophia_property('compaction.redzone', False)
    compaction_redzone_mode = _sophia_property(
        'compaction.redzone.mode',
        False)
    compaction_redzone_async = _sophia_property(
        'compaction.redzone.async',
        False)

    # Scheduler properties.
    scheduler_threads = _sophia_property('scheduler.threads', False)
    scheduler_checkpoint_active = _sophia_property(
        'scheduler.checkpoint_active',
        False,
        True)
    scheduler_gc_active = _sophia_property(
        'scheduler.gc_active',
        False,
        True)
    scheduler_reqs = _sophia_property('scheduler.reqs', False)

    def checkpoint(self):
        self._config('scheduler.checkpoint', 0)

    def gc(self):
        self._config('scheduler.gc', 0)

    # WAL properties.
    log_enable = _sophia_property('log.enable', False)
    log_path = _sophia_property('log.path')
    log_sync = _sophia_property('log.sync', False)
    log_files = _sophia_property('log.files', False, True)

    def log_rotate(self):
        self._config('log.rotate', 0)

    def log_gc(self):
        self._config('log.gc', 0)

    # Backup properties.
    backup_path = _sophia_property('backup.path')
    backup_active = _sophia_property('backup.active', False)
    backup_last = _sophia_property('backup.last', False)
    backup_last_complete = _sophia_property('backup.last_complete', False)

    def backup(self):
        self._config('backup.run', 0)

    # Sophia properites.
    build = _sophia_property('sophia.build', readonly=True)
    version = _sophia_property('sophia.version', readonly=True)

    cdef _config(self, key, value):
        cdef:
            bytes bkey = encode(key)
            int rc

        if isinstance(value, bool):
            value = value and 1 or 0
        if isinstance(value, int):
            rc = sp_setint(self.env, <const char *>bkey, <int>value)
        elif isinstance(value, basestring):
            bvalue = encode(value)
            rc = sp_setstring(
                self.env,
                <const char *>bkey,
                <const char *>bvalue,
                0)
        _check(self.env, rc)

    cpdef bint open(self):
        if self.is_open:
            return False

        sp_setstring(self.env, 'sophia.path', <const char *>self.b_path, 0)
        sp_setstring(self.env, 'db', <const char *>self.b_name, 0)

        for key, value in self.config.items():
            self._config(key, value)

        self.db = sp_getobject(self.env, 'db.%s' % self.b_name)
        if not self.db:
            raise MemoryError('unable to allocate db object.')

        self.idx = self._create_index()
        self.idx.configure()
        _check(self.env, sp_open(self.env))

        self.is_open = True
        return True

    cdef _Index _create_index(self, target=None):
        return _Index(self, target=target)

    cpdef bint close(self):
        if not self.is_open:
            return False

        sp_destroy(self.db)
        self.db = <void *>0
        sp_destroy(self.env)
        self.env = sp_env()
        self.is_open = False
        return True

    cpdef transaction(self):
        return Transaction(self)

    cpdef batch(self):
        return Batch(self)

    cpdef cursor(self, order='>=', key=None, prefix=None, keys=True,
                 values=True):
        return Cursor(self, order=order, key=key, prefix=prefix, keys=keys,
                      values=values)

    def update(self, dict _data=None, **k):
        with self.batch() as wb:
            if _data:
                for key in _data:
                    wb[key] = _data[key]
            for key in k:
                wb[key] = k[key]

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.get_range(key.start, key.stop, key.step)
        return self.idx.get(key)

    def __setitem__(self, key, value):
        self.idx.set(key, value)

    def __delitem__(self, key):
        self.idx.delete(key)

    def __contains__(self, key):
        return self.idx.exists(key)

    def get_range(self, start=None, stop=None, reverse=False):
        cdef:
            Cursor cursor

        start_key = start or ''
        end_key = stop or ''

        if start_key > end_key and not reverse and end_key:
            reverse = True

        if reverse:
            if ((end_key and not start_key) or
                (start_key and not end_key) or
                (start_key < end_key)):

                start_key, end_key = end_key, start_key


        order = '<=' if reverse else '>='
        cursor = self.cursor(order=order, key=start_key)
        for key, value in cursor:
            if end_key:
                if reverse and key < end_key:
                    raise StopIteration
                elif not reverse and key > end_key:
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


cdef class Cursor(object):
    cdef:
        public Sophia sophia
        readonly bytes order
        readonly bytes key
        readonly bytes prefix
        bint keys
        bint values
        bytes idx_key
        _Index idx
        void *cursor
        void *handle

    def __cinit__(self, Sophia sophia, order='>=', key=None, prefix=None,
                  keys=True, values=True):
        self.sophia = sophia
        self.order = encode(order)
        self.keys = keys
        self.values = values
        self.idx_key = self.sophia.idx.key
        if key:
            self.key = encode(key)
        if prefix:
            self.prefix = encode(prefix)

    def __dealloc__(self):
        if self.sophia.env and self.cursor:
            sp_destroy(self.cursor)

    def __iter__(self):
        if self.cursor:
            sp_destroy(self.cursor)
        self.cursor = sp_cursor(self.sophia.env)
        self.handle = sp_object(self.sophia.db)
        sp_setstring(self.handle, 'order', <char *>self.order, 0)
        if self.key:
            sp_setstring(self.handle, 'key', <char *>self.key, 0)
        if self.prefix:
            sp_setstring(
                self.handle,
                'prefix',
                <char *>self.prefix,
                (sizeof(char) * len(self.prefix)))
        return self

    def __next__(self):
        self.handle = sp_get(self.cursor, self.handle)
        if not self.handle:
            sp_destroy(self.cursor)
            self.cursor = NULL
            raise StopIteration

        if self.keys and self.values:
            return (_getstring(self.handle, self.idx_key),
                    _getstring(self.handle, 'value'))
        elif self.keys:
            return _getstring(self.handle, self.idx_key)
        elif self.values:
            return _getstring(self.handle, 'value')


cdef class _BaseTransaction(object):
    cdef:
        public Sophia sophia
        _Index idx
        void *handle

    def __cinit__(self, Sophia sophia):
        self.sophia = sophia

    cdef void *create_handle(self):
        raise NotImplementedError

    def __enter__(self):
        self.handle = self.create_handle()
        self.idx = self.sophia._create_index(target=self)
        return self

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
        self.check(sp_commit(self.handle))
        if begin:
            self.__enter__()

    cpdef rollback(self, begin=True):
        sp_destroy(self.handle)
        if begin:
            self.__enter__()

    cdef check(self, int rc):
        pass

    def __getitem__(self, key):
        return self.idx.get(key)

    def __setitem__(self, key, value):
        self.idx.set(key, value)

    def __delitem__(self, key):
        self.idx.delete(key)

    def __contains__(self, key):
        return self.idx.exists(key)

    def __call__(self, fn):
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner


cdef class Transaction(_BaseTransaction):
    cdef void *create_handle(self):
        return sp_begin(self.sophia.env)

    cdef check(self, int rc):
        try:
            _check(self.sophia.env, rc)
        except:
            sp_destroy(self.handle)
            raise
        else:
            if rc == 1:
                raise Exception('transaction was rolled back by another '
                                'concurrent transaction.')
            elif rc == 2:
                sp_destroy(self.handle)
                raise Exception('transaction is not finished, waiting for a '
                                'concurrent transaction to finish.')


cdef class Batch(_BaseTransaction):
    cdef void *create_handle(self):
        return sp_batch(self.sophia.db)

    cdef check(self, int rc):
        try:
            _check(self.sophia.env, rc)
        except:
            sp_destroy(self.handle)
            raise

    def __getitem__(self, key):
        raise Exception('Batches only support writes.')


cdef class _Index(object):
    cdef:
        _BaseTransaction target
        bytes key
        public Sophia sophia
        void *db
        void *env
        void *handle

    def __cinit__(self, Sophia sophia, key='key', target=None, *a):
        self.sophia = sophia
        self.key = encode(key)
        self.db = sophia.db
        self.env = sophia.env
        self.target = target
        if self.target:
            self.handle = self.target.handle
        else:
            self.handle = self.db

    cdef configure(self):
        sp_setstring(
            self.env,
            'db.%s.index.%s' % (self.sophia.b_name, self.key),
            'string',
            0)

    cdef set(self, key, value):
        cdef:
            char *kbuf
            char *vbuf
            Py_ssize_t klen, vlen
            void *obj = sp_object(self.db)

        PyString_AsStringAndSize(key, &kbuf, &klen)
        PyString_AsStringAndSize(value, &vbuf, &vlen)

        sp_setstring(obj, 'key', kbuf, klen + 1)
        sp_setstring(obj, 'value', vbuf, vlen + 1)
        _check(self.env, sp_set(self.handle, obj))

    cdef get(self, key):
        cdef:
            char *kbuf
            Py_ssize_t klen
            void *obj = sp_object(self.db)

        PyString_AsStringAndSize(key, &kbuf, &klen)
        sp_setstring(obj, 'key', kbuf, klen + 1)

        obj = sp_get(self.handle, obj)
        if not obj:
            raise KeyError(key)

        return _getstring(obj, 'value')

    cdef delete(self, key):
        cdef:
            char *kbuf
            Py_ssize_t klen
            void *obj = sp_object(self.db)

        PyString_AsStringAndSize(key, &kbuf, &klen)
        sp_setstring(obj, 'key', kbuf, klen + 1)
        _check(self.env, sp_delete(self.handle, obj))

    cdef exists(self, key):
        cdef:
            char *kbuf
            Py_ssize_t klen
            void *obj = sp_object(self.db)

        PyString_AsStringAndSize(key, &kbuf, &klen)
        sp_setstring(obj, 'key', kbuf, klen + 1)

        obj = sp_get(self.handle, obj)
        if not obj:
            return False
        return True
