from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

import json
import uuid
from pickle import dumps as pdumps
from pickle import loads as ploads
try:
    from msgpack import packb as mpackb
    from msgpack import unpackb as munpackb
except ImportError:
    mpackb = munpackb = None


cdef extern from "src/sophia.h" nogil:
    cdef void *sp_env()
    cdef void *sp_document(void *)
    cdef int sp_setstring(void*, const char*, const void*, int)
    cdef int sp_setint(void*, const char*, int64_t)
    cdef void *sp_getobject(void*, const char*)
    cdef void *sp_getstring(void*, const char*, int*)
    cdef int64_t sp_getint(void*, const char*)
    cdef int sp_open(void *)
    cdef int sp_destroy(void *)
    cdef int sp_set(void*, void*)
    cdef int sp_upsert(void*, void*)
    cdef int sp_delete(void*, void*)
    cdef void *sp_get(void*, void*)
    cdef void *sp_cursor(void*)
    cdef void *sp_begin(void *)
    cdef int sp_prepare(void *)
    cdef int sp_commit(void *)


class SophiaError(Exception): pass

cdef class Sophia(object)
cdef class Schema(object)
cdef class Transaction(object)


cdef bint IS_PY3K = PY_MAJOR_VERSION == 3

cdef inline bytes encode(obj):
    cdef bytes result
    if PyUnicode_Check(obj):
        result = PyUnicode_AsUTF8String(obj)
    elif PyBytes_Check(obj):
        result = <bytes>obj
    elif obj is None:
        return None
    elif IS_PY3K:
        result = PyUnicode_AsUTF8String(str(obj))
    else:
        result = bytes(obj)
    return result

cdef inline _getstring(void *obj, const char *key):
    cdef:
        char *buf
        int nlen

    buf = <char *>sp_getstring(obj, key, &nlen)
    if buf:
        value = buf[:nlen - 1]
        return value

cdef inline _check(void *env, int rc):
    if rc == -1:
        error = _getstring(env, 'sophia.error')
        if error:
            raise SophiaError(error)
        else:
            raise SophiaError('unknown error occurred.')

cdef inline check_open(Sophia env):
    if not env.is_open:
        raise SophiaError('Sophia environment is closed.')


cdef class Configuration(object):
    cdef:
        dict settings
        Sophia env

    def __cinit__(self, Sophia env):
        self.env = env
        self.settings = {}

    def set_option(self, key, value):
        self.settings[key] = value
        if self.env.is_open:
            self._set(key, value)

    def get_option(self, key, is_string=True):
        check_open(self.env)
        cdef bytes bkey = encode(key)
        if is_string:
            return _getstring(self.env.env, <const char *>bkey)
        else:
            return sp_getint(self.env.env, <const char *>bkey)

    cdef clear_option(self, key):
        try:
            del self.settings[key]
        except KeyError:
            pass

    cdef int _set(self, key, value) except -1:
        cdef:
            bytes bkey = encode(key)
            int rc

        if isinstance(value, bool):
            value = value and 1 or 0
        if isinstance(value, int):
            rc = sp_setint(self.env.env, <const char *>bkey, <int>value)
        elif isinstance(value, basestring):
            bvalue = encode(value)
            rc = sp_setstring(self.env.env, <const char *>bkey,
                              <const char *>bvalue, 0)
        else:
            raise Exception('Setting value must be bool, int or string.')

        if rc == -1:
            error = _getstring(self.env.env, 'sophia.error')
            if error:
                raise SophiaError(error)
            else:
                raise SophiaError('unknown error occurred.')
        return rc

    cdef configure(self):
        for key, value in self.settings.items():
            self._set(key, value)


def __config__(config_name, is_string=False, is_readonly=False):
    cdef bytes name = encode(config_name)
    def _getter(self):
        return self.config.get_option(name, is_string)
    if is_readonly:
        return property(_getter)
    def _setter(self, value):
        self.config.set_option(name, value)
    return property(_getter, _setter)

def __config_ro__(name, is_string=False):
    return __config__(name, is_string, True)

def __operation__(name):
    def _method(self):
        self.config.set_option(encode(name), 0)
    return _method

def __dbconfig__(config_name, is_string=False, is_readonly=False):
    cdef bytes name = encode(config_name)
    def _getter(self):
        return self.env.config.get_option(b'.'.join((b'db', self.name, name)),
                                          is_string)
    if is_readonly:
        return property(_getter)
    def _setter(self, value):
        self.env.config.set_option(b'.'.join((b'db', self.name, name)), value)
    return property(_getter, _setter)

def __dbconfig_ro__(name, is_string=False):
    return __dbconfig__(name, is_string, True)

def __dbconfig_s__(name, is_readonly=False):
    return __dbconfig__(name, True, is_readonly)


cdef class Sophia(object):
    cdef:
        bint is_open
        readonly Configuration config
        dict database_lookup
        list databases
        readonly bytes path
        void *env

    def __cinit__(self):
        self.env = <void *>0

    def __init__(self, path):
        self.config = Configuration(self)
        self.is_open = False
        self.database_lookup = {}
        self.databases = []
        self.path = encode(path)

    def add_database(self, name, Schema schema):
        cdef Database db

        if self.is_open:
            raise SophiaError('cannot add database to open environment.')

        db = Database(self, name, schema)
        self.databases.append(db)
        self.database_lookup[name] = db
        return db

    def remove_database(self, name):
        if self.is_open:
            raise SophiaError('cannot remove database from open environment.')

        db = self.database_lookup.pop(name)
        self.databases.remove(db)

    def get_database(self, name):
        return self.database_lookup[name]

    def __getitem__(self, name):
        return self.database_lookup[name]

    cdef configure_database(self, Database db):
        cdef:
            BaseIndex index
            int i

        self.set_string(b'db', db.name)

        for i, index in enumerate(db.schema.key):
            # db.<name>.scheme = <index name>
            # db.<name>.scheme.<index name> = <dtype>,key(i)
            self.set_string(b'.'.join((b'db', db.name, b'scheme')), index.name)
            self.set_string(b'.'.join((b'db', db.name, b'scheme', index.name)),
                            encode('%s,key(%d)' % (
                                index.data_type.decode('utf-8'), i)))

        for index in db.schema.value:
            self.set_string(b'.'.join((b'db', db.name, b'scheme')), index.name)
            self.set_string(b'.'.join((b'db', db.name, b'scheme', index.name)),
                            index.data_type)

        db.db = sp_getobject(self.env, b'db.' + db.name)

    def open(self):
        if self.is_open:
            return False

        cdef Database db

        self.env = sp_env()
        self.set_string(b'sophia.path', <const char *>self.path)

        for db in self.databases:
            self.configure_database(db)

        self.config.configure()

        cdef int rc
        with nogil:
            rc = sp_open(self.env)
        _check(self.env, rc)

        self.is_open = True
        return self.is_open

    def close(self):
        if not self.is_open or not self.env:
            return False
        sp_destroy(self.env)
        self.env = <void *>0
        self.is_open = False
        return True

    def __dealloc__(self):
        if self.is_open and self.env:
            sp_destroy(self.env)

    cdef set_string(self, const char *key, const char *value):
        sp_setstring(self.env, key, value, 0)

    cpdef Transaction transaction(self):
        return Transaction(self)

    version = __config_ro__('sophia.version', is_string=True)
    version_storage = __config_ro__('sophia.version_storage', is_string=True)
    build = __config_ro__('sophia.build', is_string=True)
    status = __config_ro__('sophia.status', is_string=True)
    errors = __config_ro__('sophia.errors')
    error = __config_ro__('sophia.error', is_string=True)

    backup_path = __config__('backup.path', is_string=True)
    backup_run = __operation__('backup.run')
    backup_active = __config_ro__('backup.active')
    backup_last = __config_ro__('backup.last')
    backup_last_complete = __config_ro__('backup.last_complete')

    scheduler_threads = __config__('scheduler.threads')
    def scheduler_trace(self, thread_id):
        return self.config.get_option('scheduler.%s.trace' % thread_id)

    transaction_online_rw = __config_ro__('transaction.online_rw')
    transaction_online_ro = __config_ro__('transaction.online_ro')
    transaction_commit = __config_ro__('transaction.commit')
    transaction_rollback = __config_ro__('transaction.rollback')
    transaction_conflict = __config_ro__('transaction.conflict')
    transaction_lock = __config_ro__('transaction.lock')
    transaction_latency = __config_ro__('transaction.latency', is_string=True)
    transaction_log = __config_ro__('transaction.log', is_string=True)
    transaction_vlsn = __config_ro__('transaction.vlsn')
    transaction_gc = __config_ro__('transaction.gc')

    metric_lsn = __config_ro__('metric.lsn')
    metric_tsn = __config_ro__('metric.tsn')
    metric_nsn = __config_ro__('metric.nsn')
    metric_dsn = __config_ro__('metric.dsn')
    metric_bsn = __config_ro__('metric.bsn')
    metric_lfsn = __config_ro__('metric.lfsn')

    log_enable = __config__('log.enable')
    log_path = __config__('log.path', is_string=True)
    log_sync = __config__('log.sync')
    log_rotate_wm = __config__('log.rotate_wm')
    log_rotate_sync = __config__('log.rotate_sync')
    log_rotate = __operation__('log.rotate')
    log_gc = __operation__('log.gc')
    log_files = __config_ro__('log.files')


cdef class Transaction(object):
    cdef:
        Sophia env
        void *txn

    def __cinit__(self, Sophia env):
        self.env = env
        self.txn = <void *>0

    def __dealloc__(self):
        if self.env.is_open and self.txn:
            sp_destroy(self.txn)

    cdef _reset(self, bint begin):
        self.txn = <void *>0
        if begin:
            self.begin()

    def begin(self):
        check_open(self.env)
        if self.txn:
            raise SophiaError('This transaction has already been started.')
        with nogil:
            self.txn = sp_begin(self.env.env)
        return self

    def commit(self, begin=True):
        check_open(self.env)
        if not self.txn:
            raise SophiaError('Transaction is not currently open. Cannot '
                              'commit.')

        cdef int rc
        with nogil:
            rc = sp_commit(self.txn)
        if rc == 1:
            self.txn = <void *>0
            raise SophiaError('transaction was rolled back by another '
                              'concurrent transaction.')
        elif rc == 2:
            # Do not clear out self.txn because we may be able to commit later.
            raise SophiaError('transaction is not finished, waiting for a '
                              'concurrent transaction to finish.')
        self._reset(begin)

    def rollback(self, begin=True):
        check_open(self.env)
        if not self.txn:
            raise SophiaError('Transaction is not currently open. Cannot '
                              'rollback.')
        with nogil:
            sp_destroy(self.txn)
        self._reset(begin)

    def __enter__(self):
        self.begin()
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

    def __getitem__(self, database):
        if not isinstance(database, Database):
            raise SophiaError('Transaction __getitem__ value must be a '
                              'Database instance.')
        return DatabaseTransaction(database, self)

    cdef Database get_database(self, Database database):
        return DatabaseTransaction(database, self)


SCHEMA_STRING = b'string'
SCHEMA_U64 = b'u64'
SCHEMA_U32 = b'u32'
SCHEMA_U16 = b'u16'
SCHEMA_U8 = b'u8'
SCHEMA_U64_REV = b'u64_rev'
SCHEMA_U32_REV = b'u32_rev'
SCHEMA_U16_REV = b'u16_rev'
SCHEMA_U8_REV = b'u8_rev'


cdef class BaseIndex(object):
    cdef:
        bytes name

    by_reference = False
    data_type = b''

    def __init__(self, name):
        self.name = encode(name)

    cdef set_key(self, void *obj, value): pass
    cdef get_key(self, void *obj): pass


cdef class SerializedIndex(BaseIndex):
    cdef object _serialize, _deserialize

    by_reference = True
    data_type = SCHEMA_STRING

    def __init__(self, name, serialize, deserialize):
        self.name = encode(name)
        self._serialize = serialize
        self._deserialize = deserialize

    cdef set_key(self, void *obj, value):
        cdef:
            bytes bvalue
            char *buf
            Py_ssize_t buflen

        bvalue = self._serialize(value)
        if not PyBytes_Check(bvalue):
            bvalue = encode(bvalue)

        PyBytes_AsStringAndSize(bvalue, &buf, &buflen)
        sp_setstring(obj, <const char *>self.name, buf, buflen + 1)
        return bvalue

    cdef get_key(self, void *obj):
        cdef:
            char *buf
            int buflen

        buf = <char *>sp_getstring(obj, <const char *>self.name, &buflen)
        if buf:
            return self._deserialize(buf[:buflen - 1])


cdef class BytesIndex(BaseIndex):
    by_reference = True
    data_type = SCHEMA_STRING

    cdef set_key(self, void *obj, value):
        cdef:
            bytes bvalue
            char *buf
            Py_ssize_t buflen

        if not PyBytes_Check(value):
            if IS_PY3K:
                bvalue = bytes(value, 'raw_unicode_escape')
            else:
                bvalue = bytes(value)
        else:
            bvalue = value

        PyBytes_AsStringAndSize(bvalue, &buf, &buflen)
        sp_setstring(obj, <const char *>self.name, buf, buflen + 1)
        return bvalue

    cdef get_key(self, void *obj):
        cdef:
            char *buf
            int buflen

        buf = <char *>sp_getstring(obj, <const char *>self.name, &buflen)
        if buf:
            return buf[:buflen - 1]


cdef class StringIndex(BaseIndex):
    by_reference = True
    data_type = SCHEMA_STRING

    cdef set_key(self, void *obj, value):
        cdef:
            bytes bvalue = encode(value)
            char *buf
            Py_ssize_t buflen

        PyBytes_AsStringAndSize(bvalue, &buf, &buflen)
        sp_setstring(obj, <const char *>self.name, buf, buflen + 1)
        return bvalue

    cdef get_key(self, void *obj):
        cdef:
            char *buf
            int buflen

        buf = <char *>sp_getstring(obj, <const char *>self.name, &buflen)
        if buf:
            return buf[:buflen - 1].decode('utf-8')


cdef class U64Index(BaseIndex):
    data_type = SCHEMA_U64

    cdef set_key(self, void *obj, value):
        cdef:
            uint64_t ival = <uint64_t>value
        sp_setint(obj, <const char *>self.name, ival)

    cdef get_key(self, void *obj):
        return sp_getint(obj, <const char *>self.name)


cdef class U32Index(U64Index):
    data_type = SCHEMA_U32

    cdef set_key(self, void *obj, value):
        cdef:
            uint32_t ival = <uint32_t>value
        sp_setint(obj, <const char *>self.name, ival)


cdef class U16Index(U64Index):
    data_type = SCHEMA_U16

    cdef set_key(self, void *obj, value):
        cdef:
            uint16_t ival = <uint16_t>value
        sp_setint(obj, <const char *>self.name, ival)


cdef class U8Index(U64Index):
    data_type = SCHEMA_U8

    cdef set_key(self, void *obj, value):
        cdef:
            uint8_t ival = <uint8_t>value
        sp_setint(obj, <const char *>self.name, ival)


cdef class U64RevIndex(U64Index):
    data_type = SCHEMA_U64_REV

cdef class U32RevIndex(U32Index):
    data_type = SCHEMA_U32_REV

cdef class U16RevIndex(U16Index):
    data_type = SCHEMA_U16_REV

cdef class U8RevIndex(U8Index):
    data_type = SCHEMA_U8_REV


cdef class JsonIndex(SerializedIndex):
    def __init__(self, name):
        jdumps = lambda v: json.dumps(v, separators=(',', ':')).encode('utf-8')
        jloads = lambda v: json.loads(v.decode('utf-8'))
        super(JsonIndex, self).__init__(name, jdumps, jloads)

cdef class MsgPackIndex(SerializedIndex):
    def __init__(self, name):
        if mpackb is None or munpackb is None:
            raise SophiaError('msgpack-python library not installed!')
        super(MsgPackIndex, self).__init__(name, mpackb, munpackb)

cdef class PickleIndex(SerializedIndex):
    def __init__(self, name):
        super(PickleIndex, self).__init__(name, pdumps, ploads)

cdef class UUIDIndex(SerializedIndex):
    def __init__(self, name):
        uuid_encode = lambda u: u.bytes
        uuid_decode = lambda b: uuid.UUID(bytes=b)
        super(UUIDIndex, self).__init__(name, uuid_encode, uuid_decode)


cdef class Document(object):
    cdef:
        list refs
        void *handle

    def __cinit__(self):
        self.handle = <void *>0
        self.refs = []

    cdef release_refs(self):
        self.refs = []


cdef Document create_document(void *handle):
    cdef Document doc = Document.__new__(Document)
    doc.handle = handle
    return doc


cdef class Schema(object):
    cdef:
        bint multi_key, multi_value
        int key_n_ref, value_n_ref
        list key
        list value

    def __init__(self, key_parts=None, value_parts=None):
        cdef:
            BaseIndex index

        self.key_n_ref = self.value_n_ref = 0
        self.key = []
        self.value = []
        if key_parts is not None:
            if isinstance(key_parts, BaseIndex):
                key_parts = (key_parts,)
            for index in key_parts:
                self.add_key(index)
        if value_parts is not None:
            if isinstance(value_parts, BaseIndex):
                value_parts = (value_parts,)
            for index in value_parts:
                self.add_value(index)

    def add_key(self, BaseIndex index):
        self.key.append(index)
        self.multi_key = len(self.key) != 1
        if index.by_reference:
            self.key_n_ref += 1

    def add_value(self, BaseIndex index):
        self.value.append(index)
        self.multi_value = len(self.value) != 1
        if index.by_reference:
            self.value_n_ref += 1

    cdef set_key(self, Document doc, tuple parts):
        cdef:
            BaseIndex index
            int i

        for i, index in enumerate(self.key):
            ref = index.set_key(doc.handle, parts[i])
            if index.by_reference:
                doc.refs.append(ref)

    cdef tuple get_key(self, Document doc):
        cdef:
            BaseIndex index
            list accum = []

        for index in self.key:
            accum.append(index.get_key(doc.handle))
        return tuple(accum)

    cdef set_value(self, Document doc, tuple parts):
        cdef:
            BaseIndex index
            int i

        for i, index in enumerate(self.value):
            ref = index.set_key(doc.handle, parts[i])
            if index.by_reference:
                doc.refs.append(ref)

    cdef tuple get_value(self, Document doc):
        cdef:
            BaseIndex index
            list accum = []

        for index in self.value:
            accum.append(index.get_key(doc.handle))
        return tuple(accum)

    @classmethod
    def key_value(cls):
        return Schema([StringIndex('key')], [StringIndex('value')])


cdef class Database(object):
    cdef:
        readonly bytes name
        readonly Sophia env
        Schema schema
        void *db

    def __cinit__(self):
        self.db = <void *>0

    def __init__(self, Sophia env, name, schema):
        self.env = env
        self.name = encode(name)
        self.schema = schema

    def __dealloc__(self):
        self.db = <void *>0

    cdef void *_get_target(self) except NULL:
        return self.db

    cdef _set(self, tuple key, tuple value):
        cdef:
            void *handle = sp_document(self.db)
            Document doc = create_document(handle)

        self.schema.set_key(doc, key)
        self.schema.set_value(doc, value)
        sp_set(self._get_target(), doc.handle)
        doc.release_refs()

    def set(self, key, value):
        check_open(self.env)
        key = (key,) if not isinstance(key, tuple) else key
        value = (value,) if not isinstance(value, tuple) else value
        return self._set(key, value)

    cdef tuple _get(self, tuple key):
        cdef:
            void *handle = sp_document(self.db)
            void *result
            Document doc = create_document(handle)

        self.schema.set_key(doc, key)
        result = sp_get(self._get_target(), doc.handle)
        doc.release_refs()
        if not result:
            return

        doc.handle = result
        data = self.schema.get_value(doc)
        sp_destroy(result)
        return data

    def get(self, key, default=None):
        check_open(self.env)
        data = self._get((key,) if not isinstance(key, tuple) else key)
        if data is None:
            return default

        return data if self.schema.multi_value else data[0]

    cdef bint _exists(self, tuple key):
        cdef:
            void *handle = sp_document(self.db)
            void *result
            Document doc = create_document(handle)

        self.schema.set_key(doc, key)
        result = sp_get(self._get_target(), doc.handle)
        doc.release_refs()
        if result:
            sp_destroy(result)
            return True
        return False

    cdef bint _delete(self, tuple key):
        cdef:
            bint ret
            void *handle = sp_document(self.db)
            Document doc = create_document(handle)
        self.schema.set_key(doc, key)
        sp_delete(self._get_target(), doc.handle)
        doc.release_refs()

    def delete(self, key):
        check_open(self.env)
        return self._delete((key,) if not isinstance(key, tuple) else key)

    def multi_delete(self, keys):
        check_open(self.env)
        for key in keys:
            self._delete((key,) if not isinstance(key, tuple) else key)

    def __getitem__(self, key):
        check_open(self.env)
        if isinstance(key, slice):
            return self.get_range(key.start, key.stop, key.step)
        else:
            key = (key,) if not isinstance(key, tuple) else key
            data = self._get(key)
            if data is None:
                raise KeyError(key)
            return data if self.schema.multi_value else data[0]

    def exists(self, key):
        check_open(self.env)
        return self._exists((key,) if not isinstance(key, tuple) else key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __contains__(self, key):
        return self.exists(key)

    cdef _update(self, dict _data, dict k):
        cdef tuple tkey, tvalue
        for source in (_data, k):
            if not source: continue
            for key in source:
                tkey = (key,) if not isinstance(key, tuple) else key
                value = source[key]
                tvalue = (value,) if not isinstance(value, tuple) else value
                self._set(tkey, tvalue)

    def update(self, dict _data=None, **kwargs):
        cdef Transaction txn
        check_open(self.env)
        with self.env.transaction() as txn:
            txn.get_database(self)._update(_data, kwargs)

    multi_set = update

    def multi_get(self, keys):
        cdef list accum = []
        for key in keys:
            try:
                accum.append(self[key])
            except KeyError:
                accum.append(None)
        return accum

    def multi_get_dict(self, keys):
        cdef dict accum = {}
        for key in keys:
            try:
                accum[key] = self[key]
            except KeyError:
                pass
        return accum

    def get_range(self, start=None, stop=None, reverse=False):
        cdef Cursor cursor
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
        return self.cursor(values=False)

    def values(self):
        return self.cursor(keys=False)

    def items(self):
        return self.cursor()

    def __iter__(self):
        return iter(self.cursor())

    def __len__(self):
        cdef:
            int i = 0
            Cursor curs = self.cursor(keys=False, values=False)
        for _ in curs: i += 1
        return i

    cpdef Cursor cursor(self, order='>=', key=None, prefix=None, keys=True,
                        values=True):
        check_open(self.env)
        return Cursor(db=self, order=order, key=key, prefix=prefix, keys=keys,
                      values=values)

    database_name = __dbconfig_ro__('name', is_string=True)
    database_id = __dbconfig_ro__('id')
    database_path = __dbconfig_ro__('path', is_string=True)

    mmap = __dbconfig__('mmap')
    direct_io = __dbconfig__('direct_io')
    sync = __dbconfig__('sync')
    expire = __dbconfig__('expire')
    compression = __dbconfig_s__('compression')  # lz4, zstd, none

    limit_key = __dbconfig_ro__('limit.key')
    limit_field = __dbconfig__('limit.field')

    index_memory_used = __dbconfig_ro__('index.memory_used')
    index_size = __dbconfig_ro__('index.size')
    index_size_uncompressed = __dbconfig_ro__('index.size_uncompressed')
    index_count = __dbconfig_ro__('index.count')
    index_count_dup = __dbconfig_ro__('index.count_dup')
    index_read_disk = __dbconfig_ro__('index.read_disk')
    index_read_cache = __dbconfig_ro__('index.read_cache')
    index_node_count = __dbconfig_ro__('index.node_count')
    index_page_count = __dbconfig_ro__('index.page_count')

    compaction_cache = __dbconfig__('compaction.cache')
    compaction_checkpoint = __dbconfig__('compaction.checkpoint')
    compaction_node_size = __dbconfig__('compaction.node_size')
    compaction_page_size = __dbconfig__('compaction.page_size')
    compaction_page_checksum = __dbconfig__('compaction.page_checksum')
    compaction_expire_period = __dbconfig__('compaction.expire_period')
    compaction_gc_wm = __dbconfig__('compaction.gc_wm')
    compaction_gc_period = __dbconfig__('compaction.gc_period')

    stat_documents_used = __dbconfig_ro__('stat.documents_used')
    stat_documents = __dbconfig_ro__('stat.documents')
    stat_field = __dbconfig_ro__('stat.field', is_string=True)
    stat_set = __dbconfig_ro__('stat.set')
    stat_set_latency = __dbconfig_ro__('stat.set_latency', is_string=True)
    stat_delete = __dbconfig_ro__('stat.delete')
    stat_delete_latency = __dbconfig_ro__('stat.delete_latency', True)
    stat_get = __dbconfig_ro__('stat.get')
    stat_get_latency = __dbconfig_ro__('stat.get_latency', is_string=True)
    stat_get_read_disk = __dbconfig_ro__('stat.get_read_disk', is_string=True)
    stat_get_read_cache = __dbconfig_ro__('stat.get_read_cache', True)
    stat_pread = __dbconfig_ro__('stat.pread')
    stat_pread_latency = __dbconfig_ro__('stat.pread_latency', is_string=True)
    stat_cursor = __dbconfig_ro__('stat.cursor')
    stat_cursor_latency = __dbconfig_ro__('stat.cursor_latency', True)
    stat_cursor_read_disk = __dbconfig_ro__('stat.cursor_read_disk', True)
    stat_cursor_read_cache = __dbconfig_ro__('stat.cursor_read_cache', True)
    stat_cursor_ops = __dbconfig_ro__('stat.cursor_ops', True)

    scheduler_checkpoint = __dbconfig_ro__('scheduler.checkpoint')
    scheduler_gc = __dbconfig_ro__('scheduler.gc')
    scheduler_expire = __dbconfig_ro__('scheduler.expire')
    scheduler_backup = __dbconfig_ro__('scheduler.backup')


cdef class DatabaseTransaction(Database):
    cdef:
        Transaction transaction

    def __init__(self, Database db, Transaction transaction):
        super(DatabaseTransaction, self).__init__(db.env, db.name, db.schema)
        self.transaction = transaction
        self.db = db.db

    cdef void *_get_target(self) except NULL:
        if not self.transaction.txn:
            raise SophiaError('Transaction is not active.')
        return self.transaction.txn


cdef class Cursor(object):
    cdef:
        Database db
        Document current_item
        readonly bint keys
        readonly bint values
        readonly bytes order
        readonly bytes prefix
        readonly key
        void *cursor

    def __cinit__(self, Database db, order='>=', key=None, prefix=None,
                  keys=True, values=True):
        self.db = db
        self.order = encode(order)
        if key:
            self.key = (key,) if not isinstance(key, tuple) else key
        self.prefix = encode(prefix) if prefix else None
        self.keys = keys
        self.values = values
        self.current_item = None
        self.cursor = <void *>0

    def __dealloc__(self):
        if not self.db.env.is_open:
            return

        if self.cursor:
            sp_destroy(self.cursor)

    def __iter__(self):
        check_open(self.db.env)
        if self.cursor:
            sp_destroy(self.cursor)
            self.cursor = <void *>0

        self.cursor = sp_cursor(self.db.env.env)
        cdef void *handle = sp_document(self.db.db)
        self.current_item = create_document(handle)
        if self.key:
            self.db.schema.set_key(self.current_item, self.key)
        sp_setstring(self.current_item.handle, 'order', <char *>self.order, 0)
        if self.prefix:
            sp_setstring(self.current_item.handle, 'prefix',
                         <char *>self.prefix,
                         (sizeof(char) * len(self.prefix)))
        return self

    def __next__(self):
        cdef void *handle = sp_get(self.cursor, self.current_item.handle)
        if not handle:
            sp_destroy(self.cursor)
            self.cursor = <void *>0
            raise StopIteration
        else:
            self.current_item.handle = handle

        cdef:
            Schema schema = self.db.schema
            tuple key, value

        if self.keys and self.values:
            key = schema.get_key(self.current_item)
            value = schema.get_value(self.current_item)
            return (key if schema.multi_key else key[0],
                    value if schema.multi_value else value[0])
        elif self.keys:
            key = schema.get_key(self.current_item)
            return key if schema.multi_key else key[0]
        elif self.values:
            value = schema.get_value(self.current_item)
            return value if schema.multi_value else value[0]
