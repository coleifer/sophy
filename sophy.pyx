from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.version cimport PY_MAJOR_VERSION
from libc.stdlib cimport free
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

from functools import wraps


cdef extern from "src/sophia.h":
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


cdef bint IS_PY3K = PY_MAJOR_VERSION == 3

cdef inline bytes encode(obj):
    cdef bytes result
    if isinstance(obj, unicode):
        result = PyUnicode_AsUTF8String(obj)
    elif PyBytes_Check(obj):
        result = <bytes>obj
    elif obj is None:
        return None
    elif IS_PY3K:
        result = bytes(str(obj), 'utf-8')
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

cdef inline check_open(Environment env):
    if not env.is_open:
        raise SophiaError('Sophia environment is closed.')

class SophiaError(Exception): pass

cdef class Schema(object)
cdef class Transaction(object)


cdef class Environment(object):
    cdef:
        bint is_open
        list databases
        readonly bytes path
        Transaction _transaction
        void *env

    def __cinit__(self):
        self.env = <void *>0

    def __init__(self, path):
        self.is_open = False
        self.databases = []
        self.path = encode(path)
        self._transaction = None

    def add_database(self, name, Schema schema):
        cdef:
            db = Database(self, name, schema)
        self.databases.append(db)
        return db

    cdef configure_database(self, Database db):
        cdef:
            BaseIndex index
            int i

        self.set_string(b'db', db.name)

        for i, index in enumerate(db.schema.key):
            self.set_string(b'db.%s.scheme' % (db.name), index.name)
            self.set_string(b'db.%s.scheme.%s' % (db.name, index.name),
                            b'%s,key(%d)' % (index.data_type, i))

        for index in db.schema.value:
            self.set_string(b'db.%s.scheme' % (db.name), index.name)
            self.set_string(b'db.%s.scheme.%s' % (db.name, index.name),
                            index.data_type)

        db.db = sp_getobject(self.env, b'db.%s' % db.name)

    def open(self):
        if self.is_open:
            return False

        cdef Database db

        self.env = sp_env()
        self.set_string(b'sophia.path', <const char *>self.path)

        for db in self.databases:
            self.configure_database(db)

        cdef int rc = sp_open(self.env)
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


cdef class Transaction(object):
    cdef:
        Environment env
        void *txn

    def __cinit__(self, Environment env):
        self.env = env
        self.txn = <void *>0

    def __dealloc__(self):
        if self.env.is_open and self.txn:
            sp_destroy(self.txn)

    cdef _reset(self, bint begin):
        self.txn = <void *>0
        self.env._transaction = None
        if begin:
            self.begin()

    cpdef begin(self):
        check_open(self.env)
        if self.txn:
            raise SophiaError('This transaction has already been started.')
        if self.env._transaction:
            raise SophiaError('Another transaction is currently open.')
        self.txn = sp_begin(self.env.env)
        self.env._transaction = self

    cpdef commit(self, begin=True):
        check_open(self.env)
        if not self.txn:
            raise SophiaError('Transaction is not currently open. Cannot '
                              'commit.')

        cdef int rc = sp_commit(self.txn)
        if rc == 1:
            raise SophiaError('transaction was rolled back by another '
                              'concurrent transaction.')
        elif rc == 2:
            raise SophiaError('transaction is not finished, waiting for a '
                              'concurrent transaction to finish.')
        self._reset(begin)

    cpdef rollback(self, begin=True):
        check_open(self.env)
        if not self.txn:
            raise SophiaError('Transaction is not currently open. Cannot '
                              'rollback.')
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

    data_type = b''

    def __init__(self, name):
        self.name = encode(name)

    cdef set_key(self, void *obj, value): pass
    cdef get_key(self, void *obj): pass


cdef class StringIndex(BaseIndex):
    data_type = SCHEMA_STRING

    cdef set_key(self, void *obj, value):
        cdef:
            char *buf
            Py_ssize_t buflen

        value = encode(value)
        PyBytes_AsStringAndSize(value, &buf, &buflen)
        sp_setstring(obj, <const char *>self.name, buf, buflen + 1)

    cdef get_key(self, void *obj):
        return _getstring(obj, <const char *>self.name)


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


cdef class Schema(object):
    cdef:
        bint multi_key, multi_value
        list key
        list value

    def __init__(self, key_parts=None, value_parts=None):
        cdef:
            BaseIndex index

        self.key = []
        self.value = []
        if key_parts is not None:
            for index in key_parts:
                self.add_key(index)
        if value_parts is not None:
            for index in value_parts:
                self.add_value(index)

    def add_key(self, BaseIndex index):
        self.key.append(index)
        self.multi_key = len(self.key) != 1

    def add_value(self, BaseIndex index):
        self.value.append(index)
        self.multi_value = len(self.key) != 1

    cdef set_key(self, void *obj, tuple parts):
        cdef:
            BaseIndex index
            int i

        for i, index in enumerate(self.key):
            index.set_key(obj, parts[i])

    cdef set_value(self, void *obj, tuple parts):
        cdef:
            BaseIndex index
            int i

        for i, index in enumerate(self.value):
            index.set_key(obj, parts[i])

    cdef get_value(self, void *obj):
        cdef:
            BaseIndex index
            list accum = []

        for index in self.value:
            accum.append(index.get_key(obj))
        return tuple(accum)


cdef class Database(object):
    cdef:
        readonly bytes name
        Environment env
        Schema schema
        void *db

    def __cinit__(self):
        self.db = <void *>0

    def __init__(self, Environment env, name, schema):
        self.env = env
        self.name = encode(name)
        self.schema = schema

    def __dealloc__(self):
        self.db = <void *>0

    cdef void *_get_target(self):
        cdef void *target
        if self.env._transaction:
            target = self.env._transaction.txn
        else:
            target = self.db
        return target

    cdef _set(self, tuple key, tuple value):
        cdef:
            void *doc = sp_document(self.db)

        self.schema.set_key(doc, key)
        self.schema.set_value(doc, value)
        sp_set(self._get_target(), doc)

    cdef tuple _get(self, tuple key):
        cdef:
            void *doc = sp_document(self.db)
            void *result

        self.schema.set_key(doc, key)
        result = sp_get(self._get_target(), doc)
        if not result:
            return

        data = self.schema.get_value(result)
        sp_destroy(result)
        return data

    cdef bint _exists(self, tuple key):
        cdef:
            void *doc = sp_document(self.db)
            void *result

        self.schema.set_key(doc, key)
        result = sp_get(self._get_target(), doc)
        if result:
            sp_destroy(result)
            return True
        else:
            return False

    cdef bint _delete(self, tuple key):
        cdef:
            void *doc = sp_document(self.db)

        self.schema.set_key(doc, key)
        return sp_delete(self._get_target(), doc) == 0

    def __getitem__(self, key):
        check_open(self.env)
        if isinstance(key, slice):
            pass
        else:
            key = (key,) if not isinstance(key, tuple) else key
            data = self._get(key)
            if data is None:
                raise KeyError(key)
            return data if self.schema.multi_value else data[0]

    def __setitem__(self, key, value):
        check_open(self.env)
        key = (key,) if not isinstance(key, tuple) else key
        value = (value,) if not isinstance(value, tuple) else value
        self._set(key, value)

    def __delitem__(self, key):
        check_open(self.env)
        self._delete((key,) if not isinstance(key, tuple) else key)

    def __contains__(self, key):
        check_open(self.env)
        return self._exists((key,) if not isinstance(key, tuple) else key)
