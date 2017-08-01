from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.version cimport PY_MAJOR_VERSION
from libc.stdint cimport int64_t
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


cdef class Schema(object)


cdef class Environment(object):
    cdef:
        bint is_open
        list databases
        readonly bytes path
        void *env

    def __cinit__(self):
        self.env = <void *>0

    def __init__(self, path):
        self.is_open = False
        self.databases = []
        self.path = encode(path)

    def add_database(self, name, Schema schema):
        cdef:
            db = Database(self, name, schema)
        self.databases.append(db)
        return db

    cdef configure_database(self, Database db):
        cdef:
            bytes key, data_type, value
            int i

        self.set_string(b'db', db.name)

        for i, (key, data_type) in enumerate(db.schema.key):
            self.set_string(b'db.%s.scheme' % (db.name), key)
            self.set_string(b'db.%s.scheme.%s' % (db.name, key),
                            b'%s,key(%d)' % (data_type, i))

        for (value, data_type) in db.schema.value:
            self.set_string(b'db.%s.scheme' % (db.name), value)
            self.set_string(b'db.%s.scheme.%s' % (db.name, value), data_type)

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


SCHEMA_STRING = 'string'
SCHEMA_U64 = 'u64'
SCHEMA_U32 = 'u32'
SCHEMA_U16 = 'u16'
SCHEMA_U8 = 'u8'
SCHEMA_U64_REV = 'u64_rev'
SCHEMA_U32_REV = 'u32_rev'
SCHEMA_U16_REV = 'u16_rev'
SCHEMA_U8_REV = 'u8_rev'


cdef class Schema(object):
    cdef:
        list key
        list value

    def __init__(self, key_parts=None, value_parts=None):
        self.key = []
        self.value = []
        if key_parts is not None:
            for key, data_type in key_parts:
                self.add_key(key, data_type)
        if value_parts is not None:
            for value, data_type in value_parts:
                self.add_value(value, data_type)

    def add_key(self, key, data_type):
        self.key.append((encode(key), encode(data_type)))

    def add_value(self, value, data_type):
        self.value.append((encode(value), encode(data_type)))


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
