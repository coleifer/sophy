import pytest
import pickle
import uuid
from itertools import product, permutations, chain

from sonya import (
    Schema, StringIndex, BytesIndex, U64Index, U32Index, U16Index, U8Index,
    U64RevIndex, U32RevIndex, U16RevIndex, U8RevIndex,
)


class PickleIndex(BytesIndex):
    def encode(self, value):
        return pickle.dumps(value)

    def decode(self, obj):
        return pickle.loads(obj)


KEY_TYPES = iter([
    [StringIndex('key')],
    [BytesIndex('key')],
    [PickleIndex('key')],
    [U8Index('key')],
    [U16Index('key')],
    [U32Index('key')],
    [U64Index('key')],
    [U8RevIndex('key')],
    [U16RevIndex('key')],
    [U32RevIndex('key')],
    [U64RevIndex('key')],
])

KEY_TYPES = chain(KEY_TYPES, permutations([
        StringIndex(uuid.uuid4().hex[:8]),
        BytesIndex(uuid.uuid4().hex[:8]),
        U8Index(uuid.uuid4().hex[:8]),
        U16Index(uuid.uuid4().hex[:8]),
        U32Index(uuid.uuid4().hex[:8]),
        U64Index(uuid.uuid4().hex[:8]),
    ], 2)
)


VALUE_TYPES = iter([
    [StringIndex('value')],
    [BytesIndex('value')],
    [PickleIndex('value')],
    [U8Index('value')],
    [U16Index('value')],
    [U32Index('value')],
    [U64Index('value')],
    [U8RevIndex('value')],
    [U16RevIndex('value')],
    [U32RevIndex('value')],
    [U64RevIndex('value')],
])

VALUE_TYPES = chain(VALUE_TYPES, permutations([
        StringIndex(uuid.uuid4().hex[:8]),
        BytesIndex(uuid.uuid4().hex[:8]),
        U8Index(uuid.uuid4().hex[:8]),
        U16Index(uuid.uuid4().hex[:8]),
        U32Index(uuid.uuid4().hex[:8]),
        U64Index(uuid.uuid4().hex[:8]),
    ], 2)
)


@pytest.mark.parametrize("key_t,value_t", product(KEY_TYPES, VALUE_TYPES))
def test_create_schema(key_t, value_t, sonya_env):
    db = sonya_env.add_database(uuid.uuid4().hex, Schema(key_t, value_t))

    if not sonya_env.open():
        raise Exception('Unable to open Sophia environment.')

    assert list(db.items()) == []

    key_sample = ()
    for idx in key_t:
        if isinstance(idx, U64Index):
            key_sample += (1,)
        elif isinstance(idx, PickleIndex):
            key_sample += (frozenset({1, 2, 3}),)
        elif isinstance(idx, BytesIndex):
            key_sample += (b'\0',)
        elif isinstance(idx, StringIndex):
            key_sample += ('Hello',)
        else:
            raise NotImplementedError

    if len(key_sample) == 1:
        key_sample = key_sample[0]

    value_sample = ()
    for idx in value_t:
        if isinstance(idx, U64Index):
            value_sample += (1,)
        elif isinstance(idx, PickleIndex):
            value_sample += (frozenset({3, 2, 1}),)
        elif isinstance(idx, BytesIndex):
            value_sample += (b'\0',)
        elif isinstance(idx, StringIndex):
            value_sample += ('Hello',)
        else:
            raise NotImplementedError

    if len(value_sample) == 1:
        value_sample = value_sample[0]

    db[key_sample] = value_sample

    print("key=%r\tvalue=%r" % (key_sample, value_sample))

    value = db[key_sample]

    assert value == value_sample, "%r != %r" % (value, value_sample)
