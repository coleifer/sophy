import pytest

from sonya import Schema, BytesIndex, StringIndex


databases = (
    ('string',
     Schema([StringIndex('key')],
            [StringIndex('value')])),
    ('bytes',
     Schema([BytesIndex('key')],
            [BytesIndex('value')])),
)


@pytest.fixture()
def env(sonya_env):
    for name, schema in databases:
        sonya_env.add_database(name, schema)

    if not sonya_env.open():
        raise RuntimeError('Unable to open Sophia environment.')

    try:
        yield sonya_env
    finally:
        sonya_env.close()


def test_string_encoding(env):
    sdb = env['string']
    bdb = env['bytes']

    sdb[u'k1'] = u'v1'
    assert sdb[u'k1'] == u'v1'

    smartquotes = u'\u2036hello\u2033'
    encoded = smartquotes.encode('utf-8')

    sdb[smartquotes] = smartquotes
    assert sdb[encoded] == smartquotes

    bdb[encoded] = encoded
    assert bdb[encoded] == encoded

    bdb[b'\xff'] = b'\xff'
    assert bdb[b'\xff'] == b'\xff'
