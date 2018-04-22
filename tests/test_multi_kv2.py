import pytest

from sonya import Schema, U32Index, BytesIndex


databases = (
    ('main',
     Schema([U32Index('a'), U32Index('b'), U32Index('c')],
            [U32Index('value')])),
    ('secondary',
     Schema([BytesIndex('a'), U32Index('b')],
            [U32Index('value')])),
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


@pytest.fixture()
def db(env):
    return env['main']


@pytest.fixture()
def secondary(env):
    return env['secondary']


def test_cursor_ops(db):
    for i in range(10):
        for j in range(5):
            for k in range(3):
                db[i, j, k] = i * j * k

    data = db[(3, 3, 0):(4, 2, 1)]
    assert list(data) == [
        ((3, 3, 0), 0),
        ((3, 3, 1), 9),
        ((3, 3, 2), 18),
        ((3, 4, 0), 0),
        ((3, 4, 1), 12),
        ((3, 4, 2), 24),
        ((4, 0, 0), 0),
        ((4, 0, 1), 0),
        ((4, 0, 2), 0),
        ((4, 1, 0), 0),
        ((4, 1, 1), 4),
        ((4, 1, 2), 8),
        ((4, 2, 0), 0),
        ((4, 2, 1), 8),
    ]


def test_ordering_string(secondary):
    secondary['a', 0] = 1
    secondary['b', 1] = 2
    secondary['b', 0] = 3
    secondary['d', 0] = 4
    secondary['c', 9] = 5
    secondary['c', 3] = 6

    data = list(secondary[(b'b', 0):(b'\xff', 5)])
    assert data == [
        ((b'b', 0), 3),
        ((b'b', 1), 2),
        ((b'c', 3), 6),
        ((b'c', 9), 5),
        ((b'd', 0), 4)
    ]

    data = list(secondary[(b'\x00', 0):(b'b', 5)])
    assert data == [
        ((b'a', 0), 1),
        ((b'b', 0), 3),
        ((b'b', 1), 2)
    ]

    data = list(secondary[(b'bb', 0):(b'cc', 5)])
    assert data == [
        ((b'c', 3), 6),
        ((b'c', 9), 5)
    ]
