import pytest

from sophy import (
    Schema, U64Index, U32Index, U16Index, StringIndex, U16RevIndex, U8Index
)


databases = (
    ('main',
     Schema([U64Index('year'), U32Index('month'), U16Index('day'),
             StringIndex('event')],
            [StringIndex('source'), StringIndex('data')])),
    ('numbers',
     Schema([U16RevIndex('key')],
            [U16Index('v1'), U16Index('v2'), U16Index('v3'),
             U16Index('v4'), U8Index('v5')])),
)
test_data = (
    ((2017, 1, 1, 'holiday'), ('us', 'new years')),
    ((2017, 5, 29, 'holiday'), ('us', 'memorial day')),
    ((2017, 7, 4, 'holiday'), ('us', 'independence day')),
    ((2017, 9, 4, 'holiday'), ('us', 'labor day')),
    ((2017, 11, 23, 'holiday'), ('us', 'thanksgiving')),
    ((2017, 12, 25, 'holiday'), ('us', 'christmas')),
    ((2017, 7, 1, 'birthday'), ('private', 'huey')),
    ((2017, 5, 1, 'birthday'), ('private', 'mickey')),
)


@pytest.fixture()
def env(sophy_env):
    for name, schema in databases:
        sophy_env.add_database(name, schema)

    if not sophy_env.open():
        raise RuntimeError('Unable to open Sophia environment.')

    try:
        yield sophy_env
    finally:
        sophy_env.close()


@pytest.fixture()
def db(env):
    return env['main']


@pytest.fixture()
def nums(env):
    return env['numbers']


def test_multi_key_crud(db):
    for key, value in test_data:
        db[key] = value

    for key, value in test_data:
        assert db[key] == value

    del db[2017, 11, 12, 'holiday']
    with pytest.raises(KeyError):
        print(db[2017, 11, 12, 'holiday'])


def test_iteration(db):
    for key, value in test_data:
        db[key] = value

    assert list(db) == sorted(test_data)
    assert list(db.items()) == sorted(test_data)
    assert list(db.keys()) == sorted(key for key, _ in test_data)
    assert list(db.values()) == [value for _, value in sorted(test_data)]


def test_update_multiget(db):
    db.update(dict(test_data))
    events = ((2017, 1, 1, 'holiday'),
              (2017, 12, 25, 'holiday'),
              (2017, 7, 1, 'birthday'))

    assert db.multi_get(*events) == [
        ('us', 'new years'),
        ('us', 'christmas'),
        ('private', 'huey')
    ]


def test_ranges(db):
    db.update(dict(test_data))
    items = db[(2017, 2, 1, ''):(2017, 6, 1, '')]
    assert list(items) == [
        ((2017, 5, 1, 'birthday'), ('private', 'mickey')),
        ((2017, 5, 29, 'holiday'), ('us', 'memorial day'))]

    items = db[:(2017, 2, 1, '')]
    assert list(items) == [((2017, 1, 1, 'holiday'), ('us', 'new years'))]

    items = db[(2017, 11, 1, '')::True]
    assert list(items) == [
        ((2017, 12, 25, 'holiday'), ('us', 'christmas')),
        ((2017, 11, 23, 'holiday'), ('us', 'thanksgiving'))
    ]


def test_rev_indexes(nums):
    for i in range(100):
        key, v1, v2, v3, v4, v5 = range(i, 6 + i)
        nums[key] = (v1, v2, v3, v4, v5)

    assert len(nums) == 100
    assert nums[0] == (1, 2, 3, 4, 5)
    assert nums[99] == (100, 101, 102, 103, 104)

    assert list(nums[:2]) == []
    assert list(nums[2:]) == [
        (2, (3, 4, 5, 6, 7)),
        (1, (2, 3, 4, 5, 6)),
        (0, (1, 2, 3, 4, 5))
    ]

    assert list(nums.keys())[:3] == [99, 98, 97]
    assert list(nums.values())[:3] == [
        (100, 101, 102, 103, 104),
        (99, 100, 101, 102, 103),
        (98, 99, 100, 101, 102)
    ]


def test_bounds(nums):
    nums[0] = (0, 0, 0, 0, 0)
    assert nums[0] == (0, 0, 0, 0, 0)

    nums[1] = (0, 0, 0, 0, 255)
    assert nums[1] == (0, 0, 0, 0, 255)
