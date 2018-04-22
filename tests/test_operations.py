import pytest
from sophy import SophiaError


def test_crud(string_db):
    db = string_db

    vals = (('huey', 'cat'), ('mickey', 'dog'), ('zaizee', 'cat'))
    for key, value in vals:
        db[key] = value

    for key, value in vals:
        assert db[key] == value
        assert key in db

    del db['mickey']

    assert 'mickey' not in db

    with pytest.raises(KeyError):
        db['mickey']

    db['huey'] = 'kitten'
    assert db['huey'] == 'kitten'

    db.delete('huey')
    assert db.multi_get('huey') == [None]

    db.set('k1', 'v1')
    db.set('k2', 'v2')
    assert db.get('k1') == 'v1'
    assert db.get('k2') == 'v2'
    assert db.get('k3') is None
    assert db.get('k3', 'xx') == 'xx'
    assert db.delete('k1')
    assert db.delete('k1')


def test_iterables(string_db):
    db = string_db

    for i in range(4):
        db['k%s' % i] = 'v%s' % i

    items = list(db)
    assert items == [
        ('k0', 'v0'), ('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')
    ]

    assert list(db.items()) == items
    assert list(db.keys()) == ['k0', 'k1', 'k2', 'k3']
    assert list(db.values()) == ['v0', 'v1', 'v2', 'v3']
    assert len(db) == 4
    assert db.index_count == 4


def test_multi_get_set(string_db):
    db = string_db
    for i in range(4):
        db['k%s' % i] = 'v%s' % i

    assert db.multi_get('k0', 'k3', 'k99') == ['v0', 'v3', None]

    db.update(k0='v0-e', k3='v3-e', k99='v99-e')
    assert list(db) == [
        ('k0', 'v0-e'), ('k1', 'v1'), ('k2', 'v2'),
        ('k3', 'v3-e'), ('k99', 'v99-e')
    ]


def test_get_range(string_db):
    db = string_db
    for i in range(4):
        db['k%s' % i] = 'v%s' % i

    assert list(db['k1':'k2']) == [('k1', 'v1'), ('k2', 'v2')]
    assert list(db['k01':'k21']) == [('k1', 'v1'), ('k2', 'v2')]
    assert list(db['k2':]) == [('k2', 'v2'), ('k3', 'v3')]
    assert list(db[:'k1']) == [('k0', 'v0'), ('k1', 'v1')]
    assert list(db['k2':'kx']) == [('k2', 'v2'), ('k3', 'v3')]
    assert list(db['a1':'k1']) == [('k0', 'v0'), ('k1', 'v1')]
    assert list(db[:'a1']) == []
    assert list(db['z1':]) == []
    assert list(db[:]) == [
        ('k0', 'v0'), ('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')
    ]

    assert list(db['k2':'k1']) == [('k2', 'v2'), ('k1', 'v1')]
    assert list(db['k21':'k01']) == [('k2', 'v2'), ('k1', 'v1')]
    assert list(db['k2'::True]) == [('k3', 'v3'), ('k2', 'v2')]
    assert list(db[:'k1':True]) == [('k1', 'v1'), ('k0', 'v0')]
    assert list(db['kx':'k2']) == [('k3', 'v3'), ('k2', 'v2')]
    assert list(db['k1':'a1']) == [('k1', 'v1'), ('k0', 'v0')]
    assert list(db[:'a1':True]) == []
    assert list(db['z1'::True]) == []
    assert list(db[::True]) == [
        ('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1'), ('k0', 'v0')
    ]

    assert list(db['k1':'k2':True]) == [('k2', 'v2'), ('k1', 'v1')]
    assert list(db['k2':'k1':True]) == [('k2', 'v2'), ('k1', 'v1')]


def test_open_close(sophy_env, string_db):
    db = string_db
    db['k1'] = 'v1'
    db['k2'] = 'v2'
    assert sophy_env.close()
    assert sophy_env.open()

    assert not sophy_env.open()

    assert db['k1'] == 'v1'
    assert db['k2'] == 'v2'
    db['k2'] = 'v2-e'

    assert sophy_env.close()
    assert sophy_env.open()
    assert db['k2'] == 'v2-e'


def test_transaction(sophy_env, string_db):
    db = string_db
    db['k1'] = 'v1'
    db['k2'] = 'v2'

    with sophy_env.transaction() as txn:
        txn_db = txn[db]
        assert txn_db['k1'] == 'v1'
        txn_db['k1'] = 'v1-e'
        del txn_db['k2']
        txn_db['k3'] = 'v3'

    assert db['k1'] == 'v1-e'

    with pytest.raises(KeyError):
        print("Never print this value:", db['k2'])

    assert db['k3'] == 'v3'


def test_rollback(sophy_env, string_db):
    db = string_db
    db['k1'] = 'v1'
    db['k2'] = 'v2'
    with sophy_env.transaction() as txn:
        txn_db = txn[db]
        assert txn_db['k1'] == 'v1'
        txn_db['k1'] = 'v1-e'
        del txn_db['k2']
        txn.rollback()
        txn_db['k3'] = 'v3'

    assert db['k1'] == 'v1'
    assert db['k2'] == 'v2'
    assert db['k3'] == 'v3'


def test_multiple_transaction(sophy_env, string_db):
    db = string_db
    db['k1'] = 'v1'
    txn = sophy_env.transaction()
    txn.begin()

    txn_db = txn[db]
    txn_db['k2'] = 'v2'
    txn_db['k3'] = 'v3'

    txn2 = sophy_env.transaction()
    txn2.begin()

    txn2_db = txn2[db]
    txn2_db['k1'] = 'v1-e'
    txn2_db['k4'] = 'v4'

    txn.commit()
    txn2.commit()

    assert list(db) == [
        ('k1', 'v1-e'), ('k2', 'v2'), ('k3', 'v3'), ('k4', 'v4')
    ]


def test_transaction_conflict(sophy_env, string_db):
    db = string_db
    db['k1'] = 'v1'
    txn = sophy_env.transaction()
    txn.begin()

    txn_db = txn[db]
    txn_db['k2'] = 'v2'
    txn_db['k3'] = 'v3'

    txn2 = sophy_env.transaction()
    txn2.begin()

    txn2_db = txn2[db]
    txn2_db['k2'] = 'v2-e'

    # txn is not finished, waiting for concurrent txn to finish.
    with pytest.raises(SophiaError):
        txn2.commit()

    txn.commit()

    # txn2 was rolled back by another concurrent txn.
    with pytest.raises(SophiaError):
        txn2.commit()

    # Only changes from txn are present.
    assert list(db) == [('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]


def test_cursor(sophy_env, string_db):
    db = string_db
    db.update(k1='v1', k2='v2', k3='v3')

    curs = db.cursor()
    assert list(curs) == [('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')]

    curs = db.cursor(order='<')
    assert list(curs) == [('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1')]