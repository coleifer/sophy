import uuid

import pytest

from sophy import Schema, StringIndex, SophiaError
from .conftest import b_lit


@pytest.fixture()
def main(sophy_env):
    sophy_env.close()

    db = sophy_env.add_database(
        uuid.uuid4().hex,
        Schema([StringIndex('key')], [StringIndex('value')])
    )

    if not sophy_env.open():
        raise RuntimeError('Unable to open Sophia environment.')

    return db


@pytest.fixture()
def scnd(sophy_env):
    sophy_env.close()

    db = sophy_env.add_database(
        uuid.uuid4().hex,
        Schema([StringIndex('key')], [StringIndex('value')])
    )

    if not sophy_env.open():
        raise RuntimeError('Unable to open Sophia environment.')

    return db


def test_multiple_databases(main, scnd):
    main.update(k1='v1', k2='v2', k3='v3')
    scnd.update(k1='v1_2', k2='v2_2', k3='v3_2')

    del main['k1']
    del scnd['k2']

    with pytest.raises(KeyError):
        print(main['k1'])

    with pytest.raises(KeyError):
        print(scnd['k2'])

    assert list(main) == [('k2', 'v2'), ('k3', 'v3')]
    assert list(scnd) == [('k1', 'v1_2'), ('k3', 'v3_2')]


def test_multiple_db_txn(main, scnd, sophy_env):
    main.update(k1='v1', k2='v2')
    scnd.update(k1='v1_2', k2='v2_2')

    with sophy_env.transaction() as txn:
        t_main = txn[main]
        t_scnd = txn[scnd]

        del t_main['k1']
        t_main['k2'] = 'v2-e'
        t_main['k3'] = 'v3'
        del t_scnd['k2']
        t_scnd['k1'] = 'v1_2-e'

    assert list(main) == [('k2', 'v2-e'), ('k3', 'v3')]
    assert list(scnd) == [('k1', 'v1_2-e')]

    with sophy_env.transaction() as txn:
        t_main = txn[main]
        t_scnd = txn[scnd]
        del t_main['k2']

        t_scnd['k3'] = 'v3_2'
        txn.rollback()
        assert t_main['k2'] == 'v2-e'

        with pytest.raises(KeyError):
            print(t_scnd['k3'])

        t_main['k3'] = 'v3-e'
        t_scnd['k2'] = 'v2_2-e'

    assert list(main) == [('k2', 'v2-e'), ('k3', 'v3-e')]
    assert list(scnd) == [('k1', 'v1_2-e'), ('k2', 'v2_2-e')]


def test_open_close(sophy_env, main):
    assert main.env == sophy_env
    assert sophy_env.close()
    assert sophy_env.open()


def test_add_db(sophy_env, main, scnd):
    assert main.env.status == b_lit('online')
    assert scnd.env.status == b_lit('online')

    schema = Schema([StringIndex('key')], [StringIndex('value')])
    third_name = uuid.uuid4().hex

    with pytest.raises(SophiaError):
        sophy_env.add_database(third_name, schema)

    sophy_env.close()

    sophy_env.add_database(third_name, schema)
    sophy_env.open()

    db = sophy_env[third_name]
    db['k1'] = 'v1'
    assert db['k1'] == 'v1'
