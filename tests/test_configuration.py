from .conftest import b_lit


def test_version(bytes_db):
    assert bytes_db.env.version == b_lit('2.2')


def test_status(bytes_db):
    assert bytes_db.env.status == b_lit('online')
