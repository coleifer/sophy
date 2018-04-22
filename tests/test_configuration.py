def test_version(bytes_db):
    assert bytes_db.env.version == '2.2'


def test_status(bytes_db):
    assert bytes_db.env.status == 'online'
