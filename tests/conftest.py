import sys
import uuid

import pytest

try:
    from tempfile import TemporaryDirectory
except ImportError:
    from backports.tempfile import TemporaryDirectory

from sophy import Sophia, Schema, BytesIndex, StringIndex


if sys.version_info < (3,):
    b_lit = lambda s: s
else:
    b_lit = lambda s: s.encode('latin-1') if not isinstance(s, bytes) else s


@pytest.fixture()
def sophy_env():
    with TemporaryDirectory() as env_path:
        env = Sophia(env_path)

        try:
            yield env
        finally:
            env.close()


@pytest.fixture()
def bytes_db(sophy_env):
    db = sophy_env.add_database(
        uuid.uuid4().hex,
        Schema([BytesIndex('key')], [BytesIndex('value')])
    )

    if not sophy_env.open():
        raise RuntimeError('Unable to open Sophia environment.')

    return db


@pytest.fixture()
def string_db(sophy_env):
    db = sophy_env.add_database(
        uuid.uuid4().hex,
        Schema([StringIndex('key')], [StringIndex('value')])
    )

    if not sophy_env.open():
        raise RuntimeError('Unable to open Sophia environment.')

    return db
