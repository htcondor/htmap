import pytest

from pathlib import Path

import htcmap


@pytest.fixture(scope = 'function', autouse = True)
def set_htcmap_dir(tmpdir_factory):
    path = Path(tmpdir_factory.mktemp('.htcmap'))
    htcmap.settings.HTCMAP_DIR = path


@pytest.fixture(scope = 'function')
def doubler():
    def doubler(x):
        return 2 * x

    return doubler


@pytest.fixture(scope = 'function')
def mapped_doubler(doubler):
    return htcmap.htcmap(doubler)
