import sys
from . import mock_htcondor

sys.modules['htcondor'] = mock_htcondor

from pathlib import Path

import pytest

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


@pytest.fixture(scope = 'function')
def power():
    def power(x = 0, p = 0):
        return x ** p

    return power


@pytest.fixture(scope = 'function')
def mapped_power(power):
    return htcmap.htcmap(power)
