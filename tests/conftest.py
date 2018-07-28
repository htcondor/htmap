import sys
from . import mock_htcondor

sys.modules['htcondor'] = mock_htcondor

from pathlib import Path
import multiprocessing

import pytest

import htcmap


@pytest.fixture(scope = 'function', autouse = True)
def set_htcmap_dir(tmpdir_factory):
    """Use a fresh HTCMAP_DIR for every test."""
    path = Path(tmpdir_factory.mktemp('.htcmap'))
    htcmap.settings.HTCMAP_DIR = path


@pytest.fixture(scope = 'function')
def doubler():
    def doubler(x):
        return 2 * x

    return doubler


@pytest.fixture(scope = 'function')
def mapped_doubler(doubler, set_mapper):
    mapper = htcmap.htcmap(doubler)
    set_mapper(mapper)
    return mapper


@pytest.fixture(scope = 'function')
def power():
    def power(x = 0, p = 0):
        return x ** p

    return power


@pytest.fixture(scope = 'function')
def mapped_power(power, set_mapper):
    mapper = htcmap.htcmap(power)
    set_mapper(mapper)
    return mapper


@pytest.fixture(scope = 'function')
def set_mapper():
    def set(mapper):
        mock_htcondor._mapper = mapper

    yield set

    mock_htcondor._mapper = None


@pytest.fixture(scope = 'function', autouse = True)
def create_pool():
    with multiprocessing.Pool() as pool:
        mock_htcondor._pool = pool
        yield
