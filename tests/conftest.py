import sys
import time

from . import mock_htcondor

sys.modules['htcondor'] = mock_htcondor

from pathlib import Path
import multiprocessing

import pytest

import htmap


@pytest.fixture(scope = 'function', autouse = True)
def set_htmap_dir(tmpdir_factory):
    """Use a fresh HTMAP_DIR for every test."""
    path = Path(tmpdir_factory.mktemp('.htmap'))
    htmap.settings.HTMAP_DIR / settings.MAPS_DIR_NAME / settings.MAPS_DIR_NAME = path


@pytest.fixture(scope = 'function')
def doubler():
    def doubler(x):
        return 2 * x

    return doubler


@pytest.fixture(scope = 'function')
def mapped_doubler(doubler, set_mapper):
    mapper = htmap.htmap(doubler)
    set_mapper(mapper)
    return mapper


@pytest.fixture(scope = 'function')
def power():
    def power(x = 0, p = 0):
        return x ** p

    return power


@pytest.fixture(scope = 'function')
def mapped_power(power, set_mapper):
    mapper = htmap.htmap(power)
    set_mapper(mapper)
    return mapper


@pytest.fixture(scope = 'function')
def sleepy_double():
    def sleepy_double(x):
        time.sleep(x)
        return 2 * x

    return sleepy_double


@pytest.fixture(scope = 'function')
def mapped_sleepy_double(sleepy_double, set_mapper):
    mapper = htmap.htmap(sleepy_double)
    set_mapper(mapper)
    return mapper


@pytest.fixture(scope = 'function')
def set_mapper():
    def set(mapper):
        mock_htcondor._mapper = mapper

    yield set

    mock_htcondor._mapper = None


@pytest.fixture(scope = 'function')
def mock_pool():
    with multiprocessing.Pool(2) as pool:
        mock_htcondor._pool = pool
        yield
