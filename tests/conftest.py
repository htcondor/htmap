import time

from pathlib import Path

import pytest

import htmap


@pytest.fixture(scope = 'function', autouse = True)
def set_htmap_dir_and_clean_afterwards(tmpdir_factory):
    """Use a fresh HTMAP_DIR for every test."""
    path = Path(tmpdir_factory.mktemp('htmap_dir'))
    htmap.settings['HTMAP_DIR'] = path


@pytest.fixture(scope = 'session')
def doubler():
    def doubler(x):
        return 2 * x

    return doubler


@pytest.fixture(scope = 'session')
def mapped_doubler(doubler):
    mapper = htmap.htmap(doubler)
    return mapper


@pytest.fixture(scope = 'session')
def power():
    def power(x = 0, p = 0):
        return x ** p

    return power


@pytest.fixture(scope = 'session')
def mapped_power(power):
    mapper = htmap.htmap(power)
    return mapper


@pytest.fixture(scope = 'session')
def sleepy_double():
    def sleepy_double(x):
        time.sleep(5)
        return 2 * x

    return sleepy_double


@pytest.fixture(scope = 'session')
def mapped_sleepy_double(sleepy_double):
    mapper = htmap.htmap(sleepy_double)
    return mapper
