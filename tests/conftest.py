import functools
import time
import itertools
import subprocess
from pathlib import Path

import pytest

import htmap
from htmap.options import get_default_options


def test_get_default_options(map_id, map_dir, test_id = None):
    opts = get_default_options(map_id, map_dir)
    opts['+htmap_test_id'] = str(test_id)

    return opts


ids = itertools.count()


@pytest.fixture(scope = 'function', autouse = True)
def set_htmap_dir_and_clean_afterwards(tmpdir_factory, mock):
    """Use a fresh HTMAP_DIR for every test."""
    path = Path(tmpdir_factory.mktemp('htmap_dir'))
    htmap.settings['HTMAP_DIR'] = path

    test_id = next(ids)
    mock.patch('htmap.options.get_default_options', functools.partial(test_get_default_options, test_id = test_id))

    yield

    subprocess.run(
        f'condor_rm --constraint htmap_test_id=={test_id}',
        stdout = subprocess.DEVNULL,
        stderr = subprocess.DEVNULL,
    )


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


def exception_msg(exc_info) -> str:
    return str(exc_info.value)
