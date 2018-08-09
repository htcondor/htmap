import functools
import sys
import time

from . import mock_htcondor

sys.modules['htcondor'] = mock_htcondor

from pathlib import Path
import multiprocessing

import pytest

import cloudpickle

import htmap


@pytest.fixture(scope = 'function', autouse = True)
def set_htmap_dir(tmpdir_factory):
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
        time.sleep(x)
        return 2 * x

    return sleepy_double


@pytest.fixture(scope = 'session')
def mapped_sleepy_double(sleepy_double):
    mapper = htmap.htmap(sleepy_double)
    return mapper


@pytest.fixture(scope = 'session')
def mock_pool():
    with multiprocessing.Pool() as pool:
        yield pool


def htc_run(map_dir, input_hash):
    inputs_dir = map_dir / 'inputs'
    outputs_dir = map_dir / 'outputs'
    with (map_dir / 'fn.pkl').open(mode = 'rb') as file:
        fn = cloudpickle.load(file)

    with (inputs_dir / f'{input_hash}.in').open(mode = 'rb') as file:
        args, kwargs = cloudpickle.load(file)

    output = fn(*args, **kwargs)

    with (outputs_dir / f'{input_hash}.out').open(mode = 'wb') as file:
        cloudpickle.dump(output, file)


def submit(map_id, map_dir, submit_object, input_hashes, pool = None):
    schedd = mock_htcondor.Schedd()
    with schedd.transaction() as txn:
        submit_result = mock_htcondor.SubmitResult()
        cluster_id = submit_result.cluster()

        with (map_dir / 'cluster_id').open(mode = 'w') as file:
            file.write(str(cluster_id))

        pool.starmap_async(
            htc_run,
            (
                (map_dir, input_hash)
                for input_hash in input_hashes
            ),
        )

        return htmap.MapResult(
            map_id = map_id,
            cluster_id = cluster_id,
            hashes = input_hashes,
        )


@pytest.fixture(scope = 'function')
def mock_submit(mock_pool, mocker):
    return mocker.patch.object(
        htmap.HTMapper,
        '_submit',
        functools.partial(submit, pool = mock_pool),
    )
