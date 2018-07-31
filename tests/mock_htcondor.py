from typing import Iterable

import enum
import itertools
from pathlib import Path

import cloudpickle

integer_generator = itertools.count(0)

# this variable is used to communicate the current mapper from the test environment
_mapper = None

# this variable is used by pytest to managed the mock_pool of workers that simulate the cluster
_pool = None


def htc_run(job_dir, inputs_dir, outputs_dir, arg_hash):
    job_dir = Path(job_dir)
    inputs_dir = Path(inputs_dir)
    outputs_dir = Path(outputs_dir)
    with (job_dir / 'fn.pkl').open(mode = 'rb') as file:
        fn = cloudpickle.load(file)

    with (inputs_dir / f'{arg_hash}.in').open(mode = 'rb') as file:
        args, kwargs = cloudpickle.load(file)

    output = fn(*args, **kwargs)

    with (outputs_dir / f'{arg_hash}.out').open(mode = 'wb') as file:
        cloudpickle.dump(output, file)


class Submit:
    def __init__(self, submit_dictionary):
        self.submit_dictionary = submit_dictionary

    def queue_with_itemdata(self, transaction: 'Transaction', num: int, items: Iterable):
        try:
            _pool.starmap_async(
                htc_run,
                (
                    (_mapper.map_dir, _mapper.inputs_dir, _mapper.outputs_dir, item)
                    for item in items
                ),
            )
        except AttributeError as e:
            raise Exception('Did you forget to use set_mapper or mock_pool?')

        return SubmitResult(next(integer_generator))


class SubmitResult:
    def __init__(self, clusterid):
        self.clusterid = clusterid

    def cluster(self):
        return self.clusterid


class Schedd:
    def transaction(self):
        return Transaction()


class Transaction:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class JobAction(enum.Enum):
    Remove = 'remove'
