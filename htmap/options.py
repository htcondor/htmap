import itertools
from typing import Union, Iterable

import collections
from pathlib import Path

import htcondor

from . import exceptions


class MapOptions(collections.UserDict):
    reserved_keys = {
        'jobbatchname',
        'arguments',
        'executable',
        'log',
        'output',
        'error',
        'transfer_output_remaps',
        'transfer_input_files',
        'should_transfer_files',
        'when_to_transfer_output',
    }

    def __init__(
        self,
        request_memory: Union[int, str] = '100MB',
        request_disk: Union[int, str] = '1GB',
        fixed_input_files: Iterable[str] = None,
        input_files: Iterable[Iterable[str]] = None,
        **kwargs,
    ):
        self._check_keyword_arguments(kwargs)

        super().__init__(**kwargs)

        if not isinstance(request_memory, str):
            request_memory = f'{request_memory}MB'
        self['request_memory'] = request_memory

        if not isinstance(request_disk, str):
            request_disk = f'{request_disk}GB'
        self['request_disk'] = request_disk

        if fixed_input_files is None:
            fixed_input_files = []
        self.fixed_input_files = fixed_input_files

        if input_files is None:
            input_files = []
        self.input_files = input_files

    def _check_keyword_arguments(self, kwargs):
        normalized_keys = set(k.lower() for k in kwargs.keys())
        reserved_keys_in_kwargs = normalized_keys.intersection(self.reserved_keys)
        if len(reserved_keys_in_kwargs) != 0:
            if len(reserved_keys_in_kwargs) == 1:
                s = 'is a reserved keyword'
            else:
                s = 'are reserved keywords'
            raise exceptions.ReservedOptionKeyword(f'{",".join(reserved_keys_in_kwargs)} {s} and cannot be used')


def create_submit_object_and_itemdata(map_id, map_dir, hashes, map_options):
    if map_options is None:
        map_options = MapOptions()

    extra_input_files = [
        ','.join(Path(f).absolute().as_posix() for f in files)
        for files in map_options.input_files
    ]

    itemdata = [
        {
            'hash': h,
            'extra_input_files': files,
        }
        for h, files
        in zip_first(  # todo: this is really "zip first", not "zip longest"
            hashes,
            extra_input_files,
            fill_value = '',
        )
    ]

    input_files = [
        (map_dir / 'fn.pkl').as_posix(),
        (map_dir / 'inputs' / '$(hash).in').as_posix(),
    ]

    output_remaps = [
        f'$(hash).out={(map_dir / "outputs" / "$(hash).out").as_posix()}',
    ]

    base_options = {
        'JobBatchName': map_id,
        'executable': (Path(__file__).parent / 'run' / 'run.py').as_posix(),
        'arguments': '$(hash)',
        'log': (map_dir / 'cluster_logs' / '$(ClusterId).log').as_posix(),
        'output': (map_dir / 'job_logs' / '$(hash).output').as_posix(),
        'error': (map_dir / 'job_logs' / '$(hash).error').as_posix(),
        'should_transfer_files': 'YES',
        'when_to_transfer_output': 'ON_EXIT',
        'request_cpus': '1',
        'request_memory': '100MB',
        'request_disk': '5GB',
        'transfer_input_files': ','.join(input_files + map_options.fixed_input_files + ['$(extra_input_files)']),
        'transfer_output_remaps': f'"{";".join(output_remaps)}"',
    }
    sub = htcondor.Submit(dict(collections.ChainMap(map_options, base_options)))

    return sub, itemdata


def zip_first(*args, fill_value = None):
    # zip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
    iterators = [iter(it) for it in args]
    num_active = len(iterators)
    if not num_active:
        return
    while True:
        values = []
        for i, it in enumerate(iterators):
            try:
                value = next(it)
            except StopIteration:
                if i == 0:
                    return
                iterators[i] = itertools.repeat(fill_value)
                value = fill_value
            values.append(value)
        yield tuple(values)
