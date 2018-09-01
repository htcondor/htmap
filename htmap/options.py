import itertools
from typing import Union, List, Iterable, Tuple

import collections
from pathlib import Path

import htcondor

from . import exceptions, settings


class MapOptions(collections.UserDict):
    reserved_keys = {
        'transfer_input_files',
    }

    def __init__(
        self,
        request_memory: Union[int, str] = '100MB',
        request_disk = '1GB',
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
        reserved_keys_in_kwargs = set(kwargs.keys()).intersection(self.reserved_keys)
        if len(reserved_keys_in_kwargs) != 0:
            if len(reserved_keys_in_kwargs) == 1:
                s = 'is a reserved keyword'
            else:
                s = 'are reserved keywords'
            raise exceptions.ReservedOptionKeyword(f'{",".join(reserved_keys_in_kwargs)} {s} and cannot be used')


def create_submit_object(map_id, map_dir, input_files, output_remaps, map_options):
    if map_options is None:
        map_options = MapOptions()

    extra_input_files = [','.join(Path(f).absolute().as_posix() for f in files) for files in map_options.input_files]

    extra_itemdata = [
        {
            'extra_input_files': files,
        }
        for files in extra_input_files
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

    print(sub)
    print()
    print(extra_itemdata)

    return sub, extra_itemdata
