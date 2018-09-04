from typing import Union, Iterable, Optional

import collections
from pathlib import Path

import htcondor

from . import exceptions, settings


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
        'htmap',
        '+htmap',
    }

    def __init__(
        self,
        request_memory: Union[int, str, float, Iterable[Union[int, str, float]]] = '100MB',
        request_disk: Union[int, str, float, Iterable[Union[int, str, float]]] = '1GB',
        fixed_input_files: Optional[Union[Union[str, Path], Iterable[Union[str, Path]]]] = None,
        input_files: Optional[Union[Iterable[Union[str, Path]], Iterable[Iterable[Union[str, Path]]]]] = None,
        **kwargs,
    ):
        self._check_keyword_arguments(kwargs)

        super().__init__(**kwargs)

        if isinstance(request_memory, str):
            self['request_memory'] = request_memory
        elif isinstance(request_memory, (int, float)):
            self['request_memory'] = f'{request_memory}MB'
        else:  # implies it is iterable
            self['request_memory'] = [
                rm if isinstance(rm, str)
                else f'{int(rm)}MB'
                for rm in request_memory
            ]

        if isinstance(request_disk, str):
            self['request_disk'] = request_disk
        elif isinstance(request_disk, (int, float)):
            self['request_disk'] = f'{request_disk}GB'
        else:  # implies it is iterable
            self['request_disk'] = [
                rd if isinstance(rd, str)
                else f'{int(rd)}GB'
                for rd in request_disk
            ]

        if fixed_input_files is None:
            fixed_input_files = []
        if isinstance(fixed_input_files, str):
            fixed_input_files = [fixed_input_files]
        self.fixed_input_files = fixed_input_files

        print('__init__', input_files)
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

    @classmethod
    def merge(cls, *others: 'MapOptions'):
        new = cls()
        for other in reversed(others):
            new.data.update(other.data)
            new.fixed_input_files.extend(other.fixed_input_files)
            new.input_files = other.input_files

        return new


def create_submit_object_and_itemdata(map_id, map_dir, hashes, map_options):
    if map_options is None:
        map_options = MapOptions()

    options_dict = get_base_options(map_id, map_dir)

    itemdata = [{'hash': h} for h in hashes]

    input_files = [
        (map_dir / 'fn.pkl').as_posix(),
        (map_dir / 'inputs' / '$(hash).in').as_posix(),
    ]
    input_files.extend(Path(f).absolute().as_posix() for f in map_options.fixed_input_files)

    if map_options.input_files is not None:
        input_files.append('$(extra_input_files)')

        joined = [
            Path(files).absolute().as_posix() if isinstance(files, str)
            else ', '.join(Path(f).absolute().as_posix() for f in files)
            for files in map_options.input_files
        ]
        if len(hashes) != len(joined):
            raise exceptions.MisalignedInputData(f'length of input_files does not match length of input (len(input_files) = {len(input_files)}, len(inputs) = {len(hashes)})')
        for d, f in zip(itemdata, joined):
            d['extra_input_files'] = f

    options_dict['transfer_input_files'] = ', '.join(input_files)

    output_remaps = [
        f'$(hash).out={(map_dir / "outputs" / "$(hash).out").as_posix()}',
    ]

    options_dict['transfer_output_remaps'] = f'"{";".join(output_remaps)}"'

    for opt_key, opt_value in map_options.items():
        if not isinstance(opt_value, str):  # implies it is iterable
            itemdata_key = f'itemdata_for_{opt_key}'
            opt_value = tuple(opt_value)
            if len(opt_value) != len(hashes):
                raise exceptions.MisalignedInputData(f'length of {opt_key} does not match length of input (len({opt_key}) = {len(opt_value)}, len(inputs) = {len(hashes)})')
            for dct, v in zip(itemdata, opt_value):
                dct[itemdata_key] = v
            options_dict[opt_key] = f'$({itemdata_key})'
        else:
            options_dict[opt_key] = opt_value

    sub = htcondor.Submit(options_dict)

    return sub, itemdata


def get_base_options(map_id, map_dir):
    base = {
        'JobBatchName': map_id,
        'executable': (Path(__file__).parent / 'run' / 'run.py').as_posix(),
        'arguments': '$(hash)',
        'log': (map_dir / 'cluster_logs' / '$(ClusterId).log').as_posix(),
        'output': (map_dir / 'job_logs' / '$(hash).output').as_posix(),
        'error': (map_dir / 'job_logs' / '$(hash).error').as_posix(),
        'should_transfer_files': 'YES',
        'when_to_transfer_output': 'ON_EXIT',
        '+htmap': 'True',
    }

    return {**base, **settings.get('MAP_OPTIONS', default = {})}
