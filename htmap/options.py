# Copyright 2018 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Union, Iterable, Optional, Callable, Dict, List
import logging

import sys
import shutil
import collections
from pathlib import Path

import htcondor

from . import utils, exceptions, settings

logger = logging.getLogger(__name__)

BASE_OPTIONS_FUNCTION_BY_DELIVERY = {}
SETUP_FUNCTION_BY_DELIVERY = {}


class MapOptions(collections.UserDict):
    RESERVED_KEYS = {
        'jobbatchname',
        'universe',
        'arguments',
        'executable',
        'transfer_executable',
        'log',
        'stdout',
        'stderr',
        'transfer_output_files',
        'transfer_output_remaps',
        'transfer_input_files',
        'should_transfer_files',
        'when_to_transfer_output',
        'IsHTMapJob',
        '+IsHTMapJob',
        'MY.IsHTMapJob',
    }

    def __init__(
        self,
        *,
        fixed_input_files: Optional[Union[Union[str, Path], Iterable[Union[str, Path]]]] = None,
        input_files: Optional[Union[Iterable[Union[str, Path]], Iterable[Iterable[Union[str, Path]]]]] = None,
        custom_options: Dict[str, str] = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        fixed_input_files
            A single file, or an iterable of files, to send to all components of the map.
            Local files can be specified as string paths or as actual :class:`pathlib.Path` objects.
            You can also specify a file to fetch from an URL like ``http://www.full.url/path/to/filename``.
        input_files
            An iterable of single files or iterables of files to map over.
            Local files can be specified as string paths or as actual :class:`pathlib.Path` objects.
            You can also specify a file to fetch from an URL like ``http://www.full.url/path/to/filename``.
        custom_options
            A dictionary of submit descriptors that are *not* built-in HTCondor descriptors.
            These are the descriptors that, if you were writing a submit file, would have a leading ``+`` or ``MY.``.
            The leading characters are unnecessary here, but can be included if you'd like.
        kwargs
            Additional keyword arguments are interpreted as HTCondor submit descriptors.
            Values that are single strings are used for all components of the map.
            Providing an iterable for the value will map that option.
            Certain keywords are reserved for internal use (see the RESERVED_KEYS class attribute).
        """
        self._check_keyword_arguments(kwargs)

        if custom_options is None:
            custom_options = {}
        cleaned_custom_options = {
            key.lower().replace('+', '').replace('my.', ''): val
            for key, val in custom_options.items()
        }
        self._check_keyword_arguments(cleaned_custom_options)
        kwargs = {**kwargs, **{'+' + key: val for key, val in cleaned_custom_options.items()}}

        super().__init__(**kwargs)

        if fixed_input_files is None:
            fixed_input_files = []
        if isinstance(fixed_input_files, (str, Path)):
            fixed_input_files = [fixed_input_files]
        self.fixed_input_files = fixed_input_files

        self.input_files = input_files

    def _check_keyword_arguments(self, kwargs):
        normalized_keys = set(k.lower() for k in kwargs.keys())
        reserved_keys_in_kwargs = normalized_keys.intersection(self.RESERVED_KEYS)
        if len(reserved_keys_in_kwargs) != 0:
            if len(reserved_keys_in_kwargs) == 1:
                s = 'is a reserved keyword'
            else:
                s = 'are reserved keywords'
            raise exceptions.ReservedOptionKeyword(f'{",".join(reserved_keys_in_kwargs)} {s} and cannot be used')

    @classmethod
    def merge(cls, *others: 'MapOptions') -> 'MapOptions':
        """
        Merge any number of :class:`MapOptions` together, like a :class:`collections.ChainMap`.
        Options closer to the left take priority.

        .. note::

            ``fixed_input_files`` is a special case, and is merged up the chain instead of being overwritten.
        """
        new = cls()
        for other in reversed(others):
            new.data.update(other.data)
            new.fixed_input_files.extend(other.fixed_input_files)
            new.input_files = other.input_files

        return new


def normalize_path(path: Union[str, Path]) -> str:
    """
    Turn input file paths into a format that HTCondor can understand.
    In particular, all local file paths must be turned into posix-style paths (even on Windows!)
    """
    if isinstance(path, Path):
        return path.absolute().as_posix()

    if '://' in path:  # i.e., this is an url-like input file path
        return path

    return normalize_path(Path(path))  # local file path, but as a string


def create_submit_object_and_itemdata(
    map_id: str,
    map_dir: Path,
    hashes: List[int],
    map_options: Optional[MapOptions] = None,
):
    if map_options is None:
        map_options = MapOptions()

    run_delivery_setup(
        map_id,
        map_dir,
        settings['DELIVERY_METHOD'],
    )

    descriptors = get_base_descriptors(
        map_id,
        map_dir,
        settings['DELIVERY_METHOD'],
    )

    itemdata = [{'hash': h} for h in hashes]
    descriptors['transfer_output_files'] = '$(hash).out'

    input_files = descriptors.get('transfer_input_files', [])
    input_files += [
        (map_dir / 'func').as_posix(),
        (map_dir / 'inputs' / '$(hash).in').as_posix(),
    ]
    input_files.extend(normalize_path(f) for f in map_options.fixed_input_files)

    if map_options.input_files is not None:
        input_files.append('$(extra_input_files)')

        joined = [
            normalize_path(files) if isinstance(files, str)
            else ', '.join(normalize_path(f) for f in files)
            for files in map_options.input_files
        ]
        if len(hashes) != len(joined):
            raise exceptions.MisalignedInputData(f'length of input_files does not match length of input (len(input_files) = {len(input_files)}, len(inputs) = {len(hashes)})')
        for d, f in zip(itemdata, joined):
            d['extra_input_files'] = f
    descriptors['transfer_input_files'] = ','.join(input_files)

    output_remaps = [
        f'$(hash).out={(map_dir / "outputs" / "$(hash).out").as_posix()}',
    ]
    descriptors['transfer_output_remaps'] = f'"{";".join(output_remaps)}"'

    for opt_key, opt_value in map_options.items():
        if not isinstance(opt_value, str):  # implies it is iterable
            itemdata_key = f'itemdata_for_{opt_key}'
            opt_value = tuple(opt_value)
            if len(opt_value) != len(hashes):
                raise exceptions.MisalignedInputData(f'length of {opt_key} does not match length of input (len({opt_key}) = {len(opt_value)}, len(inputs) = {len(hashes)})')
            for dct, v in zip(itemdata, opt_value):
                dct[itemdata_key] = v
            descriptors[opt_key] = f'$({itemdata_key})'
        else:
            descriptors[opt_key] = opt_value

    sub = htcondor.Submit(descriptors)

    return sub, itemdata


def register_delivery_mechanism(
    name: str,
    options_func: Callable[[str, Path], dict],
    setup_func: Optional[Callable[[str, Path], None]] = None,
):
    if setup_func is None:
        setup_func = lambda *args: None

    BASE_OPTIONS_FUNCTION_BY_DELIVERY[name] = options_func
    SETUP_FUNCTION_BY_DELIVERY[name] = setup_func


def unregister_delivery_mechanism(name: str):
    BASE_OPTIONS_FUNCTION_BY_DELIVERY.pop(name)
    SETUP_FUNCTION_BY_DELIVERY.pop(name)


def get_base_descriptors(
    map_id: str,
    map_dir: Path,
    delivery: str,
) -> dict:
    core = {
        'JobBatchName': map_id,
        'arguments': '$(hash)',
        'log': (map_dir / 'cluster_logs' / '$(ClusterId).log').as_posix(),
        'stdout': (map_dir / 'job_logs' / '$(hash).stdout').as_posix(),
        'stderr': (map_dir / 'job_logs' / '$(hash).stderr').as_posix(),
        'should_transfer_files': 'YES',
        'when_to_transfer_output': 'ON_EXIT',
        '+IsHTMapJob': 'True',
    }

    try:
        base = BASE_OPTIONS_FUNCTION_BY_DELIVERY[delivery](map_id, map_dir)
    except KeyError:
        raise exceptions.UnknownPythonDeliveryMethod(f"'{delivery}' is not a known delivery mechanism")

    return {
        **core,
        **base,
        **settings.get('MAP_OPTIONS', default = {})
    }


def run_delivery_setup(
    map_id: str,
    map_dir: Path,
    delivery: str,
) -> None:
    try:
        SETUP_FUNCTION_BY_DELIVERY[delivery](map_id, map_dir)
    except KeyError:
        raise exceptions.UnknownPythonDeliveryMethod(f"'{delivery}' is not a known delivery mechanism")


def _get_base_descriptors_for_assume(
    map_id: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'vanilla',
        'executable': (Path(__file__).parent / 'run' / 'run.py').as_posix(),
    }


register_delivery_mechanism(
    'assume',
    options_func = _get_base_descriptors_for_assume,
)


def _get_base_descriptors_for_docker(
    map_id: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'docker',
        'docker_image': settings['DOCKER.IMAGE'],
        'executable': (Path(__file__).parent / 'run' / 'run.py').as_posix(),
        'transfer_executable': 'True',
    }


register_delivery_mechanism(
    'docker',
    options_func = _get_base_descriptors_for_docker,
)


def _get_base_descriptors_for_transplant(
    map_id: str,
    map_dir: Path,
) -> dict:
    tif_path = settings['TRANSPLANT.ALTERNATE_INPUT_PATH']
    if tif_path is None:
        tif_path = (Path(settings['TRANSPLANT.PATH']) / 'htmap_python.tar.gz').as_posix()

    return {
        'universe': 'vanilla',
        'executable': (Path(__file__).parent / 'run' / 'run_with_transplant.sh').as_posix(),
        'transfer_input_files': [
            (Path(__file__).parent / 'run' / 'run.py').as_posix(),
            tif_path,
        ],
    }


def _run_delivery_setup_for_transplant(
    map_id: str,
    map_dir: Path,
):
    if not _cached_py_is_current() or settings['TRANSPLANT.ASSUME_EXISTS']:
        transplant_path = Path(settings['TRANSPLANT.PATH'])
        py_dir = Path(sys.executable).parent.parent
        target = transplant_path / 'htmap_python'

        logger.debug(f'creating zipped Python install for transplant from {py_dir} in {target.parent}...')

        try:
            shutil.make_archive(
                base_name = target,
                format = 'gztar',
                root_dir = py_dir,
            )
        except BaseException as e:
            target.with_name('htmap_python.tar.gz').unlink()
            raise e

        logger.debug('created zipped Python install for transplant')

        cached_req_path = transplant_path / 'freeze'
        cached_req_path.write_text(utils.pip_freeze(), encoding = 'utf-8')

        logger.debug(f'saved transplant cache file to {cached_req_path}')


def _cached_py_is_current() -> bool:
    logger.debug('checking if cached zipped Python install is current...')
    transplant_path = Path(settings['TRANSPLANT.PATH'])
    cached_req_path = transplant_path / 'freeze'
    py_install_path = transplant_path / 'htmap_python.tar.gz'
    if not cached_req_path.exists() or not py_install_path.exists():
        logger.debug('did not find cached zipped Python install')
        return False

    cached_reqs = cached_req_path.read_text(encoding = 'utf-8')
    current_reqs = utils.pip_freeze()

    reqs_match = (current_reqs == cached_reqs)

    logger.debug(f'cached zipped Python install {"is" if reqs_match else "is not"} current')

    return reqs_match


register_delivery_mechanism(
    'transplant',
    options_func = _get_base_descriptors_for_transplant,
    setup_func = _run_delivery_setup_for_transplant,
)
