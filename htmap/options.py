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

from typing import Union, Iterable, Optional, Callable, Dict, List, Tuple
import logging

import sys
import shutil
import collections
import hashlib
from pathlib import Path

import htcondor

from . import utils, exceptions, names, settings

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
        'submit_event_notes',
        'stdout',
        'stderr',
        'when_to_transfer_output',
        'transfer_output_files',
        'transfer_output_remaps',
        'transfer_input_files',
        'should_transfer_files',
        'component',
        '+component',
        'MY.component',
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
        **kwargs: Union[str, Iterable[str]],
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
            # todo: needs test
            new_reqs = new.get('requirements', None)
            other_reqs = other.pop('requirements', None)
            if new_reqs is not None and other_reqs is not None:
                new['requirements'] = f'({new_reqs}) && ({other_reqs})'

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
    tag: str,
    map_dir: Path,
    num_components: int,
    map_options: MapOptions,
) -> Tuple[htcondor.Submit, List[Dict[str, str]]]:
    run_delivery_setup(
        tag,
        map_dir,
        settings['DELIVERY_METHOD'],
    )

    descriptors = get_base_descriptors(
        tag,
        map_dir,
        settings['DELIVERY_METHOD'],
    )

    descriptors['requirements'] = merge_requirements(
        descriptors.get('requirements', None),
        map_options.get('requirements', None),
    )

    itemdata = [{'component': str(idx)} for idx in range(num_components)]

    input_files = descriptors.get('transfer_input_files', [])
    input_files += [
        (map_dir / names.FUNC).as_posix(),
        (map_dir / names.INPUTS_DIR / f'$(component).{names.INPUT_EXT}').as_posix(),
    ]
    input_files.extend(normalize_path(f) for f in map_options.fixed_input_files)

    if map_options.input_files is not None:
        input_files.append('$(extra_input_files)')

        joined = [
            normalize_path(files) if isinstance(files, (str, Path))  # single file
            else ', '.join(normalize_path(f) for f in files)  # multiple files
            for files in map_options.input_files
        ]
        if len(joined) != num_components:
            raise exceptions.MisalignedInputData(f'length of input_files does not match length of input (len(input_files) = {len(input_files)}, len(inputs) = {num_components})')
        for d, f in zip(itemdata, joined):
            d['extra_input_files'] = f
    descriptors['transfer_input_files'] = ','.join(input_files)

    for opt_key, opt_value in map_options.items():
        if not isinstance(opt_value, str):  # implies it is iterable
            itemdata_key = f'itemdata_for_{opt_key}'
            opt_value = tuple(opt_value)
            if len(opt_value) != num_components:
                raise exceptions.MisalignedInputData(f'length of {opt_key} does not match length of input (len({opt_key}) = {len(opt_value)}, len(inputs) = {num_components})')
            for dct, v in zip(itemdata, opt_value):
                dct[itemdata_key] = v
            descriptors[opt_key] = f'$({itemdata_key})'
        else:
            descriptors[opt_key] = opt_value

    if descriptors['requirements'] is None:
        descriptors.pop('requirements')

    sub = htcondor.Submit(descriptors)

    return sub, itemdata


def register_delivery_mechanism(
    name: str,
    options_func: Callable[[str, Path], dict],
    setup_func: Optional[Callable[[str, Path], None]] = None,
) -> None:
    if setup_func is None:
        setup_func = lambda *args: None

    BASE_OPTIONS_FUNCTION_BY_DELIVERY[name] = options_func
    SETUP_FUNCTION_BY_DELIVERY[name] = setup_func


def unregister_delivery_mechanism(name: str) -> None:
    BASE_OPTIONS_FUNCTION_BY_DELIVERY.pop(name)
    SETUP_FUNCTION_BY_DELIVERY.pop(name)


def merge_requirements(*requirements: Optional[str]) -> Optional[str]:
    requirements = [req for req in requirements if req is not None]
    if len(requirements) == 0:
        return None
    return ' && '.join(f'({req})' for req in requirements)


def get_base_descriptors(
    tag: str,
    map_dir: Path,
    delivery: str,
) -> dict:
    map_dir = map_dir.absolute()
    core = {
        'JobBatchName': tag,
        'log': (map_dir / names.EVENT_LOG).as_posix(),
        'submit_event_notes': '$(component)',
        'stdout': (map_dir / names.JOB_LOGS_DIR / f'$(component).{names.STDOUT_EXT}').as_posix(),
        'stderr': (map_dir / names.JOB_LOGS_DIR / f'$(component).{names.STDERR_EXT}').as_posix(),
        'should_transfer_files': 'YES',
        'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
        'transfer_output_files': f'{names.TRANSFER_DIR}/, {names.USER_TRANSFER_DIR}/$(component)',
        'transfer_output_remaps': f'"$(component).{names.OUTPUT_EXT}={(map_dir / names.OUTPUTS_DIR / f"$(component).{names.OUTPUT_EXT}").as_posix()}"',
        'on_exit_hold': 'ExitCode =!= 0',
        'initialdir': f"{(map_dir / names.OUTPUT_FILES_DIR).as_posix()}",
        '+component': '$(component)',
        '+IsHTMapJob': 'True',
    }

    try:
        base = BASE_OPTIONS_FUNCTION_BY_DELIVERY[delivery](tag, map_dir)
    except KeyError:
        raise exceptions.UnknownPythonDeliveryMethod(f"'{delivery}' is not a known delivery mechanism")

    from_settings = settings.get('MAP_OPTIONS', default = {})

    merged = {
        **core,
        **base,
        **from_settings,
    }

    # manually fix-up requirements
    merged['requirements'] = merge_requirements(
        core.get('requirements', None),
        base.get('requirements', None),
        from_settings.get('requirements', None),
    )

    return merged


def _copy_run_scripts():
    run_script_source_dir = Path(__file__).parent / names.RUN_DIR
    run_scripts = [
        run_script_source_dir / 'run.py',
        run_script_source_dir / 'run_with_singularity.sh',
        run_script_source_dir / 'run_with_transplant.sh',
    ]
    target_dir = Path(settings['HTMAP_DIR']) / 'run'
    target_dir.mkdir(parents = True, exist_ok = True)
    for src in run_scripts:
        target = target_dir / src.name
        shutil.copy2(src, target)


def run_delivery_setup(
    tag: str,
    map_dir: Path,
    delivery: str,
) -> None:
    _copy_run_scripts()

    try:
        SETUP_FUNCTION_BY_DELIVERY[delivery](tag, map_dir)
    except KeyError:
        raise exceptions.UnknownPythonDeliveryMethod(f"'{delivery}' is not a known delivery mechanism")


def _get_base_descriptors_for_assume(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'vanilla',
        'executable': (Path(settings['HTMAP_DIR']) / names.RUN_DIR / 'run.py').as_posix(),
        'arguments': '$(component)',
    }


register_delivery_mechanism(
    'assume',
    options_func = _get_base_descriptors_for_assume,
)


def _get_base_descriptors_for_docker(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'docker',
        'docker_image': settings['DOCKER.IMAGE'],
        'executable': (Path(settings['HTMAP_DIR']) / names.RUN_DIR / 'run.py').as_posix(),
        'arguments': '$(component)',
        'transfer_executable': 'True',
    }


register_delivery_mechanism(
    'docker',
    options_func = _get_base_descriptors_for_docker,
)


def _get_base_descriptors_for_singularity(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'vanilla',
        'requirements': 'HasSingularity == true',
        'executable': (Path(settings['HTMAP_DIR']) / names.RUN_DIR / 'run_with_singularity.sh').as_posix(),
        'transfer_input_files': [
            (Path(settings['HTMAP_DIR']) / names.RUN_DIR / 'run.py').as_posix(),
        ],
        'arguments': f'{settings["SINGULARITY.IMAGE"]} $(component)',
        'transfer_executable': 'True',
    }


register_delivery_mechanism(
    'singularity',
    options_func = _get_base_descriptors_for_singularity,
)


def _get_base_descriptors_for_transplant(
    tag: str,
    map_dir: Path,
) -> dict:
    pip_freeze = _get_pip_freeze()
    h = _get_transplant_hash(pip_freeze)
    tif_path = settings.get('TRANSPLANT.ALTERNATE_INPUT_PATH')
    if tif_path is None:
        tif_path = (Path(settings['TRANSPLANT.DIR']) / h).as_posix()

    return {
        'universe': 'vanilla',
        'executable': (Path(settings['HTMAP_DIR']) / names.RUN_DIR / 'run_with_transplant.sh').as_posix(),
        'arguments': f'$(component) {h}',
        'transfer_input_files': [
            (Path(settings['HTMAP_DIR']) / names.RUN_DIR / 'run.py').as_posix(),
            tif_path,
        ],
    }


def _run_delivery_setup_for_transplant(
    tag: str,
    map_dir: Path,
):
    if not settings.get('TRANSPLANT.ASSUME_EXISTS', False):
        if 'usr' in sys.executable:
            raise exceptions.CannotTransplantPython('system Python installations cannot be transplanted')
        if sys.platform == 'win32':
            raise exceptions.CannotTransplantPython('transplant delivery does not work from Windows')

        py_dir = Path(sys.executable).parent.parent
        pip_freeze = _get_pip_freeze()

        target = Path(settings['TRANSPLANT.DIR']) / _get_transplant_hash(pip_freeze)
        zip_path = target.with_name(f'{target.stem}.tar.gz')

        if zip_path.exists():  # cached version already exists
            logger.debug(f'using cached zipped python install at {zip_path}')
            return

        logger.debug(f'creating zipped Python install for transplant from {py_dir} in {target.parent} ...')

        try:
            shutil.make_archive(
                base_name = target,
                format = 'gztar',
                root_dir = py_dir,
            )
        except BaseException as e:
            zip_path.unlink()
            logger.debug(f'removed partial zipped Python install at {target}')
            raise e

        zip_path.rename(target)

        pip_path = zip_path.with_name(f'{target.stem}.pip')
        pip_path.write_bytes(pip_freeze)

        logger.debug(f'created zipped Python install for transplant, stored at {zip_path}')


def _get_pip_freeze() -> bytes:
    return utils.pip_freeze().encode('utf-8')


def _get_transplant_hash(pip_freeze_output: bytes) -> str:
    h = hashlib.md5()
    h.update(pip_freeze_output)
    return h.hexdigest()


register_delivery_mechanism(
    'transplant',
    options_func = _get_base_descriptors_for_transplant,
    setup_func = _run_delivery_setup_for_transplant,
)
