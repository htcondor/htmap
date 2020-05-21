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

from . import utils, transfer, exceptions, names, settings
from .types import TRANSFER_PATH, REMAPS

logger = logging.getLogger(__name__)

BASE_OPTIONS_FUNCTION_BY_DELIVERY = {}
SETUP_FUNCTION_BY_DELIVERY = {}

REQUIREMENTS = 'requirements'


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
        fixed_input_files: Optional[Union[TRANSFER_PATH, Iterable[TRANSFER_PATH]]] = None,
        input_files: Optional[Union[Iterable[TRANSFER_PATH], Iterable[Iterable[TRANSFER_PATH]]]] = None,
        output_remaps: Optional[Union[REMAPS, Iterable[REMAPS]]] = None,
        custom_options: Optional[Dict[str, str]] = None,
        **kwargs: Union[str, Iterable[str]],
    ):
        """
        Parameters
        ----------
        fixed_input_files
            A single file, or an iterable of files, to send to all components of the map.
        input_files
            An iterable of single files or iterables of files to map over.
            This may be useful if you want additional files to be sent to each
            map component, but don't want them in your mapped function's
            arguments.
        output_remaps
            A dictionary, or an iterable of dictionaries, specifying output
            transfer remaps. A remapped output file is sent to a specified
            destination instead of back to the submit machine. If a single
            dictionary is passed, it will be applied to every map component
            (in this case, you may want to use the ``$(component)`` submit
            macro to differentiate them).
            Each dictionary should be a "mapping"
            between the **names** (last path component, as a string) of o
            utput files and their **destinations**, given as a :class:`TransferPath`.
            You must still call :func:`transfer_output_files` on the files for
            the them to be transferred at all;
            listing them here *only* sets up the remapping.
        custom_options
            A dictionary of submit descriptors that are *not* built-in HTCondor descriptors.
            These are the descriptors that, if you were writing a submit file, would have a leading ``+`` or ``MY.``.
            The leading characters are unnecessary here, but can be included if you'd like.
        kwargs
            Additional keyword arguments are interpreted as HTCondor submit descriptors.
            Values that are single strings are used for all components of the map.
            Providing an iterable for the value will map that option.
            Certain keywords are reserved for internal use (see the RESERVED_KEYS class attribute).

        Notes
        -----
        .. warning::
           The representation of the values in ``fixed_input_files``,
           ``input_files``, ``custom_options`` and ``kwargs`` should
           exactly match the characters in the submit file after the ``=``.

           For example, let's
           say your job requires this submit file:

           .. code::

              # file: job.submit
              foo = "bar"
              aaa = xyz
              bbb = false
              ccc = 1

           The ``MapOptions`` that express the same submit options would be:

           .. code:: python

               >>> options = {"foo": '"bar"', "aaa": "xyz", "bbb": "false", "ccc": "1"}
               >>> print(options["foo"])  # exactly matches the value in the submit file
               ... "bar"
               >>> options["foo"] = "\\"bar\\""  # alternative value
               >>> MapOptions(**options)

           Submit file values with quotes require escaped quotes in the
           Python string.

        """
        self._check_keyword_arguments(kwargs)

        if custom_options is None:
            custom_options = {}
        cleaned_custom_options = {
            key.lower().replace('+', '').replace('my.', ''): val
            for key, val in custom_options.items()
        }
        self._check_keyword_arguments(cleaned_custom_options)
        kwargs = {**kwargs, **{f'MY.{key}': val for key, val in cleaned_custom_options.items()}}

        super().__init__(**kwargs)

        if fixed_input_files is None:
            fixed_input_files = []
        if isinstance(fixed_input_files, (str, Path)):
            fixed_input_files = [fixed_input_files]
        self.fixed_input_files = fixed_input_files

        self.input_files = input_files
        self.output_remaps = output_remaps

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
            ``requirements`` are also combined, in a way where all requirements must be satisfied.
        """
        new = cls()

        for other in reversed(others):
            new.data.update(other.data)

            # these need special handling, because they are stored as attributes
            # instead of in the dictionary
            new.fixed_input_files.extend(other.fixed_input_files)
            new.input_files = other.input_files

        merged_requirements = merge_requirements(
            *(other.get(REQUIREMENTS, None) for other in others)
        )
        if merged_requirements is not None:
            new[REQUIREMENTS] = merged_requirements

        return new


def normalize_path(path: Union[str, Path]) -> str:
    """
    Turn input file paths into a format that HTCondor can understand.
    In particular, all local file paths must be turned into posix-style paths (even on Windows!)
    """
    if isinstance(path, transfer.TransferPath):
        return path.as_url()
    elif isinstance(path, Path) or '://' not in path:
        return Path(path).absolute().as_posix()
    return path


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

    descriptors[REQUIREMENTS] = merge_requirements(
        descriptors.get(REQUIREMENTS, None),
        map_options.get(REQUIREMENTS, None),
    )

    itemdata = [{'component': str(idx)} for idx in range(num_components)]

    input_files = descriptors.get('transfer_input_files', [])
    input_files += [
        (map_dir / names.FUNC).as_posix(),
        (map_dir / names.INPUTS_DIR / f'$(component).{names.INPUT_EXT}').as_posix(),
    ]
    input_files.extend(normalize_path(f) for f in map_options.fixed_input_files)

    # if any of the components have per-component input files, use a submit macro to insert them
    if map_options.input_files is not None and any(map_options.input_files):
        input_files.append('$(extra_input_files)')

        joined = [
            normalize_path(files) if isinstance(files, (str, Path, transfer.TransferPath))  # single file
            else ', '.join(normalize_path(f) for f in files)  # multiple files
            for files in map_options.input_files
        ]
        if len(joined) != num_components:
            raise exceptions.MisalignedInputData(f'Length of input_files does not match length of input (len(input_files) = {len(input_files)}, len(inputs) = {num_components})')
        for itemdatum, files in zip(itemdata, joined):
            itemdatum['extra_input_files'] = files
    descriptors['transfer_input_files'] = ','.join(input_files)

    if map_options.output_remaps is not None and any(map_options.output_remaps):
        # TODO: I would prefer to do this in the base descriptors, but it looks like an "empty" remap triggers strange behavior
        descriptors["transfer_output_remaps"] = descriptors["transfer_output_remaps"].rstrip('"') + '; $(extra_remaps) "'

        if isinstance(map_options.output_remaps, dict):
            output_remaps = [map_options.output_remaps] * num_components
        else:
            output_remaps = map_options.output_remaps

        for component, (itemdatum, remaps) in enumerate(zip(itemdata, output_remaps)):
            itemdatum['extra_remaps'] = " ; ".join(f"{Path(names.USER_TRANSFER_DIR) / str(component) / k}={v.as_url()}" for k, v in remaps.items())

    for opt_key, opt_value in map_options.items():
        if not isinstance(opt_value, str):  # implies it is iterable
            itemdata_key = f'itemdata_for_{opt_key}'
            opt_value = tuple(opt_value)
            if len(opt_value) != num_components:
                raise exceptions.MisalignedInputData(f'Length of {opt_key} does not match length of input (len({opt_key}) = {len(opt_value)}, len(inputs) = {num_components})')
            for dct, v in zip(itemdata, opt_value):
                dct[itemdata_key] = v
            descriptors[opt_key] = f'$({itemdata_key})'
        else:
            descriptors[opt_key] = opt_value

    if descriptors[REQUIREMENTS] is None:
        descriptors.pop(REQUIREMENTS)

    sub = htcondor.Submit(descriptors)

    return sub, itemdata


def register_delivery_method(
    name: str,
    descriptors_func: Callable[[str, Path], dict],
    setup_func: Optional[Callable[[str, Path], None]] = None,
) -> None:
    """
    Register a new delivery method with HTMap.
    
    Parameters
    ----------
    name
        The name of the delivery method; this is what the ``DELIVERY_METHOD``
        should be set to to use this delivery method.
    descriptors_func
        The function that provides the HTCondor submit descriptors
        for this delivery method.
    setup_func
        The function that does any setup necessary to running the map.
    """
    if setup_func is None:
        setup_func = lambda *args: None

    BASE_OPTIONS_FUNCTION_BY_DELIVERY[name] = descriptors_func
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
    output_files = [
        f'{names.TRANSFER_DIR}/',
        f'{names.USER_TRANSFER_DIR}/$(component)',
    ]
    output_remaps = [
        f'$(component).{names.OUTPUT_EXT}={(map_dir / names.OUTPUTS_DIR / f"$(component).{names.OUTPUT_EXT}").as_posix()}'
    ]

    if utils.CAN_USE_URL_OUTPUT_TRANSFER:
        output_files.append(names.TRANSFER_PLUGIN_MARKER)
        output_remaps.append(f'{names.TRANSFER_PLUGIN_MARKER}=htmap://_')

    core = {
        'JobBatchName': tag,
        'log': (map_dir / names.EVENT_LOG).as_posix(),
        'submit_event_notes': '$(component)',
        'stdout': (map_dir / names.JOB_LOGS_DIR / f'$(component).{names.STDOUT_EXT}').as_posix(),
        'stderr': (map_dir / names.JOB_LOGS_DIR / f'$(component).{names.STDERR_EXT}').as_posix(),
        'should_transfer_files': 'YES',
        'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
        'transfer_output_files': " , ".join(output_files),
        'transfer_output_remaps': f'" {" ; ".join(output_remaps)} "',
        'on_exit_hold': 'ExitCode =!= 0',
        'initialdir': f"{(map_dir / names.OUTPUT_FILES_DIR).as_posix()}",
        'MY.component': '$(component)',
        'MY.IsHTMapJob': 'True',
    }

    if utils.CAN_USE_URL_OUTPUT_TRANSFER:
        core['transfer_plugins'] = f"htmap={(map_dir / names.TRANSFER_PLUGIN).as_posix()}"

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

    merged_requirements = merge_requirements(
        core.get(REQUIREMENTS, None),
        base.get(REQUIREMENTS, None),
        from_settings.get(REQUIREMENTS, None),
    )
    if merged_requirements is not None:
        merged[REQUIREMENTS] = merged_requirements

    return merged


def run_delivery_setup(
    tag: str,
    map_dir: Path,
    delivery: str,
) -> None:
    _copy_run_scripts(map_dir)

    try:
        SETUP_FUNCTION_BY_DELIVERY[delivery](tag, map_dir)
    except KeyError:
        raise exceptions.UnknownPythonDeliveryMethod(f"'{delivery}' is not a known delivery mechanism")


def _copy_run_scripts(map_dir: Path):
    run_script_source_dir = Path(__file__).parent / 'run'
    run_scripts = [
        run_script_source_dir / names.RUN_SCRIPT,
        run_script_source_dir / names.RUN_WITH_SINGULARITY_SCRIPT,
        run_script_source_dir / names.RUN_WITH_TRANSPLANT_SCRIPT,
        run_script_source_dir / names.TRANSFER_PLUGIN,
    ]
    for src in run_scripts:
        target = map_dir / src.name
        shutil.copy2(src, target)


def _get_base_descriptors_for_assume(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'vanilla',
        'executable': (map_dir / names.RUN_SCRIPT).as_posix(),
        'arguments': '$(component)',
    }


register_delivery_method(
    'assume',
    descriptors_func = _get_base_descriptors_for_assume,
)


def _get_base_descriptors_for_docker(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'docker',
        'docker_image': settings['DOCKER.IMAGE'],
        'executable': (map_dir / names.RUN_SCRIPT).as_posix(),
        'arguments': '$(component)',
        'transfer_executable': 'True',
    }


register_delivery_method(
    'docker',
    descriptors_func = _get_base_descriptors_for_docker,
)


def _get_base_descriptors_for_shared(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'vanilla',
        'executable': Path(sys.executable).absolute().as_posix(),
        'transfer_executable': 'False',
        'arguments': f'{names.RUN_SCRIPT} $(component)',
        'transfer_input_files': [
            (map_dir / names.RUN_SCRIPT).as_posix(),
        ],
    }


register_delivery_method(
    'shared',
    descriptors_func = _get_base_descriptors_for_shared,
)


def _get_base_descriptors_for_singularity(
    tag: str,
    map_dir: Path,
) -> dict:
    return {
        'universe': 'vanilla',
        REQUIREMENTS: 'HasSingularity == true',
        'executable': (map_dir / names.RUN_WITH_SINGULARITY_SCRIPT).as_posix(),
        'transfer_input_files': [
            (map_dir / names.RUN_SCRIPT).as_posix(),
        ],
        'arguments': f'{settings["SINGULARITY.IMAGE"]} $(component)',
        'transfer_executable': 'True',
    }


register_delivery_method(
    'singularity',
    descriptors_func = _get_base_descriptors_for_singularity,
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
        'executable': (map_dir / names.RUN_WITH_TRANSPLANT_SCRIPT).as_posix(),
        'arguments': f'$(component) {h}',
        'transfer_input_files': [
            (map_dir / names.RUN_SCRIPT).as_posix(),
            tif_path,
        ],
    }


def _run_delivery_setup_for_transplant(
    tag: str,
    map_dir: Path,
) -> None:
    if not settings.get('TRANSPLANT.ASSUME_EXISTS', False):
        if 'usr' in sys.executable:
            raise exceptions.CannotTransplantPython('System Python installations cannot be transplanted')
        if sys.platform == 'win32':
            raise exceptions.CannotTransplantPython('Transplant delivery does not work from Windows')

        py_dir = Path(sys.executable).parent.parent
        pip_freeze = _get_pip_freeze()

        target = Path(settings['TRANSPLANT.DIR']) / _get_transplant_hash(pip_freeze)
        zip_path = target.with_name(f'{target.stem}.tar.gz')

        if zip_path.exists():  # cached version already exists
            logger.debug(f'Using cached zipped python install at {zip_path}')
            return

        logger.debug(f'Creating zipped Python install for transplant from {py_dir} in {target.parent} ...')

        try:
            shutil.make_archive(
                base_name = str(target),
                format = 'gztar',
                root_dir = py_dir,
            )
        except BaseException as e:
            zip_path.unlink()
            logger.debug(f'Removed partial zipped Python install at {target}')
            raise e

        zip_path.rename(target)

        pip_path = zip_path.with_name(f'{target.stem}.pip')
        pip_path.write_bytes(pip_freeze)

        logger.debug(f'Created zipped Python install for transplant, stored at {zip_path}')


def _get_pip_freeze() -> bytes:
    return utils.pip_freeze().encode('utf-8')


def _get_transplant_hash(pip_freeze_output: bytes) -> str:
    h = hashlib.md5()
    h.update(pip_freeze_output)
    return h.hexdigest()


register_delivery_method(
    'transplant',
    descriptors_func = _get_base_descriptors_for_transplant,
    setup_func = _run_delivery_setup_for_transplant,
)
