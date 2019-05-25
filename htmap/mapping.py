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

from typing import Tuple, Iterable, Dict, Optional, Callable, Iterator, Any, List, Generator, Union
import logging

import uuid
import shutil
from pathlib import Path
import itertools

import htcondor

from . import htio, tags, exceptions, maps, transfer, options, settings, names, utils

logger = logging.getLogger(__name__)


def maps_dir_path() -> Path:
    """The path to the directory where map directories are stored."""
    return Path(settings['HTMAP_DIR']) / names.MAPS_DIR


def map_dir_path(uid: Union[uuid.UUID, str]) -> Path:
    """The path to the directory for the given ``uid``."""
    return maps_dir_path() / str(uid)


def get_schedd():
    """Get the :class:`htcondor.Schedd` that represents the HTCondor scheduler."""
    s = settings['HTCONDOR.SCHEDULER']
    if s is None:
        return htcondor.Schedd()

    coll = htcondor.Collector(settings['HTCONDOR.COLLECTOR'])
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, s)
    return htcondor.Schedd(schedd_ad)


def map(
    func: Callable,
    args: Iterable[Any],
    map_options: Optional[options.MapOptions] = None,
    tag: Optional[str] = None,
) -> maps.Map:
    """
    Map a function call over a one-dimensional iterable of arguments.
    The function must take exactly one positional argument and no keyword arguments.

    Parameters
    ----------
    func
        The function to map the arguments over.
    args
        An iterable of arguments to pass to the mapped function.
    map_options
        An instance of :class:`htmap.MapOptions`.
    tag
        The ``tag`` to assign to this map.

    Returns
    -------
    map :
        A :class:`htmap.Map` representing the map.
    """
    args = ((arg,) for arg in args)
    args_and_kwargs = zip(args, itertools.repeat({}))
    return create_map(
        tag,
        func,
        args_and_kwargs,
        map_options = map_options,
    )


def starmap(
    func: Callable,
    args: Optional[Iterable[tuple]] = None,
    kwargs: Optional[Iterable[Dict[str, Any]]] = None,
    map_options: options.MapOptions = None,
    tag: Optional[str] = None,
) -> maps.Map:
    """
    Map a function call over aligned iterables of arguments and keyword arguments.
    Each element of ``args`` and ``kwargs`` is unpacked into the signature of the function,
    so their elements should be tuples and dictionaries corresponding to position and keyword arguments of the mapped function.

    Parameters
    ----------
    func
        The function to map the arguments over.
    args
        An iterable of tuples of positional arguments to unpack into the mapped function.
    kwargs
        An iterable of dictionaries of keyword arguments to unpack into the mapped function.
    map_options
        An instance of :class:`htmap.MapOptions`.
    tag
        The ``tag`` to assign to this map.

    Returns
    -------
    map :
        A :class:`htmap.Map` representing the map.
    """
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = ()

    args_and_kwargs = zip_args_and_kwargs(args, kwargs)
    return create_map(
        tag,
        func,
        args_and_kwargs,
        map_options = map_options,
    )


class MapBuilder:
    """
    The :class:`htmap.MapBuilder` provides an alternate way to create maps.
    Once created via :meth:`htmap.build_map` or similar as a context manager,
    the map builder can be called as if it were the function you're mapping over.
    When the ``with`` block exits, the inputs are collected and submitted as a single map.

    .. code-block:: python

        with htmap.build_map(tag = 'pow', func = lambda x, p: x ** p) as builder:
            for x in range(1, 4):
                builder(x, x)

        map = builder.map
        print(list(map))  # [1, 4, 27]
    """

    def __init__(
        self,
        func: Callable,
        map_options: options.MapOptions = None,
        tag: Optional[str] = None,
    ):
        self.func = func
        self.map_options = map_options
        self.tag = tag

        self.args: List[Tuple[Any, ...]] = []
        self.kwargs: List[Dict[str, Any]] = []

        self._map = None

        logger.debug(f'initialized map builder for map {tag} for {self.func}')

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func}, map_options = {self.map_options})>'

    def __enter__(self) -> 'MapBuilder':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # if an exception is raised in the with, re-raise without submitting jobs
        if exc_type is not None:
            logger.exception(f'map builder for map {self.tag} aborted due to')
            return False

        self._map = starmap(
            tag = self.tag,
            func = self.func,
            args = self.args,
            kwargs = self.kwargs,
            map_options = self.map_options
        )

        logger.debug(f'finished executing map builder for map {self.tag}')

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Adds the given inputs to the map."""
        self.args.append(args)
        self.kwargs.append(kwargs)

    @property
    def map(self) -> maps.Map:
        """
        The :class:`Map` associated with this :class:`MapBuilder`.
        Will raise :class:`htmap.exceptions.NoMapYet` when accessed until the ``with`` block for this :class:`MapBuilder` completes.
        """
        if self._map is None:
            raise exceptions.NoMapYet('map does not exist until after with block')
        return self._map

    def __len__(self) -> int:
        """The length of a :class:`MapBuilder` is the number of inputs it has been sent."""
        return len(self.args)


def build_map(
    func: Callable,
    map_options: options.MapOptions = None,
    tag: Optional[str] = None,
) -> MapBuilder:
    """
    Return a :class:`MapBuilder` for the given function.

    Parameters
    ----------
    func
        The function to map over.
    map_options
        An instance of :class:`htmap.MapOptions`.
    tag
        The ``tag`` to assign to this map.

    Returns
    -------
    map_builder :
        A :class:`MapBuilder` for the given function.
    """
    return MapBuilder(
        func = func,
        map_options = map_options,
        tag = tag,
    )


def create_map(
    tag: Optional[str],
    func: Callable,
    args_and_kwargs: Iterator[Tuple[Tuple, Dict]],
    map_options: Optional[options.MapOptions] = None,
) -> maps.Map:
    """
    All map calls lead here.
    This function performs various checks on the ``tag``,
    constructs a submit object that represents the map for HTCondor,
    saves all of the map's definitional data to the map directory,
    and submits the map job,
    returning the map's :class:`Map`.

    Parameters
    ----------
    tag
        The ``tag`` to assign to this map.
    func
        The function to map the arguments over.
    args_and_kwargs
        The arguments and keyword arguments to map over - the output of :func:`zip_args_and_kwargs`.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    map :
        A :class:`htmap.Map` representing the map.
    """
    if tag is None:
        tag = tags.random_tag()
        transient = True
    else:
        transient = False

    tags.raise_if_tag_is_invalid(tag)
    tags.raise_if_tag_already_exists(tag)

    logger.debug(f'creating map {tag}...')

    if map_options is None:
        map_options = options.MapOptions()

    uid = uuid.uuid4()
    map_dir = map_dir_path(uid)
    try:
        make_map_dir_and_subdirs(map_dir)
        htio.save_func(map_dir, func)

        args_and_kwargs, extra_input_files = process_args_and_kwargs(args_and_kwargs)
        num_components = len(args_and_kwargs)
        if num_components == 0:
            raise exceptions.EmptyMap()
        if map_options.input_files is None and len(extra_input_files) > 0:
            map_options.input_files = [[] for _ in range(len(extra_input_files))]
        for tif, extra in zip(map_options.input_files, extra_input_files):
            tif.extend(extra)
        htio.save_inputs(map_dir, args_and_kwargs)

        submit_obj, itemdata = options.create_submit_object_and_itemdata(
            tag,
            map_dir,
            num_components,
            map_options,
        )

        htio.save_num_components(map_dir, num_components)
        htio.save_submit(map_dir, submit_obj)
        htio.save_itemdata(map_dir, itemdata)

        logger.debug(f'submitting map {tag}...')
        cluster_id = execute_submit(
            submit_object = submit_obj,
            itemdata = itemdata,
        )

        logger.debug(f'map {tag} was assigned clusterid {cluster_id}')

        with (map_dir / names.CLUSTER_IDS).open(mode = 'a') as file:
            file.write(str(cluster_id) + '\n')

        tags.tag_file_path(tag).write_text(str(uid))

        m = maps.Map(
            tag = tag,
            map_dir = map_dir,
            cluster_ids = [cluster_id],
            num_components = num_components,
        )

        if transient:
            m._make_transient()

        logger.info(f'submitted map {m.tag}')
        if utils.is_interactive_session():
            print(f'created map {m.tag} with {len(m)} components')

        return m
    except BaseException as e:
        # something went wrong during submission, and the job is malformed
        # so delete the entire map directory
        # the condor bindings should prevent any jobs from being submitted
        logger.exception(f'map submission for map {tag} aborted due to')
        shutil.rmtree(str(map_dir.absolute()))
        logger.debug(f'removed malformed map directory {map_dir}')
        raise e


MAP_SUBDIR_NAMES = (
    names.INPUTS_DIR,
    names.OUTPUTS_DIR,
    names.JOB_LOGS_DIR,
    names.OUTPUT_FILES_DIR,
)


def make_map_dir_and_subdirs(map_dir: Path) -> None:
    """Create the input, output, and log subdirectories inside the map directory."""
    for path in (map_dir / d for d in MAP_SUBDIR_NAMES):
        path.mkdir(parents = True, exist_ok = True)

    logger.debug(f'created map directory {map_dir} and subdirectories')


def execute_submit(submit_object: htcondor.Submit, itemdata: List[Dict[str, str]]) -> int:
    """
    Execute a map via the scheduler defined by the settings.
    Return the HTCondor cluster ID of the map's jobs.
    """
    schedd = get_schedd()
    with schedd.transaction() as txn:
        submit_result = submit_object.queue_with_itemdata(
            txn,
            1,
            iter(itemdata),
        )

        return submit_result.cluster()


def zip_args_and_kwargs(
    args: Iterable[Tuple[Any, ...]],
    kwargs: Iterable[Dict[str, Any]],
) -> Generator[Tuple[Tuple[Any, ...], Dict[str, Any]], None, None]:
    """
    Combine iterables of arguments and keyword arguments into a zipped,
    filled iterator of arguments and keyword arguments (i.e., tuples and dictionaries).

    .. caution ::

        This function will happily run forever when given infinite iterator inputs.
        Be careful!

    Parameters
    ----------
    args
        A list of tuples.
    kwargs
        A list of dictionaries.

    Returns
    -------
    args_and_kwargs
        A zipped iterator of tuples (positional arguments) and dictionaries (keyword arguments).
    """
    iterators: List[Iterator] = [iter(args), iter(kwargs)]
    fills = {0: (), 1: {}}
    num_active = 2
    while True:
        values = []
        for i, it in enumerate(iterators):
            try:
                values.append(next(it))
            except StopIteration:
                num_active -= 1
                if num_active == 0:
                    return
                iterators[i] = itertools.repeat(fills[i])  # replace the iterator
                values.append(fills[i])  # for this iteration, insert fills[i] manually
        yield tuple(values)


def process_args_and_kwargs(args_and_kwargs):
    """
    Perform any pre-processing on the positional and keyword arguments.

    Currently checks for files that should be transferred along with the component input.
    """
    processed = []
    extra_input_files = []
    for args, kwargs in args_and_kwargs:
        extra_local_paths = []
        args = tuple(_check_for_input_files(arg, extra_local_paths) for arg in args)
        kwargs = {k: _check_for_input_files(v, extra_local_paths) for k, v in kwargs.items()}

        processed.append((args, kwargs))
        extra_input_files.append(sorted(set(extra_local_paths)))

    return processed, extra_input_files


def _normalize(path: transfer.TransferPath, local_paths_accumulator: List[Path]):
    """Helper function which replaces htmap.TransferPath with Path"""
    local_paths_accumulator.append(path)
    return Path('.') / path.name


def _check_for_input_files(object_to_check: Any, local_paths_accumulator: List[Path]):
    """
    Descends recursively through primitive containers or top-level function arguments and keyword arguments, looking for :class:`htmap.TransferPath` objects.
    When it encounters one, it adds the local path to the accumulator and replaces the path in the container with the appropriate path for the scratch directory on the execute node.
    """
    if isinstance(object_to_check, transfer.TransferPath):
        return _normalize(object_to_check, local_paths_accumulator)

    # look inside built-in containers recursively
    elif isinstance(object_to_check, (list, tuple, set)):
        return type(object_to_check)(_check_for_input_files(c, local_paths_accumulator) for c in object_to_check)
    elif isinstance(object_to_check, dict):
        return {k: _check_for_input_files(v, local_paths_accumulator) for k, v in object_to_check.items()}

    return object_to_check
