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

from typing import Tuple, Iterable, Dict, Optional, Callable, Iterator, Any
import logging

import time
import shutil
from pathlib import Path
import itertools

import htcondor

from . import htio, exceptions, maps, options, settings

logger = logging.getLogger(__name__)


def maps_dir_path() -> Path:
    """The path to the directory where map directories are stored."""
    return settings['HTMAP_DIR'] / settings['MAPS_DIR_NAME']


def map_dir_path(map_id: str) -> Path:
    """The path to the directory for the given ``map_id``."""
    return maps_dir_path() / map_id


def get_schedd():
    """Get the :class:`htcondor.Schedd` that represents the HTCondor scheduler."""
    s = settings.get('HTCONDOR.SCHEDD', default = None)
    if s is not None:
        return htcondor.Schedd(s)

    return htcondor.Schedd()


def map(
    map_id: str,
    func: Callable,
    args: Iterable[Any],
    map_options: options.MapOptions = None,
    **kwargs,
) -> maps.Map:
    """
    Map a function call over a one-dimensional iterable of arguments.
    The function must take a single positional argument and any number of keyword arguments.

    The same keyword arguments are passed to *each call*, not mapped over.

    Parameters
    ----------
    map_id
        The ``map_id`` to assign to this map.
    func
        The function to map the arguments over.
    args
        An iterable of arguments to pass to the mapped function.
    kwargs
        Any additional keyword arguments are passed as keyword arguments to the mapped function.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    result :
        A :class:`htmap.Map` representing the map.
    """
    args = ((arg,) for arg in args)
    args_and_kwargs = zip(args, itertools.repeat(kwargs))
    return create_map(
        map_id,
        func,
        args_and_kwargs,
        map_options = map_options,
    )


def starmap(
    map_id: str,
    func: Callable,
    args: Optional[Iterable[tuple]] = None,
    kwargs: Optional[Iterable[Dict[str, Any]]] = None,
    map_options: options.MapOptions = None,
) -> maps.Map:
    """
    Map a function call over aligned iterables of arguments and keyword arguments.
    Each element of ``args`` and ``kwargs`` is unpacked into the signature of the function,
    so their elements should be tuples and dictionaries corresponding to position and keyword arguments of the mapped function.

    Parameters
    ----------
    map_id
        The ``map_id`` to assign to this map.
    func
        The function to map the arguments over.
    args
        An iterable of tuples of positional arguments to unpack into the mapped function.
    kwargs
        An iterable of dictionaries of keyword arguments to unpack into the mapped function.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    result :
        A :class:`htmap.Map` representing the map.
    """
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = ()

    args_and_kwargs = zip_args_and_kwargs(args, kwargs)
    return create_map(
        map_id,
        func,
        args_and_kwargs,
        map_options = map_options,
    )


id_gen = itertools.count()


def get_transient_map_id() -> str:
    return f'tmp-{int(time.time())}-{next(id_gen)}'


class TransientMap:
    def __init__(self, map: maps.Map):
        self._map = map

    def __repr__(self):
        return f'<{self.__class__.__name__}(map = {self._map})>'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

        return False  # re-raise exceptions

    def __iter__(self):
        try:
            yield from self._map
        finally:
            self._cleanup()

    def __del__(self):
        self._cleanup()

    def _cleanup(self):
        try:
            self._map.remove()
            logger.debug(f'removed transient map {self._map.map_id}')
        except exceptions.MapWasRemoved:
            pass


def transient_map(
    func: Callable,
    args: Iterable[Any],
    map_options: options.MapOptions = None,
    **kwargs,
) -> TransientMap:
    """
    As :func:`htmap.map`, except that it doesn't need a ``map_id``, it returns an iterator over the outputs, and the map is immediately removed after use.
    """
    m = map(
        map_id = get_transient_map_id(),
        func = func,
        args = args,
        map_options = map_options,
        **kwargs,
    )

    return TransientMap(m)


def transient_starmap(
    func: Callable,
    args: Optional[Iterable[tuple]] = None,
    kwargs: Optional[Iterable[Dict[str, Any]]] = None,
    map_options: options.MapOptions = None,
) -> TransientMap:
    """
    As :func:`htmap.starmap`, except that it doesn't need a ``map_id``, it returns an iterator over the outputs, and the map is immediately removed after use.
    """
    m = starmap(
        map_id = get_transient_map_id(),
        func = func,
        args = args,
        kwargs = kwargs,
        map_options = map_options,
    )

    return TransientMap(m)


class MapBuilder:
    """
    The :class:`htmap.MapBuilder` provides an alternate way to create maps.
    Once created via :meth:`htmap.build_map` or similar as a context manager,
    the map builder can be called as if it were the function you're mapping over.
    When the ``with`` block exits, the inputs are collected and submitted as a single map.

    .. code-block:: python

        with htmap.build_map(map_id = 'pow', func = lambda x, p: x ** p) as builder:
            for x in range(1, 4):
                builder(x, x)

        result = builder.result
        print(list(result))  # [1, 4, 27]
    """

    def __init__(
        self,
        map_id: str,
        func: Callable,
        map_options: options.MapOptions = None,
    ):
        self.func = func
        self.map_id = map_id
        self.map_options = map_options

        self.args = []
        self.kwargs = []

        self._result = None

        logger.debug(f'initialized map builder for map {map_id} for {self.func}')

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func}, map_options = {self.map_options})>'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # if an exception is raised in the with, re-raise without submitting jobs
        if exc_type is not None:
            logger.exception(f'map builder for map {self.map_id} aborted due to')
            return False

        self._result = starmap(
            map_id = self.map_id,
            func = self.func,
            args = self.args,
            kwargs = self.kwargs,
            map_options = self.map_options
        )

        logger.debug(f'finished executing map builder for map {self.map_id}')

    def __call__(self, *args, **kwargs):
        """Adds the given inputs to the map."""
        self.args.append(args)
        self.kwargs.append(kwargs)

    @property
    def result(self) -> maps.Map:
        """
        The :class:`Map` associated with this :class:`MapBuilder`.
        Will raise :class:`htmap.exceptions.NoResultYet` when accessed until the ``with`` block for this :class:`MapBuilder` completes.
        """
        if self._result is None:
            raise exceptions.NoResultYet('result does not exist until after with block')
        return self._result

    def __len__(self):
        """The length of a :class:`MapBuilder` is the number of inputs it has been sent."""
        return len(self.args)


def build_map(
    map_id: str,
    func: Callable,
    map_options: options.MapOptions = None,
) -> MapBuilder:
    """
    Return a :class:`MapBuilder` for the given function.

    Parameters
    ----------
    map_id
        The ``map_id`` to assign to this map.
    func
        The function to map over.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    map_builder :
        A :class:`MapBuilder` for the given function.
    """
    return MapBuilder(
        map_id = map_id,
        func = func,
        map_options = map_options,
    )


def build_transient_map(
    map_id: str,
    func: Callable,
    map_options: options.MapOptions = None,
) -> TransientMap:
    """
    As :func:`htmap.build_map`, except that it doesn't need a ``map_id``, it returns an iterator over the outputs, and the map is immediately removed after use.
    """
    mb = MapBuilder(
        map_id = map_id,
        func = func,
        map_options = map_options,
    )

    return TransientMap(mb.result)


def create_map(
    map_id: str,
    func: Callable,
    args_and_kwargs: Iterator[Tuple[Tuple, Dict]],
    map_options: Optional[options.MapOptions] = None,
) -> maps.Map:
    """
    All map calls lead here.
    This function performs various checks on the ``map_id``,
    constructs a submit object that represents the map for HTCondor,
    saves all of the map's definitional data to the map directory,
    and submits the map job,
    returning the map's :class:`Map`.

    Parameters
    ----------
    map_id
        The ``map_id`` to assign to this map.
    func
        The function to map the arguments over.
    args_and_kwargs
        The arguments and keyword arguments to map over - the output of :func:`zip_args_and_kwargs`.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    result :
        A :class:`htmap.Map` representing the map.
    """
    raise_if_map_id_is_invalid(map_id)
    raise_if_map_id_already_exists(map_id)

    logger.debug(f'creating map {map_id}...')

    map_dir = map_dir_path(map_id)
    try:
        make_map_dir_and_subdirs(map_dir)
        htio.save_func(map_dir, func)
        num_components = htio.save_args_and_kwargs(map_dir, args_and_kwargs)

        submit_obj, itemdata = options.create_submit_object_and_itemdata(
            map_id,
            map_dir,
            num_components,
            map_options,
        )

        htio.save_num_components(map_dir, num_components)
        htio.save_submit(map_dir, submit_obj)
        htio.save_itemdata(map_dir, itemdata)

        logger.debug(f'submitting map {map_id}...')
        cluster_id = execute_submit(
            submit_object = submit_obj,
            itemdata = itemdata,
        )

        logger.debug(f'map {map_id} was assigned clusterid {cluster_id}')

        with (map_dir / 'cluster_ids').open(mode = 'a') as file:
            file.write(str(cluster_id) + '\n')

        with (map_dir / 'cluster_ids').open() as file:
            cluster_ids = [int(cid.strip()) for cid in file]

        r = maps.Map(
            map_id = map_id,
            cluster_ids = cluster_ids,
            submit = submit_obj,
            num_components = num_components,
        )

        logger.info(f'submitted map {map_id}')

        return r
    except BaseException as e:
        # something went wrong during submission, and the job is malformed
        # so delete the entire map directory
        # the condor bindings should prevent any jobs from being submitted
        logger.exception(f'map submission for map {map_id} aborted due to')
        shutil.rmtree(str(map_dir.absolute()))
        logger.debug(f'removed malformed map directory {map_dir}')
        raise e


def raise_if_map_id_already_exists(map_id: str):
    """Raise a :class:`htmap.exceptions.MapIdAlreadyExists` if the ``map_id`` already exists."""
    if map_dir_path(map_id).exists():
        raise exceptions.MapIdAlreadyExists(f'the requested map_id {map_id} already exists (recover the Map, then either use or delete it).')


INVALID_FILENAME_CHARACTERS = {
    '/',
    '\\',  # backslash
    '<',
    '>',
    ':',
    '"',
    '|',
    '?',
    '*',
    '.',
    ' ',
}


def raise_if_map_id_is_invalid(map_id: str):
    """Raise a :class:`htmap.exceptions.InvalidMapId` if the ``map_id`` contains any invalid characters."""
    invalid_chars = set(map_id).intersection(INVALID_FILENAME_CHARACTERS)
    if len(invalid_chars) != 0:
        raise exceptions.InvalidMapId(f'These characters in map_id {map_id} are not valid: {invalid_chars}')


MAP_SUBDIR_NAMES = (
    'inputs',
    'outputs',
    'job_logs',
)


def make_map_dir_and_subdirs(map_dir):
    """Create the input, output, and log subdirectories inside the map directory."""
    for path in (map_dir / d for d in MAP_SUBDIR_NAMES):
        path.mkdir(parents = True, exist_ok = True)

    logger.debug(f'created map directory {map_dir} and subdirectories')


def execute_submit(submit_object, itemdata) -> int:
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
    args: Iterable[Tuple],
    kwargs: Iterable[Dict],
) -> Iterator[Tuple[Tuple, Dict]]:
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
    """
    iterators = [iter(args), iter(kwargs)]
    fills = {0: (), 1: {}}
    num_active = 2
    while True:
        values = []
        for i, it in enumerate(iterators):
            try:
                value = next(it)
            except StopIteration:
                num_active -= 1
                if num_active == 0:
                    return
                iterators[i] = itertools.repeat(fills[i])
                value = fills[i]
            values.append(value)
        yield tuple(values)
