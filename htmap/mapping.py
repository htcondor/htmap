from typing import Tuple, Iterable, Dict, Optional, List, Callable, Iterator, Any

import shutil
from pathlib import Path
import itertools
import json

import htcondor

from . import htio, exceptions, result, options, settings


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
    force_overwrite: bool = False,
    map_options: options.MapOptions = None,
    **kwargs,
) -> result.MapResult:
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
    force_overwrite
        If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    result :
        A :class:`htmap.MapResult` representing the map.
    """
    args = ((arg,) for arg in args)
    args_and_kwargs = zip(args, itertools.repeat(kwargs))
    return submit_map(
        map_id,
        func,
        args_and_kwargs,
        force_overwrite = force_overwrite,
        map_options = map_options,
    )


def starmap(
    map_id: str,
    func: Callable,
    args: Optional[Iterable[tuple]] = None,
    kwargs: Optional[Iterable[Dict[str, Any]]] = None,
    force_overwrite: bool = False,
    map_options: options.MapOptions = None,
) -> result.MapResult:
    """
    Map a function call over aligned iterables of arguments and keyword arguments.
    Each element of ``args`` and ``kwargs`` is unpacked into the signature of the function, so their elements should be tuples and dictionaries corresponding to position and keyword arguments of the mapped function.

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
    force_overwrite
        If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    result :
        A :class:`htmap.MapResult` representing the map.
    """
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = ()

    args_and_kwargs = zip_args_and_kwargs(args, kwargs)
    return submit_map(
        map_id,
        func,
        args_and_kwargs,
        force_overwrite = force_overwrite,
        map_options = map_options,
    )


class MapBuilder:
    def __init__(
        self,
        map_id: str,
        func: Callable,
        force_overwrite: bool = False,
        map_options: options.MapOptions = None,
    ):
        self.func = func
        self.map_id = map_id
        self.force_overwrite = force_overwrite
        self.map_options = map_options

        self.args = []
        self.kwargs = []

        self._result = None

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func}, map_options = {self.map_options})>'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # if an exception is raised in the with, re-raise without submitting jobs
        if exc_type is not None:
            return False

        self._result = starmap(
            map_id = self.map_id,
            func = self.func,
            args = self.args,
            kwargs = self.kwargs,
            force_overwrite = self.force_overwrite,
            map_options = self.map_options
        )

    def __call__(self, *args, **kwargs):
        """Adds the given inputs to the map."""
        self.args.append(args)
        self.kwargs.append(kwargs)

    @property
    def result(self) -> result.MapResult:
        """
        The :class:`MapResult` associated with this :class:`MapBuilder`.
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
    force_overwrite: bool = False,
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
    force_overwrite
        If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
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
        force_overwrite = force_overwrite,
        map_options = map_options,
    )


def submit_map(
    map_id: str,
    func: Callable,
    args_and_kwargs: Iterable[Tuple],
    force_overwrite: bool = False,
    map_options: Optional[options.MapOptions] = None,
) -> result.MapResult:
    """
    All map calls lead here.
    This function performs various checks on the ``map_id``,
    constructs a submit object that represents the map for HTCondor,
    saves all of the map's definitional data to the map directory,
    and submits the map job,
    returning the map's :class:`MapResult`.

    Parameters
    ----------
    map_id
        The ``map_id`` to assign to this map.
    func
        The function to map the arguments over.
    args_and_kwargs
        The arguments and keyword arguments to map over - the output of :func:`zip_args_and_kwargs`.
    force_overwrite
        If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
    map_options
        An instance of :class:`htmap.MapOptions`.

    Returns
    -------
    result :
        A :class:`htmap.MapResult` representing the map.
    """
    raise_if_map_id_is_invalid(map_id)

    if force_overwrite:
        try:
            existing_result = result.MapResult.recover(map_id)
            existing_result.remove()
        except exceptions.MapIdNotFound:
            pass
    else:
        raise_if_map_id_already_exists(map_id)

    map_dir = map_dir_path(map_id)
    try:
        make_map_subdirs(map_dir)
        save_func(map_dir, func)
        hashes = save_args_and_kwargs(map_dir, args_and_kwargs)
        save_hashes(map_dir, hashes)

        submit_obj, itemdata = options.create_submit_object_and_itemdata(
            map_id,
            map_dir,
            hashes,
            map_options,
        )
        save_submit_object(map_dir, submit_obj)
        save_itemdata(map_dir, itemdata)

        submit_result = execute_submit(
            submit_object = submit_obj,
            itemdata = itemdata,
        )
        cluster_id = submit_result.cluster()

        with (map_dir / 'cluster_ids').open(mode = 'a') as file:
            file.write(str(cluster_id) + '\n')

        with (map_dir / 'cluster_ids').open() as file:
            cluster_ids = [int(cid.strip()) for cid in file]

        return result.MapResult(
            map_id = map_id,
            cluster_ids = cluster_ids,
            submit = submit_obj,
            hashes = hashes,
        )
    except Exception as e:
        # something went wrong during submission, and the job is malformed
        # so delete the entire map directory
        # the condor bindings should prevent any jobs from being submitted
        shutil.rmtree(map_dir)
        raise e


def raise_if_map_id_already_exists(map_id: str):
    """Raise a :class:`htmap.exceptions.MapIdAlreadyExists` if the ``map_id`` already exists."""
    if map_dir_path(map_id).exists():
        raise exceptions.MapIdAlreadyExists(f'the requested map_id {map_id} already exists (recover the MapResult, then either use or delete it).')


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
    'cluster_logs',
)


def make_map_subdirs(map_dir):
    """Create the input, output, and log subdirectories inside the map directory."""
    for path in (map_dir / d for d in MAP_SUBDIR_NAMES):
        path.mkdir(parents = True, exist_ok = True)


def save_func(map_dir, func):
    """Save the mapped function to the map directory."""
    fn_path = map_dir / 'fn.pkl'
    htio.save_object(func, fn_path)


def save_args_and_kwargs(map_dir: Path, args_and_kwargs) -> List[str]:
    """Save the arguments to the mapped function to the map's input directory."""
    hashes = []
    num_inputs = 0
    for a_and_k in args_and_kwargs:
        b = htio.to_bytes(a_and_k)
        h = htio.hash_bytes(b)
        hashes.append(h)

        input_path = map_dir / 'inputs' / f'{h}.in'
        htio.save_bytes(b, input_path)

        num_inputs += 1

    if num_inputs == 0:
        raise exceptions.EmptyMap()

    return hashes


def save_hashes(map_dir: Path, hashes: Iterable[str]):
    """Save a file containing the hashes of the arguments to the map directory."""
    with (map_dir / 'hashes').open(mode = 'w') as file:
        file.write('\n'.join(hashes))


def save_submit_object(map_dir: Path, submit):
    """Save a dictionary that represents the map's :class:`htcondor.Submit` object."""
    htio.save_object(dict(submit), map_dir / 'submit')


def save_itemdata(map_dir: Path, itemdata: List[dict]):
    """Save the map's itemdata as a list of JSON dictionaries."""
    with (map_dir / 'itemdata').open(mode = 'w') as f:
        json.dump(itemdata, f, indent = None, separators = (',', ':'))


def execute_submit(submit_object, itemdata):
    """Execute a map via the scheduler defined by the settings."""
    schedd = get_schedd()
    with schedd.transaction() as txn:
        submit_result = submit_object.queue_with_itemdata(
            txn,
            1,
            iter(itemdata),
        )

        return submit_result


def zip_args_and_kwargs(args: Iterable[Tuple], kwargs: Iterable[Dict]) -> Iterator[Tuple[Tuple, Dict]]:
    """
    Combine iterables of arguments and keyword arguments into a zipped, filled iterator of arguments and keyword arguments (i.e., tuples and dictionaries).

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
