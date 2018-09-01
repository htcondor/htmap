from typing import Tuple, Iterable, Dict, Union, Optional, List, Callable, Iterator, Any

import shutil
from pathlib import Path
import itertools
import json

import htcondor

from . import htio, exceptions, result, options, settings


def map_dir_path(map_id: str) -> Path:
    return settings['HTMAP_DIR'] / settings['MAPS_DIR_NAME'] / map_id


def check_map_id(map_id: str):
    """Raise a :class:`htmap.exceptions.MapIDAlreadyExists` if the ``map_id`` already exists."""
    if map_dir_path(map_id).exists():
        raise exceptions.MapIDAlreadyExists(f'the requested map_id {map_id} already exists (recover the MapResult, then either use or delete it).')


def get_schedd():
    s = settings.get('HTCONDOR.SCHEDD', default = None)
    if s is not None:
        return htcondor.Schedd(s)

    return htcondor.Schedd()


MAP_SUBDIR_NAMES = (
    'inputs',
    'outputs',
    'job_logs',
    'cluster_logs',
)


def mk_map_subdirs(map_dir):
    for path in (map_dir / d for d in MAP_SUBDIR_NAMES):
        path.mkdir(parents = True, exist_ok = True)


def map(
    map_id: str,
    func: Callable,
    args: Iterable[Any],
    force_overwrite: bool = False,
    map_options = None,
    **kwargs,
) -> result.MapResult:
    """
    Map a function call over a one-dimensional iterable of arguments.
    The function must take a single positional argument and any number of keyword arguments.

    Parameters
    ----------
    func
        The function to call.
    map_id
        The ``map_id`` to assign to this map.
    args
        An iterable of arguments to pass to the mapped function.
    kwargs
        Any additional keyword arguments are passed as keyword arguments to the mapped function.
    force_overwrite
        If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.

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
    args: Optional[Iterable[Tuple]] = None,
    kwargs: Optional[Iterable[Dict]] = None,
    force_overwrite: bool = False,
    map_options = None,
) -> result.MapResult:
    """
    Map a function call over aligned iterables of arguments and keyword arguments.
    Each element of ``args`` and ``kwargs`` is unpacked into the signature of the function, so their elements should be tuples and dictionaries corresponding to position and keyword arguments of the mapped function.

    Parameters
    ----------
    map_id
        The ``map_id`` to assign to this map.
    args
        An iterable of tuples of positional arguments to unpack into the mapped function.
    kwargs
        An iterable of dictionaries of keyword arguments to unpack into the mapped function.
    force_overwrite
        If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.

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


def submit_map(
    map_id: str,
    func: Callable,
    args_and_kwargs: Iterable[Tuple],
    force_overwrite: bool = False,
    map_options: Optional[options.MapOptions] = None,
) -> result.MapResult:
    if force_overwrite:
        try:
            existing_result = result.MapResult.recover(map_id)
            existing_result.remove()
        except exceptions.MapIDNotFound:
            pass
    else:
        check_map_id(map_id)

    map_dir = map_dir_path(map_id)
    try:
        mk_map_subdirs(map_dir)
        save_func(map_dir, func)
        hashes = save_args_and_kwargs(map_dir, args_and_kwargs)
        save_hashes(map_dir, hashes)

        submit_obj, itemdata = options.create_submit_object(
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


def save_func(map_dir, func):
    fn_path = map_dir / 'fn.pkl'
    htio.save_object(func, fn_path)


def save_args_and_kwargs(map_dir: Path, args_and_kwargs) -> List[str]:
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
    with (map_dir / 'hashes').open(mode = 'w') as file:
        file.write('\n'.join(hashes))


def save_submit_object(map_dir: Path, submit):
    htio.save_object(dict(submit), map_dir / 'submit')


def save_itemdata(map_dir: Path, extra_itemdata: List[dict]):
    with (map_dir / 'extra_itemdata').open(mode = 'w') as f:
        json.dump(extra_itemdata, f, indent = None, separators = (',', ':'))


def execute_submit(submit_object, itemdata):
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
    Combine iterables of arguments and keyword arguments into
    an iterable zipped, filled iterator of arguments and keyword arguments.

    Parameters
    ----------
    args
    kwargs

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
