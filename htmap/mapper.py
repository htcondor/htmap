from typing import Tuple, Iterable, Dict, Union, Optional, List, Callable, Iterator

import collections
import shutil
from pathlib import Path
import itertools
from copy import deepcopy

import htcondor

from . import htio, exceptions, result, utils, shortcuts


class MapBuilder:
    def __init__(self, mapper: 'HTMapper', map_id: str, force_overwrite: bool = False):
        self.mapper = mapper
        self.map_id = map_id
        self.force_overwrite = force_overwrite

        self.args = []
        self.kwargs = []

        self._result = None

    def __repr__(self):
        return f'<{self.__class__.__name__}(mapper = {self.mapper})>'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # if an exception is raised in the with, re-raise without submitting jobs
        if exc_type is not None:
            return False

        self._result = self.mapper.starmap(
            self.map_id,
            self.args,
            self.kwargs,
            force_overwrite = self.force_overwrite
        )

    def __call__(self, *args, **kwargs):
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
        return len(self.args)


class HTMapper:
    _map_dir_names = (
        'inputs',
        'outputs',
        'job_logs',
        'cluster_logs',
    )

    def __init__(self, func: Callable, **submit_options):
        self.func = func
        self.submit_options = submit_options

    def _mkdirs(self, map_id: str):
        """Create the various directories needed by the mapper."""
        for path in (utils.map_dir_path(map_id) / dir_name for dir_name in self._map_dir_names):
            path.mkdir(parents = True, exist_ok = True)

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func})>'

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def map(
        self,
        map_id: str,
        args,
        force_overwrite: bool = False,
        **kwargs,
    ) -> result.MapResult:
        args = ((arg,) for arg in args)
        args_and_kwargs = zip(args, itertools.repeat(kwargs))
        return self._map(map_id, args_and_kwargs, force_overwrite = force_overwrite)

    def productmap(
        self,
        map_id: str,
        *args,
        force_overwrite: bool = False,
        **kwargs,
    ) -> result.MapResult:
        dicts = [{}]
        for key, values in kwargs.items():
            values = tuple(values)
            dicts = [deepcopy(d) for d in dicts for _ in range(len(values))]
            for d, v in zip(dicts, itertools.cycle(values)):
                d[key] = v

        args = itertools.repeat(args)
        args_and_kwargs = zip(args, dicts)

        return self._map(map_id, args_and_kwargs, force_overwrite = force_overwrite)

    def starmap(
        self,
        map_id: str,
        args: Optional[Iterable[Tuple]] = None,
        kwargs: Optional[Iterable[Dict]] = None,
        force_overwrite: bool = False
    ) -> result.MapResult:
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = ()

        args_and_kwargs = zip_args_and_kwargs(args, kwargs)
        return self._map(map_id, args_and_kwargs, force_overwrite = force_overwrite)

    def build_map(self, map_id: str, force_overwrite: bool = False):
        """
        Return a :class:`htmap.MapBuilder` for the wrapped function.

        Parameters
        ----------
        map_id
        force_overwrite
        """
        return MapBuilder(mapper = self, map_id = map_id, force_overwrite = force_overwrite)

    def _map(self, map_id: str, args_and_kwargs: Iterable[Tuple], force_overwrite: bool = False) -> result.MapResult:
        if force_overwrite:
            shortcuts.remove(map_id)

        utils.check_map_id(map_id)

        self._mkdirs(map_id)
        map_dir = utils.map_dir_path(map_id)

        fn_path = self._save_func(map_dir)
        hashes = self._save_inputs(map_dir, args_and_kwargs)
        self._save_hashes(map_dir, hashes)

        input_files = [
            fn_path.as_posix(),
            (map_dir / 'inputs' / '$(Item).in').as_posix(),
        ]
        output_remaps = [
            f'$(Item).out={(map_dir / "outputs" / "$(Item).out").as_posix()}',
        ]

        submit_dict = {
            'JobBatchName': map_id,
            'executable': (Path(__file__).parent / 'run' / 'run.py').as_posix(),
            'arguments': '$(Item)',
            'log': (map_dir / 'cluster_logs' / '$(ClusterId).log').as_posix(),
            'output': (map_dir / 'job_logs' / '$(Item).output').as_posix(),
            'error': (map_dir / 'job_logs' / '$(Item).error').as_posix(),
            'should_transfer_files': 'YES',
            'when_to_transfer_output': 'ON_EXIT',
            'request_cpus': '1',
            'request_memory': '100MB',
            'request_disk': '5GB',
            'transfer_input_files': ','.join(input_files),
            'transfer_output_remaps': f'"{";".join(output_remaps)}"'
        }
        sub = htcondor.Submit(dict(collections.ChainMap(self.submit_options, submit_dict)))

        self._save_submit(map_dir, sub)

        try:
            return self._submit(
                map_id = map_id,
                map_dir = map_dir,
                submit_object = sub,
                input_hashes = hashes,
            )
        except Exception as e:
            # something went wrong during submission, and the job is malformed
            # so delete the entire map directory
            # the condor bindings should prevent any jobs from being submitted
            shutil.rmtree(map_dir)
            raise e

    def _save_func(self, map_dir):
        fn_path = map_dir / 'fn.pkl'
        htio.save_object(self.func, fn_path)

        return fn_path

    @staticmethod
    def _save_inputs(map_dir: Path, args_and_kwargs) -> List[str]:
        hashes = []
        for a_and_k in args_and_kwargs:
            b = htio.to_bytes(a_and_k)
            h = htio.hash_bytes(b)
            hashes.append(h)

            input_path = map_dir / 'inputs' / f'{h}.in'
            htio.save_bytes(b, input_path)

        return hashes

    @staticmethod
    def _save_hashes(map_dir: Path, hashes: Iterable[str]):
        with (map_dir / 'hashes').open(mode = 'w') as file:
            file.write('\n'.join(hashes))

    @staticmethod
    def _save_submit(map_dir: Path, submit):
        htio.save_object(dict(submit), map_dir / 'submit')

    @staticmethod
    def _submit(map_id, map_dir, submit_object, input_hashes) -> result.MapResult:
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            submit_result = submit_object.queue_with_itemdata(txn, 1, iter(input_hashes))

            cluster_id = submit_result.cluster()

            with (map_dir / 'cluster_ids').open(mode = 'a') as file:
                file.write(str(cluster_id) + '\n')

            with (map_dir / 'cluster_ids').open() as file:
                cluster_ids = [int(cid.strip()) for cid in file]

            return result.MapResult(
                map_id = map_id,
                cluster_ids = cluster_ids,
                submit = submit_object,
                hashes = input_hashes,
            )


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


def htmap(*args, **submit_options) -> Union[Callable, HTMapper]:
    """
    A decorator that wraps a function in an :class:`HTMapper`,
    which provides an interface for mapping functions calls out to an HTCondor cluster.

    Parameters
    ----------

    Returns
    -------
    mapper
        An :class:`HTMapper` that wraps the function (or a wrapper function that does the wrapping).
    """

    def wrapper(func: Callable) -> HTMapper:
        # prevent nesting HTMappers inside each other by accident
        if isinstance(func, HTMapper):
            func = func.func

        return HTMapper(func, **submit_options)

    if len(args) == 0 and len(submit_options) >= 0:  # normal call
        return wrapper
    elif len(args) == 1 and len(submit_options) == 0:  # call without parens
        return wrapper(args[0])  # if no parens, args[0] is the function
    else:
        raise exceptions.HTMapException('incorrect syntax for htmap decorator')
