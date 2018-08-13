from typing import Any, Tuple, Iterable, Dict, Union, Optional, List, Callable, Iterator, Set

import collections
import datetime
import shutil
import enum
from pathlib import Path
import time
import itertools
from copy import deepcopy, copy

import htcondor
from tqdm import tqdm

from . import htio, utils
from .settings import settings
from . import exceptions


def map_dir_path(map_id: str) -> Path:
    return settings.HTMAP_DIR / settings.MAPS_DIR_NAME / map_id


def check_map_id(map_id: str):
    """Raise a :class:`htmap.exceptions.MapIDAlreadyExists` if the ``map_id`` already exists."""
    if map_dir_path(map_id).exists():
        raise exceptions.MapIDAlreadyExists(f'the requested map_id {map_id} already exists (recover the MapResult, then either use or delete it).')


class JobStatus(enum.IntEnum):
    IDLE = 1
    RUNNING = 2
    REMOVED = 3
    COMPLETED = 4
    HELD = 5
    TRANSFERRING_OUTPUT = 6
    SUSPENDED = 7

    def __str__(self):
        return JOB_STATUS_STRINGS[self]

    @classmethod
    def display_statuses(cls) -> Tuple['JobStatus', ...]:
        return (
            cls.HELD,
            cls.IDLE,
            cls.RUNNING,
            cls.COMPLETED,
        )


JOB_STATUS_STRINGS = {
    JobStatus.IDLE: 'Idle',
    JobStatus.RUNNING: 'Run',
    JobStatus.REMOVED: 'Removed',
    JobStatus.COMPLETED: 'Done',
    JobStatus.HELD: 'Held',
    JobStatus.TRANSFERRING_OUTPUT: 'Transferring Output',
    JobStatus.SUSPENDED: 'Suspended',
}


class MapResult:
    """
    Represents the results from a map call.
    """

    def __init__(self, map_id: str, cluster_ids: List[int], submit, hashes: Iterable[str]):
        """

        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this :class:`MapResult`.
        cluster_id
            The ``cluster_id`` for the jobs associated with this :class:`MapResult`.
            This is an implementation detail and should not be relied on.
        hashes
            The hashes of the inputs for this :class:`MapResult`.
            This is an implementation detail and should not be relied on.
        """
        self.map_id = map_id
        self.cluster_ids = cluster_ids
        self.submit = submit
        self.hashes = tuple(hashes)
        self.hash_set = set(self.hashes)

    @classmethod
    def recover(cls, map_id: str) -> 'MapResult':
        """
        Reconstruct a :class:`MapResult` from its ``map_id``.

        Parameters
        ----------
        map_id

        Returns
        -------
        result
            The result with the given ``map_id``.
        """
        map_dir = map_dir_path(map_id)
        try:
            with (map_dir / 'cluster_ids').open() as file:
                cluster_ids = [int(cid.strip()) for cid in file]

            with (map_dir / 'hashes').open() as file:
                hashes = tuple(h.strip() for h in file)

            submit = htcondor.Submit(htio.load_object(map_dir / 'submit'))

        except FileNotFoundError:
            raise exceptions.MapIDNotFound(f'the map_id {map_id} could not be found')

        return cls(
            map_id = map_id,
            cluster_ids = cluster_ids,
            submit = submit,
            hashes = hashes,
        )

    def __len__(self):
        """The length of a :class:`MapResult` is the number of inputs it contains."""
        return len(self.hashes)

    @property
    def _map_dir(self) -> Path:
        """The path to the map directory."""
        return map_dir_path(self.map_id)

    @property
    def _inputs_dir(self) -> Path:
        """The path to the inputs directory, inside the map directory."""
        return self._map_dir / 'inputs'

    @property
    def _outputs_dir(self) -> Path:
        """The path to the outputs directory, inside the map directory."""
        return self._map_dir / 'outputs'

    @property
    def _input_file_paths(self):
        """The paths to the input files."""
        yield from (self._inputs_dir / f'{h}.in' for h in self.hashes)

    @property
    def _output_file_paths(self):
        """The paths to the output files."""
        yield from (self._outputs_dir / f'{h}.out' for h in self.hashes)

    def __repr__(self):
        return f'<{self.__class__.__name__}(map_id = {self.map_id})>'

    def _item_to_hash(self, item: int) -> str:
        """Return the hash associated with an input index."""
        return self.hashes[item]

    def __getitem__(self, item: int) -> Any:
        """Return the output associated with the input index. Does not block."""
        return self.get(item, timeout = 0)

    def get(
        self,
        item: int,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
    ) -> Any:
        """
        Return the output associated with the input index.

        Parameters
        ----------
        item
            The index of the input to get the output for.
        timeout
            How long to wait for the output to exist before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        if isinstance(timeout, datetime.timedelta):
            timeout = timeout.total_seconds()

        h = self._item_to_hash(item)
        path = self._outputs_dir / f'{h}.out'

        try:
            utils.wait_for_path_to_exist(path, timeout)
        except exceptions.TimeoutError as e:
            if timeout <= 0:
                raise exceptions.OutputNotFound(f'output for hash {h} not found')
            else:
                raise e

        return htio.load_object(path)

    def wait(
        self,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
        show_progress_bar: bool = False,
    ) -> datetime.timedelta:
        """
        Wait until all output associated with this :class:`MapResult` is available.

        Parameters
        ----------
        timeout
            How long to wait for the map to complete before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        show_progress_bar
            If ``True``, a progress bar will be displayed.

        Returns
        -------
        elapsed_time
            The time elapsed from the beginning of the wait to the end.
        """
        t = datetime.datetime.now()
        start_time = time.time()
        if isinstance(timeout, datetime.timedelta):
            timeout = timeout.total_seconds()

        if show_progress_bar:
            pbar = tqdm(
                desc = self.map_id,
                total = len(self),
                unit = 'input',
                ncols = 80,
                ascii = True,
            )

            previous_pbar_len = 0

        expected_num_hashes = len(self)

        while True:
            num_missing_hashes = len(self._missing_hashes)
            if show_progress_bar:
                pbar_len = expected_num_hashes - num_missing_hashes
                pbar.update(pbar_len - previous_pbar_len)
                previous_pbar_len = pbar_len
            if num_missing_hashes == 0:
                break

            if timeout is not None and time.time() - timeout > start_time:
                raise exceptions.TimeoutError(f'timeout while waiting for {self}')

            time.sleep(1)

        if show_progress_bar:
            pbar.close()

        return datetime.datetime.now() - t

    @property
    def _missing_hashes(self) -> List[str]:
        done = set(f.stem for f in self._outputs_dir.iterdir())
        return [h for h in self.hashes if h not in done]

    @property
    def is_done(self) -> bool:
        return len(self._missing_hashes) == 0

    def __iter__(self) -> Iterable[Any]:
        """
        Iterating over the :class:`htmap.MapResult` yields the outputs in the same order as the inputs,
        waiting on each individual output to become available.
        """
        yield from self.iter()

    def iter(
        self,
        callback: Optional[Callable] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
    ) -> Iterator[Any]:
        """
        Returns an iterator over the output of the :class:`htmap.MapResult` in the same order as the inputs,
        waiting on each individual output to become available.

        Parameters
        ----------
        callback
            A function to call on each output as the iteration proceeds.
        timeout
            How long to wait for each output to be available before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        if callback is None:
            callback = lambda o: o

        for output_path in self._output_file_paths:
            utils.wait_for_path_to_exist(output_path, timeout)

            out = htio.load_object(output_path)
            callback(out)
            yield out

    def iter_with_inputs(
        self,
        callback: Optional[Callable] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
    ) -> Iterator[Tuple[Any, Any]]:
        """
        Returns an iterator over the inputs and output of the :class:`htmap.MapResult` in the same order as the inputs,
        waiting on each individual output to become available.

        Parameters
        ----------
        callback
            A function to call on each (input, output) pair as the iteration proceeds.
        timeout
            How long to wait for each output to be available before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        if callback is None:
            callback = lambda i, o: (i, o)

        for input_path, output_path in zip(self._input_file_paths, self._output_file_paths):
            utils.wait_for_path_to_exist(output_path, timeout)

            inp = htio.load_object(input_path)
            out = htio.load_object(output_path)
            callback(inp, out)
            yield inp, out

    def iter_as_available(
        self,
        callback: Optional[Callable] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
    ) -> Iterator[Any]:
        """
        Returns an iterator over the output of the :class:`htmap.MapResult`,
        yielding individual outputs as they become available.

        The iteration order is initially random, but is consistent within a single interpreter session once the map is completed.

        Parameters
        ----------
        callback
            A function to call on each output as the iteration proceeds.
        timeout
            How long to wait for the entire iteration to complete before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        if isinstance(timeout, datetime.timedelta):
            timeout = timeout.total_seconds()
        start_time = time.time()

        if callback is None:
            callback = lambda o: o

        paths = set(self._output_file_paths)
        while len(paths) > 0:
            for path in copy(paths):
                if not path.exists():
                    continue

                paths.remove(path)
                obj = htio.load_object(path)
                callback(obj)
                yield obj

            if time.time() > start_time + timeout:
                break

            time.sleep(1)

    def iter_as_available_with_inputs(
        self,
        callback: Optional[Callable] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
    ) -> Iterator[Tuple[Any, Any]]:
        """
        Returns an iterator over the inputs and output of the :class:`htmap.MapResult`,
        yielding individual ``(input, output)`` pairs as they become available.

        The iteration order is initially random, but is consistent within a single interpreter session once the map is completed.

        Parameters
        ----------
        callback
            A function to call on each ``(input, output)`` as the iteration proceeds.
        timeout
            How long to wait for the entire iteration to complete before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        if isinstance(timeout, datetime.timedelta):
            timeout = timeout.total_seconds()
        start_time = time.time()

        if callback is None:
            callback = lambda i, o: (i, o)

        paths = set((i, o) for i, o in zip(self._input_file_paths, self._output_file_paths))
        while len(paths) > 0:
            for input_output_paths in copy(paths):
                input_path, output_path = input_output_paths
                if not output_path.exists():
                    continue

                paths.remove(input_output_paths)
                inp = htio.load_object(input_path)
                out = htio.load_object(output_path)
                callback(inp, out)
                yield inp, out

            if time.time() > start_time + timeout:
                break

            time.sleep(1)

    @property
    def _requirements(self):
        return ' || '.join(f'ClusterId=={cid}' for cid in self.cluster_ids)

    def query(
        self,
        requirements: Optional[str] = None,
        projection: Optional[List[str]] = None,
    ):
        """
        Perform a query against the HTCondor cluster to get information about the map jobs.

        Parameters
        ----------
        requirements
            A ClassAd expression to use as the requirements for the query.
            In addition to whatever restrictions given in this expression, the query will only target the jobs for this map.
        projection
            The ClassAd attributes to return from the query.

        Returns
        -------

        """
        if self.cluster_ids is None:
            yield from ()
        if projection is None:
            projection = []

        requirements = self._requirements + (f' && {requirements}' if requirements is not None else '')
        yield from htcondor.Schedd().xquery(
            requirements = requirements,
            projection = projection,
        )

    def _status_counts(self) -> collections.Counter:
        query = self.query(projection = ['JobStatus'])
        counter = collections.Counter(JobStatus(classad['JobStatus']) for classad in query)

        # if the job has fully completed, we'll get zero for everything
        # so make sure the total makes sense
        counter[JobStatus.COMPLETED] += len(self) - sum(counter.values())

        return counter

    def status(self) -> str:
        """Return a string containing the number of jobs in each status."""
        counts = self._status_counts()
        stat = ' | '.join(f'{str(js)} = {counts[js]}' for js in JobStatus.display_statuses())
        msg = f'Map {self.map_id} ({len(self)} inputs): {stat}'

        return utils.rstr(msg)

    def hold_reasons(self) -> str:
        """Return a string containing a table showing any held jobs, along with their hold reasons."""
        query = self.query(
            requirements = f'JobStatus=={JobStatus.HELD}',
            projection = ['ProcId', 'HoldReason', 'HoldReasonCode']
        )

        return utils.table(
            headers = ['Input Index', 'Hold Reason Code', 'Hold Reason'],
            rows = [
                [classad['ProcId'], classad['HoldReasonCode'], classad['HoldReason']]
                for classad in query
            ]
        )

    def act(self, action: htcondor.JobAction):
        return htcondor.Schedd().act(action, self._requirements)

    def remove(self):
        """Permanently remove the map's jobs and delete all associated input and output files."""
        act_result = self._remove()
        self._rm_map_dir()
        return act_result

    def _remove(self):
        return self.act(htcondor.JobAction.Remove)

    def _rm_map_dir(self):
        shutil.rmtree(self._map_dir)

    def _clean_outputs_dir(self):
        utils.clean_dir(self._outputs_dir)

    def hold(self):
        """Temporarily remove the map's jobs from the queue, until they are released."""
        return self.act(htcondor.JobAction.Hold)

    def release(self):
        """Releases held map jobs back into the queue."""
        return self.act(htcondor.JobAction.Release)

    def pause(self):
        return self.act(htcondor.JobAction.Suspend)

    def resume(self):
        return self.act(htcondor.JobAction.Continue)

    def vacate(self):
        """Force the map's jobs to give up their currently claimed execute nodes."""
        return self.act(htcondor.JobAction.Vacate)

    def edit(self, attr: str, value: str):
        return htcondor.Schedd().act(self._requirements, attr, value)

    def _iter_output(self, item: int) -> Iterator[str]:
        h = self._item_to_hash(item)
        with (self._map_dir / 'job_logs' / f'{h}.output').open() as file:
            yield from file

    def _iter_error(self, item: int) -> Iterator[str]:
        h = self._item_to_hash(item)
        with (self._map_dir / 'job_logs' / f'{h}.error').open() as file:
            yield from file

    def output(self, item: int) -> str:
        """Return a string containing the stdout of a completed map job."""
        return utils.rstr(''.join(self._iter_output(item)))

    def error(self, item: int) -> str:
        """Return a string containing the stderr of a completed map job."""
        return utils.rstr(''.join(self._iter_error(item)))

    def tail(self):
        """Stream any new text added to the map's most recent cluster log file."""
        with (self._map_dir / 'cluster_logs' / f'{self.cluster_ids[-1]}.log').open() as file:
            file.seek(0, 2)
            while True:
                current = file.tell()
                line = file.readline()
                if line == '':
                    file.seek(current)
                    time.sleep(.1)
                else:
                    print(line, end = '')

    def rerun(self):
        self._clean_outputs_dir()

        self.rerun_incomplete()

    def rerun_incomplete(self):
        self._remove()

        missing_hashes = self._missing_hashes

        return HTMapper._submit(self.map_id, self._map_dir, self.submit, missing_hashes)


class MapBuilder:
    def __init__(self, mapper: 'HTMapper', map_id: str):
        self.mapper = mapper
        self.map_id = map_id

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
        )

    def __call__(self, *args, **kwargs):
        self.args.append(args)
        self.kwargs.append(kwargs)

    @property
    def result(self) -> MapResult:
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
        for path in (map_dir_path(map_id) / dir_name for dir_name in self._map_dir_names):
            path.mkdir(parents = True, exist_ok = True)

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func})>'

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def map(self, map_id: str, args, **kwargs) -> MapResult:
        args = ((arg,) for arg in args)
        args_and_kwargs = zip(args, itertools.repeat(kwargs))
        return self._map(map_id, args_and_kwargs)

    def productmap(self, map_id: str, *args, **kwargs) -> MapResult:
        dicts = [{}]
        for key, values in kwargs.items():
            values = tuple(values)
            dicts = [deepcopy(d) for d in dicts for _ in range(len(values))]
            for d, v in zip(dicts, itertools.cycle(values)):
                d[key] = v

        args = itertools.repeat(args)
        args_and_kwargs = zip(args, dicts)

        return self._map(map_id, args_and_kwargs)

    def starmap(self, map_id: str, args: Optional[Iterable[Tuple]] = None, kwargs: Optional[Iterable[Dict]] = None) -> MapResult:
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = ()

        args_and_kwargs = zip_args_and_kwargs(args, kwargs)
        return self._map(map_id, args_and_kwargs)

    def build_map(self, map_id: str):
        """
        Return a :class:`htmap.MapBuilder` for the wrapped function.

        Parameters
        ----------
        map_id

        """
        return MapBuilder(mapper = self, map_id = map_id)

    def _map(self, map_id: str, args_and_kwargs: Iterable[Tuple]) -> MapResult:
        check_map_id(map_id)

        self._mkdirs(map_id)
        map_dir = map_dir_path(map_id)

        fn_path = self._save_func(map_dir)
        hashes = self._save_inputs(map_dir, args_and_kwargs)
        self._save_hashes(map_dir, hashes)

        submit_dict = {
            'JobBatchName': map_id,
            'executable': str(Path(__file__).parent / 'run' / 'run.sh'),
            'arguments': '$(Item)',
            'log': str(map_dir / 'cluster_logs' / '$(ClusterId).log'),
            'output': str(map_dir / 'job_logs' / '$(Item).output'),
            'error': str(map_dir / 'job_logs' / '$(Item).error'),
            'should_transfer_files': 'YES',
            'when_to_transfer_output': 'ON_EXIT',
            'request_cpus': '1',
            'request_memory': '100MB',
            'request_disk': '5GB',
            'transfer_input_files': ','.join([
                'http://proxy.chtc.wisc.edu/SQUID/karpel/htmap.tar.gz',
                str(Path(__file__).parent / 'run' / 'run.py'),
                str(map_dir / 'inputs' / '$(Item).in'),
                str(fn_path),
            ]), 'transfer_output_remaps': '"' + ';'.join([
                f'$(Item).out={map_dir / "outputs" / "$(Item).out"}',
            ]) + '"'
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
    def _submit(map_id, map_dir, submit_object, input_hashes) -> MapResult:
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            submit_result = submit_object.queue_with_itemdata(txn, 1, iter(input_hashes))

            cluster_id = submit_result.cluster()

            with (map_dir / 'cluster_ids').open(mode = 'a') as file:
                file.write(str(cluster_id))

            with (map_dir / 'cluster_ids').open() as file:
                cluster_ids = [int(cid.strip()) for cid in file]

            return MapResult(
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
        # can't nest HTMappers inside each other by accident
        if isinstance(func, HTMapper):
            func = func.func

        return HTMapper(func, **submit_options)

    if len(args) == 0 and len(submit_options) >= 0:  # normal call
        return wrapper
    elif len(args) == 1 and len(submit_options) == 0:  # call without parens
        return wrapper(args[0])  # if no parens, args[0] is the function
    else:
        raise exceptions.HTMapException('incorrect syntax for htmap decorator')
