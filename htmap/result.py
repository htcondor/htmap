from typing import Tuple, List, Iterable, Any, Optional, Union, Callable, Iterator

import datetime
import enum
import shutil
import time
import collections
from copy import copy
from pathlib import Path

from tqdm import tqdm

import htcondor

from . import htio, exceptions, utils, mapper


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
    The constructor is documented here, but you should never need to build a :class:`MapResult` manually.
    Instead, you'll get your :class:`MapResult` by calling a :class:`HTMapper` method or by using :func:`htmap.recover`.
    """

    def __init__(self, map_id: str, cluster_ids: List[int], submit, hashes: Iterable[str]):
        """
        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this :class:`MapResult`.
        cluster_ids
            All of the ``cluster_id`` for the jobs associated with this :class:`MapResult`.
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
        map_dir = utils.map_dir_path(map_id)
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
        return utils.map_dir_path(self.map_id)

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
    def _completed_hashes(self) -> List[str]:
        done = set(f.stem for f in self._outputs_dir.iterdir())
        return [h for h in self.hashes if h in done]

    @property
    def is_done(self) -> bool:
        """``True`` if all of the output is available for this map."""
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

        paths = set(zip(self._input_file_paths, self._output_file_paths))
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
        return f"({' || '.join(f'ClusterId=={cid}' for cid in self.cluster_ids)})"

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
        # so make sure the completed count makes sense
        counter[JobStatus.COMPLETED] = len(self._completed_hashes)

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
        """Permanently remove the map and delete all associated input and output files."""
        self._remove_from_queue()
        self._rm_map_dir()

    def _remove_from_queue(self):
        return self.act(htcondor.JobAction.Remove)

    def _rm_map_dir(self):
        shutil.rmtree(self._map_dir)

    def _clean_outputs_dir(self):
        utils.clean_dir(self._outputs_dir)

    def hold(self):
        """Temporarily remove the map from the queue, until it is released."""
        return self.act(htcondor.JobAction.Hold)

    def release(self):
        """Releases a held map back into the queue."""
        return self.act(htcondor.JobAction.Release)

    def pause(self):
        return self.act(htcondor.JobAction.Suspend)

    def resume(self):
        return self.act(htcondor.JobAction.Continue)

    def vacate(self):
        """Force the map to give up any claimed resources."""
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
        """
        Return a string containing the stdout from a single completed map argument.
        The argument is the index of the input in the original arguments to the map.
        """
        return utils.rstr(''.join(self._iter_output(item)))

    def error(self, item: int) -> str:
        """
        Return a string containing the stderr from a single completed map argument.
        The argument is the index of the input in the original arguments to the map.
        """
        return utils.rstr(''.join(self._iter_error(item)))

    def tail(self):
        """Stream any new text added to the map's most recent log file."""
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
        """Reruns the entire map from scratch."""
        self._clean_outputs_dir()

        return self.rerun_incomplete()

    def rerun_incomplete(self):
        """Rerun any incomplete parts of the map from scratch."""
        self._remove_from_queue()

        missing_hashes = self._missing_hashes

        dummy = mapper.HTMapper._submit(self.map_id, self._map_dir, self.submit, missing_hashes)

        self.cluster_ids.append(dummy.cluster_ids[-1])
