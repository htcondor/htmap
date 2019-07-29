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

from typing import Tuple, List, Iterable, Any, Optional, Callable, Iterator, Dict, Set, Mapping
import logging

import datetime
import shutil
import time
import uuid
import functools
import inspect
import collections
import collections.abc
import weakref
from copy import copy
from pathlib import Path

from tqdm import tqdm

import htcondor
import classad

from . import htio, state, tags, errors, holds, mapping, settings, utils, names, exceptions

logger = logging.getLogger(__name__)


def _protector(method):
    @functools.wraps(method)
    def _protect(self, *args, **kwargs):
        if self.is_removed:
            raise exceptions.MapWasRemoved(f'cannot call {method} for map {self.tag} because it has been removed')
        return method(self, *args, **kwargs)

    return _protect


def _protect_map_after_remove(result_class):
    # decorate all public instance methods
    for key, member in inspect.getmembers(result_class, predicate = inspect.isfunction):
        if not key.startswith('_'):
            setattr(result_class, key, _protector(member))

    return result_class


# this set is used in Map.load to make Maps singletons
MAPS: Set['Map'] = weakref.WeakSet()


def maps_by_tag() -> Dict[str, 'Map']:
    """
    Get the current mapping of tags to map objects.

    Don't try to cache the results of this function; always get it fresh.
    """
    return {map.tag: map for map in MAPS}


@_protect_map_after_remove
class Map(collections.abc.Sequence):
    """
    Represents the results from a map call.

    .. warning ::

        You should never instantiate a :class:`Map` directly!
        Instead, you'll get your :class:`Map` by calling a top-level mapping function like :func:`htmap.map`, a :class:`MappedFunction` mapping method, or by using :func:`htmap.load`.
        We are not responsible for whatever vile contraption you build if you bypass the correct methods!

    """

    def __init__(
        self,
        tag: str,
        map_dir: Path,
        cluster_ids: Iterable[int],
        num_components: int,
    ):
        self.tag = tag

        self._map_dir = map_dir
        self._cluster_ids = list(cluster_ids)
        self._num_components = num_components

        self._state = state.MapState(self)
        self._local_data = None

        self._stdout: MapStdOut = MapStdOut(self)
        self._stderr: MapStdErr = MapStdErr(self)
        self._output_files: MapOutputFiles = MapOutputFiles(self)

        MAPS.add(self)

    @classmethod
    def load(cls, tag: str) -> 'Map':
        """
        Load a :class:`Map` by looking up its ``tag``.

        Raises :class:`htmap.exceptions.TagNotFound` if the ``tag`` does not exist.

        Parameters
        ----------
        tag
            The ``tag`` to search for.

        Returns
        -------
        map
            The map with the given ``tag``.
        """
        try:
            # if we already have this map in memory, return that object instead
            return maps_by_tag()[tag]
        except KeyError:
            try:
                uid = uuid.UUID(tags.tag_file_path(tag).read_text())
            except FileNotFoundError:
                raise exceptions.TagNotFound(f'the tag {tag} could not be found')

            map_dir = mapping.map_dir_path(uid)
            with (map_dir / names.CLUSTER_IDS).open() as file:
                cluster_ids = [int(cid.strip()) for cid in file]

            num_components = htio.load_num_components(map_dir)

            logger.debug(f'loaded map result for map {tag} from {map_dir}')

            return cls(
                tag = tag,
                map_dir = map_dir,
                cluster_ids = cluster_ids,
                num_components = num_components,
            )

    def __repr__(self):
        return f'{self.__class__.__name__}(tag = {self.tag})'

    def __gt__(self, other):
        return self.tag > other.tag

    def __lt__(self, other):
        return self.tag < other.tag

    def __ge__(self, other):
        return self.tag >= other.tag

    def __le__(self, other):
        return self.tag <= other.tag

    def __len__(self):
        """The length of a :class:`Map` is the number of components it contains."""
        return self._num_components

    @property
    def _tag_file_path(self) -> Path:
        return tags.tag_file_path(self.tag)

    @property
    def _inputs_dir(self) -> Path:
        """The path to the inputs directory, inside the map directory."""
        return self._map_dir / names.INPUTS_DIR

    @property
    def _outputs_dir(self) -> Path:
        """The path to the outputs directory, inside the map directory."""
        return self._map_dir / names.OUTPUTS_DIR

    def _input_file_path(self, component: int) -> Path:
        return self._inputs_dir / f'{component}.{names.INPUT_EXT}'

    def _output_file_path(self, component: int) -> Path:
        return self._outputs_dir / f'{component}.{names.OUTPUT_EXT}'

    @property
    def _job_logs_dir(self) -> Path:
        return self._map_dir / names.JOB_LOGS_DIR

    def _stdout_file_path(self, component: int) -> Path:
        return self._job_logs_dir / f'{component}.{names.STDOUT_EXT}'

    def _stderr_file_path(self, component: int) -> Path:
        return self._job_logs_dir / f'{component}.{names.STDERR_EXT}'

    @property
    def _output_files_dir(self):
        return self._map_dir / names.OUTPUT_FILES_DIR

    def _output_files_path(self, component: int) -> Path:
        return self._output_files_dir / str(component)

    @property
    def _input_file_paths(self):
        """The paths to the input files."""
        yield from (self._input_file_path(idx) for idx in self.components)

    @property
    def _output_file_paths(self):
        """The paths to the output files."""
        yield from (self._output_file_path(idx) for idx in self.components)

    @property
    def components(self) -> Tuple[int]:
        """Return a tuple containing the component indices for the :class:`htmap.Map`."""
        return tuple(range(self._num_components))

    @property
    def is_done(self) -> bool:
        """``True`` if all of the output is available for this map."""
        return all(cs is state.ComponentStatus.COMPLETED for cs in self.component_statuses)

    @property
    def is_active(self) -> bool:
        """``True`` if any map components are not complete (or errored!)."""
        return any(cs not in (state.ComponentStatus.COMPLETED, state.ComponentStatus.ERRORED) for cs in self.component_statuses)

    def wait(
        self,
        timeout: utils.Timeout = None,
        show_progress_bar: bool = False,
        holds_ok = False,
        errors_ok = False,
    ) -> None:
        """
        Wait until all output associated with this :class:`Map` is available.

        If any components in the map are held or experience an execution error,
        this method will raise an exception (:class:`htmap.exceptions.MapComponentHeld`
        or :class:`htmap.exceptions.MapComponentError`, respectively).

        Parameters
        ----------
        timeout
            How long to wait for the map to complete before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        show_progress_bar
            If ``True``, a progress bar will be displayed.
        holds_ok
            If ``True``, will not raise exceptions if components are held.
        errors_ok
            If ``True``, will not raise exceptions if components experience execution errors.
        """
        start_time = time.time()
        timeout = utils.timeout_to_seconds(timeout)

        try:
            if show_progress_bar:
                pbar = tqdm(
                    desc = self.tag,
                    total = len(self),
                    unit = 'component',
                    ascii = True,
                )

                previous_pbar_len = 0

            ok_statuses = [state.ComponentStatus.COMPLETED]
            if holds_ok:
                ok_statuses.append(state.ComponentStatus.HELD)
            if errors_ok:
                ok_statuses.append(state.ComponentStatus.ERRORED)

            while True:
                num_incomplete = sum(
                    cs not in ok_statuses
                    for cs in self.component_statuses
                )
                if show_progress_bar:
                    pbar_len = self._num_components - num_incomplete
                    pbar.update(pbar_len - previous_pbar_len)
                    previous_pbar_len = pbar_len
                if num_incomplete == 0:
                    break

                for component, status in enumerate(self.component_statuses):
                    if status is state.ComponentStatus.HELD and not holds_ok:
                        raise exceptions.MapComponentHeld(f'component {component} of map {self.tag} was held: {self.holds[component]}')
                    elif status is state.ComponentStatus.ERRORED and not errors_ok:
                        raise exceptions.MapComponentError(f'component {component} of map {self.tag} encountered error while executing. Error report:\n{self._load_error(component).report()}')

                if timeout is not None and time.time() - timeout > start_time:
                    raise exceptions.TimeoutError(f'timeout while waiting for {self}')

                time.sleep(settings['WAIT_TIME'])
        finally:
            if show_progress_bar:
                pbar.close()

    def _wait_for_component(self, component: int, timeout: utils.Timeout = None) -> None:
        """
        Wait for a map component to terminate, which could either be because it
        completes successfully or encounters an error during execution.
        """
        timeout = utils.timeout_to_seconds(timeout)
        start_time = time.time()
        while True:
            component_status = self.component_statuses[component]
            if component_status in (state.ComponentStatus.COMPLETED, state.ComponentStatus.ERRORED):
                break
            elif component_status is state.ComponentStatus.HELD:
                raise exceptions.MapComponentHeld(f'component {component} of map {self.tag} is held: {self.holds[component]}')

            if timeout is not None and (time.time() >= start_time + timeout):
                if timeout <= 0:
                    raise exceptions.OutputNotFound(f'output for component {component} of map {self.tag} not found')
                else:
                    raise exceptions.TimeoutError(f'timed out while waiting for component {component} of map {self.tag}')

            time.sleep(settings['WAIT_TIME'])

    def _load_input(self, component: int) -> Tuple[Tuple[Any], Dict[str, Any]]:
        return htio.load_object(self._input_file_path(component))

    def _peek_status(
        self,
        component: int,
    ) -> str:
        try:
            return htio.load_object(self._output_file_path(component))
        except FileNotFoundError as e:
            raise exceptions.OutputNotFound(f'output for component {component} of map {self.tag} not found') from e

    def _load_output(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> Any:
        """
        Try to load a map component as if it succeeded.
        If the component actually failed, raise :class:`MapComponentError`.
        """
        if component not in range(0, len(self)):
            raise IndexError(f'tried to get output for component {component}, but map {self.map} only has {len(self.map)} components')

        self._wait_for_component(component, timeout)

        status_and_result = htio.load_objects(self._output_file_path(component))
        status = next(status_and_result)
        if status == 'OK':
            return next(status_and_result)
        elif status == 'ERR':
            raise exceptions.MapComponentError(f'component {component} of map {self.tag} encountered error while executing. Error report:\n{self._load_error(component).report()}')
        else:
            raise exceptions.InvalidOutputStatus(f'output status {status} is not valid')

    def _load_error(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> errors.ComponentError:
        """
        Try to load a map component as if it failed.
        If the component actually succeeded, raise :class:`ExpectedError`.
        """
        self._wait_for_component(component, timeout)

        status_and_raw_error = htio.load_objects(self._output_file_path(component))
        status = next(status_and_raw_error)
        if status == 'OK':
            raise exceptions.ExpectedError(f'tried to load component {component} as an error, but it succeeded')
        elif status == 'ERR':
            return errors.ComponentError._from_raw_error(self, next(status_and_raw_error))
        else:
            raise exceptions.InvalidOutputStatus(f'output status {status} is not valid')

    def get(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> Any:
        """
        Return the output associated with the input component index.
        If the component experienced an execution error, this will raise :class:`htmap.exceptions.MapComponentError`.
        Use :meth:`get_err`, :meth:`errors`, :meth:`error_reports` to see what went wrong!

        Parameters
        ----------
        component
            The index of the input to get the output for.
        timeout
            How long to wait for the output to exist before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        return self._load_output(component, timeout = timeout)

    def __getitem__(self, item: int) -> Any:
        """Return the output associated with the input index. Does not block."""
        return self.get(item, timeout = 0)

    def get_err(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> errors.ComponentError:
        """
        Return the error associated with the input component index.
        If the component actually succeeded, this will raise :class:`htmap.exceptions.ExpectedError`.

        Parameters
        ----------
        component
            The index of the input to get the output for.
        timeout
            How long to wait for the output to exist before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        """
        return self._load_error(component, timeout = timeout)

    def __iter__(self) -> Iterable[Any]:
        """
        Iterating over the :class:`htmap.Map` yields the outputs in the same order as the inputs,
        waiting on each individual output to become available.
        """
        yield from self.iter()

    def iter(
        self,
        callback: Optional[Callable] = None,
        timeout: utils.Timeout = None,
    ) -> Iterator[Any]:
        """
        Returns an iterator over the output of the :class:`htmap.Map` in the same order as the inputs,
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

        for component in self.components:
            output = self._load_output(component, timeout = timeout)
            callback(output)
            yield output

    def iter_with_inputs(
        self,
        callback: Optional[Callable] = None,
        timeout: utils.Timeout = None,
    ) -> Iterator[Tuple[Tuple[tuple, Dict[str, Any]], Any]]:
        """
        Returns an iterator over the inputs and output of the :class:`htmap.Map` in the same order as the inputs,
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

        for component in self.components:
            output = self._load_output(component, timeout = timeout)
            input = self._load_input(component)
            callback(input, output)
            yield input, output

    def iter_as_available(
        self,
        callback: Optional[Callable] = None,
        timeout: utils.Timeout = None,
    ) -> Iterator[Any]:
        """
        Returns an iterator over the output of the :class:`htmap.Map`,
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
        timeout = utils.timeout_to_seconds(timeout)
        start_time = time.time()

        if callback is None:
            callback = lambda o: o

        remaining_indices = set(self.components)
        while len(remaining_indices) > 0:
            for component in copy(remaining_indices):
                try:
                    output = self._load_output(component, timeout = 0)
                    remaining_indices.remove(component)
                    callback(output)
                    yield output
                except exceptions.OutputNotFound:
                    pass

            if timeout is not None and time.time() > start_time + timeout:
                raise exceptions.TimeoutError('timed out while waiting for more output')

            time.sleep(settings['WAIT_TIME'])

    def iter_as_available_with_inputs(
        self,
        callback: Optional[Callable] = None,
        timeout: utils.Timeout = None,
    ) -> Iterator[Tuple[Tuple[tuple, Dict[str, Any]], Any]]:
        """
        Returns an iterator over the inputs and output of the :class:`htmap.Map`,
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
        timeout = utils.timeout_to_seconds(timeout)
        start_time = time.time()

        if callback is None:
            callback = lambda i, o: (i, o)

        remaining_indices = set(self.components)
        while len(remaining_indices) > 0:
            for component in copy(remaining_indices):
                try:
                    output = self._load_output(component, timeout = 0)
                    input = self._load_input(component)
                    remaining_indices.remove(component)
                    callback(input, output)
                    yield input, output
                except exceptions.OutputNotFound:
                    pass

            if timeout is not None and time.time() > start_time + timeout:
                raise exceptions.TimeoutError('timed out while waiting for more output')

            time.sleep(settings['WAIT_TIME'])

    def iter_inputs(self) -> Iterator[Any]:
        """Returns an iterator over the inputs of the :class:`htmap.Map`."""
        return (self._load_input(idx) for idx in self.components)

    def _requirements(self, requirements: Optional[str] = None) -> str:
        """Build an HTCondor requirements expression that captures all of the ``cluster_id`` for this map."""
        base = f"({' || '.join(f'ClusterId=={cid}' for cid in self._cluster_ids)})"
        extra = f' && {requirements}' if requirements is not None else ''

        return base + extra

    def _query(
        self,
        requirements: Optional[str] = None,
        projection: Optional[List[str]] = None,
    ) -> Iterator[classad.ClassAd]:
        """
        Perform a _query against the HTCondor cluster to get information about the map jobs.

        Parameters
        ----------
        requirements
            A ClassAd expression to use as the requirements for the _query.
            In addition to whatever restrictions given in this expression, the _query will only target the jobs for this map.
        projection
            The ClassAd attributes to return from the _query.

        Returns
        -------
        classads :
            An iterator of matching :class:`classad.ClassAd`, with only the projected fields.
        """
        if self._cluster_ids is None:
            yield from ()
        if projection is None:
            projection = []

        req = self._requirements(requirements)

        schedd = mapping.get_schedd()
        q = schedd.xquery(
            requirements = req,
            projection = projection,
        )

        logger.debug(f'queried for map {self.tag} (requirements = "{req}") with projection {projection}')

        yield from q

    @property
    def component_statuses(self) -> List[state.ComponentStatus]:
        """
        Return the current :class:`state.ComponentStatus` of each component in the map.
        """
        return self._state.component_statuses

    def components_by_status(self) -> Mapping[state.ComponentStatus, Tuple[int]]:
        """
        Return the component indices grouped by their states.
        """
        status_to_components = collections.defaultdict(lambda: [])
        for component, status in enumerate(self.component_statuses):
            status_to_components[status].append(component)

        status_to_components = {status: tuple(sorted(components)) for status, components in status_to_components.items()}

        return status_to_components

    def status(self) -> str:
        """Return a string containing the number of jobs in each status."""
        counts = collections.Counter(self.component_statuses)
        stat = ' | '.join(f'{str(js)} = {counts[js]}' for js in state.ComponentStatus.display_statuses())
        msg = f'{self.__class__.__name__} {self.tag} ({len(self)} components): {stat}'

        return utils.rstr(msg)

    @property
    def holds(self) -> Dict[int, holds.ComponentHold]:
        """
        A dictionary of component indices to their :class:`Hold` (if they are held).
        """
        return self._state.holds

    def hold_report(self) -> str:
        """
        Return a string containing a formatted table describing any held components.
        """
        headers = ['Component', 'Code', 'Hold Reason']
        rows = [
            (component, hold.code, hold.reason)
            for component, hold in self.holds.items()
        ]

        return utils.table(
            headers = headers,
            rows = rows,
            alignment = {
                'Component': 'ljust',
                'Hold Reason': 'ljust',
            }
        )

    @property
    def errors(self) -> Dict[int, errors.ComponentError]:
        """
        A dictionary of component indices to their :class:`ExecutionError`
        (if that component experienced an error).
        """
        err = {}
        for idx in self.components:
            try:
                err[idx] = self.get_err(idx)
            except (exceptions.OutputNotFound,
                    exceptions.ExpectedError,
                    exceptions.MapComponentHeld) as e:
                pass

        return err

    def error_reports(self) -> Iterator[str]:
        """
        Yields the error reports for any components that experienced an error during execution.
        """
        for idx in self.components:
            try:
                yield self.get_err(idx, timeout = 0).report()
            except (exceptions.OutputNotFound,
                    exceptions.ExpectedError,
                    exceptions.TimeoutError,
                    exceptions.MapComponentHeld) as e:
                pass

    @property
    def memory_usage(self) -> List[int]:
        """
        Return the latest peak memory usage of each map component, measured in MB.
        A component that hasn't reported yet will show a ``0``.

        .. warning::
            Due to current limitations in the HTCondor Python bindings, memory use for very short-lived components (<5 seconds) will not be accurate.
        """
        return self._state.memory_usage

    @property
    def runtime(self) -> List[datetime.timedelta]:
        """Return the total runtime (user + system) of each component."""
        return self._state.runtime

    @property
    def local_data(self) -> int:
        """Return the number of bytes stored on the local disk by the map."""
        # this cache is invalidated by the state reader loop when appropriate
        if self._local_data is None:
            self._local_data = utils.get_dir_size(self._map_dir, safe = False)
        return self._local_data

    def _act(
        self,
        action: htcondor.JobAction,
        requirements: Optional[str] = None,
    ) -> classad.ClassAd:
        """Perform an action on all of the jobs associated with this map."""
        if not self.is_active:
            return classad.ClassAd()

        schedd = mapping.get_schedd()
        req = self._requirements(requirements)
        a = schedd.act(action, req)

        logger.debug(f'acted on map {self.tag} (requirements = "{req}") with action {action}')

        return a

    def remove(self, force: bool = False) -> None:
        """
        Permanently remove the map and delete all associated input, output, and metadata files.

        Parameters
        ----------
        force
            If ``True``, do not wait for HTCondor to confirm that all map components have been removed.
        """
        self._remove_from_queue()
        self._cleanup_local_data(force = force)
        MAPS.remove(self)

        logger.info(f'removed map {self.tag}')

    def _remove_from_queue(self) -> classad.ClassAd:
        return self._act(htcondor.JobAction.Remove)

    def _cleanup_local_data(self, force: bool = False) -> None:
        """
        Remove all of the local data associated with this map.

        Parameters
        ----------
        force
            If ``True``, do not wait for HTCondor to confirm that all map components have been removed.
        """
        if not force:
            while not all(
                cs in (state.ComponentStatus.REMOVED, state.ComponentStatus.COMPLETED, state.ComponentStatus.ERRORED)
                for cs in self.component_statuses
            ):
                time.sleep(.01)

        shutil.rmtree(self._map_dir)
        logger.debug(f'removed map directory for map {self.tag}')

        self._tag_file_path.unlink()
        logger.debug(f'removed tag file for map {self.tag}')

    @property
    def is_removed(self) -> bool:
        return not self._map_dir.exists()

    def hold(self) -> None:
        """Temporarily remove the map from the queue, until it is released."""
        self._act(htcondor.JobAction.Hold)
        logger.debug(f'held map {self.tag}')

    def release(self) -> None:
        """Releases a held map back into the queue."""
        self._act(htcondor.JobAction.Release)
        logger.debug(f'released map {self.tag}')

    def pause(self) -> None:
        """Pause the map."""
        self._act(htcondor.JobAction.Suspend)
        logger.debug(f'paused map {self.tag}')

    def resume(self) -> None:
        """Resume the map from a paused state."""
        self._act(htcondor.JobAction.Continue)
        logger.debug(f'resumed map {self.tag}')

    def vacate(self) -> None:
        """Force the map to give up any claimed resources."""
        self._act(htcondor.JobAction.Vacate)
        logger.debug(f'vacated map {self.tag}')

    def _edit(self, attr: str, value: str, requirements: Optional[str] = None) -> None:
        if not self.is_active:
            return

        schedd = mapping.get_schedd()
        schedd.edit(self._requirements(requirements), attr, value)

        logger.debug(f'set attribute {attr} for map {self.tag} to {value}')

    def set_memory(self, memory: int) -> None:
        """
        Change the amount of memory (RAM) each map component needs.

        .. warning::

            This doesn't change anything for map components that have already started running,
            so you may need to hold and release your map to propagate this change.

        Parameters
        ----------
        memory
            The amount of memory (RAM) to request, as an integer number of MB.
        """
        self._edit('RequestMemory', str(memory))

    def set_disk(self, disk: int) -> None:
        """
        Change the amount of disk space each map component needs.

        .. warning::

            This doesn't change anything for map components that have already started running,
            so you may need to hold and release your map to propagate this change.

        Parameters
        ----------
        disk
            The amount of disk space to request, as an integer number of KB.
        """
        self._edit('RequestDisk', str(disk))

    def rerun(self, components: Optional[Iterable[int]] = None):
        """
        Re-run part of a map from scratch.
        The components must be completed or errored.
        Their existing output will be deleted before the re-run is executed.

        Parameters
        ----------
        components
            The components to rerun.
            If ``None``, the entire map will be re-run.
        """
        if components is None:
            components = self.components
        components = set(components)

        legal_components = set(self.components)
        bad_components = components.difference(legal_components)
        if len(bad_components) > 0:
            raise exceptions.CannotRerunComponents(f'cannot rerun components {bad_components} because they are not in the map')

        cant_be_rerun = {
            c for c, status in enumerate(self.component_statuses)
            if status not in (state.ComponentStatus.COMPLETED, state.ComponentStatus.ERRORED)
        }
        intersection = components.intersection(cant_be_rerun)
        if len(intersection) != 0:
            raise exceptions.CannotRerunComponents(f'cannot rerun components {sorted(intersection)} of map {self.tag} because they are not complete')

        components = sorted(components)
        for path in (self._output_file_path(c) for c in components):
            if path.exists():
                path.unlink()

        itemdata = htio.load_itemdata(self._map_dir)
        new_itemdata = [item for item in itemdata if int(item['component']) in components]

        submit_obj = htio.load_submit(self._map_dir)

        new_cluster_id = mapping.execute_submit(
            submit_obj,
            new_itemdata,
        )

        self._cluster_ids.append(new_cluster_id)
        with (self._map_dir / names.CLUSTER_IDS).open(mode = 'a') as f:
            f.write(str(new_cluster_id) + '\n')

        logger.debug(f'resubmitted {len(new_itemdata)} inputs from map {self.tag}')

    def retag(self, tag: str):
        """
        Give this map a new ``tag``.
        The old ``tag`` will be available for re-use immediately.

        Retagging a map makes it not transient.

        Parameters
        ----------
        tag
            The ``tag`` to assign to this map.
        """
        if tag == self.tag:
            raise exceptions.CannotRetagMap('cannot retag a map to the same tag it already has')

        try:
            tags.raise_if_tag_is_invalid(tag)
            tags.raise_if_tag_already_exists(tag)
        except (exceptions.InvalidTag, exceptions.TagAlreadyExists) as e:
            raise exceptions.CannotRetagMap(f'cannot retag map because of previous exception: {e}') from e

        submit_obj = htio.load_submit(self._map_dir)
        submit_obj['JobBatchName'] = tag
        htio.save_submit(self._map_dir, submit_obj)

        # self._edit('JobBatchName', tag)  # todo: this doesn't seem to work as expected

        self._tag_file_path.rename(tags.tag_file_path(tag))
        self._make_persistent()

        # must do this after everything else, because some of the things above
        # reference paths based on the tag
        self.tag = tag

    @property
    def _transient_marker(self) -> Path:
        return self._map_dir / names.TRANSIENT_MARKER

    @property
    def is_transient(self) -> bool:
        """``True`` is the map is transient, ``False`` otherwise."""
        return self._transient_marker.exists()

    def _make_transient(self):
        self._transient_marker.touch(exist_ok = True)

    def _make_persistent(self):
        if self.is_transient:
            self._transient_marker.unlink()

    @property
    def stdout(self) -> 'MapStdOut':
        """
        A sequence containing the ``stdout`` for each map component.
        You can index into it (with a component index) to get the
        ``stdout`` for that component, or iterate over the sequence to
        get all of the ``stdout`` from the map.
        """
        return self._stdout

    @property
    def stderr(self) -> 'MapStdErr':
        """
        A sequence containing the ``stderr`` for each map component.
        You can index into it (with a component index) to get the
        ``stderr`` for that component, or iterate over the sequence to
        get all of the ``stderr`` from the map.
        """
        return self._stderr

    @property
    def output_files(self) -> 'MapOutputFiles':
        """
        A sequence containing the path to the directory containing the
        output files for each map component.
        You can index into it (with a component index) to get the
        path for that component, or iterate over the sequence to
        get all of the paths from the map.
        """
        return self._output_files


class MapStdX(collections.abc.Sequence):
    """
    An object that helps implement a map's sequence over its ``stdout`` or ``stdin``.
    Don't both instantiating one yourself: use the ``Map.stdout`` or ``Map.stderr``
    attributes instead.
    """

    _func = None

    def __init__(self, map):
        self.map = map

    def __len__(self):
        return len(self.map)

    def __getitem__(self, component: int) -> str:
        return self.get(component)

    def get(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> str:
        """
        Return a string containing the stdout/stderr from a single map component.

        Parameters
        ----------
        component
            The index of the map component to look up.
        timeout
            How long to wait before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.

        Returns
        -------
        stdx :
            The standard output/error of the map component.
        """
        if component not in range(0, len(self)):
            raise IndexError(f'tried to get stdout/err file for component {component}, but map {self.map} only has {len(self.map)} components')

        path = getattr(self.map, f'_{self._func}_file_path')(component)
        utils.wait_for_path_to_exist(
            path,
            timeout = timeout,
            wait_time = settings['WAIT_TIME'],
        )
        return utils.rstr(path.read_text())


class MapStdOut(MapStdX):
    """
    An object that helps implement a map's sequence over its ``stdout``.
    Don't both instantiating one yourself: use the ``Map.stdout``
    attribute instead.
    """

    _func = 'stdout'


class MapStdErr(MapStdX):
    """
    An object that helps implement a map's sequence over its ``stderr``.
    Don't both instantiating one yourself: use the ``Map.stderr``
    attribute instead.
    """

    _func = 'stderr'


class MapOutputFiles:
    """
    An object that helps implement a map's sequence over its output file directories.
    Don't both instantiating one yourself: use the ``Map.output_files``
    attribute instead.
    """

    def __init__(self, map):
        self.map = map

    def __len__(self):
        return len(self.map)

    def __getitem__(self, component: int) -> Path:
        return self.get(component)

    def get(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> Path:
        """
        Return the :class:`pathlib.Path` to the directory containing the output
        files for the given component.

        Parameters
        ----------
        component
            The index of the map component to look up.
        timeout
            How long to wait before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.

        Returns
        -------
        path :
            The path to the directory containing the output files for the given component.
        """
        if component not in range(0, len(self)):
            raise IndexError(f'tried to get output files for component {component}, but map {self.map} only has {len(self.map)} components')

        path = self.map._output_files_path(component)
        utils.wait_for_path_to_exist(
            path,
            timeout = timeout,
            wait_time = settings['WAIT_TIME'],
        )
        return path
