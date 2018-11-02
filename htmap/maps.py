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

from typing import NamedTuple, Tuple, List, Iterable, Any, Optional, Union, Callable, Iterator, Dict
import logging

import datetime
import shutil
import time
import textwrap
import functools
import inspect
import collections
import weakref
from copy import copy
from pathlib import Path
import gc

from tqdm import tqdm

import htcondor
import classad

from . import htio, exceptions, utils, mapping, settings

logger = logging.getLogger(__name__)


class ComponentStatus(utils.StrEnum):
    IDLE = 'IDLE'
    RUNNING = 'RUNNING'
    REMOVED = 'REMOVED'
    COMPLETED = 'COMPLETED'
    HELD = 'HELD'
    SUSPENDED = 'SUSPENDED'

    @classmethod
    def display_statuses(cls) -> Tuple['ComponentStatus', ...]:
        return (
            cls.HELD,
            cls.IDLE,
            cls.RUNNING,
            cls.COMPLETED,
        )


class ComponentError:
    def __init__(
        self,
        *,
        map,
        component,
        exception_msg,
        node_info,
        python_info,
        working_dir_contents,
        stack_summary,
    ):
        self.map = map
        self.component = component
        self.exception_msg = exception_msg
        self.node_info = node_info
        self.python_info = python_info
        self.working_dir_contents = working_dir_contents
        self.stack_summary = stack_summary

    def __repr__(self):
        return f'<ComponentError(map = {self.map}, component = {self.component})>'

    @classmethod
    def from_error(cls, map, error):
        return cls(
            map = map,
            component = error.component,
            exception_msg = error.exception_msg,
            node_info = error.node_info,
            python_info = error.python_info,
            working_dir_contents = error.working_dir_contents,
            stack_summary = error.stack_summary,
        )

    def _indent(self, text, multiple = 1):
        return textwrap.indent(text, ' ' * 2 * multiple)

    def _format_stack_trace(self):
        # modified from https://github.com/python/cpython/blob/3.7/Lib/traceback.py
        _RECURSIVE_CUTOFF = 3
        result = []
        last_file = None
        last_line = None
        last_name = None
        count = 0
        for frame in self.stack_summary:
            if (
                last_file is None or last_file != frame.filename or
                last_line is None or last_line != frame.lineno or
                last_name is None or last_name != frame.name
            ):
                if count > _RECURSIVE_CUTOFF:
                    count -= _RECURSIVE_CUTOFF
                    result.append(
                        self._indent(f'  [Previous line repeated {count} more time{"s" if count > 1 else ""}]\n')
                    )
                last_file = frame.filename
                last_line = frame.lineno
                last_name = frame.name
                count = 0
            count += 1
            if count > _RECURSIVE_CUTOFF:
                continue
            row = []
            row.append(self._indent(f'File "{frame.filename}", line {frame.lineno}, in {frame.name}\n'))
            if frame.line:
                row.append(self._indent(f'{frame.line.strip()}\n', multiple = 2))
            row.append(self._indent('\nLocal variables:\n', multiple = 2))
            if frame.locals:
                for name, value in sorted(frame.locals.items()):
                    row.append(self._indent(f'{name} = {value}\n', multiple = 3))
            result.append(''.join(row))
        if count > _RECURSIVE_CUTOFF:
            count -= _RECURSIVE_CUTOFF
            result.append(
                self._indent(f'[Previous line repeated {count} more time{"s" if count > 1 else ""}]\n')
            )
        return result

    def report(self):
        lines = [f'  Start error report for component {self.component} of map {self.map.map_id}  '.center(80, '=')]

        lines.append('Landed on execute node {} ({}) at {}'.format(*self.node_info))

        if self.python_info is not None:
            executable, version, packages = self.python_info
            lines.append(f'\nPython executable is {executable} (version {version})')
            lines.append(f'with installed packages')
            lines.append(self._indent(packages))
        else:
            lines.append('\nPython executable information not available')

        lines.append('\nWorking directory contents are')
        for path in self.working_dir_contents:
            lines.append(self._indent(path))

        lines.append('\nException and traceback (most recent call last):')
        lines.extend(self._format_stack_trace())
        lines.append(self._indent(self.exception_msg, multiple = 1))

        lines.append('')
        lines.append(f'  End error report for component {self.component} of map {self.map.map_id}  '.center(80, '='))

        return '\n'.join(lines)


class Hold(NamedTuple):
    code: int
    reason: str

    def __str__(self):
        return f'[{self.code}] {self.reason}'

    def __repr__(self):
        return f'<{self.__class__.__name__}(code = {self.code}, reason = {self.reason}>'


class Usage(NamedTuple):
    memory: int  # MB
    disk: int  # KB

    def __str__(self):
        return f'mem: {self.memory} | disk: {self.disk}'


def _protector(method):
    @functools.wraps(method)
    def _protect(self, *args, **kwargs):
        if self._is_removed:
            raise exceptions.MapWasRemoved(f'cannot call {method} for map {self.map_id} because it has been removed')
        return method(self, *args, **kwargs)

    return _protect


def _protect_map_after_remove(result_class):
    # decorate all public instance methods
    for key, member in inspect.getmembers(result_class, predicate = inspect.isfunction):
        if not key.startswith('_'):
            setattr(result_class, key, _protector(member))

    return result_class


MAPS = weakref.WeakValueDictionary()


@_protect_map_after_remove
class Map:
    """
    Represents the results from a map call.

    .. warning ::

        You should never instantiate a :class:`Map` directly!
        Instead, you'll get your :class:`Map` by calling a top-level mapping function like :func:`htmap.map`, a :class:`MappedFunction` mapping method, or by using :func:`htmap.load`.
        We are not responsible for whatever vile contraption you build if you bypass the correct methods!

    """

    def __init__(
        self,
        map_id: str,
        cluster_ids: Iterable[int],
        num_components: int,
        submit: htcondor.Submit,
    ):
        self.map_id = map_id
        self._cluster_ids = list(cluster_ids)
        self._submit = submit
        self._num_components = num_components

        self._is_removed = False

        self._events = None
        self._clusterproc_to_component = {}
        self._component_statuses = [ComponentStatus.IDLE for _ in self.component_indices]
        self._hold_reasons = {}
        self._usage = {}

        MAPS[self.map_id] = self

    @classmethod
    def load(cls, map_id: str) -> 'Map':
        """
        Load a :class:`Map` by looking up its ``map_id``.

        Raises :class:`htmap.exceptions.MapIdNotFound` if the ``map_id`` does not exist.

        Parameters
        ----------
        map_id
            The ``map_id`` to search for.

        Returns
        -------
        map
            The map with the given ``map_id``.
        """
        try:
            return MAPS[map_id]
        except KeyError:
            map_dir = mapping.map_dir_path(map_id)
            try:
                with (map_dir / 'cluster_ids').open() as file:
                    cluster_ids = [int(cid.strip()) for cid in file]

                num_components = htio.load_num_components(map_dir)
                submit = htio.load_submit(map_dir)

            except FileNotFoundError:
                raise exceptions.MapIdNotFound(f'the map_id {map_id} could not be found')

            logger.debug(f'recovered map result for map {map_id} from {map_dir}')

            return cls(
                map_id = map_id,
                cluster_ids = cluster_ids,
                num_components = num_components,
                submit = submit,
            )

    def __repr__(self):
        return f'<{self.__class__.__name__}(map_id = {self.map_id})>'

    def __len__(self):
        """The length of a :class:`Map` is the number of inputs it contains."""
        return self._num_components

    @property
    def component_indices(self):
        return range(self._num_components)

    @property
    def _map_dir(self) -> Path:
        """The path to the map directory."""
        return mapping.map_dir_path(self.map_id)

    @property
    def _event_log_path(self) -> Path:
        return self._map_dir / 'event_log'

    @property
    def _inputs_dir(self) -> Path:
        """The path to the inputs directory, inside the map directory."""
        return self._map_dir / 'inputs'

    @property
    def _outputs_dir(self) -> Path:
        """The path to the outputs directory, inside the map directory."""
        return self._map_dir / 'outputs'

    def _input_file_path(self, component):
        return self._inputs_dir / f'{component}.in'

    def _output_file_path(self, component):
        return self._outputs_dir / f'{component}.out'

    @property
    def _input_file_paths(self):
        """The paths to the input files."""
        yield from (self._input_file_path(idx) for idx in self.component_indices)

    @property
    def _output_file_paths(self):
        """The paths to the output files."""
        yield from (self._output_file_path(idx) for idx in self.component_indices)

    def _remove_from_queue(self):
        return self._act(htcondor.JobAction.Remove)

    def _rm_map_dir(self):
        shutil.rmtree(str(self._map_dir.absolute()))
        logger.debug(f'removed map directory for map {self.map_id}')

    def _clean_outputs_dir(self):
        def update_status(path: Path):
            self.component_statuses[int(path.stem)] = ComponentStatus.REMOVED

        utils.clean_dir(self._outputs_dir, on_file = update_status)

    @property
    def _missing_components(self) -> List[int]:
        """Return a list of component indices that are not complete."""
        return [
            idx
            for idx in self.component_indices
            if self.component_statuses[idx] != ComponentStatus.COMPLETED
        ]

    @property
    def _completed_components(self) -> List[str]:
        """Return a list of component indices that are complete."""
        return [
            idx
            for idx in self.component_indices
            if self.component_statuses[idx] == ComponentStatus.COMPLETED
        ]

    @property
    def is_done(self) -> bool:
        """``True`` if all of the output is available for this map."""
        return len(self._missing_components) == 0

    @property
    def is_running(self) -> bool:
        """
        ``True`` if any of the map's components are in a non-completed status.
        That means that this doesn't literally mean "running" - instead, it means that components could be running, idle, held, completed according to HTCondor but ran into an error internally and didn't produce output, etc.
        """
        return any(
            v != 0
            for k, v in self.status_counts().items()
            if k != ComponentStatus.COMPLETED
        )

    def wait(
        self,
        timeout: utils.Timeout = None,
        show_progress_bar: bool = False,
    ) -> datetime.timedelta:
        """
        Wait until all output associated with this :class:`Map` is available.

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
        timeout = utils.timeout_to_seconds(timeout)

        try:
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
                num_missing_components = len(self._missing_components)
                if show_progress_bar:
                    pbar_len = expected_num_hashes - num_missing_components
                    pbar.update(pbar_len - previous_pbar_len)
                    previous_pbar_len = pbar_len
                if num_missing_components == 0:
                    break

                missing_component_statuses = zip(self._missing_components, (self.component_statuses[idx] for idx in self._missing_components))
                for component, status in missing_component_statuses:
                    if status is ComponentStatus.HELD:
                        raise exceptions.MapComponentHeld(f'component {component} of map {self.map_id} was held')

                if timeout is not None and time.time() - timeout > start_time:
                    raise exceptions.TimeoutError(f'timeout while waiting for {self}')

                time.sleep(settings['WAIT_TIME'])
        finally:
            if show_progress_bar:
                pbar.close()

        return datetime.datetime.now() - t

    def _load_result(self, component: int, timeout = None):
        timeout = utils.timeout_to_seconds(timeout)
        start_time = time.time()
        while True:
            status = self.component_statuses[component]
            if status == ComponentStatus.COMPLETED:
                break
            elif status == ComponentStatus.HELD:
                raise exceptions.MapComponentHeld(f'component {component} of map {self.map_id} is held. Reason: {self.hold_reasons[component]}')

            if timeout is not None and (time.time() >= start_time + timeout):
                if timeout <= 0:
                    raise exceptions.OutputNotFound(f'output for component {component} of map {self.map_id} not found')
                else:
                    raise exceptions.TimeoutError(f'timed out while waiting for component {component} of map {self.map_id}')

            time.sleep(settings['WAIT_TIME'])

        return htio.load_object(self._output_file_path(component))

    def _load_input(self, component: int):
        return htio.load_object(self._input_file_path(component))

    def _load_output(self, component: int, timeout = None) -> Any:
        result = self._load_result(component, timeout)

        if result.status == 'OK':
            return result.output
        elif result.status == 'ERR':
            raise exceptions.MapComponentError(f'component {component} of map {self.map_id} encountered stderr while executing. Error report:\n{self._load_error(component).report()}')
        else:
            raise exceptions.InvalidOutputStatus(f'output status {result.status} is not valid')

    def _load_error(self, component: int, timeout = None) -> ComponentError:
        result = self._load_result(component, timeout)

        if result.status == 'OK':
            raise exceptions.ExpectedError
        elif result.status == 'ERR':
            return ComponentError.from_error(map = self, error = result)
        else:
            raise exceptions.InvalidOutputStatus(f'output status {result.status} is not valid')

    def get(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> Any:
        """
        Return the output associated with the input index.

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
    ) -> ComponentError:
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

        for component in self.component_indices:
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

        for component in self.component_indices:
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

        remaining_indices = set(self.component_indices)
        while len(remaining_indices) > 0:
            for component in copy(remaining_indices):
                try:
                    output = self._load_output(component, timeout = 0)
                    remaining_indices.remove(component)
                    callback(output)
                    yield output
                except exceptions.TimeoutError:
                    pass

            if timeout is not None and time.time() > start_time + timeout:
                break

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

        remaining_indices = set(self.component_indices)
        while len(remaining_indices) > 0:
            for component in copy(remaining_indices):
                try:
                    output = self._load_output(component, timeout = 0)
                    input = self._load_input(component)
                    remaining_indices.remove(component)
                    callback(input, output)
                    yield input, output
                except exceptions.TimeoutError:
                    pass

            if timeout is not None and time.time() > start_time + timeout:
                break

            time.sleep(settings['WAIT_TIME'])

    def iter_inputs(self):
        yield from (self._load_input(idx) for idx in self.component_indices)

    def error_reports(self):
        for idx in self.component_indices:
            try:
                yield self.get_err(idx).report()
            except (exceptions.OutputNotFound, exceptions.ExpectedError) as e:
                pass

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

        logger.debug(f'queried for map {self.map_id} (requirements = "{req}") with projection {projection}')

        yield from q

    @property
    def component_statuses(self) -> List[ComponentStatus]:
        """Return a list"""
        self._update_component_statuses()
        return self._component_statuses

    def _update_component_statuses(self):
        time.sleep(.01)  # smooth things out by giving condor time to write to the event log
        if self._events is None:
            self._events = htcondor.JobEventLog(self._event_log_path.as_posix()).events(0)

        for event in self._events:
            if event.type == htcondor.JobEventType.SUBMIT:
                self._clusterproc_to_component[(event.cluster, event.proc)] = int(event.LogNotes)

            # this lookup is safe because the SUBMIT event always comes first
            component = self._clusterproc_to_component[(event.cluster, event.proc)]
            new_status = None

            # START SWITCH

            if event.type is htcondor.JobEventType.JOB_TERMINATED:
                new_status = ComponentStatus.COMPLETED
                u = Usage(memory = event.MemoryUsage, disk = event.DiskUsage)
                self._usage[component] = u

            elif event.type is htcondor.JobEventType.EXECUTE:
                new_status = ComponentStatus.RUNNING

            elif event.type in (
                htcondor.JobEventType.SUBMIT,
                htcondor.JobEventType.JOB_EVICTED,
                htcondor.JobEventType.JOB_UNSUSPENDED,
            ):
                new_status = ComponentStatus.IDLE

            elif event.type is htcondor.JobEventType.JOB_RELEASED:
                new_status = ComponentStatus.IDLE
                self._hold_reasons.pop(component, None)

            elif event.type is htcondor.JobEventType.JOB_HELD:
                new_status = ComponentStatus.HELD
                h = Hold(code = event.HoldReasonCode, reason = event.HoldReason.strip())
                self._hold_reasons[component] = h

            elif event.type is htcondor.JobEventType.JOB_SUSPENDED:
                new_status = ComponentStatus.SUSPENDED

            # END SWITCH

            if new_status is not None:
                self._component_statuses[component] = new_status
                logger.debug(f'status of component {component} of map {self.map_id} changed to {new_status}')

    def status_counts(self) -> collections.Counter:
        """Return a dictionary that describes how many map components are in each status."""
        return collections.Counter(self.component_statuses)

    def status(self) -> str:
        """Return a string containing the number of jobs in each status."""
        counts = self.status_counts()
        stat = ' | '.join(f'{str(js)} = {counts[js]}' for js in ComponentStatus.display_statuses())
        msg = f'{self.__class__.__name__} {self.map_id} ({len(self)} components): {stat}'

        return utils.rstr(msg)

    @property
    def hold_reasons(self) -> Dict[int, Hold]:
        """Return a dictionary that maps component indices to their :class:`Hold` (if they are held)."""
        self._update_component_statuses()
        return self._hold_reasons

    def holds(self) -> str:
        """Return a string containing a table describing any held components."""
        top = 'Component │ Hold Reason'
        under_top = ''.join('─' if char != '│' else '┼' for char in top)
        bottom = ''.join('─' if char != '│' else '┴' for char in top)

        lines = []
        for component, hold in self.hold_reasons.items():
            component_text = str(component).center(len('Component'))
            hold_text = str(hold)

            lines.append(f'{component_text} │ {hold_text}')

        return '\n'.join([top, under_top, *lines, bottom])

    def _act(self, action: htcondor.JobAction, requirements: Optional[str] = None) -> classad.ClassAd:
        schedd = mapping.get_schedd()
        req = self._requirements(requirements)
        a = schedd.act(action, req)

        logger.debug(f'acted on map {self.map_id} (requirements = "{req}") with action {action}')

        return a

    def remove(self):
        """
        Permanently remove the map and delete all associated input, output, and metadata files.
        """
        del self._events  # todo: this is a workaround for the file object not being exposed
        gc.collect()

        self._remove_from_queue()
        self._rm_map_dir()
        self._is_removed = True
        try:
            MAPS.pop(self.map_id)
        except KeyError:  # may already be gone depending on when GC runs
            pass
        logger.info(f'removed map {self.map_id}')

    def hold(self):
        """Temporarily remove the map from the queue, until it is released."""
        self._act(htcondor.JobAction.Hold)
        logger.debug(f'held map {self.map_id}')

    def release(self):
        """Releases a held map back into the queue."""
        self._act(htcondor.JobAction.Release)
        logger.debug(f'released map {self.map_id}')

    def pause(self):
        self._act(htcondor.JobAction.Suspend)
        logger.debug(f'paused map {self.map_id}')

    def resume(self):
        self._act(htcondor.JobAction.Continue)
        logger.debug(f'resumed map {self.map_id}')

    def vacate(self):
        """Force the map to give up any claimed resources."""
        self._act(htcondor.JobAction.Vacate)
        logger.debug(f'vacated map {self.map_id}')

    def _edit(self, attr: str, value: str, requirements: Optional[str] = None):
        schedd = mapping.get_schedd()
        schedd.edit(self._requirements(requirements), attr, value)

        logger.debug(f'set attribute {attr} for map {self.map_id} to {value}')

    def set_memory(self, memory: Union[str, int, float]):
        """
        Change the amount of memory (RAM) each map component needs.

        .. warning::

            This doesn't change anything for map components that have already started running,
            so you may need to hold and release your map to propagate this change.

        Parameters
        ----------
        memory
            The amount of memory (RAM) to request.
            Can either be a :class:`str` (``'100MB'``, ``'1GB'``, etc.), or a number, in which case it is interpreted as a number of **MB**.
        """
        if isinstance(memory, (int, float)):
            memory = f'{memory}MB'
        self._edit('RequestMemory', memory)

    def set_disk(self, disk: Union[str, int, float]):
        """
        Change the amount of disk space each map component needs.

        .. warning::

            This doesn't change anything for map components that have already started running,
            so you may need to hold and release your map to propagate this change.

        Parameters
        ----------
        disk
            The amount of disk space to use.
            Can either be a :class:`str` (``'100MB'``, ``'1GB'``, etc.), or a number, in which case it is interpreted as a number of **GB**.
        """
        if isinstance(disk, (int, float)):
            disk = f'{disk}MB'
        self._edit('RequestDisk', disk)

    def stdout(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> str:
        """
        Return a string containing the stdout from a single map component.

        Parameters
        ----------
        component
            The index of the map component to look up.
        timeout
            How long to wait before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.

        Returns
        -------
        stderr :
            The standard output of the map component.
        """
        timeout = utils.timeout_to_seconds(timeout)

        path = self._map_dir / 'job_logs' / f'{component}.stdout'

        try:
            utils.wait_for_path_to_exist(path, timeout)
        except exceptions.TimeoutError as e:
            if timeout <= 0:
                raise exceptions.OutputNotFound(f'stdout for component {component} not found') from e
            else:
                raise e

        return utils.rstr(path.read_text())

    def stderr(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> str:
        """
        Return a string containing the stderr from a single map component.

        Parameters
        ----------
        component
            The index of the map component to look up.
        timeout
            How long to wait before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.

        Returns
        -------
        stderr :
            The standard error of the map component.
        """
        timeout = utils.timeout_to_seconds(timeout)

        path = self._map_dir / 'job_logs' / f'{component}.stderr'

        try:
            utils.wait_for_path_to_exist(path, timeout)
        except exceptions.TimeoutError as e:
            if timeout <= 0:
                raise exceptions.OutputNotFound(f'stderr for component {component} not found') from e
            else:
                raise e

        return utils.rstr(path.read_text())

    def rerun(self):
        """Reruns the entire map from scratch."""
        self._clean_outputs_dir()
        self.rerun_incomplete()

    def rerun_incomplete(self):
        """Rerun any incomplete parts of the map from scratch."""
        self._rerun(components = self._missing_components)

    def _rerun(self, components):
        component_set = set(components)
        itemdata = htio.load_itemdata(self._map_dir)
        new_itemdata = [item for item in itemdata if int(item['component']) in component_set]

        submit_obj = htio.load_submit(self._map_dir)

        self._remove_from_queue()

        new_cluster_id = mapping.execute_submit(
            submit_obj,
            new_itemdata,
        )

        self._cluster_ids.append(new_cluster_id)
        with (self._map_dir / 'cluster_ids').open(mode = 'a') as f:
            f.write(str(new_cluster_id) + '\n')

        logger.debug(f'resubmitted {len(new_itemdata)} inputs from map {self.map_id}')

    def rename(self, map_id: str) -> 'Map':
        """
        Give this map a new ``map_id``.
        This function returns a **new** :class:`Map` for the renamed map.
        The :class:`Map` you call this on will not be connected to the new ``map_id``!
        The old ``map_id`` will be available for re-use.

        .. note::

            Only completed maps can be renamed (i.e., ``result.is_done == True``).

        .. warning::

            The old :class:`Map` will not be connected to the new ``map_id``!
            This function returns a **new** result for the renamed map.

        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this map.

        Returns
        -------
        map_result :
            A new :class:`Map` for the renamed map.
        """
        if map_id == self.map_id:
            raise exceptions.CannotRenameMap('cannot rename a map to the same map_id it already has')
        if not self.is_done:
            raise exceptions.CannotRenameMap(f'cannot rename a map that is not complete (map status: {self.status()})')

        try:
            mapping.raise_if_map_id_is_invalid(map_id)
            mapping.raise_if_map_id_already_exists(map_id)
        except (exceptions.InvalidMapId, exceptions.MapIdAlreadyExists) as e:
            raise exceptions.CannotRenameMap(f'cannot rename map because of previous exception: {e}') from e

        new_map_dir = mapping.map_dir_path(map_id)
        shutil.copytree(
            src = self._map_dir,
            dst = new_map_dir,
        )

        submit = htcondor.Submit(dict(self._submit))
        submit['JobBatchName'] = map_id

        # fix paths
        target = mapping.map_dir_path(self.map_id).as_posix()
        replace_with = mapping.map_dir_path(map_id).as_posix()
        for k, v in submit.items():
            submit[k] = v.replace(target, replace_with)

        htio.save_submit(new_map_dir, submit)

        self.remove()

        return Map(
            map_id = map_id,
            submit = submit,
            cluster_ids = self._cluster_ids,
            num_components = self._num_components,
        )


class JobEvent:
    def __init__(
        self,
        hash: str,
        type: htcondor.JobEventType,
    ):
        self.hash = hash
        self.type = type

    def __str__(self):
        return f'{self.__class__.__name__}(hash = {self.hash}, type = {self.type})'
