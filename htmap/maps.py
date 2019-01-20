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

from typing import Tuple, List, Iterable, Any, Optional, Callable, Iterator, Dict, MutableMapping
import logging

import datetime
import shutil
import time
import uuid
import tempfile
import functools
import inspect
import collections
import weakref
from copy import copy
from pathlib import Path

from tqdm import tqdm

import htcondor
import classad

from . import htio, state, errors, holds, mapping, settings, utils, names, exceptions

logger = logging.getLogger(__name__)


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


MAPS: MutableMapping[str, 'Map'] = weakref.WeakValueDictionary()


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

        self._state = state.MapState(self)
        self._local_data = None

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

    def __gt__(self, other):
        return self.map_id > other.map_id

    def __lt__(self, other):
        return self.map_id < other.map_id

    def __ge__(self, other):
        return self.map_id >= other.map_id

    def __le__(self, other):
        return self.map_id <= other.map_id

    def __len__(self):
        """The length of a :class:`Map` is the number of components it contains."""
        return self._num_components

    @property
    def _map_dir(self) -> Path:
        """The path to the map directory."""
        return mapping.map_dir_path(self.map_id)

    @property
    def _inputs_dir(self) -> Path:
        """The path to the inputs directory, inside the map directory."""
        return self._map_dir / names.INPUTS_DIR

    @property
    def _outputs_dir(self) -> Path:
        """The path to the outputs directory, inside the map directory."""
        return self._map_dir / names.OUTPUTS_DIR

    def _input_file_path(self, component):
        return self._inputs_dir / f'{component}.in'

    def _output_file_path(self, component):
        return self._outputs_dir / f'{component}.out'

    @property
    def _input_file_paths(self):
        """The paths to the input files."""
        yield from (self._input_file_path(idx) for idx in self.components)

    @property
    def _output_file_paths(self):
        """The paths to the output files."""
        yield from (self._output_file_path(idx) for idx in self.components)

    @property
    def components(self) -> List[int]:
        """Return an iterator over the component indices for the :class:`htmap.Map`."""
        return list(range(self._num_components))

    @property
    def is_done(self) -> bool:
        """``True`` if all of the output is available for this map."""
        return all(cs is state.ComponentStatus.COMPLETED for cs in self.component_statuses)

    def wait(
        self,
        timeout: utils.Timeout = None,
        show_progress_bar: bool = False,
    ) -> None:
        """
        Wait until all output associated with this :class:`Map` is available.

        Parameters
        ----------
        timeout
            How long to wait for the map to complete before raising a :class:`htmap.exceptions.TimeoutError`.
            If ``None``, wait forever.
        show_progress_bar
            If ``True``, a progress bar will be displayed.
        """
        start_time = time.time()
        timeout = utils.timeout_to_seconds(timeout)

        try:
            if show_progress_bar:
                pbar = tqdm(
                    desc = self.map_id,
                    total = len(self),
                    unit = 'component',
                    ascii = True,
                )

                previous_pbar_len = 0

            while True:
                num_incomplete = sum(
                    cs is not state.ComponentStatus.COMPLETED
                    for cs in self.component_statuses
                )
                if show_progress_bar:
                    pbar_len = self._num_components - num_incomplete
                    pbar.update(pbar_len - previous_pbar_len)
                    previous_pbar_len = pbar_len
                if num_incomplete == 0:
                    break

                for component, status in enumerate(self.component_statuses):
                    if status is state.ComponentStatus.HELD:
                        raise exceptions.MapComponentHeld(f'component {component} of map {self.map_id} was held: {self.holds[component]}')

                if timeout is not None and time.time() - timeout > start_time:
                    raise exceptions.TimeoutError(f'timeout while waiting for {self}')

                time.sleep(settings['WAIT_TIME'])
        finally:
            if show_progress_bar:
                pbar.close()

    def _wait_for_component(self, component: int, timeout: utils.Timeout = None) -> None:
        timeout = utils.timeout_to_seconds(timeout)
        start_time = time.time()
        while True:
            component_state = self.component_statuses[component]
            if component_state is state.ComponentStatus.COMPLETED:
                break
            elif component_state is state.ComponentStatus.HELD:
                raise exceptions.MapComponentHeld(f'component {component} of map {self.map_id} is held: {self.holds[component]}')

            if timeout is not None and (time.time() >= start_time + timeout):
                if timeout <= 0:
                    raise exceptions.OutputNotFound(f'output for component {component} of map {self.map_id} not found')
                else:
                    raise exceptions.TimeoutError(f'timed out while waiting for component {component} of map {self.map_id}')

            time.sleep(settings['WAIT_TIME'])

    def _load_input(self, component: int) -> Tuple[Tuple[Any], Dict[str, Any]]:
        return htio.load_object(self._input_file_path(component))

    def _load_output(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> Any:
        result = self._load_result(component, timeout)

        if result.status == 'OK':
            return result.output
        elif result.status == 'ERR':
            raise exceptions.MapComponentError(f'component {component} of map {self.map_id} encountered error while executing. Error report:\n{self._load_error(component).report()}')
        else:
            raise exceptions.InvalidOutputStatus(f'output status {result.status} is not valid')

    def _load_error(
        self,
        component: int,
        timeout: utils.Timeout = None,
    ) -> errors.ComponentError:
        result = self._load_result(component, timeout)

        if result.status == 'OK':
            raise exceptions.ExpectedError
        elif result.status == 'ERR':
            return errors.ComponentError._from_error(map = self, error = result)
        else:
            raise exceptions.InvalidOutputStatus(f'output status {result.status} is not valid')

    def _load_result(self, component: int, timeout: utils.Timeout = None):
        self._wait_for_component(component, timeout)

        try:
            return htio.load_object(self._output_file_path(component))
        except FileNotFoundError:
            raise exceptions.OutputNotFound(f'Output not found for component {component} of map {self.map_id}. Component stderr:\n{self.stderr(component)}')

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
    ) -> errors.ComponentError:
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

        remaining_indices = set(self.components)
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

        logger.debug(f'queried for map {self.map_id} (requirements = "{req}") with projection {projection}')

        yield from q

    @property
    def component_statuses(self) -> List[state.ComponentStatus]:
        """
        Return the current :class:`state.ComponentStatus` of each component in the map.
        """
        return self._state.component_statuses

    def status(self) -> str:
        """Return a string containing the number of jobs in each status."""
        counts = collections.Counter(self.component_statuses)
        stat = ' | '.join(f'{str(js)} = {counts[js]}' for js in state.ComponentStatus.display_statuses())
        msg = f'{self.__class__.__name__} {self.map_id} ({len(self)} components): {stat}'

        return utils.rstr(msg)

    @property
    def holds(self) -> Dict[int, holds.ComponentHold]:
        """Return a dictionary that maps component indices to their :class:`Hold` (if they are held)."""
        return self._state.holds

    def hold_report(self) -> str:
        """Return a string containing a table describing any held components."""
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
        err = {}
        for idx in self.components:
            try:
                err[idx] = self.get_err(idx)
            except (exceptions.OutputNotFound, exceptions.ExpectedError) as e:
                pass

        return err

    def error_report(self) -> Iterator[str]:
        """Yields the error reports for any components that experienced an error during execution."""
        for idx in self.components:
            try:
                yield self.get_err(idx).report()
            except (exceptions.OutputNotFound, exceptions.ExpectedError) as e:
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
        if self._local_data is None:
            self._local_data = utils.get_dir_size(self._map_dir)
        return self._local_data

    def _act(
        self,
        action: htcondor.JobAction,
        requirements: Optional[str] = None,
    ) -> classad.ClassAd:
        """Perform an action on all of the jobs associated with this map."""
        schedd = mapping.get_schedd()
        req = self._requirements(requirements)
        a = schedd.act(action, req)

        logger.debug(f'acted on map {self.map_id} (requirements = "{req}") with action {action}')

        return a

    def remove(self) -> None:
        """
        Permanently remove the map and delete all associated input, output, and metadata files.
        """
        self._remove_from_queue()
        self._rm_map_dir()
        self._is_removed = True
        MAPS.pop(self.map_id, None)
        logger.info(f'removed map {self.map_id}')

    def _remove_from_queue(self) -> classad.ClassAd:
        return self._act(htcondor.JobAction.Remove)

    def _rm_map_dir(self) -> None:
        # moving the map dir to a temp dir first
        # helps avoid problems where condor writes
        # to the directory while we're cleaning
        tmp = Path(tempfile.gettempdir()) / str(uuid.uuid4())
        tmp.mkdir(parents = True, exist_ok = True)
        self._map_dir.rename(tmp)
        shutil.rmtree(str(tmp.absolute()))
        logger.debug(f'removed map directory for map {self.map_id}')

    def _clean_outputs_dir(self) -> None:
        def update_status(path: Path) -> None:
            self.component_statuses[int(path.stem)] = state.ComponentStatus.REMOVED

        utils.clean_dir(self._outputs_dir, on_file = update_status)

    def hold(self) -> None:
        """Temporarily remove the map from the queue, until it is released."""
        self._act(htcondor.JobAction.Hold)
        logger.debug(f'held map {self.map_id}')

    def release(self) -> None:
        """Releases a held map back into the queue."""
        self._act(htcondor.JobAction.Release)
        logger.debug(f'released map {self.map_id}')

    def pause(self) -> None:
        """Pause the map."""
        self._act(htcondor.JobAction.Suspend)
        logger.debug(f'paused map {self.map_id}')

    def resume(self) -> None:
        """Resume the map from a paused state."""
        self._act(htcondor.JobAction.Continue)
        logger.debug(f'resumed map {self.map_id}')

    def vacate(self) -> None:
        """Force the map to give up any claimed resources."""
        self._act(htcondor.JobAction.Vacate)
        logger.debug(f'vacated map {self.map_id}')

    def _edit(self, attr: str, value: str, requirements: Optional[str] = None) -> None:
        schedd = mapping.get_schedd()
        schedd.edit(self._requirements(requirements), attr, value)

        logger.debug(f'set attribute {attr} for map {self.map_id} to {value}')

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
        stdout :
            The standard output of the map component.
        """
        path = self._map_dir / 'job_logs' / f'{component}.stdout'
        utils.wait_for_path_to_exist(
            path,
            timeout = timeout,
            wait_time = settings['WAIT_TIME'],
        )
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
        path = self._map_dir / 'job_logs' / f'{component}.stderr'
        utils.wait_for_path_to_exist(
            path,
            timeout = timeout,
            wait_time = settings['WAIT_TIME'],
        )
        return utils.rstr(path.read_text())

    def rerun(self, components: Optional[Iterable[int]] = None):
        """
        Re-run part of a map from scratch.
        The components must be completed.
        Their existing output will be deleted before the re-run is executed.

        Parameters
        ----------
        components
            The components to rerun.
            If ``None``, the entire map will be re-run.
        """
        if components is None:
            components = self.components

        component_set = set(components)
        incomplete_components = {
            c for c, status in enumerate(self.component_statuses)
            if status is not state.ComponentStatus.COMPLETED
        }
        intersection = component_set.intersection(incomplete_components)
        if len(intersection) != 0:
            raise exceptions.CannotRerunComponents(f'cannot rerun components {sorted(intersection)} of map {self.map_id} because they are not complete')

        for path in (self._output_file_path(c) for c in components):
            if path.exists():
                path.unlink()

        itemdata = htio.load_itemdata(self._map_dir)
        new_itemdata = [item for item in itemdata if int(item['component']) in component_set]

        submit_obj = htio.load_submit(self._map_dir)

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
            src = str(self._map_dir),
            dst = str(new_map_dir),
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


class TransientMap(Map):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

        return False  # re-raise exceptions

    def __iter__(self):
        try:
            yield from super().__iter__()
        finally:
            self._cleanup()

    def __del__(self):
        self._cleanup()

    def _cleanup(self):
        try:
            self.remove()
        except exceptions.MapWasRemoved:
            pass
