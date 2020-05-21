# Copyright 2019 HTCondor Team, Computer Sciences Department,
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

from typing import Dict, Tuple, List
import logging

import datetime
import threading
import pickle
from pathlib import Path

import htcondor

from . import holds, names, utils, exceptions

logger = logging.getLogger(__name__)


class ComponentStatus(utils.StrEnum):
    """
    An enumeration of the possible statuses that a map component can be in.
    These are mostly identical to the HTCondor job statuses of the same name.
    """
    UNKNOWN = 'UNKNOWN'
    UNMATERIALIZED = 'UNMATERIALIZED'
    IDLE = 'IDLE'
    RUNNING = 'RUNNING'
    REMOVED = 'REMOVED'
    COMPLETED = 'COMPLETED'
    HELD = 'HELD'
    SUSPENDED = 'SUSPENDED'
    ERRORED = 'ERRORED'

    @classmethod
    def display_statuses(cls) -> Tuple['ComponentStatus', ...]:
        return (
            cls.HELD,
            cls.ERRORED,
            cls.IDLE,
            cls.RUNNING,
            cls.COMPLETED,
        )


JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: ComponentStatus.IDLE,
    htcondor.JobEventType.JOB_EVICTED: ComponentStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: ComponentStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: ComponentStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: ComponentStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: ComponentStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: ComponentStatus.COMPLETED,
    htcondor.JobEventType.EXECUTE: ComponentStatus.RUNNING,
    htcondor.JobEventType.JOB_HELD: ComponentStatus.HELD,
    htcondor.JobEventType.JOB_SUSPENDED: ComponentStatus.SUSPENDED,
    htcondor.JobEventType.JOB_ABORTED: ComponentStatus.REMOVED,
}


class MapState:
    def __init__(self, map):
        self.map = map

        self._event_reader = None  # delayed until _read_events is called

        self._jobid_to_component: Dict[Tuple[int, int], int] = {}

        self._component_statuses = [ComponentStatus.UNMATERIALIZED for _ in self.map.components]
        self._holds: Dict[int, holds.ComponentHold] = {}
        self._memory_usage = [0 for _ in self.map.components]
        self._runtime = [datetime.timedelta(0) for _ in self.map.components]

        self._event_reader_lock = threading.Lock()

    @property
    def component_statuses(self) -> List[ComponentStatus]:
        self._read_events()
        return self._component_statuses

    @property
    def holds(self) -> Dict[int, holds.ComponentHold]:
        self._read_events()
        return self._holds

    @property
    def memory_usage(self) -> List[int]:
        self._read_events()
        return self._memory_usage

    @property
    def runtime(self) -> List[datetime.timedelta]:
        self._read_events()
        return self._runtime

    @property
    def _event_log_path(self):
        return self.map._map_dir / names.EVENT_LOG

    def _read_events(self):
        with self._event_reader_lock:  # no thread can be in here at the same time as another
            if self._event_reader is None:
                logger.debug(f'Created event log reader for map {self.map.tag}')
                self._event_reader = htcondor.JobEventLog(self._event_log_path.as_posix()).events(0)

            with utils.Timer() as timer:
                handled_events = self._handle_events()

            if handled_events > 0:
                logger.debug(f"Processed {handled_events} events for map {self.map.tag} (took {timer.elapsed:.6f} seconds)")

                self.map._local_data = None  # invalidate cache if any events were received

                if utils.BINDINGS_VERSION_INFO >= (8, 9, 3):
                    self.save()

    def _handle_events(self) -> int:
        """
        Process new events and return the number of new events processed.
        """
        handled_events = 0

        for event in self._event_reader:
            handled_events += 1

            # skip the late materialization submit event
            if event.proc == -1:
                continue

            if event.type is htcondor.JobEventType.SUBMIT:
                self._jobid_to_component[(event.cluster, event.proc)] = int(event['LogNotes'])

            # this lookup is safe because the SUBMIT event always comes first
            # ... but it can happen if the event log is corrupted somehow
            try:
                component = self._jobid_to_component[(event.cluster, event.proc)]
            except KeyError as e:
                raise exceptions.CorruptEventLog(f"Found an event for a job that we never saw a submit event for:\n{event}") from e

            if event.type is htcondor.JobEventType.IMAGE_SIZE:
                self._memory_usage[component] = max(
                    self._memory_usage[component],
                    int(event.get('MemoryUsage', 0)),
                )
            elif event.type is htcondor.JobEventType.JOB_TERMINATED:
                self._runtime[component] = parse_runtime(event['RunRemoteUsage'])
            elif event.type is htcondor.JobEventType.JOB_RELEASED:
                self._holds.pop(component, None)
            elif event.type is htcondor.JobEventType.JOB_HELD:
                h = holds.ComponentHold(
                    code = int(event['HoldReasonCode']),
                    reason = event.get('HoldReason', 'UNKNOWN').strip(),
                )
                self._holds[component] = h

            new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)

            # the component has *terminated*, but did it error?
            if new_status is ComponentStatus.COMPLETED:
                try:
                    exec_status = self.map._peek_status(component)
                except exceptions.OutputNotFound:
                    logger.warning(f'Output was not found for component {component} for map {self.map.tag}, marking as errored')
                    exec_status = 'ERR'

                if exec_status == 'ERR':
                    new_status = ComponentStatus.ERRORED

            if new_status is not None:
                if new_status is self._component_statuses[component]:
                    logger.warning(f'Component {component} of map {self.map.tag} tried to transition into the state it is already in ({new_status})')
                else:
                    # this log is commented-out because its very verbose
                    # might be helpful when debugging
                    # logger.debug(f'Component {component} of map {self.map.tag} changed state: {self._component_statuses[component]} -> {new_status}')
                    self._component_statuses[component] = new_status

        return handled_events

    def save(self) -> Path:
        final_path = self.map._map_dir / names.MAP_STATE
        working_path = final_path.with_suffix('.working')

        with working_path.open(mode = 'wb') as f:
            pickle.dump(self, f, protocol = -1)

        working_path.rename(final_path)

        logger.debug(f"Saved map state for map {self.map.tag}")

        return final_path

    @staticmethod
    def load(map):
        if utils.BINDINGS_VERSION_INFO < (8, 9, 3):
            raise exceptions.InsufficientHTCondorVersion("Map state can only be saved with HTCondor 8.9.3 or greater")

        with (map._map_dir / names.MAP_STATE).open(mode = 'rb') as f:
            state = pickle.load(f)
        state.map = map
        return state

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('_event_reader_lock')
        d.pop('map')
        return d

    def __setstate__(self, state):
        self.__dict__ = state
        self._event_reader_lock = threading.Lock()
        # note: the map reference is restored in the load method


def parse_runtime(runtime_string: str) -> datetime.timedelta:
    (_, usr_days, usr_hms), (_, sys_days, sys_hms) = [s.split() for s in runtime_string.split(',')]

    usr_h, usr_m, usr_s = usr_hms.split(':')
    sys_h, sys_m, sys_s = sys_hms.split(':')

    usr_time = datetime.timedelta(
        days = int(usr_days),
        hours = int(usr_h),
        minutes = int(usr_m),
        seconds = int(usr_s),
    )
    sys_time = datetime.timedelta(
        days = int(sys_days),
        hours = int(sys_h),
        minutes = int(sys_m),
        seconds = int(sys_s),
    )

    return usr_time + sys_time
