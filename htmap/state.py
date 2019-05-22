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

import htcondor

from . import holds, names, utils, exceptions

logger = logging.getLogger(__name__)


class ComponentStatus(utils.StrEnum):
    """
    An enumeration of the possible statuses that a map component can be in.
    These are mostly identical to the HTCondor job statuses of the same name.
    """
    UNKNOWN = 'UNKNOWN'
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
        self._clusterproc_to_component: Dict[Tuple[int, int], int] = {}

        self._component_statuses = [ComponentStatus.UNKNOWN for _ in self.map.components]
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
                logger.debug(f'created event log reader for map {self.map.tag}')
                self._event_reader = htcondor.JobEventLog(self._event_log_path.as_posix()).events(0)

            for event in self._event_reader:
                self.map._local_data = None  # invalidate cache if any events were received

                if event.type is htcondor.JobEventType.SUBMIT:
                    self._clusterproc_to_component[(event.cluster, event.proc)] = int(event['LogNotes'])

                # this lookup is safe because the SUBMIT event always comes first
                component = self._clusterproc_to_component[(event.cluster, event.proc)]

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
                        logger.warning(f'output was not found for component {component} for map {self.map.tag}, marking as errored')
                        exec_status = 'ERR'

                    if exec_status == 'ERR':
                        new_status = ComponentStatus.ERRORED

                if new_status is not None:
                    if new_status is self._component_statuses[component]:
                        logger.warning(f'component {component} of map {self.map.tag} tried to transition into the state it is already in ({new_status})')
                    self._component_statuses[component] = new_status


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
