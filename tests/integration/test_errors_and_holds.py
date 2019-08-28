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

from pathlib import Path

import pytest

import htmap

import htcondor


@pytest.fixture(scope = 'function')
def hold_before_error():
    map = htmap.map(lambda x: 1 / x, [1, 0])

    schedd = htcondor.Schedd()
    cluster_id = map._cluster_ids[0]
    schedd.act(htcondor.JobAction.Hold, f"(ClusterID == {cluster_id}) && (ProcID == 0)")

    map.wait(holds_ok = True, errors_ok = True)

    assert map.component_statuses == [htmap.ComponentStatus.HELD, htmap.ComponentStatus.ERRORED]

    return map


@pytest.fixture(scope = 'function')
def error_before_hold():
    map = htmap.map(lambda x: 1 / x, [0, 1])

    schedd = htcondor.Schedd()
    cluster_id = map._cluster_ids[0]
    schedd.act(htcondor.JobAction.Hold, f"(ClusterID == {cluster_id}) && (ProcID == 1)")

    map.wait(holds_ok = True, errors_ok = True)

    assert map.component_statuses == [htmap.ComponentStatus.ERRORED, htmap.ComponentStatus.HELD]

    return map


def test_can_get_error_if_hold_in_front(hold_before_error):
    hold_before_error.get_err(1)


def test_can_get_error_if_error_in_front(error_before_hold):
    error_before_hold.get_err(0)


def test_can_iterate_over_errors_if_hold_in_front(hold_before_error):
    assert len(list(hold_before_error.error_reports())) == 1


def test_can_iterate_over_errors_if_error_in_front(error_before_hold):
    assert len(list(error_before_hold.error_reports())) == 1


def test_can_get_errors_if_hold_in_front(hold_before_error):
    assert len(list(hold_before_error.errors)) == 1


def test_can_get_errors_if_error_in_front(error_before_hold):
    assert len(list(error_before_hold.errors)) == 1
