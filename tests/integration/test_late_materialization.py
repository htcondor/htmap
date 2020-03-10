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

import time

import pytest

import htmap


@pytest.fixture(scope = 'function')
def late_noop():
    @htmap.mapped(map_options = htmap.MapOptions(max_idle = "1"))
    def noop(_):
        return True

    return noop


@pytest.mark.timeout(10)
def test_can_be_removed_immediately(late_noop):
    m = late_noop.map(range(1000))

    m.remove()

    assert m.is_removed


def test_wait_with_late_materialization(late_noop):
    m = late_noop.map(range(3))

    m.wait(timeout = 180)

    assert m.is_done
