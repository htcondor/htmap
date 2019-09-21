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
def late_sleep():
    @htmap.mapped(map_options = htmap.MapOptions(max_idle = "1"))
    def sleep(x):
        return time.sleep(1)

    return sleep


@pytest.mark.timeout(360)
def test_can_be_removed_after_complete(late_sleep):
    m = late_sleep.map(range(3))

    m.wait(timeout = 180)
    m.remove()

    assert m.is_removed


@pytest.mark.timeout(10)
def test_can_be_removed_immediately(late_sleep):
    m = late_sleep.map(range(1000))

    m.remove()

    assert m.is_removed
