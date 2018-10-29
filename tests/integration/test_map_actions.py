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

import pytest

import htmap

N = 1


def test_hold(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(N))

    result.hold()

    assert result.status_counts()[htmap.ComponentStatus.HELD] == N


def test_hold_then_release(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(N))

    result.hold()
    assert result.status_counts()[htmap.ComponentStatus.HELD] == N

    result.release()
    assert result.status_counts()[htmap.ComponentStatus.HELD] == 0
