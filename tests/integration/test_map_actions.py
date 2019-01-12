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

import time

import htmap


def test_hold(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(1))

    result.hold()
    time.sleep(.1)
    status_counts_after_hold = result.status_counts

    print(status_counts_after_hold)
    assert status_counts_after_hold[htmap.ComponentStatus.HELD] == 1


def test_hold_then_release(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(1))

    result.hold()
    time.sleep(.1)
    status_counts_after_hold = result.status_counts

    print(status_counts_after_hold)
    assert status_counts_after_hold[htmap.ComponentStatus.HELD] == 1

    result.release()
    time.sleep(.1)
    status_counts_after_release = result.status_counts

    print(status_counts_after_release)
    assert status_counts_after_release[htmap.ComponentStatus.HELD] == 0
