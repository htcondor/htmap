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

import time

import pytest

import htmap


def test_waiting_on_held_component_raises(mapped_doubler):
    m = mapped_doubler.map(range(1))
    m.hold()

    time.sleep(1)  # wait for it to propagate

    with pytest.raises(htmap.exceptions.MapComponentHeld):
        m.wait(timeout = 180)


def test_getting_held_component_raises(mapped_doubler):
    m = mapped_doubler.map(range(1))
    m.hold()

    time.sleep(1)  # wait for it to propagate

    with pytest.raises(htmap.exceptions.MapComponentHeld):
        m[0]


def test_iterating_over_held_component_raises(mapped_doubler):
    m = mapped_doubler.map(range(1))
    m.hold()

    time.sleep(1)  # wait for it to propagate

    with pytest.raises(htmap.exceptions.MapComponentHeld):
        list(m)


def test_held_component_shows_up_in_hold_reasons(mapped_doubler):
    m = mapped_doubler.map(range(1))
    m.hold()

    time.sleep(1)  # wait for it to propagate

    assert isinstance(m.holds[0], htmap.ComponentHold)


def test_held_then_released_component_not_in_hold_reasons(mapped_doubler):
    m = mapped_doubler.map(range(1))
    m.hold()

    time.sleep(1)  # wait for it to propagate

    assert isinstance(m.holds[0], htmap.ComponentHold)

    m.release()

    time.sleep(1)  # wait for htcondor to write events

    assert len(m.holds) == 0
