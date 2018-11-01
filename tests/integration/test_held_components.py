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

import datetime

import pytest

import htmap


def test_waiting_on_held_component_raises(mapped_doubler):
    m = mapped_doubler.map('waiting-on-held-raises', range(1))
    m.hold()

    with pytest.raises(htmap.exceptions.MapComponentHeld):
        m.wait()


def test_getting_held_component_raises(mapped_doubler):
    m = mapped_doubler.map('getting-on-held-raises', range(1))
    m.hold()

    with pytest.raises(htmap.exceptions.MapComponentHeld):
        m[0]


def test_iterating_over_held_component_raises(mapped_doubler):
    m = mapped_doubler.map('getting-on-held-raises', range(1))
    m.hold()

    with pytest.raises(htmap.exceptions.MapComponentHeld):
        list(m)


def test_held_component_shows_up_in_hold_reasons(mapped_doubler):
    m = mapped_doubler.map('hold-reasons', range(1))
    m.hold()

    assert isinstance(m.hold_reasons[0], htmap.Hold)


def test_held_then_released_component_not_in_hold_reasons(mapped_doubler):
    m = mapped_doubler.map('hold-reasons', range(1))
    m.hold()

    assert isinstance(m.hold_reasons[0], htmap.Hold)

    m.release()

    assert len(m.hold_reasons) == 0
