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


def test_rerun(mapped_doubler):
    m = mapped_doubler.map([1])
    m.wait(timeout = 180)

    m.rerun()

    assert list(m) == [2]


def test_recover_then_rerun(mapped_doubler):
    m = mapped_doubler.map([1], tag = 'load-then-rerun')
    m.wait(timeout = 180)

    loaded = htmap.load('load-then-rerun')
    loaded.rerun()

    assert list(loaded) == [2]


def test_rerun_out_of_range_component_raises(mapped_doubler):
    m = mapped_doubler.map([1], tag = 'load-then-rerun')
    m.wait(timeout = 180)

    with pytest.raises(htmap.exceptions.CannotRerunComponents):
        m.rerun([5])
