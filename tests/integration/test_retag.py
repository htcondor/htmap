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
import htmap.tags


def test_new_tag_in_tags(mapped_doubler):
    m = mapped_doubler.map(range(2), tag = 'old')
    m.wait(timeout = 180)

    m.retag('new')

    assert 'new' in htmap.get_tags()


def test_old_tag_not_in_tags(mapped_doubler):
    m = mapped_doubler.map(range(2), tag = 'old')
    m.wait(timeout = 180)

    m.retag('new')

    assert 'old' not in htmap.get_tags()


def test_retag_raises_if_new_tag_already_exists(mapped_doubler):
    m = mapped_doubler.map(range(1), tag = 'old')
    m.wait(timeout = 180)

    mapped_doubler.map(range(1), tag = 'target')

    with pytest.raises(htmap.exceptions.CannotRetagMap):
        m.retag('target')


def test_complete_then_retag_then_rerun(mapped_doubler):
    m = mapped_doubler.map(range(1), tag = 'old')
    m.wait(timeout = 180)

    new_m = m.retag('new')
    new_m.rerun()

    assert list(new_m) == [0]


def test_can_be_recovered_after_retag(mapped_doubler):
    m = mapped_doubler.map(range(1), tag = 'old')
    m.wait(timeout = 180)

    m.retag('new')

    htmap.load('new')


def test_retag_while_running(mapped_doubler):
    m = mapped_doubler.map(range(5))
    m.retag('new')

    assert list(m) == [0, 2, 4, 6, 8]


def cannot_retag_to_same_tag():
    m = htmap.map(str, range(1), tag = 'same-tag')

    with pytest.raises(htmap.exceptions.CannotRetagMap):
        m.retag('same-tag')
