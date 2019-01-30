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

import pytest

import htmap


def test_map_is_done_after_wait(cli):
    m = htmap.map(str, range(1))

    result = cli(['wait', m.tag])

    assert m.is_done


def test_maps_are_done_after_wait(cli):
    maps = [
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
    ]

    result = cli(['wait', *(m.tag for m in maps)])

    assert all(m.is_done for m in maps)


def test_maps_are_done_after_wait_using_all(cli):
    maps = [
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
    ]

    result = cli(['wait', '--all'])

    assert all(m.is_done for m in maps)


def test_maps_wait_message_has_all_tags(cli):
    maps = [
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
    ]

    result = cli(['wait', *(m.tag for m in maps)])

    assert all(m.tag in result.output for m in maps)


def test_maps_wait_message_has_all_tags_using_all(cli):
    maps = [
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
    ]

    result = cli(['wait', '--all'])

    assert all(m.tag in result.output for m in maps)
