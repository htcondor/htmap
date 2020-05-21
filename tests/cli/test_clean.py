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


def test_clean_msg_has_tag_of_removed_map(cli):
    m = htmap.map(str, range(1))

    result = cli(['clean'])

    assert m.tag in result.output


def test_clean_removes_transient_map(cli):
    m = htmap.map(str, range(1))

    result = cli(['clean'])

    assert not m.exists


def test_clean_has_tags_of_all_maps_removed(cli):
    maps = [
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
    ]

    result = cli(['clean'])

    assert all(m.tag in result.output for m in maps)


def test_clean_removes_multiple_transient_maps(cli):
    maps = [
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
        htmap.map(str, range(1)),
    ]

    result = cli(['clean'])

    assert all(not m.exists for m in maps)


def test_clean_does_not_remove_persistent_map_by_default(cli):
    m = htmap.map(str, range(1), tag = 'tagged')

    result = cli(['clean'])

    assert m.exists


def test_clean_with_all_removes_persistent_maps(cli):
    m = htmap.map(str, range(1), tag = 'tagged')

    result = cli(['clean', '--all'])

    assert not m.exists
