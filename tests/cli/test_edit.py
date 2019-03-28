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


def test_edit_memory_has_memory_in_msg(cli):
    m = htmap.map(str, range(1))
    m.hold()

    result = cli(['edit', 'memory', m.tag, f'1'])

    assert 'memory' in result.output


@pytest.mark.parametrize(
    'unit',
    ['MB', 'GB'],
)
def test_edit_memory_unit_in_msg(cli, unit):
    m = htmap.map(str, range(1))
    m.hold()

    result = cli(['edit', 'memory', m.tag, f'1', '--unit', unit])

    assert unit in result.output


@pytest.mark.parametrize(
    'unit',
    ['giraffe', '0', 'foasd', ''],
)
def test_edit_memory_rejects_bad_units(cli, unit):
    m = htmap.map(str, range(1))
    m.hold()

    result = cli(['edit', 'memory', m.tag, f'1', '--unit', unit])

    assert 'invalid choice' in result.output
    assert result.exit_code != 0


def test_edit_disk_has_disk_in_msg(cli):
    m = htmap.map(str, range(1))
    m.hold()

    result = cli(['edit', 'disk', m.tag, f'1'])

    assert 'disk' in result.output


@pytest.mark.parametrize(
    'unit',
    ['KB', 'MB', 'GB'],
)
def test_edit_disk_unit_in_msg(cli, unit):
    m = htmap.map(str, range(1))
    m.hold()

    result = cli(['edit', 'disk', m.tag, f'1', '--unit', unit])

    assert unit in result.output


@pytest.mark.parametrize(
    'unit',
    ['giraffe', '0', 'foasd', ''],
)
def test_edit_disk_rejects_bad_units(cli, unit):
    m = htmap.map(str, range(1))
    m.hold()

    result = cli(['edit', 'disk', m.tag, f'1', '--unit', unit])

    assert 'invalid choice' in result.output
    assert result.exit_code != 0
