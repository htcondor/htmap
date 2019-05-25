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


def test_rerun_map(cli):
    m = htmap.map(str, range(1))
    m.wait(180)

    result = cli(['rerun', 'map', m.tag])
    m.wait(180)

    assert m[0] == '0'


def test_rerun_components(cli):
    m = htmap.map(str, [0, 1])
    m.wait(180)

    result = cli(['rerun', 'components', m.tag, '0 1'])
    m.wait(180)

    assert m[0] == '0'
    assert m[1] == '1'


def test_rerun_components_out_range_cannot_rerun(cli):
    m = htmap.map(str, [0])
    m.wait(180)

    result = cli(['rerun', 'components', m.tag, '5'])

    assert 'cannot rerun' in result.output
