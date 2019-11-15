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


def test_components(cli):
    m = htmap.map(str, range(1), tag = 'foo')
    m.wait(timeout = 180)

    result = cli(['components', m.tag])

    assert '0 COMPLETED' in result.output


def test_components_only_errors(cli):
    m = htmap.map(lambda x: 1 / x, [0, 0, 1, 1], tag = 'foo')
    m.wait(timeout = 180, errors_ok = True)

    result = cli(['components', '--status', 'errored', m.tag])

    assert '0 1' in result.output
    assert '2 3' not in result.output


def test_components_bad_status(cli):
    m = htmap.map(str, range(1), tag = 'foo')

    result = cli(['components', '--status', 'wizbang', m.tag])

    assert 'ERROR' in result.output
    assert 'wizbang' in result.output

