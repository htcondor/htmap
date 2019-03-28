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


def test_all_errors_if_no_limit(cli):
    m = htmap.map(lambda x: 1 / x, [0] * 2)
    m.wait(timeout = 180)

    result = cli(['errors', m.tag])

    for txt in ['component 0', 'component 1']:
        assert txt in result.output


def test_all_errors_if_zero_limit(cli):
    m = htmap.map(lambda x: 1 / x, [0] * 2)
    m.wait(timeout = 180)

    result = cli(['errors', '--limit', '0', m.tag])

    for txt in ['component 0', 'component 1']:
        assert txt in result.output


def test_one_error_if_limit_one(cli):
    m = htmap.map(lambda x: 1 / x, [0] * 2)
    m.wait(timeout = 180)

    result = cli(['errors', '--limit', '1', m.tag])

    assert 'component 0' in result.output
    assert 'component 1' not in result.output
