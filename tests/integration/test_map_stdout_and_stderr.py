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

import sys

import pytest

import htmap


@pytest.mark.usefixtures('delivery_methods')
def test_stdout_sees_print():
    m = htmap.map(lambda x: print('foobar'), range(1))

    assert 'foobar' in m.stdout[0]


@pytest.mark.usefixtures('delivery_methods')
def test_stderr_sees_print():
    m = htmap.map(lambda x: print('foobar', file = sys.stderr), range(2))

    assert 'foobar' in m.stderr[0]


@pytest.mark.usefixtures('delivery_methods')
def test_multiple_stdouts():
    m = htmap.map(lambda x: print(x), ['foobar', 'wizbang'])

    assert 'foobar' in m.stdout[0]
    assert 'wizbang' in m.stdout[1]


@pytest.mark.usefixtures('delivery_methods')
def test_multiple_stderrs():
    m = htmap.map(lambda x: print(x, file = sys.stderr), ['foobar', 'wizbang'])

    assert 'foobar' in m.stderr[0]
    assert 'wizbang' in m.stderr[1]


@pytest.mark.usefixtures('delivery_methods')
def test_iterating_over_multiple_stdouts():
    s = ['foobar', 'wizbang', 'googaw']
    m = htmap.map(lambda x: print(x), s)

    assert all(st in out for st, out in zip(s, m.stdout))


@pytest.mark.usefixtures('delivery_methods')
def test_iterating_over_multiple_stderrs():
    s = ['foobar', 'wizbang', 'googaw']
    m = htmap.map(lambda x: print(x, file = sys.stderr), s)

    assert all(st in err for st, err in zip(s, m.stderr))
