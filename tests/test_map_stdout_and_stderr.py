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


def test_looking_at_stdout_too_early_zero_timeout():
    m = htmap.map('stdout-too-early', lambda x: x, range(1))

    with pytest.raises(htmap.exceptions.OutputNotFound):
        print(m.stdout(0, timeout = 0))


def test_looking_at_stderr_too_early_zero_timeout():
    m = htmap.map('stderr-too-early', lambda x: x, range(1))

    with pytest.raises(htmap.exceptions.OutputNotFound):
        print(m.stderr(0, timeout = 0))


def test_looking_at_stdout_too_early_small_timeout():
    m = htmap.map('stdout-too-early', lambda x: x, range(1))

    with pytest.raises(htmap.exceptions.TimeoutError):
        print(m.stdout(0, timeout = 0.001))


def test_looking_at_stderr_too_early_small_timeout():
    m = htmap.map('stderr-too-early', lambda x: x, range(1))

    with pytest.raises(htmap.exceptions.TimeoutError):
        print(m.stderr(0, timeout = 0.001))


@pytest.mark.usefixtures('delivery_methods')
def test_stdout_sees_print():
    m = htmap.map('stdout-sees-print', lambda x: print('foobar'), range(1))

    assert 'foobar' in m.stdout(0)


@pytest.mark.usefixtures('delivery_methods')
def test_stderr_sees_print():
    m = htmap.map('stderr-sees-print', lambda x: print('foobar', file = sys.stderr), range(1))

    assert 'foobar' in m.stderr(0)
