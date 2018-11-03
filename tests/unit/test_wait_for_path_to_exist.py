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

from datetime import timedelta
from pathlib import Path

import htmap
from htmap.utils import wait_for_path_to_exist, timeout_to_seconds


def test_returns_when_path_does_exist():
    path = Path(__file__)

    wait_for_path_to_exist(path)


@pytest.mark.parametrize(
    'timeout',
    [
        0,
        -1
    ]
)
def test_timeout_on_nonexistent_path(timeout):
    path = Path('foo')

    with pytest.raises(htmap.exceptions.TimeoutError):
        wait_for_path_to_exist(path, timeout = timeout)


@pytest.mark.parametrize(
    'timeout, expected',
    [
        (1, 1.0),
        (.1, .1),
        (timedelta(seconds = 2.3), 2.3),
        (None, None),
    ]
)
def test_timeout_to_seconds(timeout, expected):
    assert timeout_to_seconds(timeout) == expected
