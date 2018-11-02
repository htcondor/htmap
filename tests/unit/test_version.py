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

from htmap import _version_info


@pytest.mark.parametrize(
    'version, expected',
    [
        ('0.1.0', (0, 1, 0, '')),
        ('0.1.0rc1', (0, 1, 0, 'rc1')),
        ('0.1.0a5', (0, 1, 0, 'a5')),
        ('2.4.3joe', (2, 4, 3, 'joe')),
        ('2.4.3', (2, 4, 3, '')),
        ('2.4.3.1', (2, 4, 3, '.1')),
    ]
)
def test_version_info(version, expected):
    assert _version_info(version) == expected
