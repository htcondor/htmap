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

from htmap import utils


@pytest.mark.parametrize(
    'num_bytes, expected',
    [
        (100, '100.0 B'),
        (1024, '1.0 KB'),
        (2048, '2.0 KB'),
        (2049, '2.0 KB'),
        (1024 * 1024 * 1024 * .5, '512.0 MB'),
        (1024 * 1024 * 1024, '1.0 GB'),
        (1024 * 1024 * 1024 * 1024 * .25, '256.0 GB'),
    ]
)
def test_num_bytes_to_str(num_bytes, expected):
    assert expected == utils.num_bytes_to_str(num_bytes)
