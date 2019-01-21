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

import time

import pytest

import htmap


def test_memory_usage_is_nonzero_after_map_complete():
    # need it run for at least 5 seconds for it generate an image size event
    m = htmap.map(lambda x: time.sleep(10), [None])

    m.wait()
    print(m.memory_usage)

    assert all(x > 0 for x in m.memory_usage)
