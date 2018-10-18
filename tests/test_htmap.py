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

import htmap


def test_htmap_map_is_cleaned_up_after_iter(doubler):
    m = htmap.htmap(doubler, range(2))

    list(m)

    assert len(htmap.map_ids()) == 0


@pytest.mark.usefixtures('delivery_methods')
def test_htmap_map_gets_right_results(doubler):
    m = htmap.htmap(doubler, range(2))

    assert list(m) == [0, 2]


def test_htstarmap_map_is_cleaned_up_after_iter(power):
    m = htmap.htstarmap(power, args = [(1, 2), ])

    list(m)

    assert len(htmap.map_ids()) == 0


@pytest.mark.usefixtures('delivery_methods')
def test_htstarmap_map_gets_right_results(power):
    m = htmap.htstarmap(power, args = [(1, 2), ])

    assert list(m) == [2, 4]
