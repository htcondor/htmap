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


def test_map_or_recover_func_with_no_existing_map(doubler):
    doubled = htmap.map_or_recover('dbl', doubler, range(1))


def test_map_or_recover_method_with_no_existing_map(mapped_doubler):
    doubled = mapped_doubler.map_or_recover('dbl', range(1))


def test_map_or_recover_func_with_existing_map(doubler):
    existing = htmap.map('dbl', doubler, range(1))
    doubled = htmap.map_or_recover('dbl', doubler, range(1))

    assert doubled is existing


def test_map_or_recover_method_with_existing_map(mapped_doubler):
    existing = mapped_doubler.map('dbl', range(1))
    doubled = mapped_doubler.map_or_recover('dbl', range(1))

    assert doubled is existing


def test_map_or_recover_method_with_existing_map_even_if_inputs_different(mapped_doubler):
    existing = mapped_doubler.map('dbl', range(1))
    doubled = mapped_doubler.map_or_recover('dbl', range(2))

    assert doubled is existing


def test_starmap_or_recover_func_with_no_existing_map(doubler):
    doubled = htmap.starmap_or_recover('dbl', doubler, args = [(1,)])


def test_starmap_or_recover_method_with_no_existing_map(mapped_doubler):
    doubled = mapped_doubler.starmap_or_recover('dbl', [(1,)])


def test_starmap_or_recover_func_with_existing_map(doubler):
    existing = htmap.starmap('dbl', doubler, [(1,)])
    doubled = htmap.starmap_or_recover('dbl', doubler, [(1,)])

    assert doubled is existing


def test_starmap_or_recover_method_with_existing_map(mapped_doubler):
    existing = mapped_doubler.starmap('dbl', range(1))
    doubled = mapped_doubler.starmap_or_recover('dbl', [(1,)])

    assert doubled is existing


def test_starmap_or_recover_method_with_existing_map_even_if_inputs_different(mapped_doubler):
    existing = mapped_doubler.starmap('dbl', [(1,)])
    doubled = mapped_doubler.starmap_or_recover('dbl', [(2,)])

    assert list(doubled) == [2]
