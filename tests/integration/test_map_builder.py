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
from htmap import Map


def test_len_of_map_builder(mapped_doubler):
    with mapped_doubler.build_map() as jb:
        jb(5)

    assert len(jb) == 1


@pytest.mark.usefixtures('delivery_methods')
def test_map_builder_produces_correct_results(mapped_doubler):
    with mapped_doubler.build_map() as jb:
        jb(5)

    assert list(jb.map) == [10]


def test_getting_result_before_ending_with_raises_no_result_yet(mapped_doubler):
    with mapped_doubler.build_map() as jb:
        jb(5)
        with pytest.raises(htmap.exceptions.NoMapYet):
            jb.map


def test_getting_result_after_ending_with_is_a_result(mapped_doubler):
    with mapped_doubler.build_map() as jb:
        jb(5)

    assert isinstance(jb.map, Map)


def test_raising_exception_inside_with_reraises(mapped_doubler):
    with pytest.raises(htmap.exceptions.HTMapException):
        with mapped_doubler.build_map() as jb:
            raise htmap.exceptions.HTMapException('foobar')


def test_empty_map_builder_raises_empty_map(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        with mapped_doubler.build_map() as jb:
            pass
