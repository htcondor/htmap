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
from htmap.tags import INVALID_TAG_CHARACTERS, raise_if_tag_is_invalid


@pytest.mark.parametrize(
    "tag", sorted(list(INVALID_TAG_CHARACTERS)) + ["/abc", "/def.", "def/", "\\\\", "",]
)
def test_bad_tags(tag):
    with pytest.raises(htmap.exceptions.InvalidTag):
        raise_if_tag_is_invalid(tag)


@pytest.mark.parametrize(
    "tag",
    [
        "joe",
        "bob",
        "map_1",
        "data_from_the_guy",
        "hello-1",
        "test-abc",
        "test__01__underscores",
    ],
)
def test_good_tags(tag):
    raise_if_tag_is_invalid(tag)
