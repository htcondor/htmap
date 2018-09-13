"""
Copyright 2018 HTCondor Team, Computer Sciences Department,
University of Wisconsin-Madison, WI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import pytest

import htmap
from htmap.mapping import raise_if_map_id_is_invalid, INVALID_FILENAME_CHARACTERS


@pytest.mark.parametrize(
    'map_id',
    list(INVALID_FILENAME_CHARACTERS) + [
        '/abc',
        '/def.',
        '\\\\',
        '\\\\',
    ]
)
def test_bad_map_ids(map_id):
    with pytest.raises(htmap.exceptions.InvalidMapId):
        raise_if_map_id_is_invalid(map_id)


@pytest.mark.parametrize(
    'map_id',
    [
        'joe',
        'bob',
        'map_1',
        'data_from_the_guy',
        'hello-1',
        'test-abc',
        'test__01__underscores',
    ]
)
def test_good_map_ids(map_id):
    raise_if_map_id_is_invalid(map_id)
