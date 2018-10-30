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

from pathlib import Path

from htmap import htio

BUILTIN_OBJECTS = [
    5,
    2.3,
    1 + 1j,
    'foo',
    ('a', 'b', 'c'),
    ['d', 'e', 'f'],
    {'h', 'i', 'j'},
    {'k': 'v'},
    frozenset(('abc', 'def', 'hij')),
    True,
    False,
    b'011010001000',
    range(5),
    None,
]


@pytest.mark.parametrize('obj', BUILTIN_OBJECTS)
def test_saved_obj_path_exists(obj, tmpdir):
    path = Path(tmpdir.mkdir('htio_save_object_path_test').join('obj'))

    htio.save_object(obj, path)

    assert path.exists()


@pytest.mark.parametrize('obj', BUILTIN_OBJECTS)
def test_loaded_obj_equals_saved_obj(obj, tmpdir):
    path = Path(tmpdir.mkdir('htio_load_object_test').join('obj'))

    htio.save_object(obj, path)

    loaded = htio.load_object(path)

    assert loaded == obj

