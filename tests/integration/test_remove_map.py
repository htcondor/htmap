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

import inspect

import pytest

import htmap
from htmap.mapping import map_dir_path


def test_map_dir_does_not_exist_after_remove(mapped_doubler):
    map_id = 'foo'
    result = mapped_doubler.map(map_id, range(2))

    assert map_dir_path(map_id).exists()

    result.remove()

    assert not map_dir_path(map_id).exists()


def test_map_dir_does_not_exist_after_remove_shortcut(mapped_doubler):
    map_id = 'foo'
    result = mapped_doubler.map(map_id, range(2))

    assert map_dir_path(map_id).exists()

    htmap.remove(map_id)

    assert not map_dir_path(map_id).exists()


def test_remove_shortcut_on_nonexistent_map_dir_raises():
    with pytest.raises(htmap.exceptions.MapIdNotFound):
        htmap.remove('no_such_mapid', not_exist_ok = False)


def test_remove_shortcut_on_nonexistent_map_dir_fails_silently_if_not_exist_ok_set():
    htmap.remove('no_such_mapid', not_exist_ok = True)


@pytest.mark.parametrize(
    'method',
    [
        key
        for key, val in inspect.getmembers(htmap.Map, predicate = inspect.isfunction)
        if not key.startswith('_')
    ],
)
def test_calling_public_methods_after_remove_raises(method, mapped_doubler):
    result = mapped_doubler.map('method_after_remove', range(1))

    result.remove()

    with pytest.raises(htmap.exceptions.MapWasRemoved):
        # some of the methods take arguments
        # but this will actually fail before that check happens
        getattr(result, method)()
