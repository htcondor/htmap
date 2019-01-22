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


def test_map_dir_does_not_exist_after_remove(mapped_doubler):
    m = mapped_doubler.map(range(2))
    map_dir = m._map_dir

    assert map_dir.exists()

    m.remove()

    assert not map_dir.exists()


def test_map_dir_does_not_exist_after_remove_shortcut(mapped_doubler):
    m = mapped_doubler.map(range(2))
    map_dir = m._map_dir

    assert map_dir.exists()

    htmap.remove(m.tag)

    assert not map_dir.exists()


def test_remove_shortcut_on_nonexistent_map_dir_raises():
    with pytest.raises(htmap.exceptions.TagNotFound):
        htmap.remove('no-such-tag', not_exist_ok = False)


def test_remove_shortcut_on_nonexistent_map_dir_fails_silently_if_not_exist_ok_set():
    htmap.remove('no-such-tag', not_exist_ok = True)


def test_map_is_marked_as_removed_after_calling_remove(mapped_doubler):
    m = mapped_doubler.map(range(1))

    m.remove()

    assert m.is_removed


@pytest.mark.parametrize(
    'method',
    [
        key
        for key, val in inspect.getmembers(htmap.Map, predicate = inspect.isfunction)
        if not key.startswith('_')
    ],
)
def test_calling_public_methods_after_remove_raises(method, mapped_doubler):
    m = mapped_doubler.map(range(1))

    m.remove()

    with pytest.raises(htmap.exceptions.MapWasRemoved):
        # some of the methods take arguments
        # but this will actually fail before that check happens
        getattr(m, method)()
