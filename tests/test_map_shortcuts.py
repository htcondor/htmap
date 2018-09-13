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

import htmap

from htmap.mapper import HTMapper


def test_map_shortcut_calls_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'map')

    htmap.map('map', doubler, range(10))

    assert mocked.call_count == 1


def test_starmap_shortcut_calls_starmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'starmap')

    htmap.starmap('map', doubler, range(10), [])

    assert mocked.call_count == 1


def test_build_map_calls_build_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'build_map')

    htmap.build_map('map', doubler)

    assert mocked.call_count == 1
