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
import htmap.mapper


def test_exception_inside_submit_removes_map_dir(mocker, mapped_doubler):
    class Marker(Exception):
        pass

    def bad_submit(*args, **kwargs):
        raise Marker()

    mocker.patch.object(htmap.HTMapper, '_submit', bad_submit)

    with pytest.raises(Marker):
        mapped_doubler.map('map', range(10))

    assert not htmap.mapper.map_dir_path('map').exists()
