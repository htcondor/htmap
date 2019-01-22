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
from htmap import htio


def test_option_set_on_mapped_function_is_inherited():
    @htmap.mapped(
        map_options = htmap.MapOptions(
            request_memory = '123MB',
        )
    )
    def double(x):
        return 2 * x

    m = double.map(range(1))

    sub = htio.load_submit(m._map_dir)

    assert sub['request_memory'] == '123MB'


def test_option_set_on_mapped_function_is_overridden():
    @htmap.mapped(
        map_options = htmap.MapOptions(
            request_memory = '123MB',
        )
    )
    def double(x):
        return 2 * x

    m = double.map(
        range(1),
        map_options = htmap.MapOptions(
            request_memory = '456MB',
        )
    )

    sub = htio.load_submit(m._map_dir)

    assert sub['request_memory'] == '456MB'
