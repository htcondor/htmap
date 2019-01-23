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

import htmap
from htmap.options import get_base_descriptors, register_delivery_mechanism, unregister_delivery_mechanism


@pytest.fixture(scope = 'module', autouse = True)
def add_null_delivery():
    register_delivery_mechanism('null', lambda tag, map_dir: {})

    yield

    unregister_delivery_mechanism('null')


def test_job_batch_name_is_tag():
    descriptors = get_base_descriptors('foo', Path.cwd(), 'null')

    assert descriptors['JobBatchName'] == 'foo'
