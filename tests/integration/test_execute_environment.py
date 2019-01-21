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

import os

import pytest

import htmap


@pytest.mark.usefixtures('delivery_methods')
def test_env_var_is_set_on_execute():
    @htmap.mapped
    def check(x):
        return os.getenv('HTMAP_ON_EXECUTE') == "1"

    assert list(check.map([0]))[0]  # that's the return value of check
