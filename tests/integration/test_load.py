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


def test_load_shortcut(mapped_doubler):
    result = mapped_doubler.map(range(3), tag = 'load-shortcut')

    recovered = htmap.load('load-shortcut')

    assert recovered is result


def test_load_classmethod(mapped_doubler):
    result = mapped_doubler.map(range(3), tag = 'load-classmethod')

    recovered = htmap.Map.load('load-classmethod')

    assert recovered is result


def test_load_on_bad_tag_raises_tag_not_found():
    with pytest.raises(htmap.exceptions.TagNotFound):
        htmap.load('no-such-tag')
