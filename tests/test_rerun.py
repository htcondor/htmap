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


def test_rerun(mapped_doubler):
    result = mapped_doubler.map('map', [1, 2, 3])

    result.rerun()

    assert list(result) == [2, 4, 6]


def test_recover_then_rerun(mapped_doubler):
    result = mapped_doubler.map('map', [1, 2, 3])
    result._remove_from_queue()

    recovered = htmap.recover('map')
    recovered.rerun()

    assert list(recovered) == [2, 4, 6]
