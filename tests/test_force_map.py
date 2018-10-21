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

import datetime
import time

import pytest

import htmap


def test_can_use_same_mapid_again_with_force_map(mapped_doubler):
    result = mapped_doubler.map('foo', range(1))

    again = mapped_doubler.force_map('foo', range(1))


def test_force_map_with_already_free_mapid(mapped_doubler):
    again = mapped_doubler.force_map('foo', range(1))


def test_can_use_same_mapid_again_with_force_starmap(mapped_power):
    result = mapped_power.map('foo', [(1,)])

    again = mapped_power.force_map('foo', [(1,)])


def test_force_starmap_with_already_free_mapid(mapped_power):
    again = mapped_power.force_map('foo', args = [(1,)])
