# Copyright 2019 HTCondor Team, Computer Sciences Department,
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

import shutil

import pytest


def untagged_map_is_transient(mapped_doubler):
    m = mapped_doubler.map(range(1))

    assert m.is_transient


def tagged_map_is_not_transient(mapped_doubler):
    m = mapped_doubler.map(range(1), tag = 'tagged')

    assert not m.is_transient


def retagged_map_becomes_not_transient(mapped_doubler):
    m = mapped_doubler.map(range(1))

    assert m.is_transient

    m.retag('new-tag')

    assert not m.is_transient


def can_set_transient_false(mapped_doubler):
    m = mapped_doubler.map(range(1))

    assert m.is_transient

    m.is_transient = False

    assert not m.is_transient


def can_set_transient_true(mapped_doubler):
    m = mapped_doubler.map(range(1), tag = 'tagged')

    assert not m.is_transient

    m.is_transient = True

    assert m.is_transient
