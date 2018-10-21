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

import gc

import htmap

from .conftest import gc_disabled


def test_htmap_map_is_cleaned_up_after_iter(doubler):
    m = htmap.htmap(doubler, range(2))

    list(m)

    assert len(htmap.map_ids()) == 0


def test_htmap_map_gets_right_results(doubler):
    m = htmap.htmap(doubler, range(2))

    assert list(m) == [0, 2]


def test_htstarmap_map_is_cleaned_up_after_iter(power):
    m = htmap.htstarmap(power, args = [(1, 2), ])

    list(m)

    assert len(htmap.map_ids()) == 0


def test_htstarmap_map_gets_right_results(power):
    m = htmap.htstarmap(power, args = [(1,), (2,)])

    assert [1, 4] == list(m)


def test_ephemeral_map_is_cleaned_up_after_iter_if_single_error_during_execute():
    m = htmap.htmap(lambda x: 1 / 0, range(1))

    try:
        list(m)
    except htmap.exceptions.MapComponentError:
        pass

    assert len(htmap.map_ids()) == 0


def test_ephemeral_map_is_cleaned_up_after_iter_if_multiple_error_during_execute():
    m = htmap.htmap(lambda x: 1 / 0, range(2))

    try:
        list(m)
    except htmap.exceptions.MapComponentError:
        pass

    assert len(htmap.map_ids()) == 0


class DummyException(Exception):
    pass


def test_ephemeral_map_is_cleaned_up_after_iter_if_error_during_iter():
    # relies on the try/finally in __iter__
    m = htmap.htmap(lambda x: x, range(2))

    try:
        for out in m:
            raise DummyException
    except DummyException:
        pass

    assert len(htmap.map_ids()) == 0


def test_ephemeral_map_is_cleaned_up_after_iter_if_error_during_iter_inside_with():
    try:
        with htmap.htmap(lambda x: x, range(1)) as m:
            for out in m:
                raise DummyException
    except DummyException:
        pass

    assert len(htmap.map_ids()) == 0


def test_ephemeral_map_is_cleaned_up_by_gc():
    m = htmap.htmap(lambda x: x, range(1))

    assert len(htmap.map_ids()) == 1

    del m
    gc.collect()  # force a GC cycle

    assert len(htmap.map_ids()) == 0
