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

import pytest

import htmap


@pytest.mark.usefixtures('delivery_methods')
def test_map_produces_correct_output(mapped_doubler):
    n = 3
    m = mapped_doubler.map(range(n))

    assert list(m) == [2 * x for x in range(n)]


@pytest.mark.usefixtures('delivery_methods')
def test_starmap_produces_correct_output(mapped_power):
    n = 3
    m = mapped_power.starmap(
        args = ((x,) for x in range(n)),
        kwargs = ({'p': p} for p in range(n)),
    )

    assert list(m) == [x ** p for x, p in zip(range(n), range(n))]


def test_getitem_too_soon_raises_output_not_found(mapped_sleepy_double):
    n = 3
    m = mapped_sleepy_double.map(range(n))

    with pytest.raises(htmap.exceptions.OutputNotFound):
        print(m[0])


@pytest.mark.parametrize(
    'timeout',
    [
        0.01,
        datetime.timedelta(seconds = 0.01),
    ]
)
def test_get_with_too_short_timeout_raises_timeout_error(mapped_sleepy_double, timeout):
    n = 3
    m = mapped_sleepy_double.map(range(n))

    with pytest.raises(htmap.exceptions.TimeoutError):
        print(m.get(n - 1, timeout = timeout))


def test_get_waits_until_ready(mapped_doubler):
    m = mapped_doubler.map((0, 1, 2))

    assert m.get(2) == 4


def test_cannot_use_same_tag_again(mapped_doubler):
    mapped_doubler.map(range(1), tag = 'same-tag')

    with pytest.raises(htmap.exceptions.TagAlreadyExists):
        mapped_doubler.map(range(1), tag = 'same-tag')


def test_empty_map_raises_empty_map_exception(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        mapped_doubler.map([])


def test_empty_starmap_raises_empty_map_exception(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        mapped_doubler.starmap([], [])


def test_iter_inputs(mapped_doubler):
    m = mapped_doubler.map(range(3))

    assert list(m.iter_inputs()) == [((0,), {}), ((1,), {}), ((2,), {})]
