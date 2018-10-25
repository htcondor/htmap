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


def get_number_of_files_in_dir(dir):
    return len(tuple(dir.iterdir()))


def test_map_creates_correct_number_of_input_files(mapped_doubler):
    num_inputs = 3
    result = mapped_doubler.map('map', range(num_inputs))

    assert get_number_of_files_in_dir(result._inputs_dir) == num_inputs


def test_starmap_creates_correct_number_of_input_files(mapped_power):
    num_inputs = 3
    result = mapped_power.starmap(
        'map',
        args = ((x,) for x in range(num_inputs)),
        kwargs = ({'p': p} for p in range(num_inputs)),
    )

    assert get_number_of_files_in_dir(result._inputs_dir) == num_inputs


def test_map_creates_correct_number_of_outputs_files(mapped_doubler):
    num_inputs = 3
    result = mapped_doubler.map('map', range(num_inputs))

    result.wait()

    assert get_number_of_files_in_dir(result._outputs_dir) == num_inputs


def test_starmap_creates_correct_number_of_output_files(mapped_power):
    num_inputs = 3
    result = mapped_power.starmap(
        'map',
        args = ((x,) for x in range(num_inputs)),
        kwargs = ({'p': p} for p in range(num_inputs)),
    )

    result.wait()

    assert get_number_of_files_in_dir(result._outputs_dir) == num_inputs


@pytest.mark.usefixtures('delivery_methods')
def test_map_produces_correct_output(mapped_doubler):
    n = 3
    result = mapped_doubler.map('map', range(n))

    assert list(result) == [2 * x for x in range(n)]


@pytest.mark.usefixtures('delivery_methods')
def test_map_with_kwargs_produces_correct_output(mapped_power):
    n = 3
    p = 2
    result = mapped_power.map('map', range(n), p = p)

    assert list(result) == [x ** p for x in range(n)]


@pytest.mark.usefixtures('delivery_methods')
def test_starmap_produces_correct_output(mapped_power):
    n = 3
    result = mapped_power.starmap(
        'map',
        args = ((x,) for x in range(n)),
        kwargs = ({'p': p} for p in range(n)),
    )

    assert list(result) == [x ** p for x, p in zip(range(n), range(n))]


def test_getitem_with_index_with_timeout(mapped_doubler):
    result = mapped_doubler.map('map', range(2))

    result.wait()

    assert result[1] == 2


def test_getitem_too_soon_raises_output_not_found(mapped_sleepy_double):
    n = 3
    result = mapped_sleepy_double.map('map', range(n))

    with pytest.raises(htmap.exceptions.OutputNotFound):
        print(result[n - 1])


@pytest.mark.parametrize(
    'timeout',
    [
        0.01,
        datetime.timedelta(seconds = 0.01),
    ]
)
def test_get_with_too_short_timeout_raises_timeout_error(mapped_sleepy_double, timeout):
    n = 3
    result = mapped_sleepy_double.map('map', range(n))

    with pytest.raises(htmap.exceptions.TimeoutError):
        print(result.get(n - 1, timeout = timeout))


def test_get_waits_until_ready(mapped_doubler):
    result = mapped_doubler.map('map', (0, 1, 2))

    assert result.get(2) == 4


def test_cannot_use_same_mapid_again(mapped_doubler):
    result = mapped_doubler.map('foo', range(1))

    with pytest.raises(htmap.exceptions.MapIdAlreadyExists):
        again = mapped_doubler.map('foo', range(1))


def test_can_use_same_mapid_again_if_force_overwrite(mapped_doubler):
    result = mapped_doubler.map('foo', range(1))

    again = mapped_doubler.map('foo', range(1), force_overwrite = True)


def test_force_overwrite_with_already_free_mapid(mapped_doubler):
    again = mapped_doubler.map('foo', range(1), force_overwrite = True)


def test_empty_map_raises_empty_map_exception(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        mapped_doubler.map('foo', [])


def test_empty_starmap_raises_empty_map_exception(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        mapped_doubler.starmap('foo', [], [])


def test_huge_output_file_is_not_read_too_early():
    """
    https://github.com/htcondor/htmap/issues/8
    """

    @htmap.htmap
    def huge_output(_):
        return '0' * 1_000_000_000

    results = huge_output.map('huge', range(1))
    list(results)  # this will raise an unpickling error if it reads during file transfer


def test_iter_inputs(mapped_doubler):
    m = mapped_doubler.map('iter_inputs', range(3))

    assert list(m.iter_inputs()) == [((0,), {}), ((1,), {}), ((2,), {})]
