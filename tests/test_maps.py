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

    result.wait(timeout = 60)

    assert get_number_of_files_in_dir(result._outputs_dir) == num_inputs


def test_starmap_creates_correct_number_of_output_files(mapped_power):
    num_inputs = 3
    result = mapped_power.starmap(
        'map',
        args = ((x,) for x in range(num_inputs)),
        kwargs = ({'p': p} for p in range(num_inputs)),
    )

    result.wait(timeout = 60)

    assert get_number_of_files_in_dir(result._outputs_dir) == num_inputs


def test_map_produces_correct_output(mapped_doubler):
    n = 3
    result = mapped_doubler.map('map', range(n))

    assert list(result) == [2 * x for x in range(n)]


def test_map_with_kwargs_produces_correct_output(mapped_power):
    n = 3
    p = 2
    result = mapped_power.map('map', range(n), p = p)

    assert list(result) == [x ** p for x in range(n)]


def test_starmap_produces_correct_output(mapped_power):
    n = 3
    result = mapped_power.starmap(
        'map',
        args = ((x,) for x in range(n)),
        kwargs = ({'p': p} for p in range(n)),
    )

    assert list(result) == [x ** p for x, p in zip(range(n), range(n))]


def test___getitem__with_index_with_timeout(mapped_doubler):
    result = mapped_doubler.map('map', range(3))

    result.wait(timeout = 60)

    assert result[2] == 4


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
def test_get_with_short_timeout_raises_timeout_error(mapped_sleepy_double, timeout):
    n = 3
    result = mapped_sleepy_double.map('map', range(n))

    with pytest.raises(htmap.exceptions.TimeoutError):
        print(result.get(n - 1, timeout = timeout))


def test_get_waits_until_ready(mapped_doubler):
    result = mapped_doubler.map('map', (0, 1, 2))

    assert result.get(2) == 4


def test_cannot_use_same_mapid_again(mapped_doubler):
    result = mapped_doubler.map('foo', range(1))
    result.wait(timeout = 60)

    with pytest.raises(htmap.exceptions.MapIdAlreadyExists):
        again = mapped_doubler.map('foo', range(1))


def test_can_use_same_mapid_again_if_force_overwrite(mapped_doubler):
    result = mapped_doubler.map('foo', range(1))
    result.wait(timeout = 60)

    time.sleep(.1)

    again = mapped_doubler.map('foo', range(1), force_overwrite = True)


def test_can_use_same_mapid_again_if_force_overwrite_if_not_used(mapped_doubler):
    again = mapped_doubler.map('foo', range(1), force_overwrite = True)


def test_empty_map_raises_empty_map(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        mapped_doubler.map('foo', [])


def test_empty_starmap_raises_empty_map(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        mapped_doubler.starmap('foo', [], [])
