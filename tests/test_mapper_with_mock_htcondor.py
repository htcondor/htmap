import datetime

import pytest

import htcmap


def get_num_input_files(mapper):
    return len(tuple(mapper.inputs_dir.iterdir()))


def get_num_output_files(mapper):
    return len(tuple(mapper.outputs_dir.iterdir()))


@pytest.mark.usefixtures('mock_pool')
def test_map_creates_correct_number_of_input_files(mapped_doubler):
    num_inputs = 10
    mapped_doubler.map(range(num_inputs))

    assert get_num_input_files(mapped_doubler) == num_inputs


@pytest.mark.usefixtures('mock_pool')
def test_productmap_creates_correct_number_of_input_files(mapped_power):
    num_inputs = 10
    mapped_power.productmap(x = range(num_inputs), p = range(num_inputs))

    assert get_num_input_files(mapped_power) == num_inputs ** 2


@pytest.mark.usefixtures('mock_pool')
def test_starmap_creates_correct_number_of_input_files(mapped_power):
    num_inputs = 10
    mapped_power.starmap(
        args = ((x,) for x in range(num_inputs)),
        kwargs = ({'p': p} for p in range(num_inputs)),
    )

    assert get_num_input_files(mapped_power) == num_inputs


@pytest.mark.usefixtures('mock_pool')
def test_map_creates_correct_number_of_outputs_files(mapped_doubler):
    num_inputs = 10
    result = mapped_doubler.map(range(num_inputs))

    result.wait(timeout = 10)

    assert get_num_output_files(mapped_doubler) == num_inputs


@pytest.mark.usefixtures('mock_pool')
def test_productmap_creates_correct_number_of_output_files(mapped_power):
    num_inputs = 10
    result = mapped_power.productmap(x = range(num_inputs), p = range(num_inputs))

    result.wait(timeout = 10)

    assert get_num_output_files(mapped_power) == num_inputs ** 2


@pytest.mark.usefixtures('mock_pool')
def test_starmap_creates_correct_number_of_output_files(mapped_power):
    num_inputs = 10
    result = mapped_power.starmap(
        args = ((x,) for x in range(num_inputs)),
        kwargs = ({'p': p} for p in range(num_inputs)),
    )

    result.wait(timeout = 10)

    assert get_num_output_files(mapped_power) == num_inputs


@pytest.mark.usefixtures('mock_pool')
def test_map_produces_correct_output(mapped_doubler):
    n = 10
    result = mapped_doubler.map(range(n))

    assert list(result) == [2 * x for x in range(n)]


@pytest.mark.usefixtures('mock_pool')
def test_productmap_produces_correct_output(mapped_power):
    n = 10
    result = mapped_power.productmap(x = range(n), p = range(n))

    assert list(result) == [x ** p for x in range(n) for p in range(n)]


@pytest.mark.usefixtures('mock_pool')
def test_starmap_creates_correct_number_of_input_files(mapped_power):
    n = 10
    result = mapped_power.starmap(
        args = ((x,) for x in range(n)),
        kwargs = ({'p': p} for p in range(n)),
    )

    assert list(result) == [x ** p for x, p in zip(range(n), range(n))]


@pytest.mark.usefixtures('mock_pool')
def test___getitem__with_index_with_timeout(mapped_doubler):
    n = 10
    result = mapped_doubler.map(range(n))

    result.wait(timeout = 10)

    assert result[3] == 6


@pytest.mark.usefixtures('mock_pool')
def test_getitem_too_soon_raises_output_not_found(mapped_sleepy_double):
    n = 5
    result = mapped_sleepy_double.map(range(n))

    with pytest.raises(htcmap.exceptions.OutputNotFound):
        print(result[n - 1])


@pytest.mark.usefixtures('mock_pool')
@pytest.mark.parametrize(
    'timeout',
    [
        0.1,
        datetime.timedelta(seconds = 0.1),
    ]
)
def test_get_with_short_timeout_raises_timeout_error(mapped_sleepy_double, timeout):
    n = 5
    result = mapped_sleepy_double.map(range(n))

    with pytest.raises(htcmap.exceptions.TimeoutError):
        print(result.get(n - 1, timeout = timeout))


@pytest.mark.usefixtures('mock_pool')
def test_get_with_index_waits_until_ready(mapped_doubler):
    n = 10
    result = mapped_doubler.map(range(n))

    assert result.get(3) == 6


@pytest.mark.usefixtures('mock_pool')
def test_get_on_bogus_hash_raises_hash_not_in_result(mapped_doubler):
    result = mapped_doubler.map(tuple())  # empty map, no hashes

    with pytest.raises(htcmap.exceptions.HashNotInResult):
        result['a']


@pytest.mark.usefixtures('mock_pool')
def test_clean_inputs(mapped_doubler):
    result = mapped_doubler.map(range(10))
    result.wait(timeout = 10)

    num_files, num_bytes = mapped_doubler.clean_inputs()

    assert num_files == 10
    assert len(tuple(mapped_doubler.inputs_dir.iterdir())) == 0


@pytest.mark.usefixtures('mock_pool')
def test_clean_outputs(mapped_doubler):
    result = mapped_doubler.map(range(10))
    result.wait(timeout = 10)

    num_files, num_bytes = mapped_doubler.clean_outputs()

    assert num_files == 10
    assert len(tuple(mapped_doubler.outputs_dir.iterdir())) == 0
