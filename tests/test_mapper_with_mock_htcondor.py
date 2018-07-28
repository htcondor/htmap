import pytest


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
