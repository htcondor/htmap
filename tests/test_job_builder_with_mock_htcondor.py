import pytest

import htmap
from htmap.mapper import MapResult


@pytest.mark.usefixtures('mock_submit')
def test_len_of_job_builder(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        jb(5)
        jb(3)
        jb(7)

    assert len(jb) == 3


@pytest.mark.usefixtures('mock_submit')
def test_job_builder_results(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        jb(5)
        jb(3)
        jb(7)

    assert list(jb.result) == [10, 6, 14]


@pytest.mark.usefixtures('mock_submit')
def test_getting_result_before_ending_with_raises_no_result_yet(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        with pytest.raises(htmap.exceptions.NoResultYet):
            jb.result


@pytest.mark.usefixtures('mock_submit')
def test_getting_result_after_ending_with_is_a_result(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        pass

    assert isinstance(jb.result, MapResult)
