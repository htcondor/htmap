import pytest

import htmap
from htmap import MapResult


def test_len_of_map_builder(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        jb(5)
        jb(3)
        jb(7)

    assert len(jb) == 3


def test_map_builder_produces_correct_results(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        jb(5)
        jb(3)
        jb(7)

    assert list(jb.result) == [10, 6, 14]


def test_getting_result_before_ending_with_raises_no_result_yet(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        jb(5)
        with pytest.raises(htmap.exceptions.NoResultYet):
            jb.result


def test_getting_result_after_ending_with_is_a_result(mapped_doubler):
    with mapped_doubler.build_map('map') as jb:
        jb(5)

    assert isinstance(jb.result, MapResult)


def test_raising_exception_inside_with_reraises(mapped_doubler):
    with pytest.raises(htmap.exceptions.HTMapException):
        with mapped_doubler.build_map('foo') as jb:
            raise htmap.exceptions.HTMapException('foobar')


def test_empty_map_builder_raises_empty_map(mapped_doubler):
    with pytest.raises(htmap.exceptions.EmptyMap):
        with mapped_doubler.build_map('foo') as jb:
            pass
