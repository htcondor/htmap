import pytest

from htmap import mapping


def test_exception_inside_submit_removes_map_dir(mocker, doubler):
    class Marker(Exception):
        pass

    def bad_execute_submit(*args, **kwargs):
        raise Marker()

    mocker.patch('htmap.mapping.execute_submit', bad_execute_submit)

    with pytest.raises(Marker):
        mapping.map('map', doubler, range(10))

    assert not mapping.map_dir_path('map').exists()
