import pytest

import htmap
import htmap.mapper


def test_exception_inside_submit_removes_map_dir(mocker, mapped_doubler):
    class Marker(Exception):
        pass

    def bad_submit(*args, **kwargs):
        raise Marker()

    mocker.patch.object(htmap.HTMapper, '_submit', bad_submit)

    with pytest.raises(Marker):
        mapped_doubler.map('map', range(10))

    assert not htmap.mapper.map_dir_path('map').exists()
