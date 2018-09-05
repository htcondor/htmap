import pytest

import htmap
from htmap.mapping import raise_if_map_id_is_invalid, INVALID_FILENAME_CHARACTERS


@pytest.mark.parametrize(
    'map_id',
    list(INVALID_FILENAME_CHARACTERS) + [
        '/abc',
        '/def.',
        '\\\\',
        '\\\\',
    ]
)
def test_bad_map_ids(map_id):
    with pytest.raises(htmap.exceptions.InvalidMapId):
        raise_if_map_id_is_invalid(map_id)


@pytest.mark.parametrize(
    'map_id',
    [
        'joe',
        'bob',
        'map_1',
        'data_from_the_guy',
        'hello-1',
        'test-abc',
        'test__01__underscores',
    ]
)
def test_good_map_ids(map_id):
    raise_if_map_id_is_invalid(map_id)
