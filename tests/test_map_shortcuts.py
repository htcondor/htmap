import htmap

from htmap.mapper import HTMapper


def test_map_shortcut_calls_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'map')

    htmap.map('map', doubler, range(10))

    assert mocked.call_count == 1


def test_starmap_shortcut_calls_starmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'starmap')

    htmap.starmap('map', doubler, range(10), [])

    assert mocked.call_count == 1


def test_build_map_calls_build_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'build_map')

    htmap.build_map('map', doubler)

    assert mocked.call_count == 1
