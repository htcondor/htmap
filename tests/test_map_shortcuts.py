import htmap

from htmap.mapper import HTMapper


def test_map_shortcut_calls_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'map')

    htmap.map('map', doubler, range(10))

    assert mocked.was_called


def test_productmap_shortcut_calls_productmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'productmap')

    htmap.productmap('map', doubler, range(10))

    assert mocked.was_called


def test_starmap_shortcut_calls_starmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'starmap')

    htmap.starmap('map', doubler, range(10), [])

    assert mocked.was_called
