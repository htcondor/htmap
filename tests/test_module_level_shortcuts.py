import htmap

from htmap.mapper import HTMapper


def test_map_shortcut_calls_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'map')

    htmap.map(doubler, range(10))

    assert mocked.was_called


def test_productmap_shortcut_calls_productmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'productmap')

    htmap.productmap(doubler, range(10))

    assert mocked.was_called


def test_starmap_shortcut_calls_starmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTMapper, 'starmap')

    htmap.starmap(doubler, range(10), [])

    assert mocked.was_called
