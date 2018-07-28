import htcmap

from htcmap.htcmap import HTCMapper


def test_map_shortcut_calls_map_method(mocker, doubler):
    mocked = mocker.patch.object(HTCMapper, 'map')

    htcmap.map(doubler, range(10))

    assert mocked.was_called


def test_productmap_shortcut_calls_productmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTCMapper, 'productmap')

    htcmap.productmap(doubler, range(10))

    assert mocked.was_called


def test_starmap_shortcut_calls_starmap_method(mocker, doubler):
    mocked = mocker.patch.object(HTCMapper, 'starmap')

    htcmap.starmap(doubler, range(10), [])

    assert mocked.was_called
