import pytest

import htcmap
from htcmap.htcmap import HTCMapper


@pytest.mark.parametrize(
    'shortcut',
    [
        'map',
        'productmap',
        'starmap',
    ]
)
def test_shortcut_function_calls_method(mocker, doubler, shortcut):
    setattr(HTCMapper, shortcut, mocker.MagicMock())

    getattr(HTCMapper, shortcut)(doubler, range(10))

    assert getattr(HTCMapper, shortcut).called
