import pytest

import htcmap
from htcmap.settings import DotMap, Settings


def test_setting_getitem_drills_down():
    s = Settings(
        DotMap(
            foo = 'bar',
            inner = DotMap(
                foo = 'bong'
            )
        ),

    )

    assert s['inner.foo'] == 'bong'


def test_setting_getattr_drills_down():
    s = Settings(
        DotMap(
            foo = 'bar',
            inner = DotMap(
                foo = 'bong'
            )
        ),
    )

    assert s.inner.foo == 'bong'


def test_missing_raises():
    s = Settings()

    with pytest.raises(htcmap.exceptions.MissingSetting):
        s['nope']
