import pytest

import htmap
from htmap.settings import DotMap, Settings


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


def test_missing_raises_with_getitem():
    s = Settings()

    with pytest.raises(htmap.exceptions.MissingSetting):
        s['nope']


def test_missing_raises_with_getattr():
    s = Settings()

    with pytest.raises(htmap.exceptions.MissingSetting):
        s.nope
