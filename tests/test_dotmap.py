from htcmap.settings import DotMap


def test_getitem():
    d = DotMap(
        foo = 'bar',
    )

    assert d['foo'] == 'bar'


def test_getattr():
    d = DotMap(
        foo = 'bar',
    )

    assert d.foo == 'bar'


def test_get():
    d = DotMap(
        foo = 'bar',
    )

    assert d.get('foo') == 'bar'


def test_get_with_default():
    d = DotMap(
        foo = 'bar',
    )

    assert d.get('nope', 'bong') == 'bong'
