from htcmap.mapper import zip_args_and_kwargs


def test_only_args():
    args = [(x,) for x in range(5)]
    kwargs = []

    z = list(zip_args_and_kwargs(args, kwargs))

    assert z == [
        ((0,), {}),
        ((1,), {}),
        ((2,), {}),
        ((3,), {}),
        ((4,), {}),
    ]


def test_args_and_some_kwargs():
    args = [(x,) for x in range(5)]
    kwargs = [{'y': y} for y in range(3)]

    z = list(zip_args_and_kwargs(args, kwargs))

    assert z == [
        ((0,), {'y': 0}),
        ((1,), {'y': 1}),
        ((2,), {'y': 2}),
        ((3,), {}),
        ((4,), {}),
    ]


def test_only_kwargs():
    args = []
    kwargs = [{'y': y} for y in range(5)]

    z = list(zip_args_and_kwargs(args, kwargs))

    assert z == [
        (tuple(), {'y': 0}),
        (tuple(), {'y': 1}),
        (tuple(), {'y': 2}),
        (tuple(), {'y': 3}),
        (tuple(), {'y': 4}),
    ]


def test_some_args_and_kwargs():
    args = [(x,) for x in range(3)]
    kwargs = [{'y': y} for y in range(5)]

    z = list(zip_args_and_kwargs(args, kwargs))

    assert z == [
        ((0,), {'y': 0}),
        ((1,), {'y': 1}),
        ((2,), {'y': 2}),
        (tuple(), {'y': 3}),
        (tuple(), {'y': 4}),
    ]
