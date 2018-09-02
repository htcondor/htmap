import htmap


def test_decorator_without_parens():
    @htmap.htmap
    def foo(x):
        return x

    assert isinstance(foo, htmap.MappedFunction)


def test_decorator_with_parens():
    @htmap.htmap()
    def foo(x):
        return x

    assert isinstance(foo, htmap.MappedFunction)


def test_decorator_with_map_options():
    @htmap.htmap(map_options = htmap.MapOptions())
    def foo(x):
        return x

    assert isinstance(foo, htmap.MappedFunction)


def test_can_still_call_function_as_normal(mapped_doubler):
    assert mapped_doubler(5) == 10
