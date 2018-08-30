import htmap


def test_decorator_without_parens():
    @htmap.htmap
    def foo(x):
        return x

    assert isinstance(foo, htmap.HTMapper)


def test_decorator_with_parens(doubler):
    @htmap.htmap()
    def foo(x):
        return x

    assert isinstance(foo, htmap.HTMapper)


def test_htmap_is_idempotent(mapped_doubler):
    mapper = htmap.htmap(mapped_doubler)

    assert isinstance(mapper, htmap.HTMapper)
    assert not isinstance(mapper.func, htmap.HTMapper)
    assert mapper.func is mapped_doubler.func


def test_can_still_call_function_as_normal(mapped_doubler):
    assert mapped_doubler(5) == 10
