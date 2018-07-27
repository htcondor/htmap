import pytest

import htcmap


def test_decorator_without_parens(doubler):
    mapper = htcmap.htcmap(doubler)

    assert mapper.name == doubler.__name__
    assert mapper.func is doubler


def test_decorator_with_parens(doubler):
    mapper = htcmap.htcmap()(doubler)

    assert mapper.name == doubler.__name__
    assert mapper.func is doubler


def test_name_is_name_of_func_if_no_name_given(doubler):
    mapper = htcmap.htcmap(doubler)

    assert mapper.name == doubler.__name__


def test_name_is_given_name(doubler):
    n = 'joe'
    mapper = htcmap.htcmap(name = n)(doubler)

    assert mapper.name == n
