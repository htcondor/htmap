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


def test_htcmap_is_idempotent_if_no_name_given(mapped_doubler):
    mapper = htcmap.htcmap(mapped_doubler)

    assert mapper.name == mapped_doubler.name
    assert mapper.func is mapped_doubler.func


def test_repeated_htcmap_has_same_func_if_name_given(mapped_doubler):
    n = 'joe'
    mapper = htcmap.htcmap(name = n)(mapped_doubler)

    assert mapper.name == n
    assert mapper.func is mapped_doubler.func


def test_can_still_call_function_as_normal(mapped_doubler):
    assert mapped_doubler(5) == 10
