import os

import pytest

import htmap


def test_env_var_is_set_on_execute():
    @htmap.htmap
    def check(x):
        return 'HTMAP_ON_EXECUTE' in os.environ

    assert list(check.map('chk', [0]))[0]  # that's the return value of check
