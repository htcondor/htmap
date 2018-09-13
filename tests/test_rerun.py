import pytest

import htmap


def test_rerun(mapped_doubler):
    result = mapped_doubler.map('map', [1, 2, 3])

    result.rerun()

    assert list(result) == [2, 4, 6]


def test_recover_then_rerun(mapped_doubler):
    result = mapped_doubler.map('map', [1, 2, 3])
    result._remove_from_queue()

    recovered = htmap.recover('map')
    recovered.rerun()

    assert list(recovered) == [2, 4, 6]
