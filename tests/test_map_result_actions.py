import pytest

import htmap

N = 1


def test_hold(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(N))

    result.hold()

    assert result.status_counts()[htmap.Status.HELD] == N


def test_hold_then_release(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(N))

    result.hold()
    assert result.status_counts()[htmap.Status.HELD] == N

    result.release()
    assert result.status_counts()[htmap.Status.HELD] == 0
