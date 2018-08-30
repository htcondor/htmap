import pytest

import htmap

N = 1


def test_hold(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(N))

    result.hold()

    assert result._status_counts()[htmap.JobStatus.HELD] == N

    # todo: this is just cleanup, someday unnecessary
    result.remove()


def test_hold_then_release(mapped_sleepy_double):
    result = mapped_sleepy_double.map('sleepy', range(N))

    result.hold()
    assert result._status_counts()[htmap.JobStatus.HELD] == N

    result.release()
    assert result._status_counts()[htmap.JobStatus.HELD] == 0

    # todo: this is just cleanup, someday unnecessary
    result.remove()
