import pytest

import htmap


def test_map_ids(mapped_doubler):
    mapped_doubler.map('a', range(1))
    mapped_doubler.map('b', range(1))
    mapped_doubler.map('c', range(1))

    assert set(htmap.map_ids()) == set(('a', 'b', 'c'))


def test_map_results(mapped_doubler):
    mapped_doubler.map('a', range(1))
    mapped_doubler.map('b', range(1))
    mapped_doubler.map('c', range(1))

    results = htmap.map_results()

    assert len(results) == 3
    assert all(isinstance(x, htmap.MapResult) for x in results)


def test_clean_removes_all_maps(mapped_doubler):
    mapped_doubler.map('a', range(1))
    mapped_doubler.map('b', range(1))
    mapped_doubler.map('c', range(1))

    htmap.clean()

    assert len(htmap.map_ids()) == 0
