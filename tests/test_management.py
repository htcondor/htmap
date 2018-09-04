import pytest

import shutil

import htmap


def test_map_ids(mapped_doubler):
    mapped_doubler.map('a', range(1))
    mapped_doubler.map('b', range(1))
    mapped_doubler.map('c', range(1))

    assert set(htmap.map_ids()) == {'a', 'b', 'c'}


def test_map_results(mapped_doubler):
    mapped_doubler.map('a', range(1))
    mapped_doubler.map('b', range(1))
    mapped_doubler.map('c', range(1))

    results = htmap.map_results()

    assert len(results) == 3
    assert all(isinstance(x, htmap.MapResult) for x in results)


def test_clean_removes_all_maps(mapped_doubler):
    results = [mapped_doubler.map('a', range(1)), mapped_doubler.map('b', range(1)), mapped_doubler.map('c', range(1))]

    for r in results:
        r.wait(timeout = 60)

    htmap.clean()

    assert len(htmap.map_ids()) == 0


def test_clean_without_maps_dir_doesnt_raise_exception():
    shutil.rmtree(
        htmap.settings['HTMAP_DIR'] / htmap.settings['MAPS_DIR_NAME'],
        ignore_errors = True,
    )

    htmap.clean()