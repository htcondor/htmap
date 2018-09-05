import time

import pytest

import htmap


def test_rename_new_name_in_map_ids(mapped_doubler):
    result = mapped_doubler.map('old', range(2))
    result.wait()

    result.rename('new')

    assert 'new' in htmap.map_ids()


def test_rename_removes_old_map(mapped_doubler):
    result = mapped_doubler.map('old', range(2))

    result.wait()
    time.sleep(.1)

    result.rename('new')

    assert 'old' not in htmap.map_ids()


def test_rename_raises_if_not_complete(mapped_doubler):
    result = mapped_doubler.map('old', range(2))
    result.hold()

    with pytest.raises(htmap.exceptions.CannotRenameMap):
        result.rename('new')


def test_rename_raises_if_new_map_id_already_exists(mapped_doubler):
    result = mapped_doubler.map('old', range(2))
    result.wait()

    existing = mapped_doubler.map('target', range(2))
    existing.hold()  # doesn't matter, just speeds things up

    with pytest.raises(htmap.exceptions.MapIDAlreadyExists):
        result.rename('target')


def test_rerun_works_after_rename(mapped_doubler):
    result = mapped_doubler.map('old', range(2))
    result.wait()

    new_result = result.rename('new')

    new_result.rerun()

    assert list(new_result) == [0, 2]
