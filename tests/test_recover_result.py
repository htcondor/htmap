import time

import pytest

import htmap


def test_recover_shortcut(mapped_doubler):
    result = mapped_doubler.map('map', range(10))

    recovered = htmap.recover('map')

    assert result.map_id == recovered.map_id
    assert result.cluster_ids == recovered.cluster_ids
    assert result.hashes == recovered.hashes


def test_recover_shortcut_calls_recover_method(mapped_doubler, mocker):
    mocked = mocker.patch.object(htmap.result.MapResult, 'recover')

    htmap.recover('map')

    assert mocked.was_called


def test_recover_classmethod(mapped_doubler):
    result = mapped_doubler.map('map', range(10))

    recovered = htmap.MapResult.recover('map')

    assert result.map_id == recovered.map_id
    assert result.cluster_ids == recovered.cluster_ids
    assert result.hashes == recovered.hashes


def test_recover_on_bad_mapid_raises_map_id_not_found():
    with pytest.raises(htmap.exceptions.MapIdNotFound):
        htmap.recover('no_such_mapid')

