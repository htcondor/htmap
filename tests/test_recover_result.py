"""
Copyright 2018 HTCondor Team, Computer Sciences Department,
University of Wisconsin-Madison, WI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

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
