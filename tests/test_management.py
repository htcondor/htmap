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

import shutil
import time

import pytest

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

    time.sleep(.3)

    htmap.clean()

    assert len(htmap.map_ids()) == 0


def test_clean_without_maps_dir_doesnt_raise_exception():
    shutil.rmtree(
        htmap.settings['HTMAP_DIR'] / htmap.settings['MAPS_DIR_NAME'],
        ignore_errors = True,
    )

    htmap.clean()
