# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Set

import pytest

from pathlib import Path

import htmap

TEST_FILE_NAME = 'test_file'


@pytest.fixture(scope = 'function')
def transfer_path(tmp_path):
    p = htmap.TransferPath(tmp_path / TEST_FILE_NAME)
    p.touch()

    return p


def cwd_names() -> Set[str]:
    return {p.name for p in Path.cwd().iterdir()}


def test_path_in_args(transfer_path):
    m = htmap.map(lambda p: p.name in cwd_names(), [transfer_path])
    m.wait(180)

    assert m[0]


def test_path_in_kwargs(transfer_path):
    def func(path = None):
        return path.name in cwd_names()

    m = htmap.starmap(func, kwargs = [{'path': transfer_path}])
    m.wait(180)

    assert m[0]


def test_multiple_paths_in_args(tmp_path):
    paths = [
        htmap.TransferPath(tmp_path / "test-1.txt"),
        htmap.TransferPath(tmp_path / "test-2.txt"),
        htmap.TransferPath(tmp_path / "test-3.txt"),
    ]
    for p in paths:
        p.touch()

    @htmap.mapped
    def func(a, b, c):
        return all(p.name in cwd_names() for p in (a, b, c))

    m = func.starmap(args = [paths])
    m.wait(180)

    assert m[0]


def test_paths_in_list_arg(transfer_path):
    @htmap.mapped
    def func(list_of_paths):
        return all(p.name in cwd_names() for p in list_of_paths)

    m = func.map(args = [[transfer_path]])
    m.wait(180)

    assert m[0]


def test_paths_in_set_arg(transfer_path):
    @htmap.mapped
    def func(set_of_paths):
        return all(p.name in cwd_names() for p in set_of_paths)

    m = func.map(args = [{transfer_path}])
    m.wait(180)

    assert m[0]


def test_paths_in_dict_arg(transfer_path):
    @htmap.mapped
    def func(dict_of_paths):
        return all(p.name in cwd_names() for p in dict_of_paths.values())

    m = func.map(args = [{'key': transfer_path}])
    m.wait(180)

    assert m[0]


def test_duplicate_paths(transfer_path):
    @htmap.mapped
    def func(list_of_paths):
        return all(p.name in cwd_names() for p in list_of_paths)

    m = func.map(args = [[transfer_path, transfer_path]])
    m.wait(180)

    assert m[0]
