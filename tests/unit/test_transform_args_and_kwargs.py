# Copyright 2020 HTCondor Team, Computer Sciences Department,
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

from pathlib import Path

import pytest

from htmap import TransferPath
from htmap.mapping import transform_args_and_kwargs, transform_input_path, transform_input_paths


@pytest.mark.parametrize(
    "path, expected_path",
    [
        (TransferPath.cwd() / "foo.txt", Path(".") / "foo.txt",),
        (TransferPath(Path.cwd() / "foo.txt", protocol="s3"), Path(".") / "foo.txt",),
    ],
)
def test_transform_input_path(path, expected_path):
    acc = []
    transformed_path = transform_input_path(path, acc)

    assert transformed_path == expected_path
    assert acc == [path]


@pytest.mark.parametrize(
    "args_and_kwargs, expected_processed, expected_input_paths",
    [
        (
            [((TransferPath.cwd() / "foo.txt",), {})],
            [((Path(".") / "foo.txt",), {})],
            [[TransferPath.cwd() / "foo.txt"]],
        ),
        (
            [((TransferPath.cwd() / "foo.txt",), {"k": TransferPath.cwd() / "bar.txt"},)],
            [((Path(".") / "foo.txt",), {"k": Path(".") / "bar.txt"})],
            [[TransferPath.cwd() / "foo.txt", TransferPath.cwd() / "bar.txt"]],
        ),
        (
            [((TransferPath("foo.txt", protocol="s3"),), {})],
            [((Path(".") / "foo.txt",), {})],
            [[TransferPath("foo.txt", protocol="s3")]],
        ),
    ],
)
def test_transform_args_and_kwargs(args_and_kwargs, expected_processed, expected_input_paths):
    processed, input_paths = transform_args_and_kwargs(args_and_kwargs)

    print(input_paths)
    assert processed == expected_processed

    # we don't guarantee ordering
    assert all(set(i) == set(e) for i, e in zip(input_paths, expected_input_paths))
