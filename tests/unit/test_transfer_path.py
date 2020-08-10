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

from pathlib import Path

import pytest

from htmap import TransferPath


@pytest.mark.parametrize(
    "transfer_path, expected",
    [
        (TransferPath.cwd() / "foobar.txt", (Path.cwd() / "foobar.txt").as_posix(),),
        (TransferPath.home() / "foobar.txt", (Path.home() / "foobar.txt").as_posix(),),
        (
            TransferPath(path="foo/0.txt", protocol="s3", location="s3.server.com",),
            "s3://s3.server.com/foo/0.txt",
        ),
    ],
)
def test_as_url(transfer_path, expected):
    assert transfer_path.as_url() == expected


def test_must_have_protocol_if_has_location():
    with pytest.raises(ValueError):
        TransferPath("foo.txt", location="foo.bar.com")
