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

import htmap


def test_path_to_transfer_path():
    assert isinstance(Path(htmap.TransferPath('foobar')), Path)


def test_transfer_path_to_path():
    assert isinstance(htmap.TransferPath(Path('foobar')), htmap.TransferPath)


def test_transfer_path_isinstance_path():
    assert isinstance(htmap.TransferPath.cwd(), Path)


def test_path_is_not_instance_of_transfer_path():
    assert not isinstance(Path.cwd(), htmap.TransferPath)


def test_transfer_path_is_subclass_of_path():
    assert issubclass(htmap.TransferPath, Path)


def path_is_not_subclass_of_transfer_path():
    assert not issubclass(Path, htmap.TransferPath)
