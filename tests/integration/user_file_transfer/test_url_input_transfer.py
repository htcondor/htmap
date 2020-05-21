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


import pytest

from pathlib import Path

import htmap

TIMEOUT = 300


@pytest.mark.timeout(TIMEOUT)
@pytest.mark.xfail(reason="I don't understand yet why this doesn't work...")
def test_input_transfer_via_file_protocol(tmp_path):
    f = htmap.TransferPath(__file__, protocol="file")

    MARKER = 12345

    def test(file):
        return "MARKER = 12345" in file.read_text()

    m = htmap.map(test, [f])

    assert m.get(0)


@pytest.mark.timeout(TIMEOUT)
def test_input_transfer_via_https_protocol(tmp_path):
    f = htmap.TransferPath("status/200", protocol="https", location="httpbin.org")

    def test(file):
        return file.read_text() == ""

    m = htmap.map(test, [f])

    assert m.get(0)
