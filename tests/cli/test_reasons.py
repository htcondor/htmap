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

import htmap


def test_retag(cli):
    m = htmap.map(str, range(1), tag="old")

    result = cli(["retag", "old", "new"])

    assert m.tag == "new"


def test_retag_message_has_old_tag(cli):
    m = htmap.map(str, range(1), tag="old")

    result = cli(["retag", "old", "new"])

    assert "old" in result.output


def test_retag_message_has_new_tag(cli):
    m = htmap.map(str, range(1), tag="old")

    result = cli(["retag", "old", "new"])

    assert "new" in result.output
