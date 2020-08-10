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

import htmap

TIMEOUT = 300


@pytest.mark.timeout(TIMEOUT)
def test_single_output_file_single_component():
    def test(_):
        p = Path("testfile")
        p.write_text("foobar")

        htmap.transfer_output_files(p)

    m = htmap.map(test, range(1), tag="test-single-output-file-one-comp")

    m.wait()

    assert (m.output_files[0] / "testfile").read_text() == "foobar"


@pytest.mark.timeout(TIMEOUT)
def test_single_output_file_multiple_components():
    def test(x):
        p = Path("testfile")
        p.write_text(str(x))

        htmap.transfer_output_files(p)

    m = htmap.map(test, range(2), tag="test-single-output-file-multi-comps")

    m.wait()

    for i, ops in enumerate(m.output_files):
        assert (ops / "testfile").read_text() == str(i)


@pytest.mark.timeout(TIMEOUT)
def test_multiple_output_files_from_single_component():
    def test(_):
        p = Path("testfile")
        p.write_text("foobar")

        q = Path("otherfile")
        q.write_text("wizbang")

        htmap.transfer_output_files(p, q)

    m = htmap.map(test, [None], tag="test-multi-output")

    m.wait()

    assert (m.output_files[0] / "testfile").read_text() == "foobar"
    assert (m.output_files[0] / "otherfile").read_text() == "wizbang"


@pytest.mark.timeout(TIMEOUT)
def test_nested_output_file():
    def test(_):
        d = Path("nested")
        d.mkdir()

        p = d / Path("testfile")
        p.write_text("foobar")

        htmap.transfer_output_files(p)

    m = htmap.map(test, [None], tag="test-nested-output-file")

    m.wait()

    assert (m.output_files[0] / "nested" / "testfile").read_text() == "foobar"
