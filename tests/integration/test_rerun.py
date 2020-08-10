# Copyright 2018 HTCondor Team, Computer Sciences Department,
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

import time
from pathlib import Path

import pytest

import htmap

TIMEOUT = 300


@pytest.mark.timeout(TIMEOUT)
def test_rerun(mapped_doubler):
    m = mapped_doubler.map([1])
    m.wait()

    m.rerun()

    assert list(m) == [2]


@pytest.mark.timeout(TIMEOUT)
def test_load_then_rerun(mapped_doubler):
    m = mapped_doubler.map([1], tag="load-then-rerun")
    m.wait()

    loaded = htmap.load("load-then-rerun")
    loaded.rerun()

    assert list(loaded) == [2]


@pytest.mark.timeout(TIMEOUT)
def test_rerun_out_of_range_component_raises(mapped_doubler):
    m = mapped_doubler.map([1], tag="load-then-rerun")
    m.wait()

    with pytest.raises(htmap.exceptions.CannotRerunComponents):
        m.rerun([5])


@pytest.fixture(scope="function")
def sleepy_doubler_that_writes_a_file():
    @htmap.mapped
    def sleepy_double(x):
        time.sleep(1)
        r = x * 2
        p = Path("foo")
        p.write_text("hi")
        htmap.transfer_output_files(p)
        return r

    return sleepy_double


@pytest.mark.timeout(TIMEOUT)
def test_rerun_removes_current_output_file(sleepy_doubler_that_writes_a_file):
    m = sleepy_doubler_that_writes_a_file.map([1], tag="load-then-rerun")

    m.wait()

    assert m.get(0) == 2

    m.rerun()

    with pytest.raises(htmap.exceptions.OutputNotFound):
        m[0]


@pytest.mark.timeout(TIMEOUT)
def test_rerun_removes_current_user_output_file(sleepy_doubler_that_writes_a_file):
    m = sleepy_doubler_that_writes_a_file.map([1], tag="load-then-rerun")

    m.wait()

    assert (m.output_files.get(0) / "foo").read_text() == "hi"

    m.rerun()

    with pytest.raises(FileNotFoundError):
        (m.output_files[0] / "foo").read_text()
