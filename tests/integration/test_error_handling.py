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

from pathlib import Path

import pytest

import htmap


@pytest.fixture
def mapped_div_by_x():
    @htmap.mapped
    def div_by_x(x):
        return 1 / x

    return div_by_x


def test_waiting_on_errored_component_raises(mapped_div_by_x):
    m = mapped_div_by_x.map([0])

    with pytest.raises(htmap.exceptions.MapComponentError):
        m.wait(timeout = 180)


def test_iterating_over_errored_component_raises(mapped_div_by_x):
    m = mapped_div_by_x.map([0])

    with pytest.raises(htmap.exceptions.MapComponentError):
        list(m)


def test_error_report_has_exception_type():
    def bad(x):
        raise ZeroDivisionError

    m = htmap.map(bad, [0])

    err = m.get_err(0)

    assert 'ZeroDivisionError' in err.report()


def test_error_report_has_exception_message():
    def bad(x):
        raise Exception('foobar')

    m = htmap.map(bad, [0])
    err = m.get_err(0)

    assert 'foobar' in err.report()


def test_error_report_includes_input_files():
    def dummy(x):
        raise Exception

    m = htmap.map(
        dummy,
        [0],
        map_options = htmap.MapOptions(
            fixed_input_files = Path(__file__),
        )
    )
    err = m.get_err(0)

    assert 'test_error_handling.py' in err.report()


def test_correct_number_of_errors():
    def dummy(x):
        raise Exception

    m = htmap.map(dummy, range(3))

    assert len(list(m.errors)) == 3
