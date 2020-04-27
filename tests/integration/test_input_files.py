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


import pytest

from pathlib import Path

import htmap


@pytest.mark.timeout(180)
def test_fixed_input_files_are_transferred_as_list_of_paths(tmp_path):
    f1 = tmp_path / 'f1'
    f2 = tmp_path / 'f2'

    f1.write_text('f1')
    f2.write_text('f2')

    def test(_):
        return Path('f1').read_text() == 'f1' and Path('f2').read_text() == 'f2'

    m = htmap.map(test, [None], map_options = htmap.MapOptions(
        fixed_input_files = [f1, f2]
    ))

    assert m.get(0)


@pytest.mark.timeout(180)
def test_fixed_input_files_are_transferred_as_list_of_strings(tmp_path):
    f1 = tmp_path / 'f1'
    f2 = tmp_path / 'f2'

    f1.write_text('f1')
    f2.write_text('f2')

    def test(_):
        return Path('f1').read_text() == 'f1' and Path('f2').read_text() == 'f2'

    m = htmap.map(test, [None], map_options = htmap.MapOptions(
        fixed_input_files = [f1.as_posix(), f2.as_posix()]
    ))

    assert m.get(0)


