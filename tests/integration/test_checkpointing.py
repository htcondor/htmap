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
import time

import pytest

import htmap


@pytest.mark.timeout(120)
def test_checkpoint_file_exists_after_restart():
    @htmap.mapped
    def test(_):
        checkpoint_path = Path('chk')

        if checkpoint_path.exists():
            return True

        checkpoint_path.touch()
        htmap.checkpoint(checkpoint_path)

        time.sleep(30)

        return False

    m = test.map([None])

    while m.component_statuses[0] is not htmap.ComponentStatus.RUNNING:
        time.sleep(.1)

    time.sleep(5)
    m.vacate()

    while m.component_statuses[0] is not htmap.ComponentStatus.COMPLETED:
        time.sleep(.1)

    assert m[0] is True


@pytest.mark.timeout(120)
def test_checkpoint_file_has_expected_contents_after_restart():
    @htmap.mapped
    def test(_):
        checkpoint_path = Path('chk')

        if checkpoint_path.exists():
            return checkpoint_path.read_text() == 'foobar'

        checkpoint_path.write_text('foobar')
        htmap.checkpoint(checkpoint_path)

        time.sleep(30)

        return False

    m = test.map([None])

    while m.component_statuses[0] is not htmap.ComponentStatus.RUNNING:
        time.sleep(.1)

    time.sleep(5)
    m.vacate()

    while m.component_statuses[0] is not htmap.ComponentStatus.COMPLETED:
        time.sleep(.1)

    assert m[0] is True
