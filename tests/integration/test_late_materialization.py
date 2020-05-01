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

import subprocess
from pathlib import Path

import htmap

TIMEOUT = 300


@pytest.fixture(scope = 'function')
def late_noop():
    @htmap.mapped(map_options = htmap.MapOptions(max_idle = "1"))
    def noop(_):
        return True

    return noop


@pytest.mark.timeout(TIMEOUT)
def test_wait_with_late_materialization(late_noop):
    m = late_noop.map(range(3))

    try:
        cid = m._cluster_ids[0]

        digest = Path(
            subprocess.run(
                ["condor_q", "-factory", str(cid)],
                stdout = subprocess.PIPE,
                text = True
            ).stdout.splitlines()[-1].split()[-1]
        ).read_text()
        items = Path(digest.splitlines()[-1].split()[-1]).read_text()

        print(digest)
        print()
        print(items)
    except:
        print('failed to get digest file')

    m.wait()


    assert m.is_done
