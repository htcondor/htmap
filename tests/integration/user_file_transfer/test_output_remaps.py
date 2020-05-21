# Copyright 2020 HTCondor Team, Computer Sciences Department,
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
from htmap import utils

import htcondor

TIMEOUT = 300


@pytest.mark.timeout(TIMEOUT)
@pytest.mark.xfail(condition = utils.HTCONDOR_VERSION_INFO < (8, 9), reason = "I don't understand yet why this doesn't work on 8.8...")
def test_output_remap_via_file_protocol(tmp_path):
    target = tmp_path / 'foo'
    destination = htmap.TransferPath(target, protocol = 'file')

    def func(_):
        output = Path('remote-foo')
        output.write_text('hi')

        htmap.transfer_output_files(output)

        return True

    m = htmap.map(func, [None], map_options = htmap.MapOptions(output_remaps = {'remote-foo': destination}))

    print(m.stdout.get(0))
    print()
    print(m.stderr.get(0))
    print()

    # Looking at the starter log may be helpful if the plugin is failing badly enough.
    # You must set STARTER_LOG_NAME_APPEND=jobid in your HTCondor config for it to work

    # starter_log = Path(htcondor.param['LOG']) / f"StarterLog.{m._cluster_ids[0]}.0"
    # print(starter_log.read_text())

    assert m.get(0)

    assert target.read_text() == "hi"

    # make sure we did NOT transfer it back as normal
    assert 'foo' not in (path.stem for path in m.output_files[0].iterdir())
