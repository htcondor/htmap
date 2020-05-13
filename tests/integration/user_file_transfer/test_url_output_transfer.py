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
from htmap import utils
import htcondor

TIMEOUT = 300


@pytest.mark.timeout(TIMEOUT)
@pytest.mark.xfail(condition = utils.HTCONDOR_VERSION_INFO < (8, 9, 2), reason = "HTMap requires HTCondor v8.9.2 or later to do URL output transfers.")
def test_output_transfer_via_file_protocol(tmp_path):
    target = tmp_path / 'foo'

    def func(_):
        output = Path('remote-foo')
        output.write_text('hi')

        destination = htmap.TransferPath(target, protocol = 'file')

        htmap.transfer_output_files((output, destination))

        return True

    m = htmap.map(func, [None])

    print(m.stdout.get(0))
    print()
    print(m.stderr.get(0))
    print()

    # Looking at the starter log may be helpful if the plugin is failing badly enough

    # logs = Path(htcondor.param['LOG']) / f"StarterLog.{m._cluster_ids[0]}.0"
    # log_lines = logs.read_text().splitlines()
    # for idx, line in enumerate(log_lines):
    #     if f'{m._cluster_ids[0]}.0' in line:
    #         break
    # for line in log_lines[idx:]:
    #     print(line)

    assert m.get(0)

    assert target.read_text() == "hi"
