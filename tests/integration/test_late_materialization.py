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
import time

from pathlib import Path

import htmap
import htcondor

TIMEOUT = 300


@pytest.fixture(scope = 'function')
def late_noop():
    @htmap.mapped(map_options = htmap.MapOptions(max_idle = "1"))
    def noop(_):
        time.sleep(1)
        return True

    return noop


# This test occasionally fails on CI on HTCondor v8.8.8; have never been able
# to reproduce locally, and has never impacted any actual users.
# Marked non-strict xfail for now; hope to revisit in the future.
@pytest.mark.timeout(TIMEOUT)
@pytest.mark.xfail(strict = False, reason = "Flaky on CI on HTCondor v8.8.8")
def test_wait_with_late_materialization(late_noop):
    m = late_noop.map(range(3))
    time.sleep(.1)

    cid = m._cluster_ids[0]

    schedd = htcondor.Schedd()
    ads = schedd.query(f"ClusterId=={cid}")

    for ad in ads:
        for k, v in sorted(ad.items()):
            print(f"{k} = {v}")
        print()

    digest = Path(ads[0]["JobMaterializeDigestFile"]).read_text()
    print(digest)

    items = Path(ads[0]["JobMaterializeItemsFile"]).read_text()
    print(items)

    try:
        m.wait()
    except Exception as e:
        sched_log = Path.home() / '.condor' / 'state' / 'log' / 'SchedLog'

        sched_log_lines = sched_log.read_text().splitlines()
        for idx, line in enumerate(sched_log_lines):
            if str(cid) in line:
                break

        for line in sched_log_lines[idx:]:
            print(line)

        raise e

    assert m.is_done
