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

import os
from pathlib import Path
import shutil


def checkpoint(*paths):
    # no-op if not on execute node
    if os.getenv('HTMAP_ON_EXECUTE') != "1":
        return

    transfer_dir = Path(os.getenv('_CONDOR_SCRATCH_DIR')) / '_htmap_transfer'

    # this is not the absolute safest method
    # but it's good enough for government work

    prep_dir = transfer_dir / 'prep_checkpoint'
    curr_dir = transfer_dir / 'current_checkpoint'
    old_dir = transfer_dir / 'old_checkpoint'

    for d in (prep_dir, curr_dir, old_dir):
        d.mkdir(parents = True, exist_ok = True)

    for path in paths:
        path.rename(prep_dir / path.name)

    curr_dir.rename(old_dir)
    prep_dir.rename(curr_dir)
    shutil.rmtree(old_dir)
