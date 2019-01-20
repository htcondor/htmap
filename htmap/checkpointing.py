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

from . import names


def checkpoint(*paths: os.PathLike):
    """
    Informs HTMap about the existence of checkpoint files.
    This function should be called every time the checkpoint files are changed, even if they have the same names as before.

    .. attention::

        This function is a no-op when executing locally, so you if you're testing your function it won't do anything.

    .. attention::

        The files will be copied, so try not to make the checkpoint files too large.

    Parameters
    ----------
    paths
        The paths to the checkpoint files.
    """
    # no-op if not on execute node
    if os.getenv('HTMAP_ON_EXECUTE') != "1":
        return

    transfer_dir = Path(os.getenv('_CONDOR_SCRATCH_DIR')) / names.TRANSFER_DIR

    # this is not the absolute safest method
    # but it's good enough for government work

    prep_dir = transfer_dir / names.CHECKPOINT_PREP
    curr_dir = transfer_dir / names.CHECKPOINT_CURRENT
    old_dir = transfer_dir / names.CHECKPOINT_OLD

    for d in (prep_dir, curr_dir, old_dir):
        d.mkdir(parents = True, exist_ok = True)

    for path in paths:
        path = Path(path)
        shutil.copy2(path, prep_dir / path.name)

    curr_dir.rename(old_dir)
    prep_dir.rename(curr_dir)
    shutil.rmtree(old_dir)
