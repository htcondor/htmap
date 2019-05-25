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


def transfer_output_files(*paths: os.PathLike):
    """
    Informs HTMap about the existence of output files.

    .. attention::

        This function is a no-op when executing locally, so you if you're testing your function it won't do anything.

    .. attention::

        The files will be **moved**, so they will not be available in their original locations.

    Parameters
    ----------
    paths
        The paths to the output files.
    """
    # no-op if not on execute node
    if os.getenv('HTMAP_ON_EXECUTE') != "1":
        return

    scratch_dir = Path(os.getenv('_CONDOR_SCRATCH_DIR'))

    user_transfer_dir = scratch_dir / names.USER_TRANSFER_DIR / os.getenv('HTMAP_COMPONENT')

    for path in paths:
        path = Path(path).absolute()
        target = user_transfer_dir / path.relative_to(scratch_dir)
        target.parent.mkdir(exist_ok = True, parents = True)
        shutil.move(path, target)
