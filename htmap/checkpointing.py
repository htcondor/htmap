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
    pass
    # the real implementation of this function is in the run.py script
