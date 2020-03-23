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

from typing import Tuple, Optional

from . import utils

__version__ = '0.6.0'


def version() -> str:
    """Return a string containing human-readable version information."""
    return f'HTMap version {__version__}'


def version_info() -> Tuple[int, int, int, Optional[str], Optional[int]]:
    """Return a tuple of version information: ``(major, minor, micro, prerelease)``."""
    return utils.parse_version(__version__)
