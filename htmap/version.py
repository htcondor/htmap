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

from typing import Tuple

__version__ = '0.4.3'


def version() -> str:
    """Return a string containing human-readable version information."""
    return f'HTMap version {__version__}'


def _version_info(v: str) -> Tuple[int, int, int, str]:
    """Un-format ``__version__``."""
    return (*(int(x) for x in v[:5].split('.')), v[5:])


def version_info() -> Tuple[int, int, int, str]:
    """Return a tuple of version information: ``(major, minor, micro, release_level)``."""
    return _version_info(__version__)
