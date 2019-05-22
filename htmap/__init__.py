# Copyright 2018 HTCondor Team, Computer Sciences Department,
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

__version__ = '0.3.1'

from typing import Tuple as _Tuple
import logging as _logging

from .settings import settings, USER_SETTINGS, BASE_SETTINGS

# SET UP NULL LOG HANDLER
logger = _logging.getLogger(__name__)
logger.setLevel(_logging.DEBUG)
logger.addHandler(_logging.NullHandler())

from .mapping import (
    map,
    starmap,
    build_map,
    MapBuilder,
)
from .mapped import mapped, MappedFunction
from .maps import Map
from .holds import ComponentHold
from .errors import ComponentError
from .state import ComponentStatus
from .options import MapOptions, register_delivery_mechanism
from .management import (
    status,
    status_json,
    status_csv,
    load,
    load_maps,
    remove,
    clean,
    Transplant,
    transplants,
    transplant_info,
)
from .tags import get_tags
from .checkpointing import checkpoint
from .transfer import TransferPath, TransferWindowsPath, TransferPosixPath
from . import exceptions

from . import _startup


def version() -> str:
    """Return a string containing human-readable version information."""
    return f'HTMap version {__version__}'


def _version_info(v: str) -> _Tuple[int, int, int, str]:
    """Un-format ``__version__``."""
    return (*(int(x) for x in v[:5].split('.')), v[5:])


def version_info() -> _Tuple[int, int, int, str]:
    """Return a tuple of version information: ``(major, minor, micro, release_level)``."""
    return _version_info(__version__)
