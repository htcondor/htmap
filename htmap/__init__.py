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

__version__ = '0.1.0'

from typing import Tuple as _Tuple
import logging as _logging
import logging.handlers as _handlers
from pathlib import Path as _Path

from .settings import settings, USER_SETTINGS, BASE_SETTINGS

# SET UP NULL LOG HANDLER
logger = _logging.getLogger(__name__)
logger.setLevel(_logging.DEBUG)
logger.addHandler(_logging.NullHandler())

# ENSURE HTMAP DIR EXISTS
htmap_dir = _Path(settings['HTMAP_DIR'])
if not htmap_dir.exists():
    try:
        htmap_dir.mkdir(parents = True, exist_ok = True)
        logger.debug(f'created HTMap dir at {htmap_dir}')
    except PermissionError as e:
        raise PermissionError(f'the HTMap directory ({htmap_dir}) needs to be writable') from e

# SET UP LOG FILE
logfile_handler = _handlers.RotatingFileHandler(
    filename = settings['HTMAP_DIR'] / 'htmap.log',
    mode = 'a',
    maxBytes = 10 * 1024 * 1024,  # 10 MB
    backupCount = 4,
)
fmt = _logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logfile_handler.setFormatter(fmt)
logfile_handler.setLevel(_logging.DEBUG)
logger.addHandler(logfile_handler)

from .mapping import (
    map, starmap, build_map,
    transient_map, transient_starmap, build_transient_map,
    MapBuilder, TransientMap,
)
from .mapped import mapped, MappedFunction
from .maps import Map, ComponentStatus, Hold
from .options import MapOptions, register_delivery_mechanism
from .management import (
    status,
    map_ids,
    load, load_maps,
    clean, force_clean,
    remove, force_remove,
)
from . import exceptions


def version():
    """Return a string containing human-readable version information."""
    return f'HTMap version {__version__}'


def _version_info(v: str) -> _Tuple[int, int, int, str]:
    return (*(int(x) for x in v[:5].split('.')), v[5:])


def version_info() -> _Tuple[int, int, int, str]:
    """Return a tuple of version information: ``(major, minor, micro, release_level)``."""
    return _version_info(__version__)
