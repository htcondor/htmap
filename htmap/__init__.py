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
import shutil as _shutil

from .settings import settings, USER_SETTINGS, BASE_SETTINGS

# SET UP NULL LOG HANDLER
_logger = _logging.getLogger(__name__)
_logger.setLevel(_logging.DEBUG)
_logger.addHandler(_logging.NullHandler())

# ENSURE HTMAP DIR EXISTS
_htmap_dir = _Path(settings['HTMAP_DIR'])
if not _htmap_dir.exists():
    try:
        _htmap_dir.mkdir(parents = True, exist_ok = True)
        _logger.debug(f'created HTMap dir at {_htmap_dir}')
    except PermissionError as e:
        raise PermissionError(f'the HTMap directory ({_htmap_dir}) needs to be writable') from e

LOG_FILE = _Path(settings['HTMAP_DIR']) / 'htmap.log'
# SET UP LOG FILE
_logfile_handler = _handlers.RotatingFileHandler(
    filename = LOG_FILE,
    mode = 'a',
    maxBytes = 10 * 1024 * 1024,  # 10 MB
    backupCount = 4,
)
_fmt = _logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_logfile_handler.setFormatter(_fmt)
_logfile_handler.setLevel(_logging.DEBUG)
_logger.addHandler(_logfile_handler)

from .mapping import (
    map, starmap, build_map,
    transient_map, transient_starmap,
    MapBuilder,
)
from .mapped import mapped, MappedFunction
from .maps import Map, TransientMap
from .holds import ComponentHold
from .errors import ComponentError
from .state import ComponentStatus
from .options import MapOptions, register_delivery_mechanism
from .management import (
    status, status_json, status_csv,
    map_ids,
    load, load_maps,
    clean, force_clean,
    remove, force_remove,
    Transplant, transplants, transplant_info,
    enable_stdout_debug_logging,
)
from .checkpointing import checkpoint
from . import exceptions


def version() -> str:
    """Return a string containing human-readable version information."""
    return f'HTMap version {__version__}'


def _version_info(v: str) -> _Tuple[int, int, int, str]:
    """Un-format ``__version__``."""
    return (*(int(x) for x in v[:5].split('.')), v[5:])


def version_info() -> _Tuple[int, int, int, str]:
    """Return a tuple of version information: ``(major, minor, micro, release_level)``."""
    return _version_info(__version__)
