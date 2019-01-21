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
import os as _os

from .settings import settings, USER_SETTINGS, BASE_SETTINGS

# SET UP NULL LOG HANDLER
_logger = _logging.getLogger(__name__)
_logger.setLevel(_logging.DEBUG)
_logger.addHandler(_logging.NullHandler())

if _os.getenv('HTMAP_ON_EXECUTE') != '1':
    # ENSURE HTMAP DIR EXISTS
    from . import names as _names

    _htmap_dir = _Path(settings['HTMAP_DIR'])
    try:
        did_not_exist = not _htmap_dir.exists()
        _htmap_dir.mkdir(parents = True, exist_ok = True)
        (_htmap_dir / _names.MAPS_DIR).mkdir(parents = True, exist_ok = True)
        (_htmap_dir / _names.TAGS_DIR).mkdir(parents = True, exist_ok = True)
        if did_not_exist:
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

    import shutil as _shutil

    _shutil.rmtree(_htmap_dir / _names.REMOVED_MAPS_DIR, ignore_errors = True)

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
