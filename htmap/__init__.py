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

import logging
import logging.handlers

from .settings import settings, USER_SETTINGS, BASE_SETTINGS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())

# SET UP LOG FILE
logfile_handler = logging.handlers.RotatingFileHandler(
    filename = settings['HTMAP_DIR'] / 'htmap.log',
    mode = 'a',
    maxBytes = 10 * 1024 * 1024,  # 10 MB
    backupCount = 4,
)
fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logfile_handler.setFormatter(fmt)
logfile_handler.setLevel(logging.DEBUG)
logger.addHandler(logfile_handler)

from .mapping import (
    map, starmap, build_map,
    transient_map, transient_starmap, build_transient_map,
    MapBuilder, TransientMap,
)
from .mapped import mapped, MappedFunction
from .maps import Map, Status
from .options import MapOptions, register_delivery_mechanism
from .management import (
    status,
    map_ids,
    load, load_maps,
    clean, force_clean,
    remove, force_remove,
)
from . import exceptions
