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


import logging as _logging

from .version import (
    __version__,
    version,
    version_info
)

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
from .maps import (
    Map,
    MapStdOut,
    MapStdErr,
)
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
from .output_files import transfer_output_files
from .transfer import TransferPath, TransferWindowsPath, TransferPosixPath
from . import exceptions

from . import _startup
