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

from .settings import BASE_SETTINGS, USER_SETTINGS, settings
from .version import __version__, version, version_info

# SET UP NULL LOG HANDLER
_logger = _logging.getLogger(__name__)
_logger.setLevel(_logging.DEBUG)
_logger.addHandler(_logging.NullHandler())

from . import _startup, exceptions
from .checkpointing import checkpoint
from .errors import ComponentError
from .holds import ComponentHold
from .management import (
    Transplant,
    clean,
    load,
    load_maps,
    remove,
    status,
    status_csv,
    status_json,
    transplant_info,
    transplants,
)
from .mapped import MappedFunction, mapped
from .mapping import MapBuilder, build_map, map, starmap
from .maps import Map, MapOutputFiles, MapStdErr, MapStdOut
from .options import MapOptions, register_delivery_method
from .state import ComponentStatus
from .tags import get_tags
from .transfer import TransferPath, transfer_output_files
