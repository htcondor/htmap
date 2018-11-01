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

logging.getLogger(__name__).addHandler(logging.NullHandler())

from .settings import settings, USER_SETTINGS, BASE_SETTINGS
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
