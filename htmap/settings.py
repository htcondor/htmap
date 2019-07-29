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

from typing import Union, Any, Optional
import logging

import os
import itertools
import functools
from pathlib import Path
from copy import copy

import toml

from . import exceptions, utils
from .version import __version__

logger = logging.getLogger(__name__)


def nested_merge(map_1: dict, map_2: dict) -> dict:
    """Return a new dictionary containing the result of recursively merging the second map into the first, overwriting values and merging maps."""
    new = copy(map_1)
    for key, value in map_2.items():
        if key in map_1 and isinstance(value, dict):
            new[key] = nested_merge(map_1[key], value)
        else:
            new[key] = value

    return new


class Settings:
    def __init__(self, *settings):
        if len(settings) == 0:
            settings = [{}]
        self.maps = list(settings)

    def __getitem__(self, key: str):
        try:
            path = key.split('.')
            r = self.to_dict()
            for component in path:
                r = r[component]
            return r
        except (KeyError, TypeError):
            raise exceptions.MissingSetting()

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.to_dict() == other.to_dict()

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        try:
            return self[key]
        except exceptions.MissingSetting:
            return default

    def __setitem__(self, key: str, value):
        old = self.get(key)  # for log message below

        *path, final = key.split('.')
        m = self.maps[0]
        for component in path:
            try:
                m = m[component]
            except KeyError:
                m[component] = {}  # create new nested dictionaries if necessary
                m = m[component]

        m[final] = value

        logger.debug(f'setting {key} changed from {old if old is not None else "<missing>"} to {value}')

    def to_dict(self) -> dict:
        """Return a single dictionary with all of the settings in this :class:`Settings`, merged according to the lookup rules."""
        return functools.reduce(nested_merge, reversed(self.maps), {})

    def replace(self, other: 'Settings') -> None:
        """Change the settings of this :class:`Settings` to be the settings from another :class:`Settings`."""
        self.maps = other.maps
        logger.debug('settings were replaced')

    def append(self, other: Union['Settings', dict]) -> None:
        """
        Add a map to the end of the search (i.e., it will be searched last, and be overridden by anything before it).

        Parameters
        ----------
        other
            Another settings-like object to insert into the :class:`Settings`.
        """
        if isinstance(other, Settings):
            self.maps.extend(other.maps)
        else:
            self.maps.append(other)

    def prepend(self, other: Union['Settings', dict]) -> None:
        """
        Add a map to the beginning of the search (i.e., it will be searched first, and override anything after it).

        Parameters
        ----------
        other
            Another settings-like object to insert into the :class:`Settings`.
        """
        if isinstance(other, Settings):
            self.maps = other.maps + self.maps
        else:
            self.maps.insert(0, other)

    @classmethod
    def from_settings(cls, *settings: 'Settings') -> 'Settings':
        """Construct a new :class:`Settings` from another :class:`Settings`."""
        return cls(*itertools.chain.from_iterable(s.maps for s in settings))

    @classmethod
    def load(cls, path: Path) -> 'Settings':
        """Load a :class:`Settings` from a file at the given path."""
        with path.open() as file:
            return cls(toml.load(file))

    def save(self, path: Path) -> None:
        """Save this :class:`Settings` to a file at the given path."""
        with path.open(mode = 'w') as file:
            toml.dump(self.maps[0], file)

        logger.debug(f'saved settings to {path}')

    def __str__(self) -> str:
        return utils.rstr(toml.dumps(self.to_dict()))

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}>'


htmap_dir = Path(os.getenv('HTMAP_DIR', Path.home() / '.htmap'))
default_docker_image = f'htcondor/htmap-exec:v{__version__}'
BASE_SETTINGS = Settings(dict(
    HTMAP_DIR = htmap_dir.as_posix(),
    DELIVERY_METHOD = os.getenv('HTMAP_DELIVERY_METHOD', 'docker'),
    WAIT_TIME = 1,
    CLI = False,
    HTCONDOR = dict(
        SCHEDULER = os.getenv('HTMAP_CONDOR_SCHEDULER', None),
        COLLECTOR = os.getenv('HTMAP_CONDOR_COLLECTOR', None),
    ),
    MAP_OPTIONS = dict(
        request_memory = '128MB',
        request_disk = '1GB',
    ),
    DOCKER = dict(
        IMAGE = os.getenv('HTMAP_DOCKER_IMAGE', default_docker_image),
    ),
    SINGULARITY = dict(
        IMAGE = os.getenv('HTMAP_SINGULARITY_IMAGE', f'docker://{default_docker_image}'),
    ),
    TRANSPLANT = dict(
        DIR = (htmap_dir / 'transplants').as_posix(),
        ALTERNATE_INPUT_PATH = None,
        ASSUME_EXISTS = False,
    ),
))

USER_SETTINGS_PATH = Path.home() / '.htmaprc'
try:
    USER_SETTINGS = Settings.load(USER_SETTINGS_PATH)
    logger.debug(f'loaded user settings from {USER_SETTINGS_PATH}')
except FileNotFoundError:
    USER_SETTINGS = Settings()
    logger.debug(f'no user settings at {USER_SETTINGS_PATH}')

settings = Settings.from_settings(Settings(), USER_SETTINGS, BASE_SETTINGS)

logger.debug(f'htmap directory is {settings["HTMAP_DIR"]}')
