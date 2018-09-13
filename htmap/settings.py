from typing import Union, MutableMapping

import itertools
import functools
from pathlib import Path
from copy import copy

import toml

from . import exceptions, utils


def nested_merge(map_1: MutableMapping, map_2: MutableMapping) -> MutableMapping:
    """Return a new dictionary containing the result of recursively merging the second map into the first, overwriting values and merging maps."""
    new = copy(map_1)
    for key, value in map_2.items():
        if key in map_1 and isinstance(value, MutableMapping):
            new[key] = nested_merge(map_1[key], value)
        else:
            new[key] = value

    return new


class Settings:
    def __init__(self, *settings):
        if len(settings) == 0:
            settings = [{}]
        self.maps = list(settings)

    def __getitem__(self, key):
        for map in self.maps:
            path = key.split('.')
            r = map
            try:
                for component in path:
                    r = r[component]
            except KeyError:
                continue
            return r

        raise exceptions.MissingSetting()

    def __eq__(self, other: 'Settings'):
        return self.to_dict() == other.to_dict()

    def get(self, key, default = None):
        try:
            return self[key]
        except exceptions.MissingSetting:
            return default

    def __setitem__(self, key, value):
        *path, final = key.split('.')
        m = self.maps[0]
        for component in path:
            try:
                m = m[component]
            except KeyError:
                m[component] = {}
                m = m[component]

        m[final] = value

    def to_dict(self) -> dict:
        """Return a single dictionary with all of the settings in this :class:`Settings`, merged according to the lookup rules."""
        return functools.reduce(nested_merge, reversed(self.maps), {})

    def replace(self, other: 'Settings'):
        """Change the settings of this :class:`Settings` to be the settings from another :class:`Settings`."""
        self.maps = other.maps

    def append(self, other: Union['Settings', dict]):
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

    def prepend(self, other: Union['Settings', dict]):
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
    def from_settings(cls, *settings):
        """Construct a new :class:`Settings` from other :class:`Settings`."""
        return cls(*itertools.chain.from_iterable(s.maps for s in settings))

    @classmethod
    def load(cls, path: Path) -> 'Settings':
        """Load a :class:`Settings` from a file at the given path."""
        with path.open() as file:
            return cls(toml.load(file))

    def save(self, path: Path):
        """Save this :class:`Settings` to a file at the given path."""
        with path.open(mode = 'w') as file:
            toml.dump(self.maps[0], file)

    def __str__(self) -> str:
        return utils.rstr(toml.dumps(self.to_dict()))

    def __repr__(self) -> str:
        return utils.rstr(f'<{self.__class__.__name__}>')


BASE_SETTINGS = Settings(dict(
    HTMAP_DIR = Path.home() / '.htmap',
    MAPS_DIR_NAME = 'maps',
    TEMPORARY_CACHE_TIMEOUT = 1,
    HTCONDOR = dict(
        SCHEDD = None,
    ),
    MAP_OPTIONS = dict(
    ),
))

USER_SETTINGS_PATH = Path.home() / '.htmap.toml'
try:
    USER_SETTINGS = Settings.load(USER_SETTINGS_PATH)
except FileNotFoundError:
    USER_SETTINGS = Settings()

settings = Settings.from_settings(USER_SETTINGS, BASE_SETTINGS)
