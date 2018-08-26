import itertools
from pathlib import Path
from copy import copy

import toml

from . import exceptions, utils


def nested_merge(map_1: dict, map_2: dict):
    new = copy(map_1)
    for key, value in map_2.items():
        if not isinstance(value, dict):
            new[key] = value
        elif key in map_1 and isinstance(value, dict):
            new[key] = nested_merge(map_1[key], value)
        elif key not in map_1 and isinstance(value, dict):
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
        d = {}
        for map in reversed(self.maps):
            d = nested_merge(d, map)

        return d

    @classmethod
    def from_settings(cls, *settings):
        return cls(*itertools.chain.from_iterable(s.maps for s in settings))

    @classmethod
    def load(cls, path: Path) -> 'Settings':
        with path.open() as file:
            return cls(toml.load(file))

    def save(self, path: Path):
        with path.open(mode = 'w') as file:
            toml.dump(self.maps[0], file)

    def __str__(self):
        return utils.rstr(toml.dumps(self.to_dict()))

    def __repr__(self):
        return f'<{self.__class__.__name__}>'


BASE_SETTINGS = Settings(dict(
    HTMAP_DIR = Path.home() / '.htmap',
    MAPS_DIR_NAME = 'maps',
    TEMPORARY_CACHE_TIMEOUT = 1,
    HTCONDOR = dict(
        SCHEDD = None,
    ),
))

USER_SETTINGS_PATH = Path.home() / '.htmap.toml'
try:
    user_settings = Settings.load(USER_SETTINGS_PATH)
except FileNotFoundError:
    user_settings = Settings()

settings = Settings.from_settings(user_settings, BASE_SETTINGS)
