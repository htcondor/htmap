from typing import Mapping, Any

import collections
import datetime
from pathlib import Path

import toml

from . import exceptions


class Settings:
    def __init__(self, *settings: Mapping[str, Any]):
        print(settings)
        self._settings = collections.ChainMap(*settings)

    def __iter__(self):
        yield from self._settings

    def __getitem__(self, item):
        try:
            path = item.split('.')  # drill through dotted attributes access
            r = self._settings
            for component in path:
                r = r[component]

            return r
        except KeyError:
            raise exceptions.MissingSetting(f'{item} is not set')

    def __getattr__(self, item):
        try:
            return self._settings[item]
        except KeyError:
            raise exceptions.MissingSetting(f'{item} is not set')

    def get(self, item, default = None):
        try:
            return self[item]
        except exceptions.MissingSetting:
            return default

    @classmethod
    def load(cls, path: Path):
        with path.open() as file:
            return cls(toml.load(file, _dict = DotMap))

    def save(self, path: Path):
        with path.open(mode = 'w') as file:
            toml.dump(self._settings, file)

    def __repr__(self):
        return toml.dumps(self._settings)


class DotMap(dict):
    def __getattr__(self, item):
        return self[item]


BASE_SETTINGS = Settings(
    DotMap(
        HTMAP_DIR = Path.home() / '.htmap',
        MAPS_DIR_NAME = 'maps',
        TEMPORARY_CACHE_TIMEOUT = 1,
        HTCONDOR = DotMap(
            SCHEDD = None,
        ),
    )
)

USER_SETTINGS_PATH = Path.home() / '.htmap.toml'
try:
    with USER_SETTINGS_PATH.open() as file:  # toml-0.10.0 or more will have pathlib support
        user_settings = toml.load(file, _dict = DotMap)
except FileNotFoundError:
    user_settings = Settings()

settings = Settings(user_settings, BASE_SETTINGS)
