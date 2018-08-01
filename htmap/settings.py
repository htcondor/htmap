from typing import Mapping, Any

import collections
from pathlib import Path

import toml

from . import exceptions


class Settings:
    def __init__(self, *settings: Mapping[str, Any]):
        self._settings = collections.ChainMap(*settings)

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


class DotMap(dict):
    def __getattr__(self, item):
        return self[item]


DEFAULT_SETTINGS = DotMap(
    HTMAP_DIR = Path.home() / '.htmap',
)

USER_SETTINGS_PATH = Path.home() / '.htmap.toml'
try:
    with USER_SETTINGS_PATH.open() as file:  # toml-0.10.0 or more will have pathlib support
        user_settings = toml.load(file, _dict = DotMap)
except FileNotFoundError:
    user_settings = DotMap()

settings = Settings(user_settings, DEFAULT_SETTINGS)
