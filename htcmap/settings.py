import collections
from pathlib import Path
from typing import Mapping, Any

import toml

from . import exceptions


class Settings:
    def __init__(self, settings: Mapping[str, Any]):
        self._settings = settings

    def get(self, item):
        try:
            # drill down if given a dotted string
            path = item.split('.')
            r = self._settings
            for component in path:
                r = r[component]

            return r
        except KeyError:
            raise exceptions.MissingSetting(f'{item} is not set')

    def __getitem__(self, item):
        return self.get(item)

    def __getattr__(self, item):
        return self.get(item)


class DotMap(dict):
    def __getattr__(self, item):
        return self[item]


DEFAULT_SETTINGS = DotMap(
    HTCMAP_DIR = Path.home() / '.htcmap',
)

user_settings_path = Path.home() / '.htcmap.toml'
try:
    with user_settings_path.open() as file:
        user_settings = toml.load(file, _dict = DotMap)
except FileNotFoundError:
    user_settings = DotMap()

settings = Settings(collections.ChainMap(user_settings, DEFAULT_SETTINGS))
