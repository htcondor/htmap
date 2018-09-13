"""
Copyright 2018 HTCondor Team, Computer Sciences Department,
University of Wisconsin-Madison, WI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Callable

from . import mapper, result, exceptions


def map(map_id: str, func: Callable, args, force_overwrite: bool = False, **kwargs) -> result.MapResult:
    return mapper.htmap(func).map(map_id, args, force_overwrite = force_overwrite, **kwargs)


def starmap(map_id: str, func: Callable, args, kwargs, force_overwrite: bool = False) -> result.MapResult:
    return mapper.htmap(func).starmap(map_id, args, kwargs, force_overwrite = force_overwrite)


def build_map(map_id: str, func: Callable, force_overwrite: bool = False) -> mapper.MapBuilder:
    return mapper.htmap(func).build_map(map_id, force_overwrite = force_overwrite)


def recover(map_id: str) -> result.MapResult:
    return result.MapResult.recover(map_id)


def remove(map_id: str, not_exist_ok = True):
    try:
        r = recover(map_id)
        r.remove()
    except exceptions.MapIDNotFound as e:
        if not not_exist_ok:
            raise e
