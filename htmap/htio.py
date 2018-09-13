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

from typing import Any

import hashlib
from pathlib import Path

import cloudpickle


def to_bytes(obj: Any) -> bytes:
    """Serialize a Python object (including things like functions) into bytes."""
    return cloudpickle.dumps(obj)


def hash_bytes(bytes: bytes) -> str:
    """Return a string-ified hash for the `bytes`."""
    return hashlib.md5(bytes).hexdigest()


def save_bytes(bytes, path: Path) -> None:
    """Write the `bytes` to a file at the given `path`."""
    path.write_bytes(bytes)


def save_object(obj: Any, path: Path) -> None:
    """Serialize the given object `obj` (including things like functions) to a file at the given `path`."""
    with path.open(mode = 'wb') as file:
        cloudpickle.dump(obj, file)


def load_object(path: Path) -> Any:
    """Deserialize an object from the file at the given `path`."""
    with path.open(mode = 'rb') as file:
        return cloudpickle.load(file)
