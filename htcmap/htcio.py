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
