import hashlib
from pathlib import Path
from typing import Any

import cloudpickle


def to_bytes(obj: Any) -> bytes:
    return cloudpickle.dumps(obj)


def hash_bytes(bytes: bytes) -> str:
    return hashlib.md5(bytes).hexdigest()


def save_bytes(bytes, path: Path) -> None:
    with path.open(mode = 'wb') as file:
        file.write(bytes)


def save_object(obj: Any, path: Path) -> None:
    with path.open(mode = 'wb') as file:
        cloudpickle.dump(obj, file)


def load_object(path: Path) -> Any:
    with path.open(mode = 'rb') as file:
        return cloudpickle.load(file)
