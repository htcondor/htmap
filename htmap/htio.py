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


from typing import Any, List, Iterable, Tuple, Iterator, Dict

import hashlib
import json
from pathlib import Path

import cloudpickle
import htcondor

from htmap import exceptions


def to_bytes(obj: Any) -> bytes:
    """Serialize a Python object (including "objects", like functions) into bytes."""
    return cloudpickle.dumps(obj)


def hash_bytes(b: bytes) -> str:
    """Return a string-ified hash for the bytes ``b``."""
    return hashlib.md5(b).hexdigest()


def save_bytes(b: bytes, path: Path) -> None:
    """Write the bytes ``b`` to a file at the given ``path``."""
    path.write_bytes(b)


def save_object(obj: Any, path: Path) -> None:
    """Serialize a Python object (including "objects", like functions) to a file at the given ``path``."""
    with path.open(mode = 'wb') as file:
        cloudpickle.dump(obj, file)


def load_object(path: Path) -> Any:
    """Deserialize an object from the file at the given ``path``."""
    with path.open(mode = 'rb') as file:
        return cloudpickle.load(file)


def save_func(map_dir, func):
    """Save the mapped function to the map directory."""
    save_object(func, map_dir / 'func')


def save_args_and_kwargs(
    map_dir: Path,
    args_and_kwargs: Iterator[Tuple[Tuple, Dict]],
) -> List[str]:
    """
    Save the arguments to the mapped function to the map's input directory.
    Returns the hashes (via :func:`hash_bytes`) of each argument.
    """
    hashes = []
    num_inputs = 0
    for a_and_k in args_and_kwargs:
        b = to_bytes(a_and_k)
        h = hash_bytes(b)
        hashes.append(h)

        input_path = map_dir / 'inputs' / f'{h}.in'
        save_bytes(b, input_path)

        num_inputs += 1

    if num_inputs == 0:
        raise exceptions.EmptyMap()

    return hashes


def save_hashes(map_dir: Path, hashes: Iterable[str]):
    """Save a file containing the hashes of the arguments to the map directory."""
    with (map_dir / 'hashes').open(mode = 'w') as file:
        file.write('\n'.join(hashes))


def load_hashes(map_dir: Path) -> Tuple[str, ...]:
    """Load hashes that were saved using :func:`save_hashes`."""
    with (map_dir / 'hashes').open() as file:
        return tuple(h.strip() for h in file)


def save_submit(map_dir: Path, submit: htcondor.Submit):
    """Save a dictionary that represents the map's :class:`htcondor.Submit` object."""
    with (map_dir / 'submit').open(mode = 'w') as f:
        json.dump(
            dict(submit),
            f,
            indent = 4,
            separators = (', ', ': '),
        )


def load_submit(map_dir: Path) -> htcondor.Submit:
    """Load an :class:`htcondor.Submit` object that was saved using :func:`save_submit`."""
    with (map_dir / 'submit').open(mode = 'r') as f:
        return htcondor.Submit(json.load(f))


def save_itemdata(map_dir: Path, itemdata: List[dict]):
    """Save the map's itemdata as a list of JSON dictionaries."""
    with (map_dir / 'itemdata').open(mode = 'w') as f:
        json.dump(
            itemdata,
            f,
            indent = None,
            separators = (',', ':'),
        )  # most compact representation


def load_itemdata(map_dir: Path) -> List[dict]:
    """Load itemdata that was saved using :func:`save_itemdata`."""
    with (map_dir / 'itemdata').open(mode = 'r') as f:
        return json.load(f)
