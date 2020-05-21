# Copyright 2020 HTCondor Team, Computer Sciences Department,
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

from typing import Optional

import random
import string
from pathlib import Path
from typing import Tuple
import fnmatch

from htmap import names, settings, exceptions


def tags_dir() -> Path:
    return Path(settings['HTMAP_DIR']) / names.TAGS_DIR


def get_tags(pattern: Optional[str] = None) -> Tuple[str, ...]:
    """
    Return a tuple containing the ``tag`` for all existing maps,
    with optional filtering based on a glob-style pattern.

    Parameters
    ----------
    pattern
        A `glob-style pattern <https://docs.python.org/3/library/fnmatch.html#module-fnmatch>`_.
        Only tags that fit the pattern will be returned.
        If ``None`` (the default), all tags will be returned.

    Returns
    -------
    tags :
        A tuple containing the tags that match the ``pattern``.
    """
    return tuple(
        path.name for path in tags_dir().iterdir()
        if pattern is None or fnmatch.fnmatchcase(path.name, pattern)
    )


def tag_file_path(tag: str) -> Path:
    return tags_dir() / tag


def raise_if_tag_already_exists(tag: str) -> None:
    """Raise a :class:`htmap.exceptions.TagAlreadyExists` if the ``tag`` already exists."""
    if tag_file_path(tag).exists():
        raise exceptions.TagAlreadyExists(f'The requested tag "{tag}" already exists. Load the Map with htmap.load("{tag}"), or remove it using htmap.remove("{tag}").')


INVALID_TAG_CHARACTERS = {
    '/',
    '\\',  # backslash
    '<',
    '>',
    ':',
    '"',
    '|',
    '?',
    '*',
    ' ',
    '[',
    ']',
    '!',
}


def raise_if_tag_is_invalid(tag: str) -> None:
    """Raise a :class:`htmap.exceptions.InvalidTag` if the ``tag`` contains any invalid characters."""
    if len(tag) < 1:
        raise exceptions.InvalidTag("The tag must be a non-empty string")
    invalid_chars = set(tag).intersection(INVALID_TAG_CHARACTERS)
    if len(invalid_chars) != 0:
        raise exceptions.InvalidTag(f'These characters in tag {tag} are not valid: {invalid_chars}')


ADJECTIVES = (
    'angry',
    'barbed',
    'bland',
    'blue',
    'breezy',
    'burst',
    'busy',
    'calm',
    'clever',
    'coy',
    'curly',
    'dark',
    'deep',
    'dire',
    'dizzy',
    'dry',
    'fair',
    'fancy',
    'fierce',
    'firm',
    'fuzzy',
    'gentle',
    'green',
    'happy',
    'happy',
    'harsh',
    'hollow',
    'husky',
    'jaded',
    'light',
    'prim',
    'proper',
    'proud',
    'puny',
    'quick',
    'rapid',
    'rare',
    'red',
    'ripe',
    'scratchy',
    'shaky',
    'shallow',
    'short',
    'sleek',
    'slow',
    'sly',
    'snide',
    'soft',
    'soggy',
    'super',
    'sweet',
    'swift',
    'tall',
    'tan',
    'thick',
    'thin',
    'tiny',
    'torrid',
    'trite',
    'twin',
    'vast',
    'wicked',
    'wise',
)
NOUNS = (
    'actor',
    'apple',
    'area',
    'axe',
    'badge',
    'beak',
    'bird',
    'box',
    'cake',
    'car',
    'cat',
    'chair',
    'city',
    'coil',
    'cookie',
    'dog',
    'drone',
    'echo',
    'exam',
    'fact',
    'farm',
    'foot',
    'frog',
    'goal',
    'hand',
    'heel',
    'idea',
    'jaw',
    'law',
    'map',
    'note',
    'oven',
    'poem',
    'rig',
    'ring',
    'river',
    'road',
    'robe',
    'rock',
    'scone',
    'sock',
    'stick',
    'stream',
    'table',
    'tooth',
    'town',
    'tub',
    'year',
    'zebra',
)


def random_tag() -> str:
    existing_tags = set(get_tags())

    for attempt in range(50):
        adj1, adj2 = random.sample(ADJECTIVES, k = 2)
        noun = random.choice(NOUNS)
        tag = f'{adj1}-{adj2}-{noun}'
        if tag not in existing_tags:
            return tag

    options = string.ascii_letters + string.digits
    for attempt in range(1_000_000):
        tag = ''.join(random.choices(options, k = 6))
        if tag not in existing_tags:
            return tag

    raise exceptions.InvalidTag('Could not find an unused random tag (try cleaning your transient maps)')
