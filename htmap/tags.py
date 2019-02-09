import random
import string
from pathlib import Path
from typing import Tuple

from htmap import names, settings, exceptions


def tags_dir() -> Path:
    return Path(settings['HTMAP_DIR']) / names.TAGS_DIR


def get_tags() -> Tuple[str, ...]:
    """Return a tuple containing the ``tag`` for all existing maps."""
    return tuple(path.name for path in tags_dir().iterdir())


def tag_file_path(tag: str) -> Path:
    return tags_dir() / tag


def raise_if_tag_already_exists(tag: str) -> None:
    """Raise a :class:`htmap.exceptions.TagAlreadyExists` if the ``tag`` already exists."""
    if tag_file_path(tag).exists():
        raise exceptions.TagAlreadyExists(f'the requested tag {tag} already exists (recover the Map, then either use or delete it).')


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
