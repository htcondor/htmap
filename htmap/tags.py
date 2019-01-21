import random
from pathlib import Path
from typing import Tuple

from htmap import names, settings, exceptions


def tags_dir_path() -> Path:
    return Path(settings['HTMAP_DIR']) / names.TAGS_DIR


def get_tags() -> Tuple[str, ...]:
    """Return a tuple containing the ``tag`` for all existing maps."""
    return tuple(path.name for path in tags_dir_path().iterdir())


def tag_file_path(tag: str) -> Path:
    return tags_dir_path() / tag


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
    'breezy',
    'boundless',
    'equitable',
    'deranged',
    'fluttering',
    'spurious',
    'harsh',
    'jaded',
    'arrogant',
    'incredible',
    'happy',
    'blushing',
    'trite',
    'vast',
    'tiny',
    'blue',
    'red',
    'green',
    'wicked',
    'torrid',
    'deep',
    'shallow',
    'quick',
    'slow',
    'soggy',
    'sweet',
)
NOUNS = (
    'apple',
    'car',
    'badge',
    'cat',
    'stick',
    'zebra',
    'coil',
    'table',
    'bird',
    'cat',
    'dog',
    'echo',
    'bird',
    'sock',
    'tub',
    'frog',
    'road',
    'cake',
    'cookie',
    'ring',
)


def random_tag() -> str:
    existing_tags = set(path.name for path in tags_dir_path().iterdir())

    while True:
        adj1, adj2 = random.sample(ADJECTIVES, k = 2)
        noun = random.choice(NOUNS)
        tag = f'{adj1}-{adj2}-{noun}'
        if tag not in existing_tags:
            return tag
