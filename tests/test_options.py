from pathlib import Path

import pytest

import htmap
from htmap.options import create_submit_object_and_itemdata


def test_one_reserved_kwarg_raises():
    with pytest.raises(htmap.exceptions.ReservedOptionKeyword) as excinfo:
        htmap.MapOptions(
            transfer_input_files = 'foobar',
        )

    assert 'keyword' in str(excinfo.value)


def test_two_reserved_kwarg_raises():
    with pytest.raises(htmap.exceptions.ReservedOptionKeyword) as excinfo:
        htmap.MapOptions(
            transfer_input_files = 'foobar',
            jobbatchname = 'pink',
        )

    assert 'keywords' in str(excinfo.value)


def test_reserved_kwargs_are_case_insensitive():
    with pytest.raises(htmap.exceptions.ReservedOptionKeyword):
        htmap.MapOptions(
            JobBatchName = 'pink',
        )


def test_request_memory_for_int():
    opts = htmap.MapOptions(
        request_memory = 200
    )

    assert opts['request_memory'] == '200MB'


def test_request_memory_for_str():
    opts = htmap.MapOptions(
        request_memory = '150MB'
    )

    assert opts['request_memory'] == '150MB'


def test_request_disk_for_int():
    opts = htmap.MapOptions(
        request_disk = 20
    )

    assert opts['request_disk'] == '20GB'


def test_request_disk_for_str():
    opts = htmap.MapOptions(
        request_disk = '1.2GB'
    )

    assert opts['request_disk'] == '1.2GB'


def test_single_shared_input_file():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        fixed_input_files = ['foo.txt'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    assert 'foo.txt' in sub['transfer_input_files']


def test_two_shared_input_files():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        fixed_input_files = ['foo.txt', 'bar.txt'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    assert 'foo.txt' in sub['transfer_input_files']
    assert 'bar.txt' in sub['transfer_input_files']


def test_aligned_input_files():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        input_files = [['foo.txt'], ['bar.txt'], ['buz.txt']],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    expected = [
        {'hash': 'a', 'extra_input_files': Path('foo.txt').absolute().as_posix()},
        {'hash': 'b', 'extra_input_files': Path('bar.txt').absolute().as_posix()},
        {'hash': 'c', 'extra_input_files': Path('buz.txt').absolute().as_posix()},
    ]

    assert itemdata == expected


def test_fewer_hashes_than_input_files():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a']
    map_options = htmap.MapOptions(
        input_files = [['foo.txt'], ['bar.txt'], ['buz.txt']],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    expected = [
        {'hash': 'a', 'extra_input_files': Path('foo.txt').absolute().as_posix()},
    ]

    assert itemdata == expected


def test_fewer_input_files_than_hashes():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        input_files = [['foo.txt']],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    expected = [
        {'hash': 'a', 'extra_input_files': Path('foo.txt').absolute().as_posix()},
        {'hash': 'b', 'extra_input_files': ''},
        {'hash': 'c', 'extra_input_files': ''},
    ]

    assert itemdata == expected
