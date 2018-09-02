from pathlib import Path

import pytest
from .conftest import exception_msg

import htmap
from htmap.options import create_submit_object_and_itemdata


def test_one_reserved_kwarg_raises():
    with pytest.raises(htmap.exceptions.ReservedOptionKeyword) as exc_info:
        htmap.MapOptions(
            transfer_input_files = 'foobar',
        )

    assert 'keyword' in exception_msg(exc_info)


def test_two_reserved_kwarg_raises():
    with pytest.raises(htmap.exceptions.ReservedOptionKeyword) as exc_info:
        htmap.MapOptions(
            transfer_input_files = 'foobar',
            jobbatchname = 'pink',
        )

    assert 'keywords' in exception_msg(exc_info)


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


def test_single_shared_input_file_can_be_single_str():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        fixed_input_files = 'foo.txt',
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


def test_list_of_list_of_str_input_files():
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


def test_list_of_str_input_files():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        input_files = ['foo.txt', 'bar.txt', 'buz.txt'],
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

    with pytest.raises(htmap.exceptions.MisalignedInputData) as exc_info:
        create_submit_object_and_itemdata(
            map_id,
            map_dir,
            hashes,
            map_options,
        )

    assert 'input_files' in exception_msg(exc_info)


def test_fewer_input_files_than_hashes():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        input_files = [['foo.txt']],
    )

    with pytest.raises(htmap.exceptions.MisalignedInputData) as exc_info:
        create_submit_object_and_itemdata(
            map_id,
            map_dir,
            hashes,
            map_options,
        )

    assert 'input_files' in exception_msg(exc_info)


@pytest.mark.parametrize(
    'rm',
    [
        ['239MB'],
        [239],
        [239.0],
    ]
)
def test_list_request_memory(rm):
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a']
    map_options = htmap.MapOptions(
        request_memory = rm,
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    expected = [
        {'hash': 'a', 'itemdata_for_request_memory': '239MB'},
    ]

    assert itemdata == expected


@pytest.mark.parametrize(
    'rd',
    [
        ['239GB'],
        [239],
        [239.0],
    ]
)
def test_list_request_disk(rd):
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a']
    map_options = htmap.MapOptions(
        request_disk = rd,
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    expected = [
        {'hash': 'a', 'itemdata_for_request_disk': '239GB'},
    ]

    assert itemdata == expected


def test_generic_itemdata():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        stooge = ['larry', 'moe', 'curly'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        map_id,
        map_dir,
        hashes,
        map_options,
    )

    expected = [
        {'hash': 'a', 'itemdata_for_stooge': 'larry'},
        {'hash': 'b', 'itemdata_for_stooge': 'moe'},
        {'hash': 'c', 'itemdata_for_stooge': 'curly'},
    ]

    assert itemdata == expected


def test_generic_itemdata_too_few():
    map_id = 'foo'
    map_dir = Path().cwd()
    hashes = ['a', 'b', 'c']
    map_options = htmap.MapOptions(
        stooge = ['larry', 'moe'],
    )

    with pytest.raises(htmap.exceptions.MisalignedInputData) as exc_info:
        create_submit_object_and_itemdata(
            map_id,
            map_dir,
            hashes,
            map_options,
        )

    assert 'stooge' in exception_msg(exc_info)
