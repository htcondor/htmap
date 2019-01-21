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

from pathlib import Path

import pytest
from tests.conftest import exception_msg

import htmap
from htmap.options import create_submit_object_and_itemdata, get_base_descriptors


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


def test_request_memory_for_str():
    opts = htmap.MapOptions(
        request_memory = '150MB'
    )

    assert opts['request_memory'] == '150MB'


def test_request_disk_for_str():
    opts = htmap.MapOptions(
        request_disk = '1.2GB'
    )

    assert opts['request_disk'] == '1.2GB'


def test_single_shared_input_file():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        fixed_input_files = ['foo.txt'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert 'foo.txt' in sub['transfer_input_files']


def test_single_shared_input_file_can_be_single_str():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        fixed_input_files = 'foo.txt',
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert 'foo.txt' in sub['transfer_input_files']


def test_two_shared_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        fixed_input_files = ['foo.txt', 'bar.txt'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert 'foo.txt' in sub['transfer_input_files']
    assert 'bar.txt' in sub['transfer_input_files']


def test_list_of_list_of_str_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 3
    map_options = htmap.MapOptions(
        input_files = [['foo.txt'], ['bar.txt'], ['buz.txt']],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'extra_input_files': Path('foo.txt').absolute().as_posix()},
        {'component': '1', 'extra_input_files': Path('bar.txt').absolute().as_posix()},
        {'component': '2', 'extra_input_files': Path('buz.txt').absolute().as_posix()},
    ]

    assert itemdata == expected


def test_list_of_str_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 3
    map_options = htmap.MapOptions(
        input_files = ['foo.txt', 'bar.txt', 'buz.txt'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'extra_input_files': Path('foo.txt').absolute().as_posix()},
        {'component': '1', 'extra_input_files': Path('bar.txt').absolute().as_posix()},
        {'component': '2', 'extra_input_files': Path('buz.txt').absolute().as_posix()},
    ]

    assert itemdata == expected


def test_list_of_path_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 3
    map_options = htmap.MapOptions(
        input_files = [Path('foo.txt'), Path('bar.txt'), Path('buz.txt')],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'extra_input_files': Path('foo.txt').absolute().as_posix()},
        {'component': '1', 'extra_input_files': Path('bar.txt').absolute().as_posix()},
        {'component': '2', 'extra_input_files': Path('buz.txt').absolute().as_posix()},
    ]

    assert itemdata == expected


def test_list_of_list_of_path_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 3
    map_options = htmap.MapOptions(
        input_files = [
            [Path('foo.txt'), Path('foo2.txt')],
            [Path('bar.txt'), Path('bar2.txt')],
            [Path('buz.txt'), Path('buz2.txt')],
        ],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'extra_input_files': f"{Path('foo.txt').absolute().as_posix()}, {Path('foo2.txt').absolute().as_posix()}"},
        {'component': '1', 'extra_input_files': f"{Path('bar.txt').absolute().as_posix()}, {Path('bar2.txt').absolute().as_posix()}"},
        {'component': '2', 'extra_input_files': f"{Path('buz.txt').absolute().as_posix()}, {Path('buz2.txt').absolute().as_posix()}"},
    ]

    assert itemdata == expected


def test_fewer_components_than_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        input_files = [['foo.txt'], ['bar.txt'], ['buz.txt']],
    )

    with pytest.raises(htmap.exceptions.MisalignedInputData) as exc_info:
        create_submit_object_and_itemdata(
            tag,
            map_dir,
            num_components,
            map_options,
        )

    assert 'input_files' in exception_msg(exc_info)


def test_fewer_input_files_than_components():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 5
    map_options = htmap.MapOptions(
        input_files = [['foo.txt']],
    )

    with pytest.raises(htmap.exceptions.MisalignedInputData) as exc_info:
        create_submit_object_and_itemdata(
            tag,
            map_dir,
            num_components,
            map_options,
        )

    assert 'input_files' in exception_msg(exc_info)


@pytest.mark.parametrize(
    'rm',
    [
        ['239MB'],
    ]
)
def test_list_request_memory(rm):
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        request_memory = rm,
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'itemdata_for_request_memory': '239MB'},
    ]

    assert itemdata == expected


@pytest.mark.parametrize(
    'rd',
    [
        ['239GB'],
    ]
)
def test_list_request_disk(rd):
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        request_disk = rd,
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'itemdata_for_request_disk': '239GB'},
    ]

    assert itemdata == expected


def test_generic_itemdata():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 3
    map_options = htmap.MapOptions(
        stooge = ['larry', 'moe', 'curly'],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    expected = [
        {'component': '0', 'itemdata_for_stooge': 'larry'},
        {'component': '1', 'itemdata_for_stooge': 'moe'},
        {'component': '2', 'itemdata_for_stooge': 'curly'},
    ]

    assert itemdata == expected


def test_generic_itemdata_too_few():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        stooge = ['larry', 'moe'],
    )

    with pytest.raises(htmap.exceptions.MisalignedInputData) as exc_info:
        create_submit_object_and_itemdata(
            tag,
            map_dir,
            num_components,
            map_options,
        )

    assert 'stooge' in exception_msg(exc_info)


def test_merging_options_override():
    a = htmap.MapOptions(
        foo = 'override',
    )
    b = htmap.MapOptions(
        foo = 'hidden'
    )

    merged = htmap.MapOptions.merge(a, b)

    assert merged['foo'] == 'override'


def test_merging_options_side_by_side():
    a = htmap.MapOptions(
        foo = 'me',
    )
    b = htmap.MapOptions(
        bar = 'too'
    )

    merged = htmap.MapOptions.merge(a, b)

    assert merged['foo'] == 'me'
    assert merged['bar'] == 'too'


def test_merging_options_merges_fixed_input():
    a = htmap.MapOptions(
        fixed_input_files = 'foo.txt',
    )
    b = htmap.MapOptions(
        fixed_input_files = 'bar.txt',
    )

    merged = htmap.MapOptions.merge(a, b)

    assert set(merged.fixed_input_files) == {'foo.txt', 'bar.txt'}


def test_option_from_settings_is_visible_in_base_options():
    htmap.settings['MAP_OPTIONS.zing'] = 'hit'

    opts = get_base_descriptors('foo', Path('bar'), delivery = 'assume')

    assert opts['zing'] == 'hit'


def test_url_in_fixed_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    url = 'http://www.baz.test'
    map_options = htmap.MapOptions(
        fixed_input_files = [url],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert url in sub['transfer_input_files']


def test_url_in_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    url = 'http://www.baz.test'
    map_options = htmap.MapOptions(
        input_files = [url],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert url in itemdata[0]['extra_input_files']


def test_two_urls_in_input_files():
    tag = 'foo'
    map_dir = Path().cwd()
    num_components = 1
    url_1 = 'http://www.baz.test'
    url_2 = 'http://www.bong.test'
    map_options = htmap.MapOptions(
        input_files = [(url_1, url_2)],
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert url_1 in itemdata[0]['extra_input_files']
    assert url_2 in itemdata[0]['extra_input_files']


def test_unknown_delivery_mechanism():
    with pytest.raises(htmap.exceptions.UnknownPythonDeliveryMethod):
        get_base_descriptors('foo', Path.cwd(), delivery = 'unknown')


@pytest.mark.parametrize(
    'key',
    [
        'foo',
        '+foo',
        'my.foo',
        'MY.foo',
        'mY.foo',
    ]
)
def test_custom_options(key):
    tag = 'test'
    map_dir = Path().cwd()
    num_components = 1
    map_options = htmap.MapOptions(
        custom_options = {key: 'bar'},
    )

    sub, itemdata = create_submit_object_and_itemdata(
        tag,
        map_dir,
        num_components,
        map_options,
    )

    assert '+foo' in sub.keys()
