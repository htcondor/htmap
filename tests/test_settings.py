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

import pytest

import htmap
from htmap.settings import Settings


def test_no_args_makes_emtpy_settings():
    s = Settings()

    assert len(s.maps) == 1
    assert s.maps[0] == {}


def test_two_maps_in_constructor():
    s = Settings({}, {})

    assert len(s.maps) == 2


def test_empty_settings_equal():
    s1 = Settings()
    s2 = Settings()

    assert s1 == s2
    assert s1 is not s2


def test_nested_settings_equal():
    s1 = Settings({'outer': {'inner': 'foo'}})
    s2 = Settings({'outer': {'inner': 'foo'}})

    assert s1 == s2
    assert s1 is not s2


def test_nested_settings_not_equal():
    s1 = Settings({'outer': {'inner': 'foo'}})
    s2 = Settings({'outer': {'inner': 'bar'}})

    assert s1 != s2


def test_getitem_drills_through_dots():
    s = Settings(
        {
            'outer': {
                'inner': 'foo',
            },
        }
    )

    assert s['outer.inner'] == 'foo'


def test_get_drills_through_dots():
    s = Settings(
        {
            'outer': {
                'inner': 'foo',
            },
        }
    )

    assert s.get('outer.inner') == 'foo'


def test_getitem_with_missing_key_raises_missing_setting():
    s = Settings()

    with pytest.raises(htmap.exceptions.MissingSetting):
        s['foo']


def test_get_with_missing_key_returns_default():
    s = Settings()

    assert s.get('foo', default = 'bar') == 'bar'


def test_setitem_drills_through_dots():
    s = Settings(
        {
            'outer': {
                'inner': 'foo',
            },
        }
    )

    s['outer.inner'] = 'bar'

    assert s['outer.inner'] == 'bar'


def test_setitem_creates_new_nested_path_when_path_doesnt_exist():
    s = Settings()

    s['foo.bar'] = 'zoo'

    assert s['foo.bar'] == 'zoo'
    assert s.maps[0] == {'foo': {'bar': 'zoo'}}


def test_to_dict_with_single_map():
    s = Settings(
        {
            'outer': {
                'inner': 'foo',
            },
        }
    )

    assert s.to_dict() == {'outer': {'inner': 'foo'}}


def test_to_dict_with_override():
    s = Settings(
        {
            'outer': {
                'inner': 'override',
            },
        },
        {
            'outer': {
                'inner': 'hidden',
            },
        }
    )

    assert s.to_dict() == {'outer': {'inner': 'override'}}


def test_to_dict_with_no_overlap():
    s = Settings(
        {
            'outer': {
                'top': 'this',
            },
        },
        {
            'outer': {
                'bottom': 'that',
            },
        }
    )

    assert s.to_dict() == {'outer': {'top': 'this', 'bottom': 'that'}}


def test_replace():
    s = Settings({'foo': 'bar'})

    new = Settings({'zing': 'zang'})

    s.replace(new)

    assert s.maps == new.maps
    with pytest.raises(htmap.exceptions.MissingSetting):
        s['foo']
    assert s['zing'] == 'zang'


def test_append_with_dict():
    s = Settings({'foo': 'override'})
    s.append({'foo': 'hidden'})

    assert len(s.maps) == 2
    assert s['foo'] == 'override'


def test_append_with_settings():
    s = Settings({'foo': 'override'})
    s.append(Settings({'foo': 'hidden'}))

    assert len(s.maps) == 2
    assert s['foo'] == 'override'


def test_prepend_with_dict():
    s = Settings({'foo': 'hidden'})
    s.prepend({'foo': 'override'})

    assert len(s.maps) == 2
    assert s['foo'] == 'override'


def test_prepend_with_settings():
    s = Settings({'foo': 'hidden'})
    s.prepend(Settings({'foo': 'override'}))

    assert len(s.maps) == 2
    assert s['foo'] == 'override'


def test_can_load_saved_settings(tmpdir):
    s = Settings({'foo': 'bar'})

    file = tmpdir.join('foo.toml')
    s.save(file)

    loaded = Settings.load(file)

    assert loaded == s
    assert loaded is not s
