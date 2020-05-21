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

import shutil

import pytest

import htmap


def test_get_tags(mapped_doubler):
    mapped_doubler.map(range(1), tag="a")
    mapped_doubler.map(range(1), tag="b")
    mapped_doubler.map(range(1), tag="c")

    assert set(htmap.get_tags()) == {"a", "b", "c"}


@pytest.mark.parametrize(
    "tags, pattern, expected",
    [
        (("a", "b", "c"), None, ("a", "b", "c")),  # None = no filter
        (("a", "b", "c"), "a", ("a",)),
        (("a", "b", "c"), "?", ("a", "b", "c")),
        (("a", "b", "c"), "[ab]", ("a", "b")),
        (("a", "b", "c"), "[!ab]", ("c",)),
        (("a", "b", "c"), "??", ()),
        (("a", "b", "cc"), "??", ("cc",)),
        (("a1", "b1", "c1"), "?1", ("a1", "b1", "c1")),
        (("a1", "bbb1", "cc1"), "*1", ("a1", "bbb1", "cc1")),
        (("A",), "a", ()),  # case-sensitive
    ],
)
def test_get_tag_with_pattern(mapped_doubler, tags, pattern, expected):
    for tag in tags:
        mapped_doubler.map(range(1), tag=tag)

    assert set(htmap.get_tags(pattern)) == set(expected)


def test_load_maps_finds_all_maps(mapped_doubler):
    mapped_doubler.map(range(1))
    mapped_doubler.map(range(1))
    mapped_doubler.map(range(1))

    results = htmap.load_maps()

    assert len(results) == 3


def test_clean_removes_all_transient_maps(mapped_doubler):
    mapped_doubler.map(range(1))
    mapped_doubler.map(range(1))
    mapped_doubler.map(range(1))

    htmap.clean()

    assert len(htmap.get_tags()) == 0


def test_clean_without_maps_dir_doesnt_raise_exception():
    shutil.rmtree(
        str((htmap.settings["HTMAP_DIR"] / "maps").absolute()), ignore_errors=True,
    )

    htmap.clean()


def test_clean_only_removes_transient_maps(mapped_doubler):
    mapped_doubler.map(range(1), tag="not-me")
    mapped_doubler.map(range(1))

    htmap.clean()

    assert htmap.get_tags() == ("not-me",)


def test_clean_all_cleans_all_maps(mapped_doubler):
    mapped_doubler.map(range(1), tag="yes-me")
    mapped_doubler.map(range(1))

    htmap.clean(all=True)

    assert len(htmap.get_tags()) == 0
