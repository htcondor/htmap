"""
Copyright 2018 HTCondor Team, Computer Sciences Department,
University of Wisconsin-Madison, WI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import htmap


def test_decorator_without_parens():
    @htmap.htmap
    def foo(x):
        return x

    assert isinstance(foo, htmap.HTMapper)


def test_decorator_with_parens(doubler):
    @htmap.htmap()
    def foo(x):
        return x

    assert isinstance(foo, htmap.HTMapper)


def test_htmap_is_idempotent(mapped_doubler):
    mapper = htmap.htmap(mapped_doubler)

    assert isinstance(mapper, htmap.HTMapper)
    assert not isinstance(mapper.func, htmap.HTMapper)
    assert mapper.func is mapped_doubler.func


def test_can_still_call_function_as_normal(mapped_doubler):
    assert mapped_doubler(5) == 10
