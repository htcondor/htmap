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

from pathlib import Path

import htmap
from htmap.options import get_base_descriptors


def test_unknown_delivery_method_raises():
    with pytest.raises(htmap.exceptions.UnknownPythonDeliveryMethod):
        get_base_descriptors('foo', Path.cwd(), 'definitely-not-real')


@pytest.mark.parametrize(
    'method, universe',
    [
        ('assume', 'vanilla'),
        ('docker', 'docker'),
        ('transplant', 'vanilla'),
    ]
)
def test_delivery_methods_have_correct_universe(method, universe):
    descriptors = get_base_descriptors('foo', Path.cwd(), method)

    assert descriptors['universe'] == universe


def test_docker_delivery_has_docker_image_descriptor_set():
    descriptors = get_base_descriptors('foo', Path.cwd(), 'docker')

    descriptors['docker_image']  # will KeyError if not set


def test_transplant_delivery_uses_run_with_transplant_script():
    descriptors = get_base_descriptors('foo', Path.cwd(), 'transplant')

    assert 'run_with_transplant' in descriptors['executable']
