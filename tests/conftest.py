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

import functools
import time
import itertools
import subprocess
from pathlib import Path

import pytest

import htmap
from htmap.options import get_base_options_dict
from htmap.settings import BASE_SETTINGS

# start with base settings (ignore user settings for tests)
htmap.settings.replace(BASE_SETTINGS)
htmap.settings['DOCKER.IMAGE'] = 'maventree/htmap:latest'  # todo: this is bad


# todo: isolate tests that don't need this from those that do
@pytest.fixture(scope = 'session', autouse = True)
def set_transplant_dir(tmpdir_factory):
    path = Path(tmpdir_factory.mktemp('htmap_transplant_dir'))
    htmap.settings['TRANSPLANT.PATH'] = path


@pytest.fixture(
    scope = 'session',
    autouse = True,
    params = [
        'assume',
        'transplant',
        'docker',
    ],
)
def delivery_methods(request):
    htmap.settings['PYTHON_DELIVERY'] = request.param


def test_get_base_options(map_id, map_dir, delivery, test_id = None):
    opts = get_base_options_dict(map_id, map_dir, delivery)
    opts['+htmap_test_id'] = str(test_id)

    return opts


ids = itertools.count()


# todo: break this into two fixtures, one for setting htmap_dir, one for test_id and cleanup
@pytest.fixture(scope = 'function', autouse = True)
def set_htmap_dir_and_clean_after(tmpdir_factory, mocker):
    """Use a fresh HTMAP_DIR for every test."""
    path = Path(tmpdir_factory.mktemp('htmap_dir'))
    htmap.settings['HTMAP_DIR'] = path

    test_id = next(ids)
    mocker.patch(
        'htmap.options.get_base_options_dict',
        functools.partial(test_get_base_options, test_id = test_id),
    )

    yield

    subprocess.run(
        [
            'condor_rm',
            f'--constraint htmap_test_id=={test_id}',
        ],
        stdout = subprocess.DEVNULL,
        stderr = subprocess.DEVNULL,
    )


@pytest.fixture(scope = 'session')
def doubler():
    def doubler(x):
        return 2 * x

    return doubler


@pytest.fixture(scope = 'session')
def mapped_doubler(doubler):
    mapper = htmap.htmap(doubler)
    return mapper


@pytest.fixture(scope = 'session')
def power():
    def power(x = 0, p = 0):
        return x ** p

    return power


@pytest.fixture(scope = 'session')
def mapped_power(power):
    mapper = htmap.htmap(power)
    return mapper


@pytest.fixture(scope = 'session')
def sleepy_double():
    def sleepy_double(x):
        time.sleep(5)
        return 2 * x

    return sleepy_double


@pytest.fixture(scope = 'session')
def mapped_sleepy_double(sleepy_double):
    mapper = htmap.htmap(sleepy_double)
    return mapper


@pytest.fixture(scope = 'session')
def mapped_exception():
    @htmap.htmap
    def fail(x):
        raise Exception(str(x))

    return fail


def exception_msg(exc_info) -> str:
    return str(exc_info.value)
