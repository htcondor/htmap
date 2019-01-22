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

import time
from pathlib import Path

import pytest

import htmap
from htmap.settings import BASE_SETTINGS

from htmap._startup import ensure_htmap_dir_exists

# start with base settings (ignore user settings for tests)
htmap.settings.replace(BASE_SETTINGS)
htmap.settings['DELIVERY_METHOD'] = 'assume'  # assume is the default for testing


@pytest.fixture(scope = 'session', autouse = True)
def set_transplant_dir(tmpdir_factory):
    path = Path(tmpdir_factory.mktemp('htmap_transplant_dir'))
    htmap.settings['TRANSPLANT.DIR'] = path


def pytest_addoption(parser):
    parser.addoption(
        "--delivery",
        action = "store",
        default = 'assume',
    )


def pytest_generate_tests(metafunc):
    if 'delivery_methods' in metafunc.fixturenames:
        metafunc.parametrize(
            'delivery_method',
            metafunc.config.getoption('delivery').split(),
        )


@pytest.fixture()
def delivery_methods(delivery_method):
    htmap.settings['DELIVERY_METHOD'] = delivery_method


@pytest.fixture(scope = 'session', autouse = True)
def set_htmap_dir(tmpdir_factory):
    path = Path(tmpdir_factory.mktemp('htmap_dir'))
    htmap.settings['HTMAP_DIR'] = path
    ensure_htmap_dir_exists()


@pytest.fixture(scope = 'function', autouse = True)
def clean_after_test():
    yield

    htmap.clean(all = True)


@pytest.fixture(scope = 'session')
def doubler():
    def doubler(x):
        return 2 * x

    return doubler


@pytest.fixture(scope = 'session')
def mapped_doubler(doubler):
    mapper = htmap.mapped(doubler)
    return mapper


@pytest.fixture(scope = 'session')
def power():
    def power(x = 0, p = 2):
        return x ** p

    return power


@pytest.fixture(scope = 'session')
def mapped_power(power):
    mapper = htmap.mapped(power)
    return mapper


@pytest.fixture(scope = 'session')
def sleepy_double():
    def sleepy_double(x):
        time.sleep(30)
        return 2 * x

    return sleepy_double


@pytest.fixture(scope = 'session')
def mapped_sleepy_double(sleepy_double):
    mapper = htmap.mapped(sleepy_double)
    return mapper


@pytest.fixture(scope = 'session')
def mapped_exception():
    @htmap.mapped
    def fail(x):
        raise Exception(str(x))

    return fail


def exception_msg(exc_info) -> str:
    return str(exc_info.value)
