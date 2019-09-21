# Copyright 2019 HTCondor Team, Computer Sciences Department,
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


def test_status_has_tag(cli):
    m = htmap.map(str, range(1))

    result = cli(['status'])

    assert m.tag in result.output


def test_status_has_num_jobs_somewhere(cli):
    htmap.map(str, range(1))

    result = cli(['status'])

    assert ' 1 ' in result.output


def test_status_with_no_state_gives_no_num_jobs(cli):
    htmap.map(str, range(1))

    result = cli(['status', '--no-state'])

    assert ' 1 ' not in result.output


@pytest.mark.parametrize('format', ['json', 'json_compact', 'csv'])
def test_live_conflicts_with_non_text_formats(cli, format):
    result = cli(['status', '--format', format, '--live'])

    assert 'ERROR' in result.output
    assert result.exit_code == 1


@pytest.mark.parametrize('format', ['wizard', 'pie', 'echo'])
def test_bad_format_prints_usage(cli, format):
    result = cli(['status', '--format', format])

    assert 'Usage' in result.output
    assert format in result.output
    assert result.exit_code == 2
