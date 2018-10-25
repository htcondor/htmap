#!/usr/bin/env python3


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
import coverage
import os

cov = coverage.coverage()
cov.start()

pytest.main()

cov.stop()
cov.save()

print('Coverage Report:')
cov.report()

report_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'covreport')
cov.html_report(directory = report_dir)
print(f'HTML Report at {os.path.join(report_dir, "index.html")}')

cov.erase()
