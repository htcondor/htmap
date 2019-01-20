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

from typing import NamedTuple


class ComponentHold(NamedTuple):
    """Represents an HTCondor hold on a map component."""

    code: int
    reason: str

    def __str__(self):
        return f'[{self.code}] {self.reason}'

    def __repr__(self):
        return f'<{self.__class__.__name__}(code = {self.code}, reason = {self.reason}>'
