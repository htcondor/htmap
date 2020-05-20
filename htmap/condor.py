# Copyright 2020 HTCondor Team, Computer Sciences Department,
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

import htcondor

from htmap import settings


def get_schedd() -> htcondor.Schedd:
    """Get the :class:`htcondor.Schedd` that represents the HTCondor scheduler."""
    s = settings['HTCONDOR.SCHEDULER']
    if s is None:
        return htcondor.Schedd()

    coll = htcondor.Collector(settings['HTCONDOR.COLLECTOR'])
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, s)
    return htcondor.Schedd(schedd_ad)
