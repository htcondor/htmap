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

FROM htcondor/htc-minimal-notebook:latest
ENV NB_USER="jovyan"

USER ${NB_USER}

# copy the tutorial notebooks into the container
COPY --chown=jovyan:0 docs/source/tutorials ${HOME}/tutorials

# use the repository version of HTMap, not whatever was in the htc notebook
# also, strip any run results out of the tutorial notebooks
COPY --chown=jovyan:0 . /tmp/htmap
RUN pip install --no-cache-dir nbstripout \
 && nbstripout ${HOME}/tutorials/* \
 && pip install --no-cache-dir --upgrade /tmp/htmap \
 && rm -rf /tmp/htmap

WORKDIR ${HOME}/tutorials
