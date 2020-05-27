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

# hadolint ignore=DL3007
FROM htcondor/htc-minimal-notebook:latest

USER root

COPY binder/find_notebooks /usr/bin/find_notebooks
RUN chmod +x /usr/bin/find_notebooks

# Use the repository version of HTMap, not whatever was in the htc-notebook.
COPY . ${HOME}/htmap
RUN chown -R "${NB_UID}":"${NB_GID}" "${HOME}"/htmap

USER ${NB_UID}:${NB_GID}

# Install HTMap and strip any run results out of the tutorial notebooks.
# hadolint ignore=DL3013,SC2046
RUN : \
 && cp -r "${HOME}"/htmap/docs/source/tutorials "${HOME}"/tutorials \
 && pip install --no-cache-dir --upgrade -e "${HOME}"/htmap[docs] \
 && nbstripout $(find_notebooks) \
 && :

WORKDIR ${HOME}/tutorials
