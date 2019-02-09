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

FROM jupyter/scipy-notebook:6c6ebe8734e9
ARG HTCONDOR_VERSION=8.8
ENV NB_USER="jovyan" \
    NB_UID="1000" \
    NB_GID="100" \
    DEBIAN_FRONTEND=noninteractive

USER root

RUN apt-get update \
 && apt-get -y install gnupg vim less git  \
 && wget -qO - https://research.cs.wisc.edu/htcondor/ubuntu/HTCondor-Release.gpg.key | apt-key add - \
 && echo "deb  http://research.cs.wisc.edu/htcondor/ubuntu/${HTCONDOR_VERSION}/bionic bionic contrib" >> /etc/apt/sources.list \
 && apt-get -y update \
 && apt-get -y install htcondor \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/*

RUN conda config --add channels conda-canary \
 && conda update -y conda \
 && conda clean -y --all \
 && pip install --no-cache-dir nbstripout

COPY docker/condor_config.local /etc/condor/condor_config.local
COPY docker/.htmaprc ${HOME}/.htmaprc
COPY . ${HOME}/htmap
COPY docs/source/tutorials ${HOME}/tutorials
RUN chown -R ${NB_UID} ${HOME} \
 && chmod +x ${HOME}/htmap/docker/entrypoint.sh

USER ${NB_USER}
RUN nbstripout ${HOME}/tutorials/* \
 && pip install --no-cache-dir -e ${HOME}/htmap

ENTRYPOINT ["htmap/docker/entrypoint.sh"]
