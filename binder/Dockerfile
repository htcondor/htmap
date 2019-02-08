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
ENV HTMAP_DOCKER_IMAGE="jupyter/scipy-notebook:6c6ebe8734e9"
ARG JUPYTERHUB_VERSION=0.9.4
ARG HTCONDOR_VERSION=8.8
ARG NB_USER="jovyan"
ARG NB_UID="1000"
ARG NB_GID="100"

USER root

# Install latest HTCondor native package, as well
# as HTCondor python bindings, htmap package, and
# a few handy things like vim, less.

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -qq \
 && apt-get install -qq gnupg vim less git \
 && wget -qO - https://research.cs.wisc.edu/htcondor/ubuntu/HTCondor-Release.gpg.key | apt-key add - \
 && echo "deb  http://research.cs.wisc.edu/htcondor/ubuntu/${HTCONDOR_VERSION}/bionic bionic contrib" >> /etc/apt/sources.list \
 && apt-get update && apt-get -qq install htcondor \
 && apt-get clean -qq \
 && rm -rf /var/lib/apt/lists/*


USER $NB_UID

# We want the end user to be able to conda install new
# packages without sudo.  To do this, we need to upgrade
# the version of Conda to avoid hitting this conda bug:
# https://github.com/conda/conda/issues/7267

RUN conda config --add channels conda-canary \
 && conda update -n base conda

USER root

COPY binder/start-htcondor.sh /usr/./local/bin/before-notebook.d/
COPY binder/condor_config.local /etc/condor/condor_config.local

USER ${NB_USER}
COPY . ${HOME}/htmap
USER root
RUN chown -R ${NB_UID} ${HOME}
USER ${NB_USER}

RUN pip install -e ${HOME}/htmap
