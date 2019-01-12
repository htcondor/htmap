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

FROM ubuntu:bionic
USER root

# config
ARG HTCONDOR_VERSION=8.8
ARG MINICONDA_VERSION=4.5.11
ARG PYTHON_VERSION=3.6

ENV DEBIAN_FRONTEND=noninteractive
ENV CONDA_DIR=/opt/conda
ENV PATH=$CONDA_DIR/bin:$PATH

# faster container builds
RUN apt-get update \
 && apt-get -y install software-properties-common \
 && add-apt-repository ppa:apt-fast/stable \
 && apt-get update \
 && apt-get -y install apt-fast

# install utils and dependendencies
RUN apt-fast -y update \
 && apt-fast -y install --no-install-recommends vim less build-essential gnupg wget ca-certificates

# install HTCondor version specified in config
RUN wget -qO - https://research.cs.wisc.edu/htcondor/ubuntu/HTCondor-Release.gpg.key | apt-key add - \
 && echo "deb  http://research.cs.wisc.edu/htcondor/ubuntu/${HTCONDOR_VERSION}/bionic bionic contrib" >> /etc/apt/sources.list \
 && apt-fast -y update \
 && apt-fast -y install htcondor \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/*

# install miniconda version specified in config
RUN cd /tmp \
 && wget --quiet https://repo.continuum.io/miniconda/Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh \
 && bash Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh -f -b -p $CONDA_DIR \
 && rm Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh \
 && conda install python=${PYTHON_VERSION} \
 && conda update -y --all \
 && conda clean -y -all

# copy HTCondor config into correct location
COPY docker/condor_config.local /etc/condor/condor_config.local

# copy htmap library into container and install it
WORKDIR $HOME/htmap
COPY . .
RUN pip install --no-cache . \
 && pip install -r requirements_dev.txt
