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

# this dockerfile builds a test environment for HTMap

ARG PYTHON_VERSION=3.7
FROM python:${PYTHON_VERSION}

# build config
ARG HTCONDOR_VERSION=8.8

# switch to root to do root-level config
USER root

# environment setup
ENV DEBIAN_FRONTEND=noninteractive

# install utils and dependencies
RUN apt-get update \
 && apt-get -y install --no-install-recommends vim less git gnupg wget ca-certificates locales graphviz pandoc \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/* \
 && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
 && locale-gen

ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8

# install HTCondor version specified in config
RUN wget -qO - https://research.cs.wisc.edu/htcondor/debian/HTCondor-Release.gpg.key | apt-key add - \
 && echo "deb http://research.cs.wisc.edu/htcondor/debian/${HTCONDOR_VERSION}/buster buster contrib" >> /etc/apt/sources.list.d/htcondor.list \
 && apt-get -y update \
 && apt-get -y install --no-install-recommends htcondor \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/*

# copy entrypoint into place and make executable
COPY docker/entrypoint.sh /.entrypoint.sh
RUN chmod +x /.entrypoint.sh

# create a user, set their PATH and PYTHONPATH
ENV SUBMIT_USER=mapper \
    PATH="/home/mapper/.local/bin:${PATH}" \
    PYTHONPATH="/home/mapper/htmap:${PYTHONPATH}"
RUN groupadd ${SUBMIT_USER} \
 && useradd -m -g ${SUBMIT_USER} ${SUBMIT_USER}

# switch to the user, don't need root anymore
USER ${SUBMIT_USER}

# install dependencies and debugging tools early for docker build caching
COPY requirements* /tmp/
RUN python -m pip install --user --no-cache-dir -r /tmp/requirements-dev.txt -r /tmp/requirements-docs.txt ipython \
 && python -m pip install --user --no-cache-dir --upgrade htcondor==${HTCONDOR_VERSION}.*

# copy testing configs into place
COPY docker/condor_config.local /etc/condor/condor_config.local
COPY --chown=mapper:mapper docker/.htmaprc /home/${SUBMIT_USER}/

# set default entrypoint and command
# the entrypoint is critical: it starts HTCondor in the container
WORKDIR /home/${SUBMIT_USER}/htmap
ENTRYPOINT ["/.entrypoint.sh"]
CMD ["bash"]
