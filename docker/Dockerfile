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

ARG PYTHON_VERSION=3.6
FROM python:${PYTHON_VERSION}

# build config
ARG HTCONDOR_VERSION=9.0

# switch to root to do root-level config
USER root

# environment setup
ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8

# install utils and dependencies
# install HTCondor version specified in config
RUN : \
 && apt-get update \
 && apt-get -y install --no-install-recommends vim less git gnupg wget ca-certificates locales graphviz pandoc strace \
 && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
 && locale-gen \
 && if expr "$HTCONDOR_VERSION" : '8\.[89]' >/dev/null; then \
        wget -qO - https://research.cs.wisc.edu/htcondor/debian/HTCondor-Release.gpg.key | apt-key add -; \
    else \
        wget -qO - https://research.cs.wisc.edu/htcondor/repo/keys/HTCondor-$HTCONDOR_VERSION-Key | apt-key add -; \
    fi \
 && echo "deb https://research.cs.wisc.edu/htcondor/repo/debian/${HTCONDOR_VERSION} buster main" >> /etc/apt/sources.list.d/htcondor.list \
 && apt-get -y update \
 && apt-get -y install --no-install-recommends htcondor \
 && condor_version | grep -E '^[$]CondorVersion: '"${HTCONDOR_VERSION}" \
 #  ^^ sanity check \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/* \
 && :

# create a user, set their PATH and PYTHONPATH
ENV USER=mapper \
    PATH="/home/mapper/.local/bin:${PATH}" \
    PYTHONPATH="/home/mapper/htmap:${PYTHONPATH}"
RUN : \
 && groupadd ${USER} \
 && useradd -m -g ${USER} ${USER} \
 && :

# switch to the user, don't need root anymore
USER ${USER}

# copy testing configs into place
COPY docker/condor_config.local /etc/condor/condor_config.local
COPY --chown=mapper:mapper docker/.htmaprc /home/${USER}/

# set default entrypoint and command
# the entrypoint is critical: it starts HTCondor in the container
ENTRYPOINT ["/home/mapper/htmap/docker/entrypoint.sh"]
CMD ["bash"]

COPY --chown=mapper:mapper . /home/${USER}/htmap
RUN python -m pip install --user --no-cache-dir --disable-pip-version-check --use-feature=2020-resolver "/home/${USER}/htmap[tests,docs]" htcondor==${HTCONDOR_VERSION}.*

WORKDIR /home/${USER}/htmap
