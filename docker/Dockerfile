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

# install utils and dependencies
# install HTCondor version specified in config
COPY docker/install-htcondor.sh /root/install-htcondor.sh
RUN /bin/bash /root/install-htcondor.sh

# Set locale (now that we have generated it with locale-gen)
ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8

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
RUN : \
 && htcondor_version_major=$(echo "$HTCONDOR_VERSION" | cut -d. -f1) \
 && htcondor_version_minor=$(echo "$HTCONDOR_VERSION" | cut -d. -f2) \
 && if [ "$htcondor_version_minor" = x ]; then \
        requirement="htcondor~=${htcondor_version_major}.1"; \
        # ^^ gets translated into e.g. htcondor>=9.1,<10.0 \
    else \
        requirement="htcondor~=${htcondor_version_major}.0.0"; \
        # ^^ gets translated into e.g. >=9.0.0,<9.1 \
    fi \
 && python -m pip install --user --no-cache-dir --disable-pip-version-check "/home/${USER}/htmap[tests,docs,widgets]" "$requirement" \
 && jupyter nbextension enable --py widgetsnbextension \
 && jupyter labextension install --minimize=False @jupyter-widgets/jupyterlab-manager \
 && :

WORKDIR /home/${USER}/htmap
