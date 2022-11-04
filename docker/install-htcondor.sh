#!/bin/bash

set -eu

apt-get update
apt-get -y install --no-install-recommends vim less git gnupg wget ca-certificates locales graphviz pandoc strace
echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
locale-gen
wget -qO - "https://research.cs.wisc.edu/htcondor/repo/keys/HTCondor-$HTCONDOR_VERSION-Key" | apt-key add -
eval "$(grep '^VERSION_CODENAME=' /etc/os-release)"
echo "deb https://research.cs.wisc.edu/htcondor/repo/debian/${HTCONDOR_VERSION} ${VERSION_CODENAME} main" >> /etc/apt/sources.list.d/htcondor.list
apt-get -y update
apt-get -y install --no-install-recommends htcondor
condor_version | grep -E '^[$]CondorVersion: '"${HTCONDOR_VERSION}"
# ^^ sanity check \
apt-get -y clean
rm -rf /var/lib/apt/lists/*
