#!/bin/bash

set -eu

# Validate $HTCONDOR_VERSION (it's really the series)
if [[ ! $HTCONDOR_VERSION =~ ^([0-9]+)\.([0x]) ]]; then
    echo "Invalid \$HTCONDOR_VERSION $HTCONDOR_VERSION" >&2
    echo "It should look like '9.0', '9.x', '10.0', '10.x', etc." >&2
    exit 2
fi

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y install --no-install-recommends vim less git gnupg wget ca-certificates locales graphviz pandoc strace
echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
locale-gen
wget -qO - "https://research.cs.wisc.edu/htcondor/repo/keys/HTCondor-${HTCONDOR_VERSION}-Key" | apt-key add -
eval "$(grep '^VERSION_CODENAME=' /etc/os-release)"
echo "deb https://research.cs.wisc.edu/htcondor/repo/debian/${HTCONDOR_VERSION} ${VERSION_CODENAME} main" >> /etc/apt/sources.list.d/htcondor.list
apt-get -y update
apt-get -y install --no-install-recommends htcondor
# Parse `condor_version` to see if the installed version matches the series we requested
condor_version | python3 -c '
import os, sys, re
requested_series = os.environ["HTCONDOR_VERSION"]
condor_version_output = sys.stdin.read()
condor_version_match = re.search(r"^[$]CondorVersion:\s([0-9.]+)\s", condor_version_output, re.MULTILINE)
if not condor_version_match:
    sys.exit("Could not match CondorVersion in condor_version output")
series_major, series_minor = condor_version_match.group(1).split(".")[0:2]
if series_minor == "0":
    series_str = series_major + ".0"
else:
    series_str = series_major + ".x"
if requested_series != series_str:
    sys.exit("Requested series %s does not match installed series %s" % (requested_series, series_str))
'
apt-get -y clean
rm -rf /var/lib/apt/lists/*
