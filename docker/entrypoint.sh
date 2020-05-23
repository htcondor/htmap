#!/usr/bin/env bash

set -e

# set up directories for state
_condor_local_dir=`condor_config_val LOCAL_DIR` || exit 5
mkdir -p "$_condor_local_dir/lock" "$_condor_local_dir/log" "$_condor_local_dir/run" "$_condor_local_dir/spool" "$_condor_local_dir/execute" "$_condor_local_dir/cred_dir"

# start condor
condor_master

# once the shared port daemon wakes up, use condor_who to wait for condor to stand up
while [[ ! -s "${_condor_local_dir}/log/SharedPortLog" ]]
do
  sleep .01
done
sleep 1  # fudge factor to let shared port *actually* wake up
condor_who -wait:60 'IsReady && STARTD_State =?= "Ready"' > /dev/null

exec "$@"
