#!/usr/bin/env bash

set -e

# set up directories for state
_condor_local_dir=`condor_config_val LOCAL_DIR` || exit 5
mkdir -p "$_condor_local_dir/lock" "$_condor_local_dir/log" "$_condor_local_dir/run" "$_condor_local_dir/spool" "$_condor_local_dir/execute" "$_condor_local_dir/cred_dir"

# start condor
condor_master
echo "HTCondor is starting..."

# wait until the scheduler is awake
while [[ ! -f ${_condor_local_dir}/spool/job_queue.log ]]
do
  sleep .01
done

exec "$@"
