#!/usr/bin/env bash
set -e

_condor_local_dir=`condor_config_val LOCAL_DIR` || exit 5
mkdir -p "$_condor_local_dir/lock" "$_condor_local_dir/log" "$_condor_local_dir/run" "$_condor_local_dir/spool" "$_condor_local_dir/execute" "$_condor_local_dir/cred_dir"
condor_master
echo "HTCondor is starting..."
sleep 5  # todo: ask the scheduler if it's ready instead of waiting

exec $@
