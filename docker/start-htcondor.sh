#!/bin/bash

# If env _condor_SCHEDD_HOST is set, then likely we have been spawned by a hub
# that wants to use an HTCondor instance outside of the container.  So
# in this case, just exit without starting a personal condor.

_condor_local_dir=`condor_config_val LOCAL_DIR` || exit 5

_MSG="Did NOT start HTCondor because using remote schedd"

[ ! -v "_condor_SCHEDD_HOST" ] && if [ $(id -u) == 0 ] ; then
  _MSG="Started HTCondor via sudo"
  sudo -E -H -u $NB_USER PATH=$PATH -- mkdir -p "$_condor_local_dir/lock" "$_condor_local_dir/log" "$_condor_local_dir/run" "$_condor_local_dir/spool" "$_condor_local_dir/execute" "$_condor_local_dir/cred_dir"
  sudo -E -H -u $NB_USER PATH=$PATH -- condor_master
else
  _MSG="Started HTCondor"
  mkdir -p "$_condor_local_dir/lock" "$_condor_local_dir/log" "$_condor_local_dir/run" "$_condor_local_dir/spool" "$_condor_local_dir/execute" "$_condor_local_dir/cred_dir"
  condor_master
fi

echo $_MSG
