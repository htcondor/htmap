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

# this file sets the names of non-configurable files and directories
# basically, it's a list of magic strings

RUN_DIR = 'run'

FUNC = 'func'
OUTPUTS_DIR = 'outputs'
INPUTS_DIR = 'inputs'
EVENT_LOG = 'event_log'
JOB_LOGS_DIR = 'job_logs'
STDOUT_EXT = 'stdout'
STDERR_EXT = 'stderr'
NUM_COMPONENTS = 'num_components'
SUBMIT = 'submit'
ITEMDATA = 'itemdata'

TRANSFER_DIR = '_htmap_transfer'
CHECKPOINT_PREP = 'prep_checkpoint'
CHECKPOINT_CURRENT = 'current_checkpoint'
CHECKPOINT_OLD = 'old_checkpoint'
