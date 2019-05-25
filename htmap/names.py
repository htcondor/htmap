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

# names of directors in the HTMAP_DIR
MAPS_DIR = 'maps'
TAGS_DIR = 'tags'
LOGS_DIR = 'logs'
REMOVED_TAGS_DIR = '.removed_tags'
RUN_DIR = 'run'

# names of files and sub-directories of individual map dirs
FUNC = 'func'
CLUSTER_IDS = 'cluster_ids'
OUTPUTS_DIR = 'outputs'
OUTPUT_FILES_DIR = 'output_files'
INPUTS_DIR = 'inputs'
EVENT_LOG = 'events'
JOB_LOGS_DIR = 'logs'
INPUT_EXT = 'in'
OUTPUT_EXT = 'out'
STDOUT_EXT = 'stdout'
STDERR_EXT = 'stderr'
NUM_COMPONENTS = 'num_components'
SUBMIT = 'submit'
ITEMDATA = 'itemdata'
TRANSIENT_MARKER = 'transient'

# execute-side directory names
# these are NOT referenced by the run script, so you need to change the names
# there as well if you change them here
TRANSFER_DIR = '_htmap_transfer'
USER_TRANSFER_DIR = '_htmap_user_transfer'
CHECKPOINT_PREP = '_htmap_prep_checkpoint'
CHECKPOINT_CURRENT = '_htmap_current_checkpoint'
CHECKPOINT_OLD = '_htmap_old_checkpoint'
