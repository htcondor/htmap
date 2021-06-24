HTMap Innards
=============

.. py:currentmodule:: htmap


Overview
--------

HTMap turns Python functions into HTCondor jobs.
There are two levels of wrapping that it does: the function call, with its
inputs and outputs (including file transfer)
and any possible errors, are implicitly wrapped,
but most other HTCondor features, like resource requests
and custom submit descriptors, are presented
more directly (though still with a Python-oriented interface).

The distinction between these two levels was chosen to provide the maximum
amount of "do the expected thing" with the Python parts of running the job
while allowing maximum flexibility for the HTCondor parts of the running job.
There is no hard line, and different parts of this have moved back and forth
over the line during development (namely file transfer, but at one point
resource requests were also treated specially).


Guiding Principles
------------------

* The only identifying piece of information about a map a **user** should ever
  need is a ``tag``.
* Users should never have to directly interact with the filesystem to look at
  any information about their map.
* We should store as little state as possible in memory. Recalculating state
  of anything but the very largest maps is very fast.
* Any state we do store should be duplicated on disk immediately.
  It should be possible to resubmit (any part of) a map based only on
  information stored on disk.


Moving Things Around
--------------------

HTMap relies on `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ to
move data back and forth the submit node and execute nodes.
It pickles the Python function that the user provides as well as all of the
input, then turns around and submits an HTCondor job cluster using HTCondor's
Python bindings.
Instead of directly running user scripts, HTMap uses a script that it controls
as the HTCondor executable.
It hands the user back an object that can be used to look at the output of the
function as well as control the execution of the underlying cluster jobs.


The ``run`` Subdirectory
------------------------

For basic functionality, HTMap itself does not need to be installed on the
execute node where jobs it creates run.
This offers the advantage of being using to use Docker images that only contain
``cloudpickle`` (which is many, because it's installed as part of the Anaconda
distribution) without modification.
Currently, if you want to use checkpointing or output file transfer,
you must also install HTMap execute-side.
In practice, we expect people to install HTMap in their execute image, and all
of the instructions in the docs say to do so.

To accomplish this decoupling, HTMap uses a Python script as its HTCondor executable that
has no dependencies except the Python standard library and ``cloudpickle``.
This script is stored inside the library at ``htmap/run/run.py``.
The transplant delivery method wraps this script with
``htmap/run/run_with_transplant.sh``, a ``bash`` script that handles
unpacking the transplanted install.
A similar script exists for Singularity.

It is critical that the ``run.py`` script make all possible efforts to exit
without an error. If the script itself generates an error, it tends to become
very difficult for users to understand what went wrong. For example,
we used to ``import cloudpickle`` in the bag of imports at the top of the script.
If ``cloudpickle`` wasn't present in the execute image, the script would
immediately bail out and HTMap wouldn't understand why; the user would have
to inspect the ``stderr`` of the map component (which also wasn't directly
supported at the time) to understand what went wrong.


Data Model
----------

Each **map** is tied to a **map directory**, which is named by a UUID.
The map directories are stored in a subdirectory of the **HTMap directory**.
The HTMap directory is located according to ``settings['HTMAP_DIR']``
(default ``~/.htmap``).

The human-readable name of each map is its **tag**.
Tags are stored in a different subdirectory of the HTMap directory, which acts
a file-based map between tags and the names of the map directories.
Each tag file's name is that map's tag, and the file's contents are the name of
the map directory.

All input, output, and HTCondor metadata (event logs, for example) for a map is
stored in its map directory.
A single input/output pair is a **component**, and the components of a map are
just referred to by their index in the input iterable.


Serializing and Deserializing Data
----------------------------------

HTMap uses a wide variety of data serialization formats, depending on what
needs to be stored.
The names of the directories and files can be found in ``htmap/names.py``.
They are all stored inside the map's directory.

The **itemdata** for each map is stored as a JSON-formatted list of
dictionaries.
The itemdata is used to call :meth:`htcondor.Submit.queue_with_itemdata` during
map creation.

The **submit object** for each map is stored as a JSON-formatted dictionary.

The **number of components** is stored as a single string-ified integer in the
file.

The **cluster IDs** of each HTCondor cluster job associated with the map are
stored as newline-separated plain-text strings.

The **event log** for each HTCondor cluster job is routed to a file inside the
map directory.

For generic data, like the **inputs** and **outputs** of mapped functions,
HTMap uses ``cloudpickle``.
The individual inputs and outputs for each component are stored in files named
by the component index.

The functions that handle storing and loading these various formats are in
the ``htmap.htio`` submodule. All IO should go through methods defined in that
submodule, with the idea that if it becomes necessary to swap out some of the
internal implementations of those methods, the changes will be isolated to
that module.

.. automodule:: htmap.htio
   :members:
