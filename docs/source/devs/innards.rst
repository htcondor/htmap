HTMap Innards
=============

.. py:currentmodule:: htmap


Guiding Principles
------------------

* The only identifying piece of information about a map a **user** should ever need is a ``map_id``.
* Users should never have to directly interact with the filesystem to look at output or logs.
* We should store as little state as possible in memory.
* Any state we do store should be duplicated on disk immediately.
* It should be possible to resubmit (any part of) a map based only on information stored on disk.

Data Model
----------

Each **map** is tied to a **map directory**.
The map directories are stored a subdirectory of the **HTMap directory**.
The HTMap directory is named according to ``settings['HTMAP_DIR']`` (default ``~/.htmap``), and the subdirectory is named according to ``settings['MAPS_DIR_NAME']`` (default ``maps``).
The name of each map directory is that map's **map ID**, a unique identifier.

All input, output, and HTCondor metadata (logs, for example) for a map is stored in its map directory.
A single input/output pair is a **component**, and the components for a map are just referred to by their index in the input iterable.

The output of each component is stored as a special :class:`ComponentResult` object, which carries both the output of the function and metadata about the execution.

Serializing and Deserializing Data
++++++++++++++++++++++++++++++++++

HTMap uses a variety of data serialization formats, depending on the particulars of what needs to be stored.

The **itemdata** for each map is stored as a JSON-formatted list of dictionaries in the file ``<map_dir>/itemdata``.
The itemdata is used to call :meth:`htcondor.Submit.queue_with_itemdata` during map creation.

The **submit object** for each map is stored as a JSON-formatted dictionary in the file ``<map_dir>/submit``.

The **number of components** is stored as a single string-ified integer in the file ``<map_dir>/num_components``.

The **cluster IDs** of each HTCondor cluster job associated with the map are stored as newline-separated plain-text strings in the file ``<map_dir>/cluster_ids``.

The **event log** for each HTCondor cluster job is routed to ``<map_dir>/event_log``.

For generic data, like the **inputs** (``<map_dir>/inputs/``) and **outputs** (``<map_dir>/outputs/``) of mapped functions, HTMap uses ``cloudpickle``.
The individual inputs and outputs for each component are stored in files named by the component index.

The functions that handle storing and loading these various formats this are in the ``htio`` submodule:

.. automodule:: htmap.htio
   :members:
