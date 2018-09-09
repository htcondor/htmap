Innards
=======

.. py:currentmodule:: htmap

.. highlight:: python

Guiding Principles
------------------

* The only identifying piece of information about a map a user should ever need is a ``map_id``.
* Users should never have to directly interact with the filesystem to manipulate their maps.
* We should store as little state as possible in memory.
* Any state we do store should be duplicated on disk immediately.
* It should be possible to resubmit a map based only on information stored on disk.

Data Model
----------

Each **map** is tied to a **map directory**.
The map directories are stored a subdirectory of the **HTMap directory**.
The HTMap directory is named according to ``settings['HTMAP_DIR']`` (default ``~/.htmap``), and the subdirectory is named according to ``settings['MAPS_DIR_NAME']`` (default ``maps``).
The name of each map directory is that map's **map ID**, a unique identifier.
All input, output, and log data for a map is stored in its map directory.

Serializing and Deserializing Data
++++++++++++++++++++++++++++++++++

HTMap uses a variety of data serialization formats, depending on the particulars of what needs to be stored.

The **itemdata** for each map is stored as a JSON-formatted list of dictionaries in the file ``<map_dir>/itemdata``.
The itemdata is used to call :meth:`htcondor.Submit.queue_with_itemdata` during map creation.

The **submit object** for each map is stored as a JSON-formatted dictionary in the file ``<map_dir>/submit``.

The **input hashes** are stored as newline-separated plain-text strings in the file ``<map_dir>/hashes``.

The **cluster IDs** of each HTCondor cluster job associated with the map are stored as newline-separated plain-text strings in the file ``<map_dir>/cluster_ids``.

For generic data, like the **inputs** (``<map_dir>/inputs/``) and **outputs** (``<map_dir>/outputs/``) of mapped functions, HTMap uses ``cloudpickle``.

The functions that handle storing and loading these various formats this are in the ``htio`` submodule:

.. automodule:: htmap.htio
   :members:

Test Suite
----------

The HTMap test suite makes several assumptions about your system:
* That the Python bindings can discover a working HTCondor pool from your computer (for example, you could run a personal HTCondor).
* That a Python executable with ``cloudpickle`` installed is visible to the user that HTCondor uses to execute jobs.
